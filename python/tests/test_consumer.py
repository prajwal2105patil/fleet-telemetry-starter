"""Consumer behaviour, with Kafka and Postgres replaced by fakes (no broker, no database needed).

What is pinned down here:
  * the offset is committed only AFTER the DB commit (and after the DLQ copy is acknowledged)
  * a bad message of ANY kind is dead-lettered with its original bytes + reason headers, and the next one is processed
  * poison rows go to the DLQ immediately; transient DB errors are retried, and never skipped
  * a re-delivered message is harmless
Real-database behaviour (idempotent insert, alerts) is in test_db_integration.py.
"""
import json
import signal

import psycopg
import pytest
from confluent_kafka import KafkaError, KafkaException

from app import alerts, consumer, repository
from tests.conftest import FakePublisher, VALID_VIN


# ---------------------------------------------------------------- helpers
def event_bytes(**overrides) -> bytes:
    e = {"schema": "v1", "vin": VALID_VIN, "ts_utc": "2026-10-28T09:00:00Z", "speed_kmph": 55.0,
         "odometer_km": 1000.0, "lat": 12.9, "lon": 77.5, "heading": 90, "soc_pct": None, "ignition": True,
         "source_oem": "OEM-A"}
    e.update(overrides)
    return json.dumps(e).encode()


class FakeMessage:
    def __init__(self, value, offset=42, key=VALID_VIN.encode(), partition=3):
        self._value, self._offset, self._key, self._partition = value, offset, key, partition

    def error(self):
        return None

    def topic(self):
        return "telemetry.raw"

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value


class FakePool:
    """pool.connection() behaves like psycopg_pool's: leaving the block normally = COMMIT, an exception = rollback."""

    def __init__(self, log):
        self.log = log

    def connection(self):
        from contextlib import contextmanager

        @contextmanager
        def cm():
            yield object()
            self.log.append("db-commit")
        return cm()


class RecordingPublisher(FakePublisher):
    def __init__(self, log):
        super().__init__()
        self.log = log

    def publish_and_wait(self, topic, key, value, headers=None, timeout=10.0):
        super().publish_and_wait(topic, key, value, headers, timeout)
        self.log.append(f"published:{topic}")

    def publish(self, topic, key, value, headers=None):
        super().publish(topic, key, value, headers)
        self.log.append(f"published:{topic}")


@pytest.fixture
def world(monkeypatch):
    """A fake DB + fake Kafka wired into the consumer. `world.run(messages)` runs the real consumer loop."""
    monkeypatch.setattr(consumer, "BACKOFF_START_S", 0)
    monkeypatch.setattr(consumer, "PAUSE_WHEN_UNAVAILABLE_S", 0)

    class World:
        def __init__(self):
            self.log = []                     # ordered record of what happened ("db-commit", "offset-commit", ...)
            self.inserts = []                 # events handed to repository.insert_telemetry
            self.db_script = []               # per attempt: True / False / an Exception to raise
            self.alert_script = []            # per new row: None or an alert dict
            self.publisher = RecordingPublisher(self.log)
            self.pool = FakePool(self.log)
            self.sleeps = []
            self.committed = []               # offsets committed
            self.seeks = []                   # offsets sought back to
            self.commit_error = None
            self.consumer = None

        def run(self, messages):
            handlers = {}
            monkeypatch.setattr(consumer.signal, "signal", lambda sig, h: handlers.__setitem__(sig, h))
            world = self

            def insert(conn, event):
                world.inserts.append(event)
                outcome = world.db_script.pop(0) if world.db_script else True
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

            def evaluate(conn, event):
                return world.alert_script.pop(0) if world.alert_script else None

            monkeypatch.setattr(repository, "insert_telemetry", insert)
            monkeypatch.setattr(alerts, "evaluate_overspeed", evaluate)

            class FakeConsumer:
                def __init__(self, conf):
                    world.consumer = self
                    self.conf = conf
                    self.pending = list(messages)
                    self.delivered = {}
                    self.closed = False

                def subscribe(self, topics):
                    self.topics = topics

                def poll(self, timeout):
                    if self.pending:
                        m = self.pending.pop(0)
                        self.delivered[m.offset()] = m
                        return m
                    handlers[signal.SIGTERM]()           # nothing left: ask the loop to stop, like `kill` would
                    return None

                def seek(self, tp):
                    world.seeks.append(tp.offset)
                    self.pending.insert(0, self.delivered[tp.offset])      # next poll() returns it again

                def commit(self, message=None, asynchronous=True, **kw):
                    if world.commit_error:
                        raise world.commit_error
                    world.log.append("offset-commit")
                    world.committed.append(message.offset())

                def close(self):
                    self.closed = True

            monkeypatch.setattr(consumer, "Consumer", FakeConsumer)
            consumer.run(pool=self.pool, publisher=self.publisher)
            return self

        @property
        def dlq(self):
            return self.publisher.on("telemetry.dlq")

    w = World()
    # run() sleeps with _stop.wait (so SIGTERM interrupts a backoff). Record the delays instead of waiting.
    monkeypatch.setattr(consumer._stop, "wait", lambda timeout=None: w.sleeps.append(timeout))
    return w


# ---------------------------------------------------------------- the happy path and ordering
def test_valid_message_is_stored_and_the_offset_is_committed_after_the_db_commit(world):
    world.run([FakeMessage(event_bytes())])
    assert world.log == ["db-commit", "offset-commit"]                # THE rule of M2
    assert [e.vin for e in world.inserts] == [VALID_VIN]
    assert world.dlq == []
    assert world.consumer.closed
    assert world.consumer.conf["enable.auto.commit"] is False


def test_event_is_parsed_into_the_columns_the_insert_receives(world):
    world.run([FakeMessage(event_bytes(vin=VALID_VIN.lower(), ts_utc="2026-10-28T14:30:00+05:30", speed_kmph=61.5))])
    e = world.inserts[0]
    assert e.vin == VALID_VIN and e.speed_kmph == 61.5
    assert e.ts_utc.isoformat() == "2026-10-28T09:00:00+00:00"      # normalised to UTC


def test_messages_are_processed_in_order_and_each_offset_committed(world):
    world.run([FakeMessage(event_bytes(), offset=1), FakeMessage(event_bytes(ts_utc="2026-10-28T09:00:05Z"), offset=2)])
    assert world.committed == [1, 2]
    assert world.log == ["db-commit", "offset-commit", "db-commit", "offset-commit"]


def test_redelivered_duplicate_is_committed_and_does_not_evaluate_alerts_or_dead_letter(world):
    world.db_script = [False]                          # the row already exists
    world.alert_script = [{"vin": VALID_VIN}]          # would be returned if (wrongly) evaluated
    world.run([FakeMessage(event_bytes())])
    assert world.committed == [42]
    assert world.dlq == [] and world.publisher.on("alerts.out") == []


def test_commit_failure_does_not_crash_the_loop(world):
    world.commit_error = KafkaException(KafkaError(KafkaError._NO_OFFSET))
    world.run([FakeMessage(event_bytes(), offset=1), FakeMessage(event_bytes(ts_utc="2026-10-28T09:00:05Z"), offset=2)])
    assert len(world.inserts) == 2 and world.consumer.closed


# ---------------------------------------------------------------- bad messages -> DLQ
BAD_MESSAGES = {
    "not json": b"this is not json {",
    "tombstone": None,
    "empty": b"",
    "invalid utf-8": b"\xff\xfe\x00garbage",
    "json array": b"[1, 2, 3]",
    "json string": b'"hello"',
    "deeply nested": b"[" * 100000,
    "wrong schema version": event_bytes(schema="v2"),
    "missing schema": json.dumps({"vin": VALID_VIN}).encode(),
    "missing fields": b'{"schema": "v1", "vin": "1FAXXK7Y3R5A2B4C6"}',
    "bad vin": event_bytes(vin="NOT-A-VIN"),
    "naive timestamp": event_bytes(ts_utc="2026-10-28T09:00:00"),
    "bad timestamp": event_bytes(ts_utc="yesterday"),
    "speed is text": event_bytes(speed_kmph="fast"),
    "NaN speed": event_bytes().replace(b"55.0", b"NaN"),
    "Infinity odometer": event_bytes().replace(b"1000.0", b"Infinity"),
    "negative speed": event_bytes(speed_kmph=-3),
    "latitude out of range": event_bytes(lat=95),
    "heading out of range": event_bytes(heading=400),
}


@pytest.mark.parametrize("name", BAD_MESSAGES)
def test_bad_message_goes_to_the_dlq_unchanged_and_the_next_message_is_still_processed(world, name):
    bad = BAD_MESSAGES[name]
    world.run([FakeMessage(bad, offset=7), FakeMessage(event_bytes(), offset=8)])

    assert len(world.dlq) == 1
    d = world.dlq[0]
    assert d.value == (bad or b"")                              # the ORIGINAL bytes, untouched
    assert d.key == VALID_VIN.encode()
    assert d.headers["error"]                                   # says why
    assert d.headers["source_topic"] == "telemetry.raw"
    assert d.headers["source_partition"] == 3
    assert d.headers["source_offset"] == 7
    assert len(world.inserts) == 1                              # only the good message reached the database
    assert world.committed == [7, 8]                            # the partition was not blocked
    assert world.log.index("published:telemetry.dlq") < world.log.index("offset-commit")   # DLQ copy BEFORE commit


def test_dlq_error_header_is_a_single_bounded_line(world):
    world.run([FakeMessage(b"x\n" * 1000)])
    err = world.dlq[0].headers["error"]
    assert "\n" not in err and len(err) <= 500


def test_a_message_without_a_key_is_dead_lettered_without_one(world):
    world.run([FakeMessage(b"junk", key=None)])
    assert world.dlq[0].key is None


# ---------------------------------------------------------------- database errors
@pytest.mark.parametrize("error", [
    psycopg.errors.NumericValueOutOfRange("numeric field overflow"),
    psycopg.errors.StringDataRightTruncation("value too long"),
    psycopg.errors.NotNullViolation("null value in column"),
    psycopg.errors.CheckViolation("check constraint"),
], ids=lambda e: type(e).__name__)
def test_poison_row_is_dead_lettered_immediately_without_retrying(world, error):
    world.db_script = [error]
    world.run([FakeMessage(event_bytes(), offset=5), FakeMessage(event_bytes(ts_utc="2026-10-28T09:00:05Z"), offset=6)])
    assert len(world.inserts) == 2                      # 1 attempt for the poison row (no retry) + 1 for the next
    assert len(world.dlq) == 1 and "database rejected the row" in world.dlq[0].headers["error"]
    assert world.committed == [5, 6]
    assert world.sleeps == []                           # no backoff for poison
    assert "db-commit" in world.log                     # the following message still committed normally


def test_transient_db_error_is_retried_with_exponential_backoff_then_stored(world, monkeypatch):
    monkeypatch.setattr(consumer, "BACKOFF_START_S", 0.5)
    world.db_script = [psycopg.OperationalError("connection lost"), psycopg.OperationalError("still down"), True]
    world.run([FakeMessage(event_bytes())])
    assert len(world.inserts) == 3
    assert world.sleeps == [0.5, 1.0]
    assert world.committed == [42] and world.dlq == []
    assert world.log == ["db-commit", "offset-commit"]          # nothing committed to Kafka during the failed attempts


def test_backoff_is_capped(world, monkeypatch):
    monkeypatch.setattr(consumer, "BACKOFF_START_S", 4)
    monkeypatch.setattr(consumer, "BACKOFF_MAX_S", 6)
    monkeypatch.setattr(consumer, "MAX_DB_ATTEMPTS", 4)
    world.db_script = [psycopg.OperationalError("down")] * 3 + [True]
    world.run([FakeMessage(event_bytes())])
    assert world.sleeps == [4, 6, 6]


def test_database_down_for_good_never_skips_the_message_or_commits_its_offset(world, monkeypatch):
    """After MAX_DB_ATTEMPTS the consumer rewinds and tries the same message again; nothing is lost or dead-lettered."""
    monkeypatch.setattr(consumer, "MAX_DB_ATTEMPTS", 2)
    # attempts 1-2 fail (round 1), attempts 3-4 fail (round 2), the database is back for attempt 5 (round 3)
    world.db_script = [psycopg.OperationalError("down")] * 4 + [True]
    world.run([FakeMessage(event_bytes(), offset=9)])
    assert world.seeks == [9, 9]                        # rewound twice, to the SAME message
    assert world.committed == [9]                       # committed exactly once, after the recovery
    assert world.dlq == []
    assert world.log == ["db-commit", "offset-commit"]


def test_unavailable_message_blocks_later_ones_only_until_the_database_is_back(world, monkeypatch):
    monkeypatch.setattr(consumer, "MAX_DB_ATTEMPTS", 1)
    world.db_script = [psycopg.OperationalError("down"), True, True]
    world.run([FakeMessage(event_bytes(), offset=1), FakeMessage(event_bytes(ts_utc="2026-10-28T09:00:05Z"), offset=2)])
    assert world.committed == [1, 2]                    # order preserved: 1 before 2


def test_kafka_down_while_dead_lettering_keeps_the_offset_uncommitted_then_retries(world):
    world.publisher.fail = True
    # the publisher "recovers" once the loop has rewound at least once
    original_seek_count = lambda: len(world.seeks)
    real_wait = world.publisher.publish_and_wait

    def flaky(topic, key, value, headers=None, timeout=10.0):
        if original_seek_count() >= 2:
            world.publisher.fail = False
        return real_wait(topic, key, value, headers, timeout)

    world.publisher.publish_and_wait = flaky
    world.run([FakeMessage(b"garbage", offset=3)])
    assert world.seeks == [3, 3]
    assert len(world.dlq) == 1                          # eventually delivered, exactly once
    assert world.committed == [3]                       # and only then committed


# ---------------------------------------------------------------- alerts
def test_new_alert_is_published_to_alerts_out_after_the_db_commit(world):
    alert = {"vin": VALID_VIN, "type": "OVERSPEED", "started_at": "2026-10-28T09:00:00Z", "details": {}}
    world.alert_script = [alert]
    world.run([FakeMessage(event_bytes())])
    assert world.publisher.json_on("alerts.out") == [alert]
    assert world.publisher.on("alerts.out")[0].key == VALID_VIN
    assert world.log == ["db-commit", "published:alerts.out", "offset-commit"]


def test_alerts_out_publish_failure_does_not_fail_the_telemetry_write(world, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("producer queue full")
    world.alert_script = [{"vin": VALID_VIN, "type": "OVERSPEED", "started_at": "2026-10-28T09:00:00Z"}]
    world.publisher.publish = boom
    world.run([FakeMessage(event_bytes())])
    assert world.committed == [42]


# ---------------------------------------------------------------- parse_event on its own
def test_parse_event_returns_a_utc_event():
    e = consumer.parse_event(event_bytes(ts_utc="2026-10-28T14:30:00+05:30", vin=VALID_VIN.lower()))
    assert e.vin == VALID_VIN
    assert e.ts_utc.utcoffset().total_seconds() == 0 and e.ts_utc.hour == 9


def test_parse_event_accepts_range_boundaries():
    e = consumer.parse_event(event_bytes(speed_kmph=300, lat=-90, lon=180, heading=359, soc_pct=100, odometer_km=0))
    assert e.speed_kmph == 300


@pytest.mark.parametrize("raw", [None, b"", b"{", b"[]", event_bytes(schema="v9")])
def test_parse_event_raises_bad_message(raw):
    with pytest.raises(consumer.BadMessage):
        consumer.parse_event(raw)
