"""M2: Kafka consumer -> Postgres.   Run:  python -m app.consumer   (from the python/ folder)

For every message on telemetry.raw:

  1. parse + validate it                         bad  -> copy to telemetry.dlq, then move on
  2. insert it into Postgres (one transaction)   idempotent: (vin, ts_utc) is UNIQUE
        poison row (DataError / IntegrityError)  -> telemetry.dlq
        transient (connection lost, pool timeout)-> retry with backoff; still failing -> do NOT skip it (see below)
  3. commit the Kafka offset                     only AFTER the DB commit / the DLQ acknowledgement

Delivery guarantee: at-least-once. A crash between step 2 and 3 re-delivers the message after the restart; the
second insert hits ON CONFLICT DO NOTHING, so the table ends up exactly as if it had been delivered once.
Committing BEFORE the insert would turn the same crash into silent data loss — that is why the order matters.

When Postgres (or the DLQ topic) stays unavailable we never skip the message: the offset is not committed, the
consumer seeks back to it and tries again after a pause. Sending valid data to the DLQ because of an outage would
only move the problem; the lag shows up on the consumer-lag metric instead, which is where it should be noticed.
"""
import json
import logging
import math
import signal
import threading
import time

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from pydantic import ValidationError

from app import alerts, config, repository
from app.db import get_pool
from app.kafka_client import EventPublisher, PublishError, get_publisher
from app.models import TelemetryEvent, normalize_vin
from app.util import UTC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("consumer")

MAX_DB_ATTEMPTS = 5          # in-process attempts per message before the consumer rewinds and pauses
BACKOFF_START_S = 0.5
BACKOFF_MAX_S = 8.0
PAUSE_WHEN_UNAVAILABLE_S = 5.0

# Errors where the ROW is the problem: retrying can never help, so the message goes to the DLQ.
POISON_DB_ERRORS = (psycopg.DataError, psycopg.IntegrityError)
# Errors where the DATABASE is the problem (connection lost, server restarting, pool exhausted).
# psycopg_pool.PoolTimeout is a subclass of OperationalError.
TRANSIENT_DB_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

_stop = threading.Event()


class BadMessage(Exception):
    """The message can never be processed, however often it is retried -> DLQ."""


class Unavailable(Exception):
    """A dependency (Postgres) is down: do not commit the offset, do not skip the message, try again later."""


# ---------------------------------------------------------------- step 1: parse
_RANGES = {"speed_kmph": (0, 300), "odometer_km": (0, None), "lat": (-90, 90), "lon": (-180, 180),
           "heading": (0, 359), "soc_pct": (0, 100)}


def parse_event(raw: bytes | None) -> TelemetryEvent:
    """Bytes from Kafka -> TelemetryEvent, or BadMessage with a human-readable reason."""
    if not raw:
        raise BadMessage("empty message (tombstone)")
    try:
        obj = json.loads(raw)
    except (ValueError, RecursionError) as e:             # JSONDecodeError and UnicodeDecodeError are ValueErrors
        raise BadMessage(f"not valid JSON: {e}") from e
    if not isinstance(obj, dict):
        raise BadMessage(f"expected a JSON object, got {type(obj).__name__}")
    # TelemetryEvent defaults a missing "schema" to v1; for data coming off a shared topic it must be explicit.
    if obj.get("schema") != "v1":
        raise BadMessage(f"unsupported schema {obj.get('schema')!r} (expected 'v1')")
    try:
        event = TelemetryEvent.model_validate(obj)
    except ValidationError as e:
        problems = "; ".join(f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors())
        raise BadMessage(f"invalid event: {problems}") from e

    try:
        vin = normalize_vin(event.vin)
    except ValueError as e:
        raise BadMessage(f"invalid vin {event.vin!r}: {e}") from e
    if event.ts_utc.tzinfo is None:
        # A naive timestamp would be read in the DB session's timezone: silent corruption. Refuse it.
        raise BadMessage("ts_utc has no timezone")
    for field, (low, high) in _RANGES.items():
        value = getattr(event, field)
        if value is None:
            continue
        if isinstance(value, float) and not math.isfinite(value):          # NaN / Infinity are valid NUMERIC values!
            raise BadMessage(f"{field} is not a finite number")
        if value < low or (high is not None and value > high):
            raise BadMessage(f"{field}={value} is out of range")
    return event.model_copy(update={"vin": vin, "ts_utc": event.ts_utc.astimezone(UTC)})


# ---------------------------------------------------------------- step 2: write
def store(pool, event: TelemetryEvent, sleep=time.sleep):
    """Insert in one transaction, retrying transient database errors with exponential backoff.

    Returns (inserted, alert) where `alert` is a new overspeed alert or None.
    Raises POISON_DB_ERRORS untouched (caller sends the message to the DLQ) and Unavailable when the database
    stays down for MAX_DB_ATTEMPTS attempts.
    """
    delay = BACKOFF_START_S
    for attempt in range(1, MAX_DB_ATTEMPTS + 1):
        try:
            with pool.connection() as conn:                  # commits on exit, rolls back on exception
                inserted = repository.insert_telemetry(conn, event)
                alert = alerts.evaluate_overspeed(conn, event) if inserted else None
            return inserted, alert
        except TRANSIENT_DB_ERRORS as e:
            if attempt == MAX_DB_ATTEMPTS:
                raise Unavailable(f"database unavailable after {attempt} attempts: {e}") from e
            log.warning("transient database error (attempt %d/%d), retrying in %.1fs: %s",
                        attempt, MAX_DB_ATTEMPTS, delay, e)
            sleep(delay)
            delay = min(delay * 2, BACKOFF_MAX_S)
    raise AssertionError("unreachable")


# ---------------------------------------------------------------- DLQ
def send_to_dlq(msg, reason: str, publisher: EventPublisher) -> None:
    """Copies the ORIGINAL bytes to telemetry.dlq, with headers saying why and where it came from.

    Blocks until Kafka acknowledges: the source offset may only be committed once the copy is safe. Raises
    PublishError if it is not; the caller then keeps the offset uncommitted and retries.
    """
    publisher.publish_and_wait(
        config.TOPIC_DLQ,
        key=msg.key(),
        value=msg.value() or b"",
        headers={
            "error": " ".join(reason.split())[:500],         # one line, bounded
            "source_topic": msg.topic(),
            "source_partition": msg.partition(),
            "source_offset": msg.offset(),
        },
    )
    log.warning("dead-lettered %s[%s]@%s: %s", msg.topic(), msg.partition(), msg.offset(), reason)


# ---------------------------------------------------------------- one message, end to end
def handle_message(msg, pool, publisher: EventPublisher, sleep=time.sleep) -> str:
    """Processes one Kafka message. Returns "stored", "duplicate" or "dead-lettered".

    Returning means it is safe to commit the offset. Raises Unavailable / PublishError when it is not
    (the message was neither stored nor dead-lettered).
    """
    try:
        event = parse_event(msg.value())
    except BadMessage as e:
        send_to_dlq(msg, str(e), publisher)
        return "dead-lettered"

    try:
        inserted, alert = store(pool, event, sleep)
    except POISON_DB_ERRORS as e:
        send_to_dlq(msg, f"database rejected the row: {type(e).__name__}: {e}", publisher)
        return "dead-lettered"

    if alert is not None:
        # AFTER the DB commit: the alerts table is the source of truth, alerts.out is the notification.
        # A crash right here loses the notification but not the alert (see README: trade-offs).
        alerts.publish_alert(publisher, alert)
    return "stored" if inserted else "duplicate"


# ---------------------------------------------------------------- the loop
def _rewind(consumer, msg) -> None:
    """Make the next poll() return `msg` again (poll() has already moved the fetch position past it)."""
    try:
        consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
    except KafkaException as e:
        # E.g. the partition was just revoked in a rebalance: whoever owns it next starts at the committed offset.
        log.warning("could not seek back to %s[%s]@%s: %s", msg.topic(), msg.partition(), msg.offset(), e)


def run(pool=None, publisher: EventPublisher | None = None) -> None:
    pool = pool if pool is not None else get_pool()
    publisher = publisher if publisher is not None else get_publisher()
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,                  # the offset is committed by hand, after the DB commit
    })
    consumer.subscribe([config.TOPIC_RAW])
    _stop.clear()

    def stop(*_):
        _stop.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log.info("consuming %s as group %s", config.TOPIC_RAW, config.CONSUMER_GROUP)
    try:
        while not _stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            try:
                outcome = handle_message(msg, pool, publisher, sleep=_stop.wait)
            except (Unavailable, PublishError) as e:
                log.error("cannot process %s[%s]@%s yet, will retry: %s", msg.topic(), msg.partition(), msg.offset(), e)
                _rewind(consumer, msg)
                _stop.wait(PAUSE_WHEN_UNAVAILABLE_S)
                continue

            log.debug("%s[%s]@%s %s", msg.topic(), msg.partition(), msg.offset(), outcome)
            try:
                consumer.commit(message=msg, asynchronous=False)
            except KafkaException as e:
                # Not fatal: the message is redelivered after a rebalance/restart and ON CONFLICT absorbs it.
                log.warning("offset commit failed for %s[%s]@%s: %s", msg.topic(), msg.partition(), msg.offset(), e)
    finally:
        consumer.close()
        publisher.flush(5.0)


if __name__ == "__main__":
    run()
