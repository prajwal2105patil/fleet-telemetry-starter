"""The consumer wiring must survive a message that is not JSON (README M2: one bad message must never block a partition)."""
import signal

import pytest

from app import consumer


class FakeMessage:
    def __init__(self, value, key=b"1FAXXK7Y3R5A2B4C6"):
        self._value, self._key = value, key

    def error(self):
        return None

    def partition(self):
        return 3

    def offset(self):
        return 42

    def key(self):
        return self._key

    def value(self):
        return self._value


@pytest.fixture
def run_consumer_with(monkeypatch):
    """Runs consumer.run() against a fake Kafka consumer that hands out `messages`, then stops the loop."""
    def runner(messages):
        handlers = {}
        # Capture the signal handlers instead of installing them: keeps pytest's own Ctrl+C handling intact.
        monkeypatch.setattr(consumer.signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler))
        fake = {}

        class FakeConsumer:
            def __init__(self, conf):
                self.pending = list(messages)
                self.closed = False
                self.commits = 0
                fake["instance"] = self

            def subscribe(self, topics):
                self.topics = topics

            def poll(self, timeout):
                if self.pending:
                    return self.pending.pop(0)
                handlers[signal.SIGTERM]()          # nothing left: ask the loop to stop, like `kill` would
                return None

            def commit(self, *args, **kwargs):
                self.commits += 1

            def close(self):
                self.closed = True

        monkeypatch.setattr(consumer, "Consumer", FakeConsumer)
        consumer.run()
        return fake["instance"]
    return runner


def test_valid_json_message_is_consumed_and_consumer_closes_cleanly(run_consumer_with):
    c = run_consumer_with([FakeMessage(b'{"schema": "v1", "vin": "1FAXXK7Y3R5A2B4C6"}')])
    assert c.closed


def test_non_json_message_does_not_crash_the_consumer(run_consumer_with):
    c = run_consumer_with([FakeMessage(b"this is not json {"), FakeMessage(b'{"schema": "v1"}')])
    assert c.closed
    assert c.pending == []          # the message after the bad one was still polled


def test_tombstone_message_with_no_value_does_not_crash_the_consumer(run_consumer_with):
    c = run_consumer_with([FakeMessage(None)])
    assert c.closed
