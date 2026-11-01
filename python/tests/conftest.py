import datetime as dt
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app import models
from app.kafka_client import PublishError, get_publisher
from app.main import app

VALID_VIN = "1FAXXK7Y3R5A2B4C6"
NOW = dt.datetime(2026, 10, 28, 10, 0, 0, tzinfo=dt.timezone.utc)      # the frozen "current time" in tests


class FakePublisher:
    """Stands in for app.kafka_client.EventPublisher. Records every message instead of talking to Kafka."""

    def __init__(self):
        self.messages = []          # every message handed over, in order
        self.fail = False           # set True to simulate Kafka being unavailable

    def _record(self, topic, key, value, headers):
        self.messages.append(SimpleNamespace(topic=topic, key=key, value=value, headers=headers or {}))

    def publish(self, topic, key, value, headers=None):
        self._record(topic, key, json.dumps(value).encode(), headers)

    def publish_and_wait(self, topic, key, value, headers=None, timeout=10.0):
        if self.fail:
            raise PublishError("simulated Kafka outage")
        self._record(topic, key, value, headers)

    def flush(self, timeout=5.0):
        return 0

    def on(self, topic):
        return [m for m in self.messages if m.topic == topic]

    def json_on(self, topic):
        return [json.loads(m.value) for m in self.on(topic)]


@pytest.fixture
def publisher():
    return FakePublisher()


@pytest.fixture
def client(publisher):
    app.dependency_overrides[get_publisher] = lambda: publisher
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def now(monkeypatch):
    """Freezes the clock the validators use at NOW (2026-10-28T10:00:00Z) so tests do not depend on today's date."""
    monkeypatch.setattr(models, "utcnow", lambda: NOW)
    return NOW


@pytest.fixture
def heartbeat(now):
    """A valid heartbeat, one minute before the frozen clock."""
    ts = now - dt.timedelta(minutes=1)
    return {"vin": VALID_VIN, "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "speed_kmph": 54.2, "odometer": 1000.0,
            "odometer_unit": "km", "lat": 12.97, "lon": 77.59, "heading": 90, "soc_pct": None,
            "ignition": True, "source_oem": "OEM-A"}
