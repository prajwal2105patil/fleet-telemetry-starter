import datetime as dt

import pytest
from fastapi.testclient import TestClient

from app.kafka_client import get_publisher
from app.main import app

VALID_VIN = "1FAXXK7Y3R5A2B4C6"


class FakePublisher:
    def __init__(self):
        self.sent = []

    def publish(self, topic, key, value, headers=None):
        self.sent.append((topic, key, value))

    def flush(self, timeout=5.0):
        return 0


@pytest.fixture
def publisher():
    return FakePublisher()


@pytest.fixture
def client(publisher):
    app.dependency_overrides[get_publisher] = lambda: publisher
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def heartbeat():
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    return {"vin": VALID_VIN, "ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "speed_kmph": 54.2, "odometer": 1000.0,
            "odometer_unit": "km", "lat": 12.97, "lon": 77.59, "heading": 90, "soc_pct": None,
            "ignition": True, "source_oem": "OEM-A"}
