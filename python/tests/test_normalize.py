import datetime as dt

import pytest

from app.models import HeartbeatRequest
from app.normalize import KM_PER_MILE, normalize

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def make(now, **overrides):
    body = {"vin": "1FAXXK7Y3R5A2B4C6", "ts": "2026-10-28T09:00:00Z", "speed_kmph": 50, "odometer": 100.0,
            "lat": 12.9, "lon": 77.5, "source_oem": "OEM-A"}
    body.update(overrides)
    return HeartbeatRequest(**body)


def test_km_odometer_is_passed_through_unchanged(now):
    assert normalize(make(now, odometer=1234.5, odometer_unit="km")).odometer_km == 1234.5


def test_default_unit_is_km(now):
    assert normalize(make(now, odometer=77.0)).odometer_km == 77.0


def test_miles_are_converted_to_km(now):
    assert normalize(make(now, odometer=100.0, odometer_unit="mi")).odometer_km == pytest.approx(160.9344)
    assert KM_PER_MILE == 1.609344


def test_ist_timestamp_is_converted_to_utc(now):
    e = normalize(make(now, ts="2026-10-28T14:30:00+05:30"))
    assert e.ts_utc == dt.datetime(2026, 10, 28, 9, 0, tzinfo=dt.timezone.utc)
    assert e.ts_utc.utcoffset() == dt.timedelta(0)


def test_event_follows_the_v1_contract(now):
    m = normalize(make(now, vin="1faxxk7y3r5a2b4c6", odometer_unit="mi", heading=90, soc_pct=55.5)).to_message()
    assert m["schema"] == "v1"
    assert m["vin"] == "1FAXXK7Y3R5A2B4C6"              # upper-cased
    assert m["ts_utc"] == "2026-10-28T09:00:00Z"
    assert set(m) == {"schema", "vin", "ts_utc", "speed_kmph", "odometer_km", "lat", "lon", "heading", "soc_pct",
                      "ignition", "source_oem"}
    assert "odometer" not in m and "odometer_unit" not in m
