"""POST /v1/heartbeats: valid -> 202 and one message keyed by VIN; invalid -> 400 and nothing published."""
import datetime as dt

import pytest

from app.models import normalize_vin
from tests.conftest import VALID_VIN

URL = "/v1/heartbeats"


def post(client, body=None, **kw):
    return client.post(URL, json=body, **kw)


# ---------------------------------------------------------------- happy path
def test_valid_heartbeat_is_accepted_and_published_once_keyed_by_vin(client, publisher, heartbeat):
    r = post(client, heartbeat)
    assert r.status_code == 202
    assert r.json() == {"status": "accepted", "vin": VALID_VIN, "ts_utc": heartbeat["ts"]}

    assert len(publisher.messages) == 1
    m = publisher.messages[0]
    assert (m.topic, m.key) == ("telemetry.raw", VALID_VIN)
    event = publisher.json_on("telemetry.raw")[0]
    assert event["schema"] == "v1" and event["vin"] == VALID_VIN and event["odometer_km"] == 1000.0


def test_lowercase_vin_is_accepted_and_normalised_to_upper_case(client, publisher, heartbeat):
    heartbeat["vin"] = VALID_VIN.lower()
    assert post(client, heartbeat).status_code == 202
    assert publisher.messages[0].key == VALID_VIN
    assert publisher.json_on("telemetry.raw")[0]["vin"] == VALID_VIN


def test_miles_are_converted_before_publishing(client, publisher, heartbeat):
    heartbeat.update(odometer=100.0, odometer_unit="mi", source_oem="OEM-B")
    assert post(client, heartbeat).status_code == 202
    assert publisher.json_on("telemetry.raw")[0]["odometer_km"] == pytest.approx(160.9344)


def test_ist_timestamp_is_published_as_utc(client, publisher, heartbeat):
    heartbeat["ts"] = "2026-10-28T15:25:00+05:30"          # 09:55 UTC, five minutes before the frozen clock
    r = post(client, heartbeat)
    assert r.status_code == 202
    assert r.json()["ts_utc"] == "2026-10-28T09:55:00Z"
    assert publisher.json_on("telemetry.raw")[0]["ts_utc"] == "2026-10-28T09:55:00Z"


def test_optional_fields_may_be_omitted(client, publisher, heartbeat):
    for k in ("heading", "soc_pct", "ignition", "odometer_unit"):
        heartbeat.pop(k)
    assert post(client, heartbeat).status_code == 202
    e = publisher.json_on("telemetry.raw")[0]
    assert e["heading"] is None and e["soc_pct"] is None and e["ignition"] is True


# ---------------------------------------------------------------- boundaries that must be ACCEPTED
@pytest.mark.parametrize("field,value", [
    ("speed_kmph", 0), ("speed_kmph", 300), ("lat", 90), ("lat", -90), ("lon", 180), ("lon", -180),
    ("odometer", 0), ("heading", 0), ("heading", 359), ("soc_pct", 0), ("soc_pct", 100),
])
def test_boundary_values_are_accepted(client, publisher, heartbeat, field, value):
    heartbeat[field] = value
    assert post(client, heartbeat).status_code == 202
    assert len(publisher.messages) == 1


def test_timestamp_exactly_five_minutes_ahead_is_accepted_but_one_second_more_is_not(client, publisher, heartbeat, now):
    heartbeat["ts"] = (now + dt.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert post(client, heartbeat).status_code == 202
    heartbeat["ts"] = (now + dt.timedelta(minutes=5, seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = post(client, heartbeat)
    assert r.status_code == 400 and r.json()["details"][0]["field"] == "ts"
    assert len(publisher.messages) == 1                    # only the first one went out


# ---------------------------------------------------------------- invalid -> 400, field named, nothing published
INVALID = [
    # (field reported, mutation of the valid heartbeat)
    ("vin", {"vin": "TOO-SHORT"}),
    ("vin", {"vin": VALID_VIN + "X"}),                      # 18 characters
    ("vin", {"vin": "1HGCM82633A0O4352"}),                  # contains O
    ("vin", {"vin": "1HGCM82633A0I4352"}),                  # contains I
    ("vin", {"vin": "1HGCM82633A0Q4352"}),                  # contains Q
    ("vin", {"vin": "1HGCM82633A0-4352"}),                  # not alphanumeric
    ("vin", {"vin": "1HGCM82633A0ſ4352"}),                  # non-ASCII letter whose upper() is an ASCII S
    ("vin", {"vin": 12345678901234567}),                    # wrong type
    ("vin", {"vin": None}),
    ("speed_kmph", {"speed_kmph": -5}),
    ("speed_kmph", {"speed_kmph": 300.01}),
    ("speed_kmph", {"speed_kmph": "fast"}),
    ("speed_kmph", {"speed_kmph": None}),
    ("lat", {"lat": 123.0}),
    ("lat", {"lat": -90.5}),
    ("lon", {"lon": 180.01}),
    ("lon", {"lon": -181}),
    ("odometer", {"odometer": -0.1}),
    ("heading", {"heading": 360}),
    ("heading", {"heading": -1}),
    ("soc_pct", {"soc_pct": 100.5}),
    ("soc_pct", {"soc_pct": -1}),
    ("odometer_unit", {"odometer_unit": "furlong"}),
    ("odometer_unit", {"odometer_unit": "KM"}),            # exact "km" / "mi" only
    ("ts", {"ts": "2026-10-28T10:00:00"}),                  # no timezone
    ("ts", {"ts": "2026-10-28"}),                           # date only -> no timezone
    ("ts", {"ts": "27/10/2026 10:00"}),                     # bad format
    ("ts", {"ts": "2026-10-28T10:06:00Z"}),                 # 6 minutes in the future
    ("ts", {"ts": "2026-10-28T20:00:00Z"}),                 # hours in the future
    ("ts", {"ts": "2026-10-28T15:36:00+05:30"}),            # 10:06 UTC: future even though the local clock looks fine
    ("ts", {"ts": 1795860000}),                             # unix seconds are ambiguous -> refused
    ("ts", {"ts": None}),
    ("source_oem", {"source_oem": ""}),
]


@pytest.mark.parametrize("field,mutation", INVALID, ids=[f"{f}:{list(m.values())[0]!r}" for f, m in INVALID])
def test_invalid_field_is_rejected_with_400_and_nothing_is_published(client, publisher, heartbeat, field, mutation):
    heartbeat.update(mutation)
    r = post(client, heartbeat)
    assert r.status_code == 400
    body = r.json()
    assert body["error"] == "validation_failed"
    assert field in [d["field"] for d in body["details"]]
    assert all(d["message"] for d in body["details"])
    assert publisher.messages == []


@pytest.mark.parametrize("field", ["vin", "ts", "speed_kmph", "odometer", "lat", "lon", "source_oem"])
def test_missing_required_field_is_rejected(client, publisher, heartbeat, field):
    del heartbeat[field]
    r = post(client, heartbeat)
    assert r.status_code == 400
    assert [d["field"] for d in r.json()["details"]] == [field]
    assert publisher.messages == []


def test_infinity_is_rejected_even_for_fields_with_only_a_lower_bound(client, publisher):
    # JSON has no Infinity, but Python's parser accepts it, and `odometer >= 0` alone would let it through.
    body = ('{"vin": "%s", "ts": "2026-10-28T09:59:00Z", "speed_kmph": 50, "odometer": Infinity, "lat": 1, "lon": 1,'
            ' "source_oem": "OEM-A"}' % VALID_VIN)
    # (uses the `now` fixture indirectly through the clock-free fields above: ts is in the past for any real clock)
    r = client.post(URL, content=body, headers={"Content-Type": "application/json"})
    assert r.status_code == 400
    assert "odometer" in [d["field"] for d in r.json()["details"]]
    assert publisher.messages == []


def test_all_problems_are_reported_together(client, publisher, heartbeat):
    heartbeat.update(vin="BAD", speed_kmph=-1, lat=200)
    r = post(client, heartbeat)
    assert r.status_code == 400
    assert {d["field"] for d in r.json()["details"]} == {"vin", "speed_kmph", "lat"}
    assert publisher.messages == []


def test_error_messages_are_readable_not_pydantic_internals(client, heartbeat):
    heartbeat["vin"] = "BAD"
    msg = post(client, heartbeat).json()["details"][0]["message"]
    assert msg == "must be 17 characters: A-Z and 0-9, never I, O or Q"        # no "Value error, " prefix


# ---------------------------------------------------------------- body problems
def test_malformed_json_is_400_and_names_the_body(client, publisher):
    r = client.post(URL, content="{bad", headers={"Content-Type": "application/json"})
    assert r.status_code == 400 and [d["field"] for d in r.json()["details"]] == ["body"]
    assert publisher.messages == []


def test_empty_body_is_400(client, publisher):
    r = client.post(URL)
    assert r.status_code == 400 and r.json()["error"] == "validation_failed"
    assert publisher.messages == []


# ---------------------------------------------------------------- Kafka down
def test_kafka_outage_is_503_not_a_false_accept(client, publisher, heartbeat):
    publisher.fail = True
    r = post(client, heartbeat)
    assert r.status_code == 503
    assert r.json()["error"] == "kafka_unavailable"


# ---------------------------------------------------------------- the VIN helper
def test_normalize_vin_helper():
    assert normalize_vin("1faxxk7y3r5a2b4c6") == VALID_VIN
    with pytest.raises(ValueError):
        normalize_vin("1FAXXK7Y3R5A2B4CO")
