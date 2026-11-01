"""Turn a device heartbeat into the normalised TelemetryEvent (all units metric, all times UTC)."""
from app.models import HeartbeatRequest, TelemetryEvent

KM_PER_MILE = 1.609344


def normalize(hb: HeartbeatRequest) -> TelemetryEvent:
    # TODO M1: build a TelemetryEvent from the heartbeat.
    #  - odometer_km: convert miles to km when odometer_unit == "mi" (OEM-B devices report miles)
    #  - ts_utc: the heartbeat timestamp in UTC
    #  - write a unit test for km, mi, and an IST (+05:30) timestamp
    raise NotImplementedError("M1")
