package com.fleetbootcamp.telemetry.service;

import com.fleetbootcamp.telemetry.dto.HeartbeatRequest;
import com.fleetbootcamp.telemetry.dto.TelemetryEvent;

/** Device heartbeat -> normalised TelemetryEvent (metric units, UTC). Keep it pure so it's easy to unit-test. */
public final class Normalizer {
    public static final double KM_PER_MILE = 1.609344;

    private Normalizer() {
    }

    public static TelemetryEvent normalize(HeartbeatRequest hb) {
        // TODO M1:
        //  - schema = TelemetryEvent.SCHEMA_V1, vin upper-cased
        //  - odometer_km: convert when odometer_unit == "mi" (OEM-B devices report miles); null unit means km
        //  - ts_utc = hb.ts().toInstant()
        //  - unit tests: km, mi, an IST (+05:30) timestamp, an unknown unit
        throw new UnsupportedOperationException("M1: implement normalize");
    }
}
