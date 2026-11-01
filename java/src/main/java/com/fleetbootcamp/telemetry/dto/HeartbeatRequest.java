package com.fleetbootcamp.telemetry.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;

/**
 * What a device (or OEM feed) sends to POST /v1/heartbeats.
 *
 * TODO M1 — add Bean Validation (jakarta.validation.constraints.*) so each of these becomes HTTP 400
 * (check with: python3 simulator/simulate.py --mode malformed):
 *   - vin: exactly 17 chars, A-Z and 0-9, never I, O or Q        (@Pattern)
 *   - ts: required; OffsetDateTime already rejects timestamps without a timezone. "Not in the future" -> do it in a service.
 *   - speed_kmph 0..300, lat -90..90, lon -180..180, odometer >= 0, heading 0..359, soc_pct 0..100
 *   - odometer_unit only "km" or "mi"
 * Remember @Valid on the controller parameter. ApiErrors already turns validation failures into the 400 body.
 */
public record HeartbeatRequest(
        @JsonProperty("vin") String vin,
        @JsonProperty("ts") OffsetDateTime ts,
        @JsonProperty("speed_kmph") Double speedKmph,
        @JsonProperty("odometer") Double odometer,
        @JsonProperty("odometer_unit") String odometerUnit,
        @JsonProperty("lat") Double lat,
        @JsonProperty("lon") Double lon,
        @JsonProperty("heading") Integer heading,
        @JsonProperty("soc_pct") Double socPct,
        @JsonProperty("ignition") Boolean ignition,
        @JsonProperty("source_oem") String sourceOem
) {
}
