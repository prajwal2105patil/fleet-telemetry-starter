package com.fleetbootcamp.telemetry.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;

/** One telemetry row as returned by the query APIs. */
@JsonInclude(JsonInclude.Include.ALWAYS)
public record TelemetryView(
        @JsonProperty("vin") String vin,
        @JsonProperty("ts_utc") OffsetDateTime tsUtc,
        @JsonProperty("speed_kmph") double speedKmph,
        @JsonProperty("odometer_km") Double odometerKm,
        @JsonProperty("lat") double lat,
        @JsonProperty("lon") double lon,
        @JsonProperty("heading") Integer heading,
        @JsonProperty("soc_pct") Double socPct,
        @JsonProperty("ignition") Boolean ignition,
        @JsonProperty("source_oem") String sourceOem
) {
}
