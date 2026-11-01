package com.fleetbootcamp.telemetry.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/** Normalised event on Kafka topic telemetry.raw (schema v1). Odometer is always km, time is UTC. */
public record TelemetryEvent(
        @JsonProperty("schema") String schema,
        @JsonProperty("vin") String vin,
        @JsonProperty("ts_utc") Instant tsUtc,
        @JsonProperty("speed_kmph") Double speedKmph,
        @JsonProperty("odometer_km") Double odometerKm,
        @JsonProperty("lat") Double lat,
        @JsonProperty("lon") Double lon,
        @JsonProperty("heading") Integer heading,
        @JsonProperty("soc_pct") Double socPct,
        @JsonProperty("ignition") Boolean ignition,
        @JsonProperty("source_oem") String sourceOem
) {
    public static final String SCHEMA_V1 = "v1";
}
