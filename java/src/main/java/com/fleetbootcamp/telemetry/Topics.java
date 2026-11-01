package com.fleetbootcamp.telemetry;

/** Kafka topic names (created by the kafka-init container in docker-compose.yml). */
public final class Topics {
    public static final String RAW = "telemetry.raw";
    public static final String DLQ = "telemetry.dlq";
    public static final String ALERTS = "alerts.out";

    private Topics() {
    }
}
