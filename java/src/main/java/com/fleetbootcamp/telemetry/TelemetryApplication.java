package com.fleetbootcamp.telemetry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * One process runs both the REST API (port 8080) and the Kafka consumer.
 * Set CONSUMER_ENABLED=false to run an API-only instance.
 */
@SpringBootApplication
public class TelemetryApplication {
    public static void main(String[] args) {
        SpringApplication.run(TelemetryApplication.class, args);
    }
}
