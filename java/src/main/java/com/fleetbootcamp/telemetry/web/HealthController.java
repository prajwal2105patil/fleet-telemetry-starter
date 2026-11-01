package com.fleetbootcamp.telemetry.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {
    private final JdbcTemplate jdbc;
    private final String kafkaBootstrap;

    public HealthController(JdbcTemplate jdbc, @Value("${spring.kafka.bootstrap-servers}") String kafkaBootstrap) {
        this.jdbc = jdbc;
        this.kafkaBootstrap = kafkaBootstrap;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        boolean db;
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
            db = true;
        } catch (Exception e) {
            db = false;
        }
        Map<String, String> body = Map.of("status", db ? "UP" : "DEGRADED", "db", db ? "up" : "down",
                "kafka_bootstrap", kafkaBootstrap);
        return ResponseEntity.status(db ? 200 : 503).body(body);
    }
}
