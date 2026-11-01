package com.fleetbootcamp.telemetry.web;

import com.fleetbootcamp.telemetry.dto.HeartbeatRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * M1 (NO AI tools): POST /v1/heartbeats  -> validate -> normalise -> publish to Kafka topic telemetry.raw
 *
 * TODO M1:
 *  1. Add @Valid and validation annotations (see HeartbeatRequest).
 *  2. Create an IngestService (in service/) that:
 *       - rejects ts more than 5 minutes in the future  (throw ApiErrors.ValidationFailedException("ts", ...))
 *       - builds a TelemetryEvent with Normalizer.normalize(...)
 *       - sends it as JSON with KafkaTemplate<String, String>.send(Topics.RAW, key, json)
 *         Which KEY keeps all events of one vehicle in order? Why does that matter?
 *  3. Return 202 Accepted with {"status":"accepted","vin":...,"ts_utc":...}
 *  4. Write a @WebMvcTest: valid -> 202; each invalid field -> 400 and nothing sent.
 *
 * M3 (AI-assisted allowed) — create a VehicleController with:
 *   GET /v1/vehicles/{vin}/latest                      404 + error body if no data
 *   GET /v1/vehicles/{vin}/telemetry?from&to&limit&cursor   keyset pagination on ts_utc (next_cursor), NOT OFFSET
 *   GET /v1/vehicles/{vin}/alerts
 *   GET /v1/fleets/{customerId}/latest                 ONE SQL query for the whole fleet (no query per VIN)
 */
@RestController
@RequestMapping("/v1")
public class HeartbeatController {

    @PostMapping("/heartbeats")
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody HeartbeatRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
                .body(Map.of("error", "not_implemented", "message", "M1: implement me"));
    }
}
