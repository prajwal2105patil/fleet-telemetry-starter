package com.fleetbootcamp.telemetry.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** All SQL lives here. Always use ? placeholders — never concatenate input into SQL. */
@Repository
public class TelemetryRepository {
    private final JdbcTemplate jdbc;

    public TelemetryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Example of the style to follow. */
    public boolean vehicleExists(String vin) {
        Integer n = jdbc.queryForObject("SELECT count(*) FROM vehicles WHERE vin = ?", Integer.class, vin);
        return n != null && n > 0;
    }

    // TODO M2: boolean insert(TelemetryEvent e)
    //   - idempotent: the same (vin, ts_utc) delivered twice must not create two rows
    //     (look at the constraints on the telemetry table in db/init/01_schema.sql)
    //   - pass timestamps as OffsetDateTime (e.tsUtc().atOffset(ZoneOffset.UTC))
    //
    // TODO M3: latest(vin), range(vin, from, to, limit, cursor) with keyset pagination, alerts(vin),
    //          fleetLatest(customerId) as ONE query. Check your range query with EXPLAIN in psql.
}
