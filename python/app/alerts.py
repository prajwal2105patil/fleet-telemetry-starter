"""M3: Overspeed rule — speed above OVERSPEED_KMPH (default 100) for OVERSPEED_MIN_PINGS (default 3)
consecutive pings of a vehicle => exactly ONE alert for that run (not one per ping).

TODO M3:
  - Write the rule as a pure function first and unit-test it (no DB, no Kafka):
        def overspeed_run_start(rows_newest_first, limit_kmph, min_pings) -> datetime | None
  - Then call it from the consumer after a new row is inserted, and insert into `alerts`.
    The table has UNIQUE (vin, type, started_at) — how does that help you avoid duplicate alerts?
  - Edge cases to test: exactly 100 km/h, run of 2, run broken by a slow ping, run that keeps growing.
"""
