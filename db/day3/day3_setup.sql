-- Day 3 setup: plants the data and tickets for the Day 3 SQL questions and data-fix tasks.
-- Run it with ./scripts/day3_setup.sh (not by hand). Safe to run again: the second run does nothing.
--
-- Everything is deterministic (rows are picked by ordering on vin / id, never random()),
-- so the trainer's answer key matches every laptop.

\set ON_ERROR_STOP 1
\pset footer off

SELECT to_regclass('public.ops_tickets') IS NOT NULL AS already_applied \gset
\if :already_applied
  \echo 'Day 3 setup: already applied (ops_tickets exists). Nothing changed.'
  \echo 'Use ./scripts/day3_setup.sh --reset to start again from a fresh database (this deletes Day 2 data).'
  \quit
\endif

BEGIN;

-- Results of each step, printed as the summary at the end.
CREATE TEMP TABLE day3_summary (step INT, item TEXT, value TEXT) ON COMMIT DROP;

DO $$
DECLARE
    t0          TIMESTAMPTZ := now();                                    -- setup start (fixed for the transaction)
    deploy_t    TIMESTAMPTZ := date_trunc('minute', now() - interval '3 hours');
    seed_rx     TIMESTAMPTZ;   -- received_at of the seed rows (one value: the seed ran in one transaction)
    day2_min    TIMESTAMPTZ;   -- oldest Day 2 (non-seed) reading, if any
    seed_max    TIMESTAMPTZ;
    delta       INTERVAL := interval '0';
    n_moved     BIGINT := 0;
    n_back      BIGINT := 0;
    n           BIGINT;
    ist_from    TIMESTAMPTZ := date_trunc('minute', now()) - interval '6 hours';
    ist_to      TIMESTAMPTZ := date_trunc('minute', now());
    fmt         TEXT := 'YYYY-MM-DD"T"HH24:MI:SS"Z"';
    fix1_vins   TEXT;
    cityride    TEXT;
    autofin     TEXT;
BEGIN
    SELECT min(received_at) INTO seed_rx FROM telemetry;
    IF seed_rx IS NULL THEN
        RAISE EXCEPTION 'telemetry is empty: is the seed loaded? Try ./scripts/day3_setup.sh --reset';
    END IF;
    SELECT min(ts_utc) INTO day2_min FROM telemetry WHERE received_at > seed_rx;

    ---------------------------------------------------------------------------------------------
    -- (a) Move the seed readings up to "now" so the time-based questions work.
    --     Only seed rows move (identified by their received_at), and only rows older than both the
    --     oldest Day 2 reading and now() - 30 min. Day 2 rows are never moved.
    --     Delete + re-insert (same ids) instead of UPDATE: shifting a 5-minute grid with one UPDATE
    --     can hit the unique (vin, ts_utc) constraint half-way through.
    ---------------------------------------------------------------------------------------------
    CREATE TEMP TABLE seed_rows ON COMMIT DROP AS
        SELECT * FROM telemetry
        WHERE received_at = seed_rx
          AND ts_utc < least(coalesce(day2_min, t0), t0 - interval '30 minutes');
    SELECT max(ts_utc) INTO seed_max FROM seed_rows;

    IF seed_max < t0 - interval '2 hours' THEN
        delta := (date_trunc('minute', t0) - interval '1 hour') - seed_max;
        DELETE FROM telemetry WHERE id IN (SELECT id FROM seed_rows);
        INSERT INTO telemetry (id, vin, ts_utc, speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem, received_at)
            SELECT id, vin, ts_utc + delta, speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem, received_at
            FROM seed_rows
            ORDER BY id
        ON CONFLICT ON CONSTRAINT uq_telemetry_vin_ts DO NOTHING;   -- only if a Day 2 row already has that exact (vin, ts)
        GET DIAGNOSTICS n_moved = ROW_COUNT;
        UPDATE seed_rows SET ts_utc = ts_utc + delta;
        seed_max := seed_max + delta;
    END IF;
    INSERT INTO day3_summary VALUES
        (1, 'seed readings moved forward', CASE WHEN delta = interval '0' THEN '0 (already recent)'
                                              ELSE format('%s rows by %s (%s skipped: a Day 2 row had the same vin+time)',
                                                          n_moved, delta, (SELECT count(*) FROM seed_rows) - n_moved) END);

    -- The seed's speeds come from random(), so they differ per laptop. Replace them with a fixed value per
    -- (vin, slot) so the speeding question (SQL 12) has the same answer everywhere.
    UPDATE telemetry t
       SET speed_kmph = (abs(hashtext(t.vin || ':' || round(extract(epoch FROM seed_max - t.ts_utc) / 300)::int)) % 12000) / 100.0
     WHERE t.received_at = seed_rx AND t.ts_utc <= seed_max;
    GET DIAGNOSTICS n = ROW_COUNT;
    INSERT INTO day3_summary VALUES (1, 'seed speeds made deterministic', n::text);

    ---------------------------------------------------------------------------------------------
    -- (b) SQL question 2: 8 active vehicles go silent (no readings in the last 24 hours).
    --     Picked from customers 7-10 (the Day 2 simulator sends for the first vehicles in
    --     data/vehicles.csv, which are customers 1-2, and its overspeed mode uses the last one, customer 11),
    --     at most 2 per customer, so 4 customers.
    --     OEM-B and the IST-trap vehicles are left out so the other traps stay complete.
    ---------------------------------------------------------------------------------------------
    CREATE TEMP TABLE ist_vins ON COMMIT DROP AS
        SELECT v.vin FROM vehicles v
        WHERE v.status = 'active' AND v.oem <> 'OEM-B'
          AND EXISTS (SELECT 1 FROM devices d
                      WHERE d.vin = v.vin AND d.provider = 'GeoBox'
                        AND (split_part(d.firmware, '.', 1)::int, split_part(d.firmware, '.', 2)::int) < (3, 2));

    CREATE TEMP TABLE silent_vins ON COMMIT DROP AS
        SELECT vin, customer_id FROM (
            SELECT v.vin, v.customer_id, row_number() OVER (PARTITION BY v.customer_id ORDER BY v.vin) AS rn
            FROM vehicles v
            WHERE v.status = 'active' AND v.customer_id BETWEEN 7 AND 10 AND v.oem <> 'OEM-B'
              AND v.vin NOT IN (SELECT vin FROM ist_vins)
        ) x
        WHERE rn <= 2
        ORDER BY vin
        LIMIT 8;

    DELETE FROM telemetry WHERE vin IN (SELECT vin FROM silent_vins) AND ts_utc > t0 - interval '26 hours';
    GET DIAGNOSTICS n = ROW_COUNT;
    INSERT INTO day3_summary VALUES
        (2, 'silent vehicles (SQL 2)', format('%s vehicles across %s customers, %s readings removed',
             (SELECT count(*) FROM silent_vins), (SELECT count(DISTINCT customer_id) FROM silent_vins), n));

    ---------------------------------------------------------------------------------------------
    -- (c) FIX-5: a "bad deploy" at deploy_t multiplied OEM-B odometers by 1.609344 (miles -> km twice).
    ---------------------------------------------------------------------------------------------
    UPDATE telemetry t
       SET odometer_km = round(t.odometer_km * 1.609344, 1)
      FROM vehicles v
     WHERE v.vin = t.vin AND v.oem = 'OEM-B' AND v.status = 'active' AND t.ts_utc >= deploy_t;
    GET DIAGNOSTICS n = ROW_COUNT;
    INSERT INTO day3_summary VALUES (3, 'OEM-B odometer x1.609344 (FIX-5)', format('%s rows since %s', n, to_char(deploy_t AT TIME ZONE 'UTC', fmt)));

    ---------------------------------------------------------------------------------------------
    -- (d) FIX-6: GeoBox devices on firmware below 3.2 sent IST times labelled as UTC for the last
    --     6 hours, so those readings are stored 5h30m ahead (some in the future).
    --     Only non-OEM-B vehicles get this, so FIX-5 (OEM-B, ts >= deploy time) and FIX-6 never touch
    --     the same rows and can be fixed in either order. 5h30m is a multiple of 5 minutes, so this is
    --     a delete + re-insert, and we check that every row was re-inserted.
    ---------------------------------------------------------------------------------------------
    CREATE TEMP TABLE ist_rows ON COMMIT DROP AS
        SELECT * FROM telemetry
        WHERE vin IN (SELECT vin FROM ist_vins) AND ts_utc > ist_from AND ts_utc <= ist_to;
    DELETE FROM telemetry WHERE id IN (SELECT id FROM ist_rows);
    INSERT INTO telemetry (id, vin, ts_utc, speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem, received_at)
        SELECT id, vin, ts_utc + interval '5 hours 30 minutes', speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem, received_at
        FROM ist_rows
        ORDER BY id
    ON CONFLICT ON CONSTRAINT uq_telemetry_vin_ts DO NOTHING;
    GET DIAGNOSTICS n_back = ROW_COUNT;
    IF n_back <> (SELECT count(*) FROM ist_rows) THEN
        RAISE EXCEPTION 'IST shift would collide: % of % rows re-inserted', n_back, (SELECT count(*) FROM ist_rows);
    END IF;
    INSERT INTO day3_summary VALUES
        (4, 'IST stored as UTC (FIX-6)', format('%s rows on %s vehicles shifted +5:30, 0 conflicts', n_back, (SELECT count(*) FROM ist_vins)));

    ---------------------------------------------------------------------------------------------
    -- (e) The Day 3 tickets.
    ---------------------------------------------------------------------------------------------
    CREATE TABLE ops_tickets (
        id          TEXT PRIMARY KEY,
        title       TEXT,
        body        TEXT,
        created_at  TIMESTAMPTZ DEFAULT now()
    );

    -- FIX-1: four of customer 4's vehicles, plus one vehicle of Prime Auto Dealers (customer 10) slipped in.
    WITH own AS (SELECT vin, row_number() OVER (ORDER BY vin) AS rn FROM vehicles WHERE customer_id = 4 AND status = 'active' ORDER BY vin LIMIT 4),
         trap AS (SELECT vin FROM vehicles WHERE customer_id = 10 AND status = 'active' ORDER BY vin LIMIT 1),
         lst AS (SELECT vin, rn AS pos FROM own WHERE rn <= 2
                 UNION ALL SELECT vin, 2.5 FROM trap
                 UNION ALL SELECT vin, rn FROM own WHERE rn > 2)
    SELECT string_agg('  - ' || vin, E'\n' ORDER BY pos) INTO fix1_vins FROM lst;

    SELECT name INTO cityride FROM customers WHERE id = 2;
    SELECT name INTO autofin FROM customers WHERE id = 5;

    INSERT INTO ops_tickets (id, title, body) VALUES
    ('FIX-1', 'Deactivate 5 vehicles for Speedway Motors (customer 4)',
     E'Speedway Motors (customer 4) has sold these vehicles and asked us to stop tracking them.\n'
     || E'Please deactivate them (set the vehicle status to ''deprovisioned''):\n' || fix1_vins),

    ('FIX-2', 'Extend FleetCo''s active subscriptions by 12 months',
     E'FleetCo renewed their contract. Please extend all of their active subscriptions by 12 months.'),

    ('FIX-3', format('Move vehicles from %s to %s', cityride, autofin),
     format(E'%s has sold its fleet to %s. The customer success team sent the list of vehicles to move:\n'
            || E'  data/day3/transfer_cityride_to_autofin.csv  (columns: vin,from_customer_id,to_customer_id)\n'
            || E'Please move every vehicle in the file to the new customer.', cityride, autofin)),

    ('FIX-4', 'Remove duplicate device records, keep the most recent',
     E'Some vehicles have more than one row in devices. Keep only the most recent device row per vehicle '
     || E'(the one with the latest last_seen_at) and delete the rest.\n'
     || E'Note: some duplicate rows have last_seen_at = NULL, so "most recent" needs a rule for NULLs and ties.'),

    ('FIX-5', 'OEM-B odometer readings too high since the last release',
     format(E'Since the release at %s, odometer_km for OEM-B vehicles is too high: the miles-to-km '
            || E'conversion (x 1.609344) was applied twice. Readings from before that time are correct.\n'
            || E'Please correct the stored readings. The fix may be run more than once by the on-call engineer.',
            to_char(deploy_t AT TIME ZONE 'UTC', fmt))),

    ('FIX-6', 'GeoBox devices on firmware below 3.2 sent IST times as UTC',
     format(E'Between %s and %s (6 hours), GeoBox devices on firmware below 3.2 sent their local IST '
            || E'clock time but labelled it as UTC. Those readings are stored 5 hours 30 minutes ahead of the '
            || E'real time, so some of them are in the future.\n'
            || E'Please move the affected readings back by 5:30, in a way that is safe to run again.',
            to_char(ist_from AT TIME ZONE 'UTC', fmt), to_char(ist_to AT TIME ZONE 'UTC', fmt)));

    INSERT INTO day3_summary VALUES (5, 'tickets created in ops_tickets', (SELECT count(*)::text FROM ops_tickets));
END $$;

INSERT INTO audit_log (table_name, change, ticket) VALUES ('*', 'Day 3 setup applied', 'DAY3-SETUP');

SELECT item AS "Day 3 setup", value AS "result" FROM day3_summary ORDER BY step, item;

COMMIT;
