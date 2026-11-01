# Fleet Telemetry Platform — Bootcamp Starter Kit

Build a small version of a connected-car data platform in one day: vehicles send signals → your **Ingest API** → **Kafka** → your **consumer** → **Postgres** → your **query APIs** and **alerts**.

Pick **one** track: `java/` (Spring Boot 3, Java 17) or `python/` (FastAPI, Python 3.10+). Everything else is shared.

```
 device simulator ──HTTP──▶ Ingest API ──▶ Kafka: telemetry.raw (12 partitions, key = VIN)
                                                   │
                                                   ▼
                    Query API ◀── Postgres ◀── Consumer (group telemetry-writer) ──▶ telemetry.dlq (bad messages)
                                                   │
                                                   └──▶ alerts table + Kafka: alerts.out
```

---

## 0. Before Day 1 (pre-work — do this at home, the images are ~1.5 GB)

1. Install **Docker Desktop**, **git**, **Python 3.10+** (needed for the simulator on both tracks; macOS ships 3.9 as `python3`, so install a newer one, e.g. `brew install python@3.12`).
   Java track: **JDK 17+** (21 LTS recommended) and **Maven** (or IntelliJ IDEA, which bundles Maven).
2. Clone this repo, then from the repo root:
   ```bash
   docker compose up -d          # Kafka, Kafka UI, Postgres (+ seed data)
   ./scripts/check_env.sh        # must end with ALL GOOD
   ```
   Python track and no JDK installed? Skip the Java check: `STACK=python ./scripts/check_env.sh`.
3. Start your track's app once (see §2) and open http://localhost:8080/health → `{"status":"UP"...}`.
4. Open Kafka UI at http://localhost:8081 — you should see topics `telemetry.raw`, `telemetry.dlq`, `alerts.out`.
5. Send your trainer a screenshot of `ALL GOOD` + the health response.

Windows: run the scripts from Git Bash or WSL.

---

## 1. What's already done for you

| Given | Where |
|---|---|
| Kafka (KRaft), Kafka UI, Postgres 15 with schema + ~56k telemetry rows | `docker-compose.yml`, `db/init/` |
| 200 vehicles across 12 customers (customer 1 = big fleet) | `data/vehicles.csv` |
| Device simulator (no pip install needed) | `simulator/simulate.py` |
| Kafka helper commands (topics, consume, lag) | `scripts/kafka.sh` |
| App skeleton: config from env vars, DB pool, Kafka producer/consumer wiring, `/health`, one passing test, 400 error format | `java/` or `python/` |

**Not done (that's your job):** validation, normalisation, publishing, the consumer's DB writes, idempotency, DLQ, query APIs, alerts, tests.

---

## 2. Run your app

**Python**
```bash
cd python
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080               # terminal 1: API
python -m app.consumer                                  # terminal 2: consumer (from M2)
pytest -q                                               # tests
```

**Java**
```bash
cd java
mvn spring-boot:run          # API + consumer in one process on :8080
mvn test                     # tests
```

Config comes from environment variables: `DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD KAFKA_BOOTSTRAP OVERSPEED_KMPH`. Defaults point at docker compose.

---

## 3. Milestones (Day 2)

Commit after every milestone: `git commit -m "M1: ingest API with validation"`.

### M1 — Ingest API · **no AI tools** · 60 min
`POST /v1/heartbeats`
```json
{"vin":"1FA6CVNCNCXW927TC","ts":"2026-10-28T10:00:00Z","speed_kmph":54.2,"odometer":10234.5,
 "odometer_unit":"km","lat":12.97,"lon":77.59,"heading":90,"soc_pct":null,"ignition":true,"source_oem":"OEM-A"}
```
- Validate: VIN = 17 chars A–Z/0–9 without I, O, Q · `ts` has a timezone and is not > 5 min in the future · speed 0–300 · lat/lon ranges · odometer ≥ 0 · unit `km`/`mi`.
- Invalid → **400** `{"error":"validation_failed","details":[{"field":"vin","message":"..."}]}` and nothing is published.
- Normalise → `TelemetryEvent` (schema `v1`, odometer in **km**, time in **UTC**; OEM-B devices send miles).
- Publish JSON to `telemetry.raw`, **keyed by VIN**. Return **202**.
- ≥ 2 tests.

✅ Check: `python3 simulator/simulate.py --mode malformed` → every line 400 · `python3 simulator/simulate.py --vehicles 5 --count 5` → all 202 · messages visible in Kafka UI.

### M2 — Consumer → Postgres · 75 min (AI allowed from here — you must be able to explain every line)
- Consume `telemetry.raw` (group `telemetry-writer`), write to `telemetry`.
- **Idempotent**: the same `(vin, ts_utc)` twice → one row (look at the table constraints).
- Unparseable / wrong-schema message → publish to `telemetry.dlq` and continue. One bad message must never block a partition.
- Commit the Kafka offset **after** the DB commit.

✅ Check: `python3 simulator/simulate.py --mode duplicate --vehicles 5 --count 10` → row count grows by 50, not 100.
`./scripts/kafka.sh lag` → LAG 0 on all partitions. Run `EXPLAIN` on a `(vin, ts_utc)` range query in psql: does it use the index?

### M3 — Query APIs + alerts · 45 min
| Endpoint | Notes |
|---|---|
| `GET /v1/vehicles/{vin}/latest` | 404 + error body if none |
| `GET /v1/vehicles/{vin}/telemetry?from=&to=&limit=&cursor=` | keyset pagination on `ts_utc` → `next_cursor` (no OFFSET) |
| `GET /v1/vehicles/{vin}/alerts` | newest first |
| `GET /v1/fleets/{customer_id}/latest` | latest position of every active vehicle — **one** SQL query |

**Overspeed alert:** speed > 100 km/h for 3 consecutive pings → exactly **one** `OVERSPEED` row in `alerts` per run (+ message on `alerts.out`). Write the rule as a pure function and unit-test: exactly 100, run of 2, run broken by a slow ping, run that keeps growing.

✅ Check: `python3 simulator/simulate.py --mode overspeed` → one alert.

**Rules for AI:** write 3 tests by hand first · review every AI diff · reject at least one AI suggestion and note why in your README.

### Pod extension (pick the one your mentor assigns)
1. Geofence enter/exit alerts · 2. EV charging-session detection from `soc_pct` (OEM-D) · 3. Odometer anomaly (decrease or > 2,000 km/day) + daily billing export · 4. Customer webhook notifications with retries, exponential backoff and an `event_id` for de-duplication.

### M4 — Ship it · 20 min
Dockerfile for your service + update this README with: your HLD diagram, schema, three trade-offs you made, what broke and how you fixed it, and what you'd do at 10× load.

---

## Day 3 — SQL and data-fix practice

On the **morning of Day 3**, before the first session, run from the repo root (with `docker compose up -d` running):
```bash
./scripts/day3_setup.sh
```
It keeps your Day 2 data and adds what Day 3 needs: recent readings for the time-based questions, a few planted data problems, and your tickets. Running it again is safe (it says "already applied" and changes nothing).

- **Tickets** are in the `ops_tickets` table: `docker compose exec postgres psql -U fleet -d fleet -c 'select id, title, body from ops_tickets order by id'`
- **Files** are in `data/day3/`: `transfer_cityride_to_autofin.csv` (ticket FIX-3) and `app.log` (log task: count errors per service in the hour ending at the last line, `2026-10-27T10:00:00Z`).
- Follow the safe steps for every fix: SELECT and count first, back up, change inside a transaction, check, then commit.

`./scripts/day3_setup.sh --reset` rebuilds the database from scratch and then applies the Day 3 setup. **It deletes your Day 2 data**, so use it only if your database is broken.

---

## 4. Useful commands
```bash
./scripts/kafka.sh consume telemetry.raw     # see messages + partition + key
./scripts/kafka.sh consume telemetry.dlq
./scripts/kafka.sh lag                       # consumer lag per partition
docker compose exec postgres psql -U fleet -d fleet
docker compose down -v && docker compose up -d   # full reset (re-seeds the DB)
```

## 5. Questions you should be able to answer
Why key by VIN? What if one huge fleet's vehicles all hash to one partition? At-least-once vs exactly-once — which do you have, and why is it OK? How do you notice consumer lag? What happens with out-of-order timestamps? How would you store a year of data (hot vs cold)? How do you scale the consumer? What would you monitor and alert on?

## 6. Python implementation notes (M1–M4)

### AI assistance
The Python implementation of M1–M4 was produced with an AI assistant (Claude), including M1, which the programme designates as no-AI. The hand-written tests and the rejected-suggestion note that the programme's AI rules ask for are not part of this repo. The Java track is untouched (starter stubs only).

### What was verified, and how
| Check | Result |
|---|---|
| `pytest` (unit tests, fake Kafka/DB) | pass |
| `pytest tests/test_db_integration.py` against a **real Postgres 16** with `db/init/*.sql` loaded (local stand-in for the compose Postgres 15; each test rolls back) | pass; skipped automatically if no Postgres is reachable |
| `simulate.py` `malformed`, `normal`, `duplicate`, `out-of-order`, `burst`, `overspeed` against the real API + real consumer code + real Postgres, **with Kafka replaced by an in-process loopback** | all as the README expects: 8×400 and nothing published; duplicates stored once (650 messages sent → 525 distinct `(vin, ts)` → 525 rows); one `OVERSPEED` alert and one `alerts.out` message |
| **Real Kafka** (`telemetry.raw` keyed by VIN, consumer-group commit, `./scripts/kafka.sh lag` = 0, DLQ headers in Kafka UI) | **not run** — no broker in the build sandbox. The Kafka wiring is exercised only through fakes. Run the ✅ checks of M1/M2/M3 yourself with `docker compose up -d`. |
| `docker build` of `python/Dockerfile` | **not run** — no Docker daemon in the build sandbox |

### HLD
```
 simulator ──HTTP──▶ POST /v1/heartbeats ──validate──▶ 400 {"error":"validation_failed","details":[…]}  (nothing published)
                              │ normalise: km, UTC, upper-case VIN
                              ▼  publish_and_wait (acks=all, idempotent producer, key = VIN)   Kafka down ⇒ 503, never a false 202
                     Kafka telemetry.raw  (12 partitions)
                              │
                              ▼  consumer group telemetry-writer, auto-commit OFF
        parse ── bad JSON / wrong schema / out-of-range / naive ts / tombstone ──▶ telemetry.dlq (original bytes + headers
                              │                                                   error, source_topic/partition/offset)
                              ▼  ONE DB transaction
        INSERT … ON CONFLICT (vin, ts_utc) DO NOTHING ─▶ new row? ─▶ overspeed rule ─▶ INSERT alert … ON CONFLICT DO NOTHING
                              │  poison row (DataError/IntegrityError) ──▶ telemetry.dlq
                              │  DB unreachable ──▶ retry with backoff; still down ⇒ do NOT commit, seek back, pause
                              ▼  COMMIT, then …
        new alert ──▶ Kafka alerts.out        …then commit the Kafka offset (synchronously)

 Query API (reads Postgres only):  /latest · /telemetry (keyset) · /alerts · /fleets/{id}/latest (one query)
```

### Endpoints
| | |
|---|---|
| `POST /v1/heartbeats` | 202 `{"status":"accepted","vin","ts_utc"}` · 400 validation (all problems listed, each with `field`) · 503 `kafka_unavailable` |
| `GET /v1/vehicles/{vin}/latest` | 200 · 404 `not_found` (unknown vehicle, or vehicle with no telemetry yet — different `message`) · 400 bad VIN |
| `GET /v1/vehicles/{vin}/telemetry?from&to&limit&cursor` | **newest first**; `from` inclusive, `to` exclusive (ISO-8601 **with timezone**; encode `+` as `%2B`); `limit` 1–1000, default 100; response `{vin, items, next_cursor}` where `next_cursor` is `null` on the last page |
| `GET /v1/vehicles/{vin}/alerts?limit` | newest first by `started_at` |
| `GET /v1/fleets/{customer_id}/latest` | every **active** vehicle of the customer with its newest ping; active vehicles that never reported appear with `"latest": null`; 404 unknown customer |
| any endpoint | 503 `database_unavailable` if Postgres is unreachable |

### Schema (unchanged from `db/init/01_schema.sql`; these are the parts the code leans on)
- `telemetry` — `UNIQUE (vin, ts_utc)` (`uq_telemetry_vin_ts`) does three jobs: it makes inserts idempotent (`ON CONFLICT DO NOTHING`), it is the index for "newest N pings of one vehicle" (an index scan **backward**, no sort), and it is what the fleet query probes once per vehicle.
- `alerts` — `UNIQUE (vin, type, started_at)`; `started_at` = timestamp of the **first** ping of the speeding run, so a run that keeps growing maps to the same row.
- `vehicles` (`status` active/deprovisioned, `customer_id`) and `customers` are read by the fleet query.

Proof the range query uses the index (`EXPLAIN (ANALYZE)` on the 56k seeded rows; the test suite asserts the same on a 100k-row table):
```
Limit (actual rows=101)
  ->  Index Scan Backward using uq_telemetry_vin_ts on telemetry (actual rows=101)
        Index Cond: ((vin = '5YJUAL4YUKPYGF1GZ') AND (ts_utc >= …) AND (ts_utc < …) AND (ts_utc < …))
Execution Time: 0.314 ms
```

### Three trade-offs I made
1. **Ingest waits for the broker's acknowledgement before answering 202** (`publish_and_wait`: produce + flush). A 202 therefore means "Kafka has it" and an outage becomes a visible 503 instead of silently lost data. The price: each request holds a worker thread for a broker round-trip, which caps throughput per instance. The alternative (fire-and-forget, 202 immediately) is faster but can lose accepted data.
2. **A database outage never sends valid data to the DLQ.** After 5 attempts with exponential backoff (0.5 s → 8 s cap) the consumer does *not* commit, seeks back to the same message and pauses. Order is preserved and the DLQ only contains data that is actually bad; the cost is that the partition stalls (consumer lag grows — that is the alarm) until Postgres is back. A message that wrongly looks "transient" forever would block its partition; only connection-class errors (`OperationalError`/`InterfaceError`) are treated that way, row-level errors (`DataError`, `IntegrityError`) go to the DLQ at once.
3. **`alerts.out` is published after the DB commit, fire-and-forget.** The `alerts` table is the source of truth; the notification can never describe an alert that doesn't exist. But a crash between the commit and the publish loses that one notification (the redelivered telemetry message is a duplicate, so the alert is not re-sent). The fix is a transactional outbox; I judged it too heavy for this scope.

Smaller choices: first write wins when the same `(vin, ts_utc)` arrives with different values; the overspeed rule reads the vehicle's pings back to its last at-or-below-limit ping (cost grows with the length of a run that is *still going* — fine for minutes, not for hours); the rule looks at the newest ping only, so a late out-of-order ping that fills a gap between two speeding runs is not re-evaluated retroactively.

### What broke, and how it was fixed
- **Consumer crashed on the first non-JSON message** (starter code: `json.loads` inside the log line) — it now logs raw bytes and the parse step dead-letters the message. Test: `tests/test_consumer.py`.
- **Malformed-JSON requests reported a nonsense `field`** (a character offset from pydantic's `loc`) — now `"body"`.
- **VIN check bypass found while writing validation:** `str.upper()` maps some non-ASCII letters onto `A–Z` (e.g. `ſ` → `S`), so a naive `upper()` + regex accepts them. Fixed with an `isascii()` guard; covered by a test.
- **`inf` passes `odometer >= 0`.** Python's JSON parser accepts `Infinity`; the field now has `allow_inf_nan=False`. The consumer also rejects `NaN`/`Infinity` because Postgres `NUMERIC` would happily store `NaN`.
- **Alert type spelled wrong:** I first wrote `overspeed`; the README specifies `OVERSPEED`. Caught by re-reading the spec before shipping.
- **Integration tests assumed an empty `telemetry` table**; the seed ships ~56k rows, so the first run failed (301 rows where 1 was expected). Tests now clear customer 1's rows *inside their own rolled-back transaction*.
- **A test recorded backoff delays on the wrong hook** (`time.sleep` instead of the stop-event wait the loop really uses), so it saw no sleeps while the code actually waited. Fixed the test.
- Six key behaviours were **mutation-checked** (break the code on purpose, confirm a test goes red): removing the telemetry `ON CONFLICT`, changing `>` to `>=` in the overspeed rule, dropping the alert de-dupe, including deprovisioned vehicles in the fleet query, `<` → `<=` in the cursor condition, and evaluating alerts on duplicate messages. Each made at least one test fail; the other tests were not mutation-checked.

### At 10× load
- **Consumer:** batch per `poll` — multi-row `INSERT … ON CONFLICT` (or `COPY` into a staging table), one commit per batch, then commit the highest offset per partition; scale out to at most 12 consumers (one per partition); raise partitions if one hot customer's VINs skew a partition.
- **Ingest:** keep the idempotent `acks=all` producer but stop flushing per request — use delivery callbacks / micro-batches and return 202 when the batch is acknowledged; add a bounded queue + 429 for back-pressure.
- **Postgres:** partition `telemetry` by month (cheap retention: drop a partition; hot vs cold storage), keep `(vin, ts_utc)` local to each partition; read replicas for the query API.
- **Fleet "latest":** replace the per-vehicle index probe by a `vehicle_latest` table updated on write (`ON CONFLICT … WHERE excluded.ts_utc > vehicle_latest.ts_utc`).
- **Alerts:** a transactional outbox table drained to `alerts.out` (at-least-once, de-duplicated by `(vin, started_at)`); store run state per vehicle instead of re-reading the run.
- **Observe:** consumer lag per partition (alert when it keeps growing), DLQ rate, 503 rate, p99 of the three query endpoints, DB connection-pool wait time.

### Short answers to §5
- *Why key by VIN?* Same vehicle → same partition → consumed in order (needed for "latest" and for the overspeed run). A huge fleet is many VINs, so it spreads across partitions; only one very chatty vehicle could skew one.
- *At-least-once vs exactly-once?* At-least-once: the offset is committed **after** the DB commit, so a crash re-delivers; `ON CONFLICT DO NOTHING` makes the repeat harmless, so the table behaves as if delivered once.
- *Out-of-order timestamps?* Rows are stored by `ts_utc`, so queries are correct regardless of arrival order; only the overspeed rule (newest-ping based) can miss a late gap-filler.

### Run it in Docker (not tested here)
```bash
docker compose up -d
docker build -t fleet-telemetry-python python/
# containers must use the compose network and the *internal* Kafka listener (kafka:29092);
# the advertised EXTERNAL listener localhost:9092 only works from the host.
docker run --rm -d --name fleet-api      --network fleet-telemetry_default -p 8080:8080 \
  -e DB_HOST=postgres -e KAFKA_BOOTSTRAP=kafka:29092 fleet-telemetry-python
docker run --rm -d --name fleet-consumer --network fleet-telemetry_default \
  -e DB_HOST=postgres -e KAFKA_BOOTSTRAP=kafka:29092 fleet-telemetry-python python -m app.consumer
```

> This is a **training project**. On your resume, describe it honestly (e.g. "Fleet Telemetry Platform — bootcamp project; built X, owned Y").

> **Origin:** based on the Fleet Telemetry bootcamp starter kit by [mdmemonyasin](https://github.com/mdmemonyasin/fleet-telemetry-starter). This copy adds small setup/bug fixes (`check_env.sh` Python-track handling, consumer no longer crashes on non-JSON messages, malformed-JSON error field, `kafka.sh` help), tests, and an AI-assisted Python implementation of M1–M4 (see §6).
