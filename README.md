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

> This is a **training project**. On your resume, describe it honestly (e.g. "Fleet Telemetry Platform — bootcamp project; built X, owned Y").

> **Origin:** based on the Fleet Telemetry bootcamp starter kit by [mdmemonyasin](https://github.com/mdmemonyasin/fleet-telemetry-starter). This copy adds small setup/bug fixes (`check_env.sh` Python-track handling, consumer no longer crashes on non-JSON messages, malformed-JSON error field, `kafka.sh` help) and tests.
