"""Tests against a REAL Postgres (the schema + seed from db/init). Skipped automatically if none is reachable.

Every test runs inside a transaction that is rolled back at the end, so the database is left exactly as it was found:
  docker compose up -d postgres     # or any Postgres 15+ with db/init/*.sql loaded; set DB_HOST / DB_PORT if needed
  pytest tests/test_db_integration.py
"""
import datetime as dt
import json
from contextlib import contextmanager

import psycopg
import pytest
from psycopg.rows import dict_row

from app import alerts, config, consumer, repository
from app.kafka_client import get_publisher
from app.main import app, get_conn
from app.models import TelemetryEvent
from tests.conftest import VALID_VIN, FakePublisher

BASE = dt.datetime(2026, 10, 28, 9, 0, 0, tzinfo=dt.timezone.utc)
ACTIVE_VIN = "5YJUAL4YUKPYGF1GZ"           # seeded: customer 1, active
DEPROVISIONED_VIN = "1FAUSRJGF4CBFPR9B"    # seeded: customer 1, deprovisioned
OTHER_VIN = "3HC6H1FV0NECRVFRG"            # seeded: customer 1, active


class RecordingCursor(psycopg.Cursor):
    """Remembers every statement, so tests can count queries and EXPLAIN the exact SQL the code ran."""
    statements: list = []

    def execute(self, query, params=None, **kw):
        RecordingCursor.statements.append((query if isinstance(query, str) else query.as_string(), params))
        return super().execute(query, params, **kw)


@pytest.fixture
def conn():
    try:
        c = psycopg.connect(config.DB_DSN, cursor_factory=RecordingCursor)
        tables = c.execute("SELECT to_regclass('public.telemetry'), to_regclass('public.vehicles')").fetchone()
        if None in tables:
            c.close()
            pytest.skip("Postgres is reachable but the schema from db/init is not loaded")
    except psycopg.OperationalError as e:
        pytest.skip(f"no Postgres reachable ({str(e).splitlines()[0]})")
    # The database may already hold telemetry (the seed / earlier simulator runs). Tests need to know exactly which
    # rows exist, so they start from "customer 1's vehicles have no pings and no alerts". This DELETE runs inside the
    # transaction that is rolled back below: the real data is untouched.
    scope = "vin IN (SELECT vin FROM vehicles WHERE customer_id = 1) OR vin = %s"
    c.execute(f"DELETE FROM alerts WHERE {scope}", (VALID_VIN,))
    c.execute(f"DELETE FROM telemetry WHERE {scope}", (VALID_VIN,))
    RecordingCursor.statements = []
    yield c
    c.rollback()                                   # nothing a test did is ever committed
    c.close()


def ping(conn, vin, i, speed=50.0, **kw):
    """Stores the i-th ping (5 s apart) the way the consumer does: insert, then evaluate the rule if it was new."""
    event = TelemetryEvent(vin=vin, ts_utc=BASE + dt.timedelta(seconds=5 * i), speed_kmph=speed,
                           odometer_km=1000 + i, lat=12.9, lon=77.5, source_oem="OEM-A", **kw)
    inserted = repository.insert_telemetry(conn, event)
    return inserted, (alerts.evaluate_overspeed(conn, event) if inserted else None)


def count(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


# ================================================================ M2: idempotent insert
def test_insert_is_idempotent_on_vin_and_ts(conn):
    assert ping(conn, ACTIVE_VIN, 0)[0] is True
    assert ping(conn, ACTIVE_VIN, 0)[0] is False
    assert ping(conn, ACTIVE_VIN, 0)[0] is False
    assert count(conn, "SELECT count(*) FROM telemetry WHERE vin = %s", ACTIVE_VIN) == 1


def test_same_timestamp_for_a_different_vehicle_is_a_different_row(conn):
    assert ping(conn, ACTIVE_VIN, 0)[0] and ping(conn, OTHER_VIN, 0)[0]


def test_first_write_wins_when_a_duplicate_carries_different_values(conn):
    ping(conn, ACTIVE_VIN, 0, speed=40.0)
    assert ping(conn, ACTIVE_VIN, 0, speed=99.0)[0] is False
    assert float(count(conn, "SELECT speed_kmph FROM telemetry WHERE vin = %s", ACTIVE_VIN)) == 40.0


def test_the_simulators_duplicate_mode_adds_each_row_once(conn):
    """simulate.py --mode duplicate: every ping sent twice. 5 vehicles x 10 pings -> 50 rows, not 100."""
    vins = [ACTIVE_VIN, OTHER_VIN, "3HC0L0ZPUELSL61UR", "1FACX2UEPXP826KTJ", "3HC420RJ98FDHKL4E"]
    before = count(conn, "SELECT count(*) FROM telemetry")
    for i in range(10):
        for vin in vins:
            for _ in range(2):
                ping(conn, vin, i)
    assert count(conn, "SELECT count(*) FROM telemetry") - before == 50


def test_stored_values_round_trip(conn):
    repository.insert_telemetry(conn, TelemetryEvent(
        vin=ACTIVE_VIN, ts_utc=BASE, speed_kmph=54.2, odometer_km=160.9344, lat=12.97, lon=77.59, heading=90,
        soc_pct=55.5, ignition=False, source_oem="OEM-D"))
    got = repository.latest_for_vin(conn, ACTIVE_VIN)
    assert got == {"ts_utc": "2026-10-28T09:00:00Z", "speed_kmph": 54.2, "odometer_km": 160.9, "lat": 12.97,
                   "lon": 77.59, "heading": 90, "soc_pct": 55.5, "ignition": False, "source_oem": "OEM-D"}


# ---- the consumer against the real database (Kafka still faked)
class SavepointPool:
    """Gives the consumer the test's connection, like pool.connection(), but inside a SAVEPOINT: a failed insert rolls
    back only itself and the test's outer transaction (rolled back at the end) stays usable."""

    def __init__(self, conn):
        self.conn = conn

    @contextmanager
    def connection(self):
        with self.conn.transaction():
            yield self.conn


class Msg:
    def __init__(self, value, offset=1):
        self._v, self._o = value, offset

    def topic(self): return "telemetry.raw"
    def partition(self): return 0
    def offset(self): return self._o
    def key(self): return ACTIVE_VIN.encode()
    def value(self): return self._v


def event_json(**over):
    e = {"schema": "v1", "vin": ACTIVE_VIN, "ts_utc": "2026-10-28T09:00:00Z", "speed_kmph": 55.0, "odometer_km": 1000.0,
         "lat": 12.9, "lon": 77.5, "heading": 90, "soc_pct": None, "ignition": True, "source_oem": "OEM-A"}
    e.update(over)
    return json.dumps(e).encode()


def test_consumer_redelivery_after_a_crash_is_harmless(conn):
    """Crash between the DB commit and the offset commit -> the same message arrives again -> still one row."""
    pool, pub = SavepointPool(conn), FakePublisher()
    assert consumer.handle_message(Msg(event_json()), pool, pub) == "stored"
    assert consumer.handle_message(Msg(event_json()), pool, pub) == "duplicate"
    assert count(conn, "SELECT count(*) FROM telemetry WHERE vin = %s", ACTIVE_VIN) == 1
    assert pub.on("telemetry.dlq") == []


@pytest.mark.parametrize("override", [
    {"odometer_km": 1e15},                  # numeric field overflow for NUMERIC(12,1)
    {"source_oem": "x" * 10, "vin": "5YJUAL4YUKPYGF1GZ"},     # (control: this one is fine and must NOT be dead-lettered)
], ids=["numeric-overflow", "control-valid"])
def test_real_database_errors_are_classified_the_way_the_consumer_expects(conn, override):
    pool, pub = SavepointPool(conn), FakePublisher()
    outcome = consumer.handle_message(Msg(event_json(**override)), pool, pub)
    if "odometer_km" in override:
        assert outcome == "dead-lettered"
        assert "NumericValueOutOfRange" in pub.on("telemetry.dlq")[0].headers["error"]
        assert count(conn, "SELECT count(*) FROM telemetry WHERE vin = %s", ACTIVE_VIN) == 0
    else:
        assert outcome == "stored" and pub.on("telemetry.dlq") == []


def test_poison_row_does_not_poison_the_transaction_for_the_next_message(conn):
    pool, pub = SavepointPool(conn), FakePublisher()
    assert consumer.handle_message(Msg(event_json(odometer_km=1e15), 1), pool, pub) == "dead-lettered"
    assert consumer.handle_message(Msg(event_json(), 2), pool, pub) == "stored"


def test_consumer_publishes_the_alert_after_storing_the_third_fast_ping(conn):
    pool, pub = SavepointPool(conn), FakePublisher()
    for i, speed in enumerate([60, 105, 112, 118, 121, 125, 80]):
        ts = (BASE + dt.timedelta(seconds=5 * i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        consumer.handle_message(Msg(event_json(ts_utc=ts, speed_kmph=speed), i), pool, pub)
    sent = pub.json_on("alerts.out")
    assert len(sent) == 1 and sent[0]["type"] == "OVERSPEED" and sent[0]["vin"] == ACTIVE_VIN
    assert sent[0]["started_at"] == "2026-10-28T09:00:05Z"             # the 105 km/h ping
    assert pub.on("alerts.out")[0].key == ACTIVE_VIN


# ================================================================ M3: overspeed alerts in the database
def run_speeds(conn, speeds, vin=ACTIVE_VIN, start_index=0):
    return [ping(conn, vin, start_index + i, speed=s)[1] for i, s in enumerate(speeds)]


def alert_rows(conn, vin=ACTIVE_VIN):
    return conn.execute("SELECT type, started_at FROM alerts WHERE vin = %s ORDER BY started_at", (vin,)).fetchall()


def test_simulator_scenario_creates_exactly_one_overspeed_alert(conn):
    created = run_speeds(conn, [60, 105, 112, 118, 121, 125, 80])
    assert [a is not None for a in created] == [False, False, False, True, False, False, False]
    rows = alert_rows(conn)
    assert len(rows) == 1 and rows[0][0] == "OVERSPEED"
    assert rows[0][1] == BASE + dt.timedelta(seconds=5)                 # starts at the 105 km/h ping
    assert created[3]["details"]["limit_kmph"] == config.OVERSPEED_KMPH


def test_exactly_100_kmph_never_alerts(conn):
    assert all(a is None for a in run_speeds(conn, [100] * 6))
    assert alert_rows(conn) == []


def test_run_of_two_does_not_alert(conn):
    run_speeds(conn, [60, 110, 120, 60])
    assert alert_rows(conn) == []


def test_run_broken_by_a_slow_ping_does_not_alert_until_a_new_run_reaches_three(conn):
    run_speeds(conn, [110, 120, 90, 110, 120])
    assert alert_rows(conn) == []
    created = run_speeds(conn, [130], start_index=5)
    assert created[0] is not None and len(alert_rows(conn)) == 1


def test_two_separate_runs_give_two_alerts(conn):
    run_speeds(conn, [110, 120, 130, 60, 110, 120, 130])
    assert len(alert_rows(conn)) == 2


def test_long_run_gives_one_alert_not_one_per_ping(conn):
    run_speeds(conn, [110 + i for i in range(30)])
    assert len(alert_rows(conn)) == 1


def test_a_redelivered_ping_cannot_create_a_second_alert(conn):
    run_speeds(conn, [110, 120, 130])
    created = run_speeds(conn, [110, 120, 130])                         # the same three pings again
    assert created == [None, None, None] and len(alert_rows(conn)) == 1


def test_alerts_of_one_vehicle_do_not_leak_into_another(conn):
    run_speeds(conn, [110, 120, 130], vin=ACTIVE_VIN)
    run_speeds(conn, [50, 50, 50], vin=OTHER_VIN)
    assert alert_rows(conn, OTHER_VIN) == [] and len(alert_rows(conn, ACTIVE_VIN)) == 1


def test_the_unique_constraint_is_the_backstop_even_if_the_rule_fires_twice(conn):
    run_speeds(conn, [110, 120, 130])
    again = alerts.evaluate_overspeed(conn, TelemetryEvent(
        vin=ACTIVE_VIN, ts_utc=BASE + dt.timedelta(seconds=10), speed_kmph=130, odometer_km=1, lat=0, lon=0, source_oem="x"))
    assert again is None and len(alert_rows(conn)) == 1


# ================================================================ M3: query APIs (through HTTP, real SQL)
@pytest.fixture
def api(conn, client):
    app.dependency_overrides[get_conn] = lambda: conn
    yield client                                   # `client` clears all overrides at teardown


def test_latest_returns_the_newest_ping(conn, api):
    for i in range(5):
        ping(conn, ACTIVE_VIN, i, speed=40 + i)
    r = api.get(f"/v1/vehicles/{ACTIVE_VIN}/latest")
    assert r.status_code == 200
    body = r.json()
    assert body["vin"] == ACTIVE_VIN and body["speed_kmph"] == 44.0 and body["ts_utc"] == "2026-10-28T09:00:20Z"


def test_latest_accepts_a_lowercase_vin(conn, api):
    ping(conn, ACTIVE_VIN, 0)
    assert api.get(f"/v1/vehicles/{ACTIVE_VIN.lower()}/latest").status_code == 200


def test_latest_404_for_a_vehicle_that_never_reported_and_for_an_unknown_one(conn, api):
    r = api.get(f"/v1/vehicles/{ACTIVE_VIN}/latest")
    assert r.status_code == 404 and r.json()["error"] == "not_found" and "no telemetry" in r.json()["message"]
    r = api.get("/v1/vehicles/1FAXXK7Y3R5A2B4C6/latest")
    assert r.status_code == 404 and "unknown vehicle" in r.json()["message"]


def test_latest_400_for_a_malformed_vin(api):
    r = api.get("/v1/vehicles/NOT-A-VIN/latest")
    assert r.status_code == 400 and r.json()["details"][0]["field"] == "vin"


# ---- pagination
def seed_pings(conn, n, vin=ACTIVE_VIN):
    for i in range(n):
        ping(conn, vin, i)


def walk(api, url, **params):
    """Follows next_cursor to the end. Returns (list of pages, list of all timestamps)."""
    pages, cursor = [], None
    while True:
        q = dict(params, **({"cursor": cursor} if cursor else {}))
        r = api.get(url, params=q)
        assert r.status_code == 200, r.text
        body = r.json()
        pages.append(body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            return pages, [p["ts_utc"] for page in pages for p in page]
        assert len(pages) < 100, "pagination does not terminate"


def test_pagination_walks_every_row_once_newest_first(conn, api):
    seed_pings(conn, 25)
    pages, stamps = walk(api, f"/v1/vehicles/{ACTIVE_VIN}/telemetry", limit=10)
    assert [len(p) for p in pages] == [10, 10, 5]
    assert len(stamps) == 25 and len(set(stamps)) == 25               # no duplicates, no gaps
    assert stamps == sorted(stamps, reverse=True)                     # newest first across page boundaries


def test_a_result_that_exactly_fills_the_last_page_has_no_next_cursor(conn, api):
    seed_pings(conn, 20)
    pages, _ = walk(api, f"/v1/vehicles/{ACTIVE_VIN}/telemetry", limit=10)
    assert [len(p) for p in pages] == [10, 10]                       # not a third, empty page


def test_pagination_is_stable_when_new_pings_arrive_between_pages(conn, api):
    """The point of keyset over OFFSET: a newer row appearing while you page does not shift the next page."""
    seed_pings(conn, 20)
    r1 = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry", params={"limit": 10}).json()
    ping(conn, ACTIVE_VIN, 100)                                       # a brand-new, newer ping
    r2 = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry", params={"limit": 10, "cursor": r1["next_cursor"]}).json()
    seen = [p["ts_utc"] for p in r1["items"] + r2["items"]]
    assert len(seen) == 20 and len(set(seen)) == 20


def test_from_is_inclusive_and_to_is_exclusive(conn, api):
    seed_pings(conn, 10)                                              # 09:00:00 ... 09:00:45
    _, stamps = walk(api, f"/v1/vehicles/{ACTIVE_VIN}/telemetry", **{"from": "2026-10-28T09:00:10Z", "to": "2026-10-28T09:00:30Z"})
    assert stamps == ["2026-10-28T09:00:25Z", "2026-10-28T09:00:20Z", "2026-10-28T09:00:15Z", "2026-10-28T09:00:10Z"]


def test_range_with_a_utc_offset_and_with_an_unencoded_plus(conn, api):
    seed_pings(conn, 10)
    expected = ["2026-10-28T09:00:25Z", "2026-10-28T09:00:20Z", "2026-10-28T09:00:15Z", "2026-10-28T09:00:10Z"]
    # 14:30:10+05:30 == 09:00:10Z. A '+' pasted into a URL arrives as a space; that must still work.
    r = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry?from=2026-10-28T14:30:10+05:30&to=2026-10-28T14:30:30%2B05:30")
    assert [p["ts_utc"] for p in r.json()["items"]] == expected


def test_only_from_and_only_to(conn, api):
    seed_pings(conn, 6)
    only_from = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry", params={"from": "2026-10-28T09:00:20Z"}).json()["items"]
    only_to = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry", params={"to": "2026-10-28T09:00:10Z"}).json()["items"]
    assert len(only_from) == 2 and len(only_to) == 2


def test_pagination_inside_a_time_range(conn, api):
    seed_pings(conn, 30)
    pages, stamps = walk(api, f"/v1/vehicles/{ACTIVE_VIN}/telemetry", limit=4,
                         **{"from": "2026-10-28T09:00:20Z", "to": "2026-10-28T09:01:00Z"})
    assert len(stamps) == 8 and len(set(stamps)) == 8                # pings 4..11


def test_default_limit_is_100_and_max_is_1000(conn, api):
    seed_pings(conn, 120)
    assert len(api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry").json()["items"]) == 100
    r = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry", params={"limit": 1001})
    assert r.status_code == 400 and r.json()["details"][0]["field"] == "limit"


@pytest.mark.parametrize("params,field", [
    ({"limit": 0}, "limit"), ({"limit": -1}, "limit"), ({"limit": "ten"}, "limit"),
    ({"cursor": "not-a-cursor"}, "cursor"), ({"cursor": "e30="}, "cursor"),          # "e30=" is base64 for {}
    ({"from": "yesterday"}, "from"), ({"from": "1795860000"}, "from"), ({"to": "2026-10-28"}, "to"),
    ({"from": "2026-10-28T09:00:00"}, "from"),                                      # no timezone
    ({"from": "2026-10-28T10:00:00Z", "to": "2026-10-28T09:00:00Z"}, "from"),        # from after to
    ({"from": "2026-10-28T10:00:00Z", "to": "2026-10-28T10:00:00Z"}, "from"),        # empty range
])
def test_bad_query_parameters_are_400_with_the_field_named(conn, api, params, field):
    r = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry", params=params)
    assert r.status_code == 400 and r.json()["error"] == "validation_failed"
    assert r.json()["details"][0]["field"] == field


def test_telemetry_404_for_unknown_vehicle_but_200_empty_for_a_known_silent_one(conn, api):
    assert api.get("/v1/vehicles/1FAXXK7Y3R5A2B4C6/telemetry").status_code == 404
    r = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry")
    assert r.status_code == 200 and r.json() == {"vin": ACTIVE_VIN, "items": [], "next_cursor": None}


def test_cursor_is_opaque_but_round_trips_the_last_timestamp(conn, api):
    seed_pings(conn, 5)
    cursor = api.get(f"/v1/vehicles/{ACTIVE_VIN}/telemetry", params={"limit": 2}).json()["next_cursor"]
    assert cursor and "09:00" not in cursor            # clients should not parse it


# ---- alerts endpoint
def test_alerts_endpoint_lists_newest_first(conn, api):
    run_speeds(conn, [110, 120, 130, 60, 110, 120, 130])               # two runs
    r = api.get(f"/v1/vehicles/{ACTIVE_VIN}/alerts")
    assert r.status_code == 200
    got = r.json()["alerts"]
    assert [a["type"] for a in got] == ["OVERSPEED", "OVERSPEED"]
    assert [a["started_at"] for a in got] == ["2026-10-28T09:00:20Z", "2026-10-28T09:00:00Z"]
    assert got[0]["details"]["limit_kmph"] == 100


def test_alerts_endpoint_empty_for_known_vehicle_and_404_for_unknown(conn, api):
    assert api.get(f"/v1/vehicles/{ACTIVE_VIN}/alerts").json() == {"vin": ACTIVE_VIN, "alerts": []}
    assert api.get("/v1/vehicles/1FAXXK7Y3R5A2B4C6/alerts").status_code == 404


# ---- fleet
def active_vins(conn, customer_id):
    return {r[0] for r in conn.execute("SELECT vin FROM vehicles WHERE customer_id = %s AND status = 'active'", (customer_id,))}


def test_fleet_latest_lists_every_active_vehicle_with_its_newest_ping(conn, api):
    seed_pings(conn, 3, ACTIVE_VIN)
    ping(conn, OTHER_VIN, 7, speed=77.0)
    ping(conn, DEPROVISIONED_VIN, 9)                                   # has data but is deprovisioned -> excluded
    r = api.get("/v1/fleets/1/latest")
    assert r.status_code == 200
    body = r.json()
    by_vin = {v["vin"]: v for v in body["vehicles"]}
    assert body["customer_id"] == 1 and set(by_vin) == active_vins(conn, 1)
    assert DEPROVISIONED_VIN not in by_vin
    assert by_vin[ACTIVE_VIN]["latest"]["ts_utc"] == "2026-10-28T09:00:10Z"
    assert by_vin[OTHER_VIN]["latest"]["speed_kmph"] == 77.0
    silent = [v for v in body["vehicles"] if v["vin"] not in (ACTIVE_VIN, OTHER_VIN)]
    assert silent and all(v["latest"] is None for v in silent)        # active but never reported: listed, latest null


def test_fleet_latest_404_for_unknown_customer_and_400_for_a_non_number(api):
    r = api.get("/v1/fleets/999999/latest")
    assert r.status_code == 404 and r.json()["error"] == "not_found"
    assert api.get("/v1/fleets/abc/latest").status_code == 400
    assert api.get("/v1/fleets/99999999999/latest").status_code == 400      # would overflow the INT column
    assert api.get("/v1/fleets/0/latest").status_code == 400


def test_fleet_latest_200_with_no_vehicles_for_a_customer_without_active_vehicles(conn, api):
    cid = conn.execute("INSERT INTO customers (name, type) VALUES ('Empty Fleet Ltd', 'fleet') RETURNING id").fetchone()[0]
    r = api.get(f"/v1/fleets/{cid}/latest")
    assert r.status_code == 200 and r.json()["vehicles"] == [] and r.json()["name"] == "Empty Fleet Ltd"


def test_fleet_latest_is_exactly_one_sql_statement(conn):
    seed_pings(conn, 3, ACTIVE_VIN)
    RecordingCursor.statements.clear()
    result = repository.fleet_latest(conn, 1)
    assert len(result["vehicles"]) >= 40                                # dozens of vehicles ...
    assert len(RecordingCursor.statements) == 1                         # ... one query, not one per vehicle


# ================================================================ M3: does the range query use the index?
@pytest.fixture
def big_table(conn):
    """100,000 rows (50 vehicles x 2,000 pings), analysed, so the planner chooses plans like it would in production."""
    conn.execute("""
        INSERT INTO telemetry (vin, ts_utc, speed_kmph, odometer_km, lat, lon, source_oem)
        SELECT lpad(v::text, 17, 'A'), TIMESTAMPTZ '2026-01-01 00:00:00+00' + (g * 5 || ' seconds')::interval,
               50, g, 12.9, 77.5, 'OEM-A'
        FROM generate_series(1, 50) v, generate_series(1, 2000) g""")
    conn.execute("ANALYZE telemetry")
    return "AAAAAAAAAAAAAAA25"


def explain(conn, statement):
    sql, params = statement
    return "\n".join(r[0] for r in conn.execute("EXPLAIN " + sql, params).fetchall())


def test_range_query_uses_the_vin_ts_index_without_a_sort(conn, big_table):
    RecordingCursor.statements.clear()
    repository.telemetry_range(conn, big_table, BASE - dt.timedelta(days=300), BASE + dt.timedelta(days=300),
                               BASE - dt.timedelta(days=200), 101)
    plan = explain(conn, RecordingCursor.statements[-1])
    assert "uq_telemetry_vin_ts" in plan, plan
    assert "Index Scan Backward" in plan, plan          # newest-first straight off the index ...
    assert "Sort" not in plan and "Seq Scan" not in plan, plan        # ... no sort step, no table scan


def test_latest_query_uses_the_index(conn, big_table):
    RecordingCursor.statements.clear()
    repository.latest_for_vin(conn, big_table)
    plan = explain(conn, RecordingCursor.statements[-1])
    assert "uq_telemetry_vin_ts" in plan and "Seq Scan" not in plan, plan


def test_fleet_query_probes_the_index_per_vehicle_instead_of_scanning_telemetry(conn, big_table):
    RecordingCursor.statements.clear()
    repository.fleet_latest(conn, 1)
    plan = explain(conn, RecordingCursor.statements[-1])
    assert "uq_telemetry_vin_ts" in plan and "Seq Scan on telemetry" not in plan, plan


def test_overspeed_lookup_uses_the_index(conn, big_table):
    RecordingCursor.statements.clear()
    alerts.evaluate_overspeed(conn, TelemetryEvent(vin=big_table, ts_utc=BASE, speed_kmph=50, odometer_km=1, lat=0, lon=0,
                                                   source_oem="x"))
    run_query = next(s for s in RecordingCursor.statements if "telemetry" in s[0] and "ORDER BY ts_utc DESC" in s[0])
    plan = explain(conn, run_query)
    assert "uq_telemetry_vin_ts" in plan and "Seq Scan on telemetry" not in plan, plan
