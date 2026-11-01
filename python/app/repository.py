"""All SQL lives here. Parameterised queries only — never build SQL with f-strings from input.

Functions that write take an open connection so the caller decides the transaction boundary
(the consumer inserts the row and evaluates alerts in ONE transaction).
"""
import datetime as dt
from typing import Optional

from psycopg.rows import dict_row

from app.db import get_pool
from app.models import TelemetryEvent
from app.util import iso_z


def vehicle_exists(vin: str) -> bool:
    with get_pool().connection() as conn:
        return conn.execute("SELECT 1 FROM vehicles WHERE vin = %s", (vin,)).fetchone() is not None


# ---------------------------------------------------------------- M2: write path
_INSERT_TELEMETRY = """
INSERT INTO telemetry (vin, ts_utc, speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (vin, ts_utc) DO NOTHING
"""


def insert_telemetry(conn, event: TelemetryEvent) -> bool:
    """Writes one event. Idempotent: (vin, ts_utc) is UNIQUE, so a re-delivered event is silently skipped.

    Returns True if a new row was written, False if the row already existed. When the same (vin, ts_utc) arrives
    again with different values, the first write wins — duplicates are assumed to be copies, not corrections.
    """
    cur = conn.execute(_INSERT_TELEMETRY, (
        event.vin, event.ts_utc, event.speed_kmph, event.odometer_km, event.lat, event.lon,
        event.heading, event.soc_pct, event.ignition, event.source_oem,
    ))
    return cur.rowcount == 1


# ---------------------------------------------------------------- M3: read path
# Every function takes the connection so tests can run them inside a transaction they roll back.
_POINT_COLUMNS = "ts_utc, speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem"


def _num(value):
    """NUMERIC columns arrive as Decimal; the API speaks plain JSON numbers."""
    return None if value is None else float(value)


def _point(row: dict) -> dict:
    return {
        "ts_utc": iso_z(row["ts_utc"]),
        "speed_kmph": _num(row["speed_kmph"]),
        "odometer_km": _num(row["odometer_km"]),
        "lat": row["lat"],
        "lon": row["lon"],
        "heading": row["heading"],
        "soc_pct": _num(row["soc_pct"]),
        "ignition": row["ignition"],
        "source_oem": row["source_oem"],
    }


def vehicle_exists_on(conn, vin: str) -> bool:
    return conn.execute("SELECT 1 FROM vehicles WHERE vin = %s", (vin,)).fetchone() is not None


def latest_for_vin(conn, vin: str) -> Optional[dict]:
    """Newest ping of a vehicle, or None. Index-only walk down (vin, ts_utc), LIMIT 1."""
    with conn.cursor(row_factory=dict_row) as cur:
        row = cur.execute(
            f"SELECT {_POINT_COLUMNS} FROM telemetry WHERE vin = %s ORDER BY ts_utc DESC LIMIT 1", (vin,)
        ).fetchone()
    return None if row is None else _point(row)


def telemetry_range(conn, vin: str, start: Optional[dt.datetime], end: Optional[dt.datetime],
                    before: Optional[dt.datetime], limit: int) -> list[dict]:
    """Pings of one vehicle, NEWEST FIRST, keyset-paginated on ts_utc (no OFFSET).

    start  inclusive lower bound            end     exclusive upper bound
    before exclusive upper bound taken from the previous page's cursor (the last ts_utc already returned)

    (vin, ts_utc) is UNIQUE, so ts_utc alone is a total order inside one vehicle: no tie-breaker column is needed.
    Returns up to `limit` rows; ask for limit+1 to learn whether another page exists.

    The SQL text is assembled from FIXED fragments only (values are always bound parameters). Optional filters are
    left out rather than written as `(%s IS NULL OR ts_utc >= %s)`, which can stop Postgres from using the index.
    """
    sql = [f"SELECT {_POINT_COLUMNS} FROM telemetry WHERE vin = %s"]
    params: list = [vin]
    if start is not None:
        sql.append("AND ts_utc >= %s")
        params.append(start)
    if end is not None:
        sql.append("AND ts_utc < %s")
        params.append(end)
    if before is not None:
        sql.append("AND ts_utc < %s")
        params.append(before)
    sql.append("ORDER BY ts_utc DESC LIMIT %s")
    params.append(limit)
    with conn.cursor(row_factory=dict_row) as cur:
        return [_point(r) for r in cur.execute(" ".join(sql), params).fetchall()]


def alerts_for_vin(conn, vin: str, limit: int) -> list[dict]:
    """Alerts of a vehicle, newest first (by when the offending run started)."""
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            "SELECT type, started_at, details, created_at FROM alerts WHERE vin = %s "
            "ORDER BY started_at DESC, id DESC LIMIT %s", (vin, limit)
        ).fetchall()
    return [{"type": r["type"], "started_at": iso_z(r["started_at"]), "details": r["details"],
             "created_at": iso_z(r["created_at"])} for r in rows]


# One statement answers three questions: does the customer exist (no row -> 404), which of its vehicles are
# active, and what is each one's newest ping. LATERAL + LIMIT 1 is one index probe per vehicle instead of
# one query per vehicle (the N+1 trap) or a window function over the whole telemetry table.
_FLEET_LATEST = f"""
SELECT c.id AS customer_id, c.name AS customer_name,
       v.vin, v.oem, v.model_year,
       t.ts_utc, t.speed_kmph, t.odometer_km, t.lat, t.lon, t.heading, t.soc_pct, t.ignition, t.source_oem
FROM customers c
LEFT JOIN vehicles v ON v.customer_id = c.id AND v.status = 'active'
LEFT JOIN LATERAL (
    SELECT {_POINT_COLUMNS}
    FROM telemetry
    WHERE telemetry.vin = v.vin
    ORDER BY telemetry.ts_utc DESC
    LIMIT 1
) t ON true
WHERE c.id = %s
ORDER BY v.vin
"""


def fleet_latest(conn, customer_id: int) -> Optional[dict]:
    """Latest ping of every ACTIVE vehicle of a customer, in ONE query. None if the customer does not exist.

    Active vehicles that never reported are listed with "latest": null, so the caller can tell
    "no data yet" from "not in the fleet".
    """
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(_FLEET_LATEST, (customer_id,)).fetchall()
    if not rows:
        return None
    vehicles = []
    for r in rows:
        if r["vin"] is None:                      # customer without active vehicles: the LEFT JOIN's single empty row
            continue
        vehicles.append({"vin": r["vin"], "oem": r["oem"], "model_year": r["model_year"],
                         "latest": None if r["ts_utc"] is None else _point(r)})
    return {"customer_id": rows[0]["customer_id"], "name": rows[0]["customer_name"], "vehicles": vehicles}
