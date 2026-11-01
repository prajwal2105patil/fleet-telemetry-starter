"""All SQL lives here. Parameterised queries only — never build SQL with f-strings from input.

Example of the style to follow:
"""
from typing import Optional

from psycopg.rows import dict_row

from app.db import get_pool


def vehicle_exists(vin: str) -> bool:
    with get_pool().connection() as conn:
        return conn.execute("SELECT 1 FROM vehicles WHERE vin = %s", (vin,)).fetchone() is not None


# TODO M2: insert_telemetry(conn, event) -> bool
#   - must be idempotent: the same (vin, ts_utc) delivered twice must not create two rows
#     (look at the constraints on the telemetry table in db/init/01_schema.sql)
#   - return True if a new row was written
#
# TODO M3: latest_for_vin, telemetry_range (keyset pagination), alerts_for_vin, fleet_latest (ONE query)
#   - check your range query with EXPLAIN in psql — does it use the (vin, ts_utc) index?
