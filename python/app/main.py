"""FastAPI app. Run:  uvicorn app.main:app --reload --port 8080   (from the python/ folder)

  POST /v1/heartbeats                                    validate -> normalise -> publish to Kafka (M1)
  GET  /v1/vehicles/{vin}/latest                         (M3)
  GET  /v1/vehicles/{vin}/telemetry?from=&to=&limit=&cursor=
  GET  /v1/vehicles/{vin}/alerts
  GET  /v1/fleets/{customer_id}/latest

Every error leaves the API as {"error": "...", "details" | "message": ...} with the right status code.
"""
import base64
import binascii
import datetime as dt
import json
import logging
import re
from contextlib import asynccontextmanager
from typing import Optional

import psycopg
from fastapi import Depends, FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import TypeAdapter, ValidationError

from app import config, repository
from app.db import db_is_up, get_pool
from app.kafka_client import EventPublisher, PublishError, flush_publisher, get_publisher
from app.models import HeartbeatRequest, normalize_vin
from app.normalize import normalize
from app.util import UTC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    flush_publisher()       # do not drop messages that are still queued in the producer on shutdown


app = FastAPI(title="Fleet Telemetry API", version="0.2", lifespan=lifespan)


class ApiError(Exception):
    """Raise anywhere in a request to answer with a specific status and error body."""

    def __init__(self, status_code: int, body: dict):
        super().__init__(body.get("message") or body.get("error"))
        self.status_code = status_code
        self.body = body


def validation_failed(field: str, message: str) -> ApiError:
    return ApiError(400, {"error": "validation_failed", "details": [{"field": field, "message": message}]})


def not_found(message: str) -> ApiError:
    return ApiError(404, {"error": "not_found", "message": message})


@app.exception_handler(ApiError)
async def api_error(_: Request, exc: ApiError):
    return JSONResponse(status_code=exc.status_code, content=exc.body)


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError):
    """Turns FastAPI's 422 into the 400 error body our API contract uses."""
    details = [{"field": _field_name(e), "message": _message(e)} for e in exc.errors()]
    return JSONResponse(status_code=400, content={"error": "validation_failed", "details": details})


def _field_name(error: dict) -> str:
    if error["type"] == "json_invalid":
        return "body"       # for a JSON syntax error loc[1] is a character offset, not a field name
    return ".".join(str(p) for p in error["loc"][1:]) or "body"


def _message(error: dict) -> str:
    return error["msg"].removeprefix("Value error, ")      # pydantic prefixes our own validator messages


@app.exception_handler(PublishError)
async def kafka_unavailable(_: Request, exc: PublishError):
    log.error("publish failed: %s", exc)
    return JSONResponse(status_code=503, content={"error": "kafka_unavailable",
                                                  "message": "could not hand the message to Kafka, retry later"})


@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(psycopg.InterfaceError)
async def database_unavailable(_: Request, exc: Exception):
    """Connection refused, pool timeout, server restarting: the caller should retry, it is not our bug (-> 503)."""
    log.error("database unavailable: %s", exc)
    return JSONResponse(status_code=503, content={"error": "database_unavailable",
                                                  "message": "the database is not reachable, retry later"})


def get_conn():
    """FastAPI dependency: one pooled connection per request. Tests override it with a rolled-back transaction."""
    with get_pool().connection() as conn:
        yield conn


@app.get("/health")
def health():
    db = db_is_up()
    return JSONResponse(status_code=200 if db else 503,
                        content={"status": "UP" if db else "DEGRADED", "db": "up" if db else "down",
                                 "kafka_bootstrap": config.KAFKA_BOOTSTRAP})


# ---------------- M1: ingest ----------------
@app.post("/v1/heartbeats", status_code=202)
def ingest_heartbeat(hb: HeartbeatRequest, publisher: EventPublisher = Depends(get_publisher)):
    # Validation already happened: an invalid body never reaches this function (-> 400, nothing published).
    event = normalize(hb)
    message = event.to_message()
    # Key = VIN: Kafka hashes the key to pick the partition, so every event of one vehicle lands in the same
    # partition and is consumed in order (per-vehicle ordering matters for "latest" and for the overspeed run).
    publisher.publish_and_wait(config.TOPIC_RAW, key=event.vin, value=json.dumps(message).encode())
    return {"status": "accepted", "vin": message["vin"], "ts_utc": message["ts_utc"]}


# ---------------- M3: query APIs ----------------
DEFAULT_PAGE = 100
MAX_PAGE = 1000
_TS = TypeAdapter(dt.datetime)


def _vin_param(vin: str) -> str:
    try:
        return normalize_vin(vin)
    except ValueError as e:
        raise validation_failed("vin", str(e))


def _instant(field: str, raw: Optional[str]) -> Optional[dt.datetime]:
    """A query-string timestamp: ISO-8601 WITH a timezone, returned in UTC. None if absent."""
    if raw is None:
        return None
    text = re.sub(r" (\d{2}:\d{2})$", r"+\1", raw.strip())          # in a URL a literal '+' arrives as a space
    # The regex refuses bare numbers, which pydantic would otherwise read as unix seconds.
    if not re.match(r"\d{4}-\d{2}-\d{2}", text):
        raise validation_failed(field, "must be an ISO-8601 timestamp, e.g. 2026-10-28T10:00:00Z")
    try:
        value = _TS.validate_python(text)
    except ValidationError:
        raise validation_failed(field, "must be an ISO-8601 timestamp, e.g. 2026-10-28T10:00:00Z")
    if value.tzinfo is None:
        raise validation_failed(field, "must include a timezone, e.g. Z or +05:30 (encode '+' as %2B in a URL)")
    return value.astimezone(UTC)


def encode_cursor(ts: str) -> str:
    return base64.urlsafe_b64encode(json.dumps({"ts": ts}).encode()).decode()


def decode_cursor(cursor: str) -> dt.datetime:
    try:
        ts = json.loads(base64.urlsafe_b64decode(cursor.encode()))["ts"]
        value = _TS.validate_python(ts)
        if value.tzinfo is None:
            raise ValueError("naive timestamp")
        return value.astimezone(UTC)
    except (binascii.Error, ValueError, KeyError, TypeError, ValidationError, UnicodeError):
        raise validation_failed("cursor", "invalid cursor: pass back next_cursor exactly as it was returned")


def _require_vehicle(conn, vin: str) -> None:
    if not repository.vehicle_exists_on(conn, vin):
        raise not_found(f"unknown vehicle {vin}")


@app.get("/v1/vehicles/{vin}/latest")
def vehicle_latest(vin: str, conn=Depends(get_conn)):
    vin = _vin_param(vin)
    point = repository.latest_for_vin(conn, vin)
    if point is None:
        _require_vehicle(conn, vin)
        raise not_found(f"no telemetry received yet for vehicle {vin}")
    return {"vin": vin, **point}


@app.get("/v1/vehicles/{vin}/telemetry")
def vehicle_telemetry(vin: str,
                      from_: Optional[str] = Query(None, alias="from", description="inclusive, ISO-8601 with timezone"),
                      to: Optional[str] = Query(None, description="exclusive, ISO-8601 with timezone"),
                      limit: int = Query(DEFAULT_PAGE, ge=1, le=MAX_PAGE),
                      cursor: Optional[str] = Query(None, description="next_cursor of the previous page"),
                      conn=Depends(get_conn)):
    """Newest first. Pass `next_cursor` back as `cursor` for the next page; null means the last page."""
    vin = _vin_param(vin)
    start, end = _instant("from", from_), _instant("to", to)
    if start is not None and end is not None and start >= end:
        raise validation_failed("from", "must be earlier than 'to'")
    before = decode_cursor(cursor) if cursor else None

    rows = repository.telemetry_range(conn, vin, start, end, before, limit + 1)    # +1 row = "is there a next page?"
    if not rows and before is None:
        _require_vehicle(conn, vin)                        # unknown vehicle -> 404; known but empty -> 200 []
    page = rows[:limit]
    return {"vin": vin, "items": page, "next_cursor": encode_cursor(page[-1]["ts_utc"]) if len(rows) > limit else None}


@app.get("/v1/vehicles/{vin}/alerts")
def vehicle_alerts(vin: str, limit: int = Query(DEFAULT_PAGE, ge=1, le=MAX_PAGE), conn=Depends(get_conn)):
    vin = _vin_param(vin)
    found = repository.alerts_for_vin(conn, vin, limit)
    if not found:
        _require_vehicle(conn, vin)
    return {"vin": vin, "alerts": found}


@app.get("/v1/fleets/{customer_id}/latest")
def fleet_latest(customer_id: int = Path(ge=1, le=2_147_483_647), conn=Depends(get_conn)):
    fleet = repository.fleet_latest(conn, customer_id)
    if fleet is None:
        raise not_found(f"unknown customer {customer_id}")
    return fleet
