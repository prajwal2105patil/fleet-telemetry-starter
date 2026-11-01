"""FastAPI app. Run:  uvicorn app.main:app --reload --port 8080   (from the python/ folder)

Milestones (see README.md):
  M1  POST /v1/heartbeats          validate -> normalise -> publish to Kafka (NO AI tools for M1)
  M3  GET  /v1/vehicles/{vin}/latest
      GET  /v1/vehicles/{vin}/telemetry?from=&to=&limit=&cursor=
      GET  /v1/vehicles/{vin}/alerts
      GET  /v1/fleets/{customer_id}/latest
"""
import logging

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app import config
from app.db import db_is_up
from app.kafka_client import EventPublisher, get_publisher
from app.models import HeartbeatRequest

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")

app = FastAPI(title="Fleet Telemetry API", version="0.1")


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError):
    """Turns FastAPI's 422 into the 400 error body our API contract uses."""
    details = [{"field": _field_name(e), "message": e["msg"]} for e in exc.errors()]
    return JSONResponse(status_code=400, content={"error": "validation_failed", "details": details})


def _field_name(error: dict) -> str:
    if error["type"] == "json_invalid":
        return "body"       # for a JSON syntax error loc[1] is a character offset, not a field name
    return ".".join(str(p) for p in error["loc"][1:]) or "body"


@app.get("/health")
def health():
    db = db_is_up()
    return JSONResponse(status_code=200 if db else 503,
                        content={"status": "UP" if db else "DEGRADED", "db": "up" if db else "down",
                                 "kafka_bootstrap": config.KAFKA_BOOTSTRAP})


# ---------------- M1: ingest (no AI tools) ----------------
@app.post("/v1/heartbeats", status_code=202)
def ingest_heartbeat(hb: HeartbeatRequest, publisher: EventPublisher = Depends(get_publisher)):
    # TODO M1:
    #  1. Validation rules belong in app/models.py (HeartbeatRequest) so bad input becomes a 400 automatically.
    #  2. event = normalize(hb)                         -> app/normalize.py
    #  3. publisher.publish(config.TOPIC_RAW, key=<?>, value=event.to_message())
    #     Which key keeps all events of one vehicle in order? Why does that matter?
    #  4. return {"status": "accepted", "vin": ..., "ts_utc": ...}
    return JSONResponse(status_code=501, content={"error": "not_implemented", "message": "M1: implement me"})


# ---------------- M3: query APIs (AI-assisted allowed) ----------------
# TODO M3: implement the four GET endpoints listed at the top of this file.
#  - 404 with an error body when a VIN has no data
#  - telemetry range: keyset pagination on ts_utc (return next_cursor), NOT OFFSET. Why?
#  - fleet latest: ONE SQL query for all vehicles of the customer (no loop of queries per VIN)
