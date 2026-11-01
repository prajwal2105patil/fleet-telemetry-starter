"""Request / event models."""
import datetime as dt
from typing import Literal, Optional

from pydantic import BaseModel, Field


class HeartbeatRequest(BaseModel):
    """What a device (or OEM feed) sends to POST /v1/heartbeats.

    TODO M1 — add validation so each of these becomes HTTP 400 (run: python3 simulator/simulate.py --mode malformed):
      - vin: exactly 17 chars, A-Z and 0-9, but never I, O or Q   (normalise to upper case)
      - ts: must include a timezone; convert to UTC; reject if more than 5 minutes in the future
      - speed_kmph 0..300, lat -90..90, lon -180..180, odometer >= 0, heading 0..359, soc_pct 0..100
      - odometer_unit only "km" or "mi"
    Hint: pydantic Field(ge=..., le=...), Literal[...], @field_validator.
    """
    vin: str
    ts: dt.datetime
    speed_kmph: float
    odometer: float
    odometer_unit: str = "km"
    lat: float
    lon: float
    heading: Optional[int] = None
    soc_pct: Optional[float] = None
    ignition: bool = True
    source_oem: str


class TelemetryEvent(BaseModel):
    """Normalised event written to Kafka topic telemetry.raw (schema v1). Odometer is always km. (Given — do not change the contract.)"""
    schema_version: Literal["v1"] = Field(default="v1", alias="schema")
    vin: str
    ts_utc: dt.datetime
    speed_kmph: float
    odometer_km: float
    lat: float
    lon: float
    heading: Optional[int] = None
    soc_pct: Optional[float] = None
    ignition: bool = True
    source_oem: str

    model_config = {"populate_by_name": True}

    def to_message(self) -> dict:
        return self.model_dump(mode="json", by_alias=True)
