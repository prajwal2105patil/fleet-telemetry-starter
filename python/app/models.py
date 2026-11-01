"""Request / event models."""
import datetime as dt
import re
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from app.util import UTC, utcnow

# 17 characters, A-Z and 0-9, never I, O or Q (ISO 3779).
VIN_RE = re.compile(r"[A-HJ-NPR-Z0-9]{17}")
VIN_RULE = "must be 17 characters: A-Z and 0-9, never I, O or Q"
MAX_FUTURE = dt.timedelta(minutes=5)


def normalize_vin(value: str) -> str:
    """Upper-case a VIN and check its format. Raises ValueError with a client-readable message."""
    # isascii(): str.upper() maps some non-ASCII letters (e.g. the long s) onto A-Z, which would sneak past the regex.
    if not value.isascii() or not VIN_RE.fullmatch(value.upper()):
        raise ValueError(VIN_RULE)
    return value.upper()


class HeartbeatRequest(BaseModel):
    """What a device (or OEM feed) sends to POST /v1/heartbeats.

    Every rule below becomes an HTTP 400 {"error": "validation_failed", "details": [...]} (see app/main.py).
    `allow_inf_nan=False` matters: odometer only has a lower bound, and `inf` would otherwise pass it.
    """
    vin: str
    ts: dt.datetime
    speed_kmph: float = Field(ge=0, le=300, allow_inf_nan=False)
    odometer: float = Field(ge=0, allow_inf_nan=False)
    odometer_unit: Literal["km", "mi"] = "km"
    lat: float = Field(ge=-90, le=90, allow_inf_nan=False)
    lon: float = Field(ge=-180, le=180, allow_inf_nan=False)
    heading: Optional[int] = Field(default=None, ge=0, le=359)
    soc_pct: Optional[float] = Field(default=None, ge=0, le=100, allow_inf_nan=False)
    ignition: bool = True
    source_oem: str = Field(min_length=1)

    @field_validator("vin")
    @classmethod
    def _vin_format(cls, v: str) -> str:
        return normalize_vin(v)

    @field_validator("ts", mode="before")
    @classmethod
    def _ts_is_a_string(cls, v):
        # Numbers would be read as unix seconds or milliseconds depending on size: ambiguous, so refuse them.
        if not isinstance(v, str):
            raise ValueError("must be an ISO-8601 string with a timezone, e.g. 2026-10-28T10:00:00Z")
        return v

    @field_validator("ts")
    @classmethod
    def _ts_has_timezone_and_is_not_in_the_future(cls, v: dt.datetime) -> dt.datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("must include a timezone, e.g. Z or +05:30")
        v = v.astimezone(UTC)
        if v > utcnow() + MAX_FUTURE:
            raise ValueError("must not be more than 5 minutes in the future")
        return v


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
