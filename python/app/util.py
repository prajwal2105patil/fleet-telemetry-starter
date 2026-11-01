"""Small time helpers shared by the API, the consumer and the alert rule."""
import datetime as dt

UTC = dt.timezone.utc


def utcnow() -> dt.datetime:
    """Current time, timezone-aware (UTC). A function so tests can freeze the clock."""
    return dt.datetime.now(UTC)


def iso_z(ts: dt.datetime) -> str:
    """ISO-8601 in UTC with a trailing Z, e.g. 2026-10-28T10:00:00Z (microseconds only when non-zero)."""
    return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")
