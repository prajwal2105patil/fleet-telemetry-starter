"""M3: Overspeed rule — speed ABOVE OVERSPEED_KMPH (default 100) for OVERSPEED_MIN_PINGS (default 3)
consecutive pings of one vehicle => exactly ONE alert for that run (not one per ping).

"Above" is strict: exactly 100 km/h is not overspeed. "Consecutive" means consecutive pings of that vehicle,
whatever the time between them.

How duplicates are avoided
  * The alert is identified by (vin, 'OVERSPEED', started_at) where started_at = timestamp of the FIRST ping of
    the run. A run that keeps growing (4th, 5th, ... ping) has the same start, so the UNIQUE constraint
    uq_alert turns every later evaluation into a no-op (ON CONFLICT DO NOTHING).
  * Re-delivery of the same Kafka message is a no-op as well: the telemetry insert reports "already there" and
    the rule is not evaluated again.
"""
import datetime as dt
import json
import logging
from typing import Optional, Sequence

from app import config
from app.kafka_client import EventPublisher
from app.models import TelemetryEvent
from app.util import iso_z

log = logging.getLogger(__name__)

ALERT_TYPE = "OVERSPEED"


def overspeed_run_start(rows_newest_first: Sequence[tuple[dt.datetime, float]], limit_kmph: float,
                        min_pings: int) -> Optional[dt.datetime]:
    """Pure rule. `rows_newest_first` = [(ts, speed_kmph), ...] of ONE vehicle, newest ping first.

    Looks at the run of over-limit pings that ends at the newest ping. Returns the timestamp of the FIRST ping of
    that run if it is at least `min_pings` long, otherwise None. A ping at or below the limit ends the run.
    """
    run_start = None
    length = 0
    for ts, speed in rows_newest_first:
        if speed > limit_kmph:
            run_start = ts
            length += 1
        else:
            break
    return run_start if length >= min_pings else None


_RUN_SINCE_LAST_SLOW_PING = """
SELECT ts_utc, speed_kmph
FROM telemetry
WHERE vin = %(vin)s
  AND ts_utc >= COALESCE((SELECT max(ts_utc) FROM telemetry WHERE vin = %(vin)s AND speed_kmph <= %(limit)s),
                         '-infinity')
ORDER BY ts_utc DESC
"""

_INSERT_ALERT = """
INSERT INTO alerts (vin, type, started_at, details)
SELECT %(vin)s, %(type)s, %(started_at)s, %(details)s::jsonb
ON CONFLICT (vin, type, started_at) DO NOTHING
"""


def evaluate_overspeed(conn, event: TelemetryEvent) -> Optional[dict]:
    """Runs the rule for event.vin inside the caller's transaction. Returns the alert if a NEW one was created.

    Reads the vehicle's pings from its last at-or-below-limit ping onwards (plus that one ping): that is exactly
    the current run and the ping that ended the previous one, using the (vin, ts_utc) index. The cost grows with
    the length of a run that is still going, which is fine for minutes of speeding and is noted in the README.
    """
    limit = config.OVERSPEED_KMPH
    rows = conn.execute(_RUN_SINCE_LAST_SLOW_PING, {"vin": event.vin, "limit": limit}).fetchall()
    started_at = overspeed_run_start([(r[0], float(r[1])) for r in rows], limit, config.OVERSPEED_MIN_PINGS)
    if started_at is None:
        return None

    pings = 0
    for _, speed in rows:
        if float(speed) <= limit:
            break
        pings += 1
    details = {"limit_kmph": limit, "min_pings": config.OVERSPEED_MIN_PINGS, "pings_when_detected": pings,
               "speed_kmph": event.speed_kmph}
    cur = conn.execute(_INSERT_ALERT, {"vin": event.vin, "type": ALERT_TYPE, "started_at": started_at,
                                       "details": json.dumps(details)})
    if cur.rowcount != 1:
        return None                      # this run already has its alert
    return {"vin": event.vin, "type": ALERT_TYPE, "started_at": iso_z(started_at), "details": details}


def publish_alert(publisher: EventPublisher, alert: dict) -> None:
    """Notification on alerts.out. Called AFTER the database commit, so a published alert always exists in the
    alerts table. If publishing fails the alert is still stored (and returned by the API); we log and carry on
    rather than re-process the telemetry message, because re-processing would not re-send it anyway."""
    try:
        publisher.publish(config.TOPIC_ALERTS, key=alert["vin"], value=alert)
    except Exception:                    # noqa: BLE001 - a notification must never fail the telemetry write path
        log.exception("could not publish alert for %s started_at=%s", alert["vin"], alert["started_at"])
