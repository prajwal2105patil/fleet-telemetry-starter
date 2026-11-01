#!/usr/bin/env python3
"""Vehicle device simulator — Python 3.9+ standard library only (no pip install).

Sends heartbeats to your Ingest API (POST /v1/heartbeats).

Examples
  python3 simulator/simulate.py                                   # 10 vehicles x 20 pings, 1 per second
  python3 simulator/simulate.py --vehicles 50 --count 100 --interval 0.2
  python3 simulator/simulate.py --mode duplicate                  # every ping sent twice  (idempotency)
  python3 simulator/simulate.py --mode malformed                  # bad payloads, expect HTTP 400
  python3 simulator/simulate.py --mode overspeed                  # one vehicle above 100 km/h for 5 pings
  python3 simulator/simulate.py --mode out-of-order               # timestamps shuffled
  python3 simulator/simulate.py --mode burst --vehicles 100 --count 50   # load, no sleep

Payload sent (OEM-B devices report odometer in miles, everyone else in km):
  {"vin": "...", "ts": "2026-10-28T10:00:00Z", "speed_kmph": 54.2, "odometer": 10234.5,
   "odometer_unit": "km", "lat": 12.97, "lon": 77.59, "heading": 90, "soc_pct": null,
   "ignition": true, "source_oem": "OEM-A"}
"""
import argparse
import csv
import datetime as dt
import json
import random
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODES = ["normal", "duplicate", "malformed", "overspeed", "out-of-order", "burst"]


def load_vehicles(n):
    with open(ROOT / "data" / "vehicles.csv") as f:
        rows = [r for r in csv.DictReader(f) if r["status"] == "active"]
    return rows[:n]


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def post(api, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(api + "/v1/heartbeats", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read().decode()[:200]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]
    except urllib.error.URLError as e:
        return 0, f"connection failed: {e.reason}"


class Vehicle:
    def __init__(self, row, rnd):
        self.vin, self.oem = row["vin"], row["oem"]
        self.miles = self.oem == "OEM-B"
        self.odo_km = rnd.uniform(5000, 90000)
        self.speed = rnd.uniform(20, 70)
        self.lat, self.lon = 12.9 + rnd.random() * 0.2, 77.5 + rnd.random() * 0.2
        self.rnd = rnd

    def ping(self, ts, speed=None):
        self.speed = speed if speed is not None else max(0.0, min(140.0, self.speed + self.rnd.uniform(-8, 8)))
        self.odo_km += self.speed / 3600 * 5
        self.lat += self.rnd.uniform(-0.001, 0.001)
        self.lon += self.rnd.uniform(-0.001, 0.001)
        odo = self.odo_km / 1.609344 if self.miles else self.odo_km
        return {
            "vin": self.vin, "ts": iso(ts), "speed_kmph": round(self.speed, 2),
            "odometer": round(odo, 1), "odometer_unit": "mi" if self.miles else "km",
            "lat": round(self.lat, 6), "lon": round(self.lon, 6), "heading": self.rnd.randint(0, 359),
            "soc_pct": round(self.rnd.uniform(20, 95), 2) if self.oem == "OEM-D" else None,
            "ignition": True, "source_oem": self.oem,
        }


def malformed_payloads(v, now):
    good = v.ping(now)
    bad = []
    for mutate, why in [
        (lambda p: p.update(vin="TOO-SHORT"), "vin length"),
        (lambda p: p.update(vin="1HGCM82633A0O4352"), "vin contains O"),
        (lambda p: p.update(speed_kmph=-5), "negative speed"),
        (lambda p: p.update(lat=123.0), "lat out of range"),
        (lambda p: p.update(ts=iso(now + dt.timedelta(hours=2))), "timestamp in future"),
        (lambda p: p.update(ts="27/10/2026 10:00"), "bad timestamp format"),
        (lambda p: p.pop("vin"), "missing vin"),
        (lambda p: p.update(odometer_unit="furlong"), "unknown unit"),
    ]:
        p = dict(good)
        mutate(p)
        bad.append((p, why))
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:8080")
    ap.add_argument("--mode", choices=MODES, default="normal")
    ap.add_argument("--vehicles", type=int, default=10)
    ap.add_argument("--count", type=int, default=20, help="pings per vehicle")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between rounds")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    rnd = random.Random(a.seed)
    fleet = [Vehicle(r, rnd) for r in load_vehicles(a.vehicles)]
    if not fleet:
        sys.exit("No vehicles found in data/vehicles.csv")
    codes = Counter()
    start = dt.datetime.now(dt.timezone.utc).replace(microsecond=0) - dt.timedelta(seconds=5 * a.count)

    if a.mode == "malformed":
        for p, why in malformed_payloads(fleet[0], start):
            code, body = post(a.api, p)
            codes[code] += 1
            mark = "OK " if code == 400 else "!! "
            print(f"{mark}{code}  {why:24s} {body}")
        print("\nExpected: every line 400. Anything else means validation is missing.")
    elif a.mode == "overspeed":
        # Use the last active vehicle (other modes use the first N) and end the run at "now", so pings
        # from an earlier normal/duplicate run can't be newer than this run and break it up.
        v = Vehicle(load_vehicles(10**6)[-1], rnd)
        speeds = [60, 105, 112, 118, 121, 125, 80]
        start = dt.datetime.now(dt.timezone.utc).replace(microsecond=0) - dt.timedelta(seconds=5 * (len(speeds) - 1))
        for i, s in enumerate(speeds):
            code, _ = post(a.api, v.ping(start + dt.timedelta(seconds=5 * i), speed=s))
            codes[code] += 1
            print(f"{v.vin} speed={s:>5} -> {code}")
        print(f"\nExpected: one OVERSPEED alert for {v.vin} starting at the 105 km/h ping.")
    else:
        events = []
        for i in range(a.count):
            ts = start + dt.timedelta(seconds=5 * i)
            for v in fleet:
                events.append(v.ping(ts))
        if a.mode == "out-of-order":
            rnd.shuffle(events)
        per_round = len(fleet)
        for i, e in enumerate(events):
            for _ in range(2 if a.mode == "duplicate" else 1):
                code, body = post(a.api, e)
                codes[code] += 1
                if code not in (200, 201, 202):
                    print(f"{code} {e['vin']} {body}")
            if a.mode != "burst" and (i + 1) % per_round == 0:
                time.sleep(a.interval)
        sent = sum(codes.values())
        print(f"mode={a.mode} vehicles={len(fleet)} pings/vehicle={a.count} requests={sent}")
        if a.mode == "duplicate":
            print(f"Expected telemetry rows added: {len(events)} (not {sent}) if ingest is idempotent.")
    print("HTTP status counts:", dict(codes))
    if codes.get(0):
        print("Status 0 = could not connect. Is your API running on", a.api, "?")


if __name__ == "__main__":
    main()
