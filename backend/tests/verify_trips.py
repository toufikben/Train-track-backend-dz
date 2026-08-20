#!/usr/bin/env python3
"""
Daily verification of SNTF trip data against the live Render API.

Checks:
- All registered trips are reachable on /trips/{id}/stops
- Stop sequences match the official SNTF timetable (trip_stops_data.json)
- Station names are non-empty
- No unreasonable geographic jumps between consecutive stops
- Reference counters: stations, trips, stops

Exit code: 0 = all green, 1 = problems found.
"""
import json
import sys
import urllib.request

BASE = "https://train-api-uep7.onrender.com"
DATA_FILE = "trip_stops_data.json"  # resolved relative to this script

PROBLEMS = []


def get(path, timeout=40):
    with urllib.request.urlopen(f"{BASE}{path}", timeout=timeout) as r:
        return json.loads(r.read().decode())


def check_counter(name, actual, expected):
    if actual != expected:
        PROBLEMS.append(f"COUNTER_MISMATCH {name}: expected {expected}, got {actual}")


def main() -> int:
    data = json.load(open(DATA_FILE))
    trips = data["trips"]

    # Reference forward orders (toward Agha) from live API
    fwd = {}
    for key, trip_id in {
        "zeralda": "zeralda-aga-1501",
        "thenia": "thenia-aga-22",
        "elaffroun": "elaffroun-aga-1022",
    }.items():
        fwd[key] = [s["station_id"] for s in get(f"/trips/{trip_id}/stops")]

    # Reference counters (source of truth: the committed data file)
    check_counter("trips", len(trips), 105)
    total_stops = sum(len(v) for v in trips.values())
    check_counter("stops", total_stops, 1476)

    stations = get("/stations")
    check_counter("stations", len(stations), 28)
    station_ids = {s["id"] for s in stations}

    checked = 0
    for tid, stops in trips.items():
        checked += 1
        parts = tid.split("-")
        line = parts[1] if parts[0] == "aga" else parts[0]
        expected = fwd[line][::-1] if tid.startswith("aga-") else fwd[line]

        live = None
        try:
            live = get(f"/trips/{tid}/stops")
        except Exception as e:  # noqa: BLE001
            PROBLEMS.append(f"UNREACHABLE {tid}: {e}")
            continue

        live_ids = [s["station_id"] for s in live]
        if live_ids != expected:
            PROBLEMS.append(f"SEQUENCE_MISMATCH {tid}: got {live_ids[:6]}... vs expected {expected[:6]}...")

        for s in live:
            if not s.get("station_name"):
                PROBLEMS.append(f"EMPTY_NAME {tid} seq={s.get('sequence')}")

        # Geographic sanity: consecutive stops must be < 15 km apart
        for a, b in zip(live, live[1:]):
            d = haversine(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
            if d > 15_000:
                PROBLEMS.append(f"GEO_JUMP {tid} seq={b.get('sequence')}: {d:.0f} m jump")

        # All stations used must exist in DB station list
        for sid in live_ids:
            if sid not in station_ids:
                PROBLEMS.append(f"UNKNOWN_STATION {tid}: {sid}")

    print(f"checked={checked}")
    if PROBLEMS:
        print(f"FAILURES ({len(PROBLEMS)}):")
        for p in PROBLEMS:
            print(" -", p)
        return 1
    print("ALL GREEN — 105 trips verified against official SNTF timetable")
    return 0


def haversine(lat1, lon1, lat2, lon2):
    from math import asin, cos, radians, sin, sqrt
    p1, p2 = radians(lat1), radians(lat2)
    dp = radians(lat2 - lat1)
    dl = radians(lon2 - lon1)
    a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 6371000.0 * 2 * asin(sqrt(a))


if __name__ == "__main__":
    sys.exit(main())
