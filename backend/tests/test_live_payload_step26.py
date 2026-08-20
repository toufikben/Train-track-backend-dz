"""Step 26 — public endpoints /trains, /trains/{id}, /nearby-trains must
return the full live payload (position + ETA + next station), not just id/
status. Verifies the fix for the missing position fields regression.
"""
from datetime import datetime, timezone, timedelta

from backend.app.main import store, _live_from_aggregate

# Build a fake publishable aggregate
now = datetime.now(timezone.utc)
store.trips["aga-elaffroun-1025"] = {"line_id": "line-suburb-elaffroun"}
store.stations["st-elaffroun"] = type(
    "S", (), {
        "id": "st-elaffroun", "name_ar": "العفرون", "name_fr": "El-Affroun",
        "name_en": "El-Affroun", "latitude": 36.4500, "longitude": 2.8400,
        "railway_line_ids": ["line-suburb-elaffroun"],
    }
)()
agg = type(
    "A", (), {
        "train_id": "aga-elaffroun-1025", "trip_id": "aga-elaffroun-1025",
        "truth": "OBSERVED", "confidence": "HIGH", "source_count": 1,
        "last_observed_at": now, "latitude": 36.5000, "longitude": 3.0000,
        "speed_mps": 10.0, "heading_deg": 225.0,
        "eta_station_id": "st-elaffroun", "eta_min_sec": 420, "eta_max_sec": 480,
        "eta_confidence": "MEDIUM",
        "next_station_id": "st-elaffroun", "next_station_name_ar": "العفرون",
        "station_event": None, "wait_decision": None, "wait_reason_ar": None,
        "freshness": "FRESH",
    }
)()
store.aggregates["aga-elaffroun-1025"] = agg

checks = []

# 1. _live_from_aggregate carries the fields
live = _live_from_aggregate(agg)
for k in ("last_observed_position", "estimated_position", "eta", "next_station",
          "speed", "heading", "truth"):
    checks.append(("live_payload_has_" + k, live.get(k) is not None))

# 2. /trains returns full payload per train
from backend.app.main import get_trains, get_train, nearby_trains

trains = get_trains()
assert trains, "get_trains empty"
entry = next(e for e in trains if e["id"] == "aga-elaffroun-1025")
for k in ("last_observed_position", "estimated_position", "eta", "next_station",
          "line_order", "latitude" if False else "speed"):
    checks.append(("get_trains_has_" + k, entry.get(k) is not None))
checks.append(("get_trains_position_values",
               entry["last_observed_position"]["latitude"] == 36.5))
checks.append(("get_trains_eta_minutes", entry["eta"]["estimated_arrival_min"] == 7))
checks.append(("get_trains_next_station_name",
               entry["next_station"]["name_ar"] == "العفرون"))
checks.append(("get_trains_line_order", entry["line_order"] == 1))

# 3. /trains/{id} returns the same full payload
single = get_train("aga-elaffroun-1025")
checks.append(("get_train_payload", single["eta"] is not None
               and single["last_observed_position"] is not None))

# 4. unknown train still 404
try:
    get_train("does-not-exist")
    checks.append(("get_train_unknown_404", False))
except Exception:
    checks.append(("get_train_unknown_404", True))

# 5. nearby returns full payload
near = nearby_trains(lat=36.5, lon=3.0, radius=5000.0)
checks.append(("nearby_not_empty", bool(near)))
checks.append(("nearby_position", near[0]["last_observed_position"] is not None))
checks.append(("nearby_eta", near[0]["eta"] is not None))
checks.append(("nearby_line_order", near[0]["line_order"] is not None))

ok = True
for name, result in checks:
    if not result:
        ok = False
    print(f"  {name}: {'OK' if result else 'FAIL'}")
if ok:
    print("step26 live-payload tests OK")
else:
    raise SystemExit(1)
