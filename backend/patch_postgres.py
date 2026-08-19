#!/usr/bin/env python3
"""Integrate PostgresStore into the backend WITHOUT changing any existing
behavior: when DATABASE_URL/SUPABASE_DB_URL is unset the memory store
behaves exactly as before."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# ---- 1. store.py: expose a switchable `store` instance -------------------
store_path = ROOT / "app" / "store.py"
src = store_path.read_text()

if "PostgresStore" not in src:
    patch = """

# ---- Postgres/PostGIS switch: same public API as MemoryStore --------------
from .postgres_store import PostgresStore

_pg = PostgresStore()
if _pg.active:
    store = _pg                    # write-through Postgres adapter
    store.load_reference_stations()
    print("postgres_store: ACTIVE (DATABASE_URL/SUPABASE_DB_URL set)")
else:
    print("postgres_store: INACTIVE — using in-memory store")
"""
    # Append; keep original `store = MemoryStore()` for memory fallback usage
    with open(store_path, "w") as f:
        f.write(src + patch)
    print("store.py patched")
else:
    print("store.py already patched")

# ---- 2. main.py: wire session/observation/aggregate writes to Postgres ---
main_path = ROOT / "app" / "main.py"
src = main_path.read_text()

changes = []

def replace(old, new, where=src):
    global src
    if old in src:
        src = src.replace(old, new, 1)
        changes.append(True)
        return True
    changes.append(False)
    return False

# a) create_monitor_session -> write session to Postgres
replace(
    "    store.sessions[sid] = row\n",
    """    store.sessions[sid] = row
    if getattr(store, "upsert_session", None):
        store.upsert_session(sid, row.trip_id, row.train_id, row.status,
                             row.started_at, row.anonymous_monitor_id)
""",
)

# b) end_monitor_session -> update status in Postgres
replace(
    "    row.status = \"ENDED\"\n    row.ended_at = utcnow()\n",
    """    row.status = "ENDED"
    row.ended_at = utcnow()
    if getattr(store, "upsert_session", None):
        store.upsert_session(row.id, row.trip_id, row.train_id, row.status,
                             row.started_at, row.anonymous_monitor_id, row.ended_at,
                             row.last_observation_at)
""",
)

# c) post_observation: update session.last_observation_at + evict stale from DB
replace(
    "    result = process_observation(\n",
    """    result = process_observation(\n""",
)  # no-op marker
# mark session last observation in DB
old = """    # Broadcast public aggregate only (never raw observation)
    agg = store.aggregates.get(body.trip_id)"""
new = """    if getattr(store, "upsert_session", None):
        store.upsert_session(
            body.session_id, body.trip_id, body.train_id,
            store.sessions.get(body.session_id).status if body.session_id in store.sessions else "ACTIVE",
            store.sessions.get(body.session_id).started_at if body.session_id in store.sessions else utcnow(),
            store.sessions.get(body.session_id).anonymous_monitor_id if body.session_id in store.sessions else None,
            None, utcnow(),
        )
    # Broadcast public aggregate only (never raw observation)
    agg = store.aggregates.get(body.trip_id)"""
replace(old, new)

# d) evict_stale in main: also evict from DB when adapter active
old = "@app.get(\"/trips/{trip_id}/live\")\ndef get_trip_live(trip_id: str) -> dict[str, Any] | None:\n    evict_stale(store)"
new = "@app.get(\"/trips/{trip_id}/live\")\ndef get_trip_live(trip_id: str) -> dict[str, Any] | None:\n    if getattr(store, \"evict_stale_db\", None):\n        store.evict_stale_db()\n    evict_stale(store)"
replace(old, new)

old = "@app.get(\"/trains/{train_id}/live\")\ndef get_train_live(train_id: str) -> dict[str, Any] | None:\n    evict_stale(store)"
new = "@app.get(\"/trains/{train_id}/live\")\ndef get_train_live(train_id: str) -> dict[str, Any] | None:\n    if getattr(store, \"evict_stale_db\", None):\n        store.evict_stale_db()\n    evict_stale(store)"
replace(old, new)

old = "    evict_stale(store)\n    out = []\n    for a in store.aggregates.values():\n        if not should_publish(a):"
new = """    if getattr(store, "evict_stale_db", None):
        store.evict_stale_db()
    evict_stale(store)
    out = []
    for a in store.aggregates.values():
        if not should_publish(a):"""
replace(old, new)

old = "@app.get(\"/admin/health\")\ndef admin_health() -> dict:\n    \"\"\"Data-health snapshot for operators (no secrets).\"\"\"\n    evict_stale(store)"
new = "@app.get(\"/admin/health\")\ndef admin_health() -> dict:\n    \"\"\"Data-health snapshot for operators (no secrets).\"\"\"\n    if getattr(store, \"evict_stale_db\", None):\n        store.evict_stale_db()\n    evict_stale(store)"
replace(old, new)

# e) set_trip_stops -> persist to Postgres
replace(
    "    store.trip_stops[body.trip_id] = rows\n",
    """    store.trip_stops[body.trip_id] = rows
    if getattr(store, "save_trip_stops", None):
        store.save_trip_stops(body.trip_id, rows)
""",
)

# f) submit_report -> persist to Postgres
replace(
    '    return {"status": "accepted"}\n\n\n',
    '    if getattr(store, "save_report", None):\n        store.save_report(store.reports[-1])\n    if getattr(store, "trim_windows", None):\n        store.trim_windows()\n    return {"status": "accepted"}\n\n\n',
)

# g) startup: skip file load when Postgres store active
old = """    ref = ROOT.parent / "data" / "reference" / "stations_suburban_provisional.json"
    if ref.exists():
        n = store.load_reference_stations(str(ref))
        print(f"Loaded {n} REFERENCE stations (not live tracking)")"""
new = """    ref = ROOT.parent / "data" / "reference" / "stations_suburban_provisional.json"
    if ref.exists() and not getattr(store, "active", False):
        n = store.load_reference_stations(str(ref))
        print(f"Loaded {n} REFERENCE stations from file (not live tracking)")
    elif getattr(store, "active", False):
        print(f"Loaded {len(store.stations)} REFERENCE stations from Postgres (not live tracking)")"""
replace(old, new)

old = """    seg = ROOT.parent / "data" / "reference" / "railway_segments_provisional.geojson"
    if seg.exists():
        ns = store.load_railway_segments_geojson(str(seg))
        print(f"Loaded {ns} railway segment polylines")
    else:
        print("No reference station file found — stations list empty")"""
new = """    seg = ROOT.parent / "data" / "reference" / "railway_segments_provisional.geojson"
    if seg.exists() and not getattr(store, "active", False):
        ns = store.load_railway_segments_geojson(str(seg))
        print(f"Loaded {ns} railway segment polylines from file")
    elif getattr(store, "active", False):
        ns = store.load_railway_segments()
        print(f"Loaded {ns} railway segment polylines from PostGIS")
    else:
        print("No reference station file found — stations list empty")"""
replace(old, new)

# h) observations list append: write through in pipeline — instead hook via
#    store.insert_observation. pipeline.py writes store.observations.append.
#    Patch store.py's MemoryStore? No — patch pipeline to also persist.
main_path.write_text(src)
print(f"main.py: {sum(changes)} replacements applied")

# ---- 3. pipeline.py: persist observations + aggregates to Postgres -------
pipe_path = ROOT / "app" / "pipeline.py"
src = pipe_path.read_text()

replace2 = []

old = "    store.observations.append(row)\n"
new = """    store.observations.append(row)
    if getattr(store, "insert_observation", None):
        store.insert_observation(row)
"""
if old in src:
    src = src.replace(old, new, 1)
    replace2.append(True)
else:
    replace2.append(False)

old = "    store.aggregates[trip_id] = public\n"
new = """    store.aggregates[trip_id] = public
    if getattr(store, "upsert_aggregate", None):
        store.upsert_aggregate(public, trip_id)
"""
if old in src:
    src = src.replace(old, new, 1)
    replace2.append(True)
else:
    replace2.append(False)

# delete from aggregates (UNKNOWN) -> remove from DB too
old = """        if trip_id in store.aggregates:
            del store.aggregates[trip_id]"""
new = """        if trip_id in store.aggregates:
            del store.aggregates[trip_id]
        if getattr(store, "upsert_aggregate", None):
            store.upsert_aggregate(None, trip_id)"""
if old in src:
    src = src.replace(old, new, 1)
    replace2.append(True)
else:
    replace2.append(False)

pipe_path.write_text(src)
print(f"pipeline.py: {sum(replace2)}/{len(replace2)} replacements applied")
print("Done.")
