"""
Train Tracking Algeria — Public API (MVP)

Pipeline:
  POST /observations → validate → aggregate → confidence → station → ETA → wait
  GET  /trips/{id}/live → public aggregated state only (no raw GPS list)

No fabricated live trains. Empty/null when no verified aggregate.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
import asyncio
import json
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.store import store, SessionRow, TripStopRow, utcnow
from app.pipeline import process_observation
from app.realtime import hub
from app.rate_limit import observation_limiter
from app.ttl import evict_stale, should_publish
from app.admin_health import health_snapshot

# Eager-load reference stations (startup event may be deferred under TestClient)
_CANDIDATES = [
    ROOT.parent / "data" / "reference",
    ROOT / "data" / "reference",
    ROOT.parent / "backend" / "data" / "reference",
    Path.cwd() / "data" / "reference",
    Path.cwd() / "backend" / "data" / "reference",
]
_REF = next((p / "stations_suburban_provisional.json" for p in _CANDIDATES
             if (p / "stations_suburban_provisional.json").exists()), None)
_SEG = next((p / "railway_segments_provisional.geojson" for p in _CANDIDATES
             if (p / "railway_segments_provisional.geojson").exists()), None)
_pg_active = getattr(store, "active", False)
if _REF is not None and not store.stations and not _pg_active:
    store.load_reference_stations(str(_REF))
if _SEG is not None and not store.railway_segments and not _pg_active:
    store.load_railway_segments_geojson(str(_SEG))

app = FastAPI(
    title="Train Tracking Algeria API",
    version="0.10.0",
    description="Community railway tracking — OBSERVED / ESTIMATED / UNKNOWN",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Schemas aligned with Android DTOs ───────────────────────────────────────

class ObservationIn(BaseModel):
    session_id: str
    trip_id: str
    train_id: str
    latitude: float
    longitude: float
    accuracy: float
    speed: float = 0.0
    heading: float = 0.0
    timestamp: int  # epoch millis (GPS event time)


class CreateMonitorSessionIn(BaseModel):
    trip_id: str
    train_id: str
    anonymous_monitor_id: str | None = None
    device_info: str | None = None
    app_version: str | None = None


class ReportIn(BaseModel):
    train_id: str
    trip_id: str | None = None
    station_id: str | None = None
    report_type: str = "OTHER"
    description: str | None = None


# ─── Startup: load reference stations (NOT live) ─────────────────────────────

@app.on_event("startup")
async def startup() -> None:
    try:
        hub.bind_loop(asyncio.get_running_loop())
    except RuntimeError:
        pass
    ref = _REF
    if ref is not None and not getattr(store, "active", False):
        n = store.load_reference_stations(str(ref))
        print(f"Loaded {n} REFERENCE stations from file (not live tracking)")
    elif getattr(store, "active", False):
        print(f"Loaded {len(store.stations)} REFERENCE stations from Postgres (not live tracking)")
    seg = _SEG
    if seg is not None and not getattr(store, "active", False):
        ns = store.load_railway_segments_geojson(str(seg))
        print(f"Loaded {ns} railway segment polylines from file")
    elif getattr(store, "active", False):
        ns = store.load_railway_segments()
        print(f"Loaded {ns} railway segment polylines from PostGIS")
    else:
        print("No reference station file found — stations list empty")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "truth_model": "OBSERVED|ESTIMATED|UNKNOWN"}


@app.get("/version")
def version() -> dict[str, str]:
    return {"api": "0.10.0", "pipeline": "validate-aggregate-confidence-station-eta-wait",
            "storage": "postgres-postgis" if getattr(store, "active", False) else "memory-mvp"}


@app.get("/admin/health")
def admin_health() -> dict:
    """Data-health snapshot for operators (no secrets)."""
    if getattr(store, "evict_stale_db", None):
        store.evict_stale_db()
    evict_stale(store)
    return health_snapshot(store)



@app.get("/admin/importprobe")
def admin_importprobe():
    """Attempt psycopg2 import inside the running process and report exact error."""
    import traceback
    result = {}
    try:
        import psycopg2
        result["import_ok"] = True
        result["version"] = psycopg2.__version__
    except Exception:
        result["import_ok"] = False
        result["error"] = traceback.format_exc()
    try:
        from app import store
        result["store_type"] = type(store.store).__name__
        result["store_active"] = getattr(store.store, "active", None)
    except Exception:
        result["store_error"] = traceback.format_exc()
    return result
@app.get("/admin/pipcheck")
def admin_pipcheck():
    """What psycopg-related packages are installed at runtime."""
    import subprocess
    try:
        out = subprocess.check_output(["pip3", "list"], text=True)
    except Exception as exc:
        out = str(exc)
    return {"installed": [l for l in out.splitlines() if "psycopg" in l.lower() or "Pyscopg" in l]}

@app.get("/admin/diag")
def admin_diag() -> dict:
    """Boot diagnostics: env vars (keys only), psycopg status, store active."""
    import os
    return {
        "database_url_set": bool(os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")),
        "supabase_pooler_url_set": bool(os.environ.get("SUPABASE_POOLER_URL")),
        "psycopg_version": getattr(store, "_PSYCOPG_VERSION", "?"),
        "psycopg2_version": getattr(store, "_PSYCOPG2_VERSION", "?"),
        "store_active": bool(getattr(store, "active", False)),
        "python": os.sys.version,
        "env_keys": sorted(k for k in os.environ if "URL" in k.upper()),
    }


@app.get("/admin/dashboard", response_class=None)
def admin_dashboard():
    from fastapi.responses import HTMLResponse
    snap = health_snapshot(store)
    rows = "".join(
        f"<tr><td>{t['trip_id']}</td><td>{t['train_id']}</td><td>{t['truth']}</td>"
        f"<td>{t['confidence']}</td><td>{t['freshness']}</td><td>{t['age_seconds']}s</td>"
        f"<td>{t['source_count']}</td></tr>"
        for t in snap.get("publishable_trips", [])
    )
    html = f"""<!DOCTYPE html>
<html lang="ar" dir="rtl"><head><meta charset="utf-8"/><title>Train Tracking — Health</title>
<style>
body{{font-family:system-ui,sans-serif;margin:24px;background:#0b1220;color:#e2e8f0}}
card{{display:inline-block;background:#1e293b;padding:16px 20px;margin:8px;border-radius:12px;min-width:140px}}
table{{border-collapse:collapse;width:100%;margin-top:16px}}
th,td{{border:1px solid #334155;padding:8px;text-align:right}}
th{{background:#1e293b}}
a{{color:#38bdf8}}
</style></head><body>
<h1>صحة البيانات — Train Tracking Algeria</h1>
<p>لا أسرار. الحالة العامة فقط. <a href="/docs">API docs</a></p>
<div>
<card>محطات مرجع<br><b>{snap['stations_reference_count']}</b></card>
<card>جلسات نشطة<br><b>{snap['active_sessions']}</b></card>
<card>ملاحظات مقبولة<br><b>{snap['observations_accepted']}</b></card>
<card>مرفوضة<br><b>{snap['observations_rejected']}</b></card>
<card>مجمّعات قابلة للنشر<br><b>{snap['aggregates_publishable']}</b></card>
<card>مقاطع سكة<br><b>{len(store.railway_segments)}</b></card>
</div>
<h2>رحلات منشورة</h2>
<table><tr><th>رحلة</th><th>قطار</th><th>حقيقة</th><th>ثقة</th><th>حداثة</th><th>عمر</th><th>مصادر</th></tr>
{rows or '<tr><td colspan="7">لا بيانات حالياً</td></tr>'}
</table>
<p style="color:#94a3b8;margin-top:24px">Generated: {snap['generated_at']}</p>
</body></html>"""
    return HTMLResponse(html)


# ─── Stations (reference network only) ───────────────────────────────────────

@app.get("/stations")
def get_stations() -> list[dict[str, Any]]:
    return [
        {
            "id": s.id,
            "name_ar": s.name_ar,
            "name_fr": s.name_fr,
            "name_en": s.name_en,
            "latitude": s.latitude,
            "longitude": s.longitude,
            "railway_line_ids": s.railway_line_ids,
        }
        for s in store.stations.values()
    ]


@app.get("/stations/{station_id}")
def get_station(station_id: str) -> dict[str, Any]:
    s = store.stations.get(station_id)
    if not s:
        raise HTTPException(404, "station_not_found")
    return {
        "id": s.id,
        "name_ar": s.name_ar,
        "name_fr": s.name_fr,
        "name_en": s.name_en,
        "latitude": s.latitude,
        "longitude": s.longitude,
        "railway_line_ids": s.railway_line_ids,
    }


# ─── Trips (empty until schedule/reference trips registered) ─────────────────

@app.get("/trips")
def get_trips() -> list[dict[str, Any]]:
    return list(store.trips.values())


@app.get("/trips/{trip_id}")
def get_trip(trip_id: str) -> dict[str, Any]:
    t = store.trips.get(trip_id)
    if not t:
        raise HTTPException(404, "trip_not_found")
    return t


@app.get("/trips/{trip_id}/stops")
def get_trip_stops(trip_id: str) -> list[dict[str, Any]]:
    stops = store.trip_stops.get(trip_id, [])
    return [
        {
            "station_id": s.station_id,
            "station_name": s.station_name,
            "sequence": s.sequence,
            "scheduled_arrival": None,
            "scheduled_departure": None,
        }
        for s in sorted(stops, key=lambda x: x.sequence)
    ]


def _live_from_aggregate(a) -> dict[str, Any]:
    """Public live payload — never raw observation list."""
    next_station = None
    if a.next_station_id and a.next_station_id in store.stations:
        st = store.stations[a.next_station_id]
        next_station = {
            "id": st.id,
            "name_ar": st.name_ar,
            "name_fr": st.name_fr,
            "name_en": st.name_en,
            "latitude": st.latitude,
            "longitude": st.longitude,
            "railway_line_ids": st.railway_line_ids,
        }
    elif a.next_station_id:
        next_station = {
            "id": a.next_station_id,
            "name_ar": a.next_station_name_ar or a.next_station_id,
            "name_fr": a.next_station_name_ar,
            "name_en": a.next_station_name_ar,
            "latitude": 0.0,
            "longitude": 0.0,
            "railway_line_ids": [],
        }

    eta = None
    if a.eta_station_id and a.eta_min_sec is not None:
        eta = {
            "station_id": a.eta_station_id,
            "estimated_arrival_min": max(0, a.eta_min_sec // 60),
            "estimated_arrival_max": max(0, (a.eta_max_sec or a.eta_min_sec) // 60),
            "confidence": a.eta_confidence or a.confidence,
        }

    pos = {"latitude": a.latitude, "longitude": a.longitude}
    return {
        "trip_id": a.trip_id,
        "train_id": a.train_id,
        "last_observed_position": pos if a.truth == "OBSERVED" else None,
        "estimated_position": pos if a.truth == "ESTIMATED" else (pos if a.truth == "OBSERVED" else None),
        "direction": None,
        "speed": a.speed_mps,
        "heading": a.heading_deg,
        "confidence": {
            "level": a.confidence,
            "source_count": a.source_count,
            "last_updated": a.last_observed_at.isoformat(),
        },
        "eta": eta,
        "last_update": a.last_observed_at.isoformat(),
        "status": "RUNNING" if a.truth != "UNKNOWN" else "UNKNOWN",
        "next_station": next_station,
        "source_count": a.source_count,
        # extensions (ignored by older clients)
        "truth": a.truth,
        "freshness": a.freshness,
        "station_event": a.station_event,
        "wait_decision": a.wait_decision,
        "wait_reason_ar": a.wait_reason_ar,
    }


@app.get("/trips/{trip_id}/live")
def get_trip_live(trip_id: str) -> dict[str, Any] | None:
    if getattr(store, "evict_stale_db", None):
        store.evict_stale_db()
    evict_stale(store)
    a = store.aggregates.get(trip_id)
    if not a or not should_publish(a):
        return None  # Android treats null as UNKNOWN — no fabricated train
    return _live_from_aggregate(a)


@app.get("/trains")
def get_trains() -> list[dict[str, Any]]:
    # Distinct train ids from active aggregates only
    seen = {}
    for a in store.aggregates.values():
        if a.truth == "UNKNOWN":
            continue
        seen[a.train_id] = {
            "id": a.train_id,
            "train_number": a.train_id,
            "line_id": None,
            "status": "RUNNING",
        }
    return list(seen.values())


@app.get("/trains/{train_id}")
def get_train(train_id: str) -> dict[str, Any]:
    for a in store.aggregates.values():
        if a.train_id == train_id and a.truth != "UNKNOWN":
            return {
                "id": train_id,
                "train_number": train_id,
                "line_id": None,
                "status": "RUNNING",
            }
    raise HTTPException(404, "train_not_found")


@app.get("/trains/{train_id}/live")
def get_train_live(train_id: str) -> dict[str, Any] | None:
    if getattr(store, "evict_stale_db", None):
        store.evict_stale_db()
    evict_stale(store)
    for a in store.aggregates.values():
        if a.train_id == train_id and should_publish(a):
            return _live_from_aggregate(a)
    return None


@app.get("/nearby-trains")
def nearby_trains(
    lat: float = Query(...),
    lon: float = Query(...),
    radius: float = Query(5000.0),
) -> list[dict[str, Any]]:
    """Only returns trains with publishable aggregate state."""
    from math import radians, sin, cos, atan2, sqrt

    def dist_m(a_lat, a_lon, b_lat, b_lon):
        r = 6371000.0
        p1, p2 = radians(a_lat), radians(b_lat)
        dp = radians(b_lat - a_lat)
        dl = radians(b_lon - a_lon)
        x = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
        return 2 * r * atan2(sqrt(x), sqrt(max(0.0, 1 - x)))

    if getattr(store, "evict_stale_db", None):
        store.evict_stale_db()
    evict_stale(store)
    out = []
    for a in store.aggregates.values():
        if not should_publish(a):
            continue
        d = dist_m(lat, lon, a.latitude, a.longitude)
        if d <= radius:
            out.append({
                "id": a.train_id,
                "train_number": a.train_id,
                "line_id": None,
                "status": "RUNNING",
                "direction": None,
                "origin": None,
                "destination": None,
                "next_station": a.next_station_name_ar,
                "confidence": a.confidence,
                "last_update": a.last_observed_at.isoformat(),
                "latitude": a.latitude,
                "longitude": a.longitude,
                "speed": a.speed_mps,
                "heading": a.heading_deg,
            })
    return out


# ─── Monitor sessions ────────────────────────────────────────────────────────

@app.post("/monitor-sessions")
def create_monitor_session(body: CreateMonitorSessionIn) -> dict[str, Any]:
    sid = str(uuid4())
    row = SessionRow(
        id=sid,
        trip_id=body.trip_id,
        train_id=body.train_id,
        anonymous_monitor_id=body.anonymous_monitor_id,
        status="STARTING",
        started_at=utcnow(),
    )
    store.sessions[sid] = row
    if getattr(store, "upsert_session", None):
        store.upsert_session(sid, row.trip_id, row.train_id, row.status,
                             row.started_at, row.anonymous_monitor_id)
    # Ensure trip shell exists for stops/ETA later
    if body.trip_id not in store.trips:
        store.trips[body.trip_id] = {
            "id": body.trip_id,
            "train_id": body.train_id,
            "line_id": "",
            "direction": "OUTBOUND",
            "scheduled_departure": None,
            "scheduled_arrival": None,
            "status": "RUNNING",
        }
    return {
        "id": row.id,
        "trip_id": row.trip_id,
        "train_id": row.train_id,
        "anonymous_monitor_id": row.anonymous_monitor_id,
        "status": row.status,
        "started_at": row.started_at.isoformat(),
        "ended_at": None,
        "last_observation_at": None,
    }


@app.post("/monitor-sessions/{session_id}/end")
def end_monitor_session(session_id: str) -> dict[str, Any]:
    row = store.sessions.get(session_id)
    if not row:
        raise HTTPException(404, "session_not_found")
    row.status = "ENDED"
    row.ended_at = utcnow()
    if getattr(store, "upsert_session", None):
        store.upsert_session(row.id, row.trip_id, row.train_id, row.status,
                             row.started_at, row.anonymous_monitor_id, row.ended_at,
                             row.last_observation_at)
    return {
        "id": row.id,
        "trip_id": row.trip_id,
        "train_id": row.train_id,
        "anonymous_monitor_id": row.anonymous_monitor_id,
        "status": row.status,
        "started_at": row.started_at.isoformat(),
        "ended_at": row.ended_at.isoformat() if row.ended_at else None,
        "last_observation_at": row.last_observation_at.isoformat() if row.last_observation_at else None,
    }


# ─── Observations (core pipeline) ────────────────────────────────────────────

@app.post("/observations")
def post_observation(body: ObservationIn) -> dict[str, Any]:
    key = f"{body.session_id}:{body.train_id}"
    if not observation_limiter.allow(key):
        raise HTTPException(429, "rate_limited")
    if body.session_id not in store.sessions:
        # Allow orphan observation only if session was optimistically local;
        # still process but mark session unknown binding.
        store.sessions[body.session_id] = SessionRow(
            id=body.session_id,
            trip_id=body.trip_id,
            train_id=body.train_id,
            anonymous_monitor_id=None,
            status="ACTIVE",
            started_at=utcnow(),
        )
    result = process_observation(
        store,
        observation_id=str(uuid4()),
        session_id=body.session_id,
        trip_id=body.trip_id,
        train_id=body.train_id,
        latitude=body.latitude,
        longitude=body.longitude,
        accuracy=body.accuracy,
        speed=body.speed,
        heading=body.heading if body.heading else None,
        timestamp_ms=body.timestamp,
    )
    if getattr(store, "upsert_session", None):
        store.upsert_session(
            body.session_id, body.trip_id, body.train_id,
            store.sessions.get(body.session_id).status if body.session_id in store.sessions else "ACTIVE",
            store.sessions.get(body.session_id).started_at if body.session_id in store.sessions else utcnow(),
            store.sessions.get(body.session_id).anonymous_monitor_id if body.session_id in store.sessions else None,
            None, utcnow(),
        )
    # Broadcast public aggregate only (never raw observation)
    agg = store.aggregates.get(body.trip_id)
    if agg and agg.truth != "UNKNOWN":
        payload = _live_from_aggregate(agg)
        hub.publish_public_state_threadsafe(payload)
    return result


@app.post("/observations/batch")
def post_observation_batch(body: list[ObservationIn]) -> list[dict[str, Any]]:
    return [post_observation(item) for item in body]


# ─── Admin helper: bind trip stops from reference stations ───────────────────

class TripStopsIn(BaseModel):
    trip_id: str
    station_ids: list[str] = Field(..., description="Ordered station ids along the trip")


@app.post("/admin/trip-stops")
def set_trip_stops(body: TripStopsIn) -> dict[str, Any]:
    """Register ordered stops for a trip so Station Detection + ETA can run."""
    rows: list[TripStopRow] = []
    for i, sid in enumerate(body.station_ids, start=1):
        st = store.stations.get(sid)
        if not st:
            raise HTTPException(400, f"unknown_station:{sid}")
        rows.append(
            TripStopRow(
                station_id=st.id,
                station_name=st.name_ar,
                sequence=i,
                latitude=st.latitude,
                longitude=st.longitude,
            )
        )
    store.trip_stops[body.trip_id] = rows
    if getattr(store, "save_trip_stops", None):
        store.save_trip_stops(body.trip_id, rows)
    return {"trip_id": body.trip_id, "stop_count": len(rows)}


# ─── Reports (evidence only — never auto-fabricated) ─────────────────────────

@app.post("/reports")
def submit_report(body: ReportIn) -> dict[str, str]:
    store.reports.append({
        "id": str(uuid4()),
        "train_id": body.train_id,
        "trip_id": body.trip_id,
        "station_id": body.station_id,
        "report_type": body.report_type,
        "description": body.description,
        "created_at": utcnow().isoformat(),
    })
    if getattr(store, "save_report", None):
        store.save_report(store.reports[-1])
    if getattr(store, "trim_windows", None):
        store.trim_windows()
    return {"status": "accepted"}


@app.get("/reports/train/{train_id}")
def reports_for_train(train_id: str) -> list[dict[str, Any]]:
    return [r for r in store.reports if r["train_id"] == train_id]




# ─── Map / GIS ───────────────────────────────────────────────────────────────

@app.get("/map/stations.geojson")
def stations_geojson() -> dict:
    """GeoJSON FeatureCollection of REFERENCE stations (not live trains)."""
    features = []
    for s in store.stations.values():
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [s.longitude, s.latitude]},
            "properties": {
                "id": s.id,
                "name_ar": s.name_ar,
                "name_fr": s.name_fr,
                "name_en": s.name_en,
                "line_ids": s.railway_line_ids,
                "source_kind": "REFERENCE_NETWORK",
            },
        })
    return {"type": "FeatureCollection", "features": features}




@app.get("/map/railway-segments.geojson")
def railway_segments_geojson() -> dict:
    """LineString features derived from reference stations per line.
    Production: replace body with PostGIS ST_AsGeoJSON(railway_segments.geometry).
    """
    by_line: dict[str, list] = {}
    for s in store.stations.values():
        for line in s.railway_line_ids:
            by_line.setdefault(line, []).append(s)
    features = []
    for line_id, sts in by_line.items():
        ordered = sorted(sts, key=lambda x: (x.longitude, -x.latitude))
        if len(ordered) < 2:
            continue
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[s.longitude, s.latitude] for s in ordered],
            },
            "properties": {
                "id": f"seg-{line_id}",
                "line_id": line_id,
                "source_kind": "REFERENCE_NETWORK_DERIVED",
                "note": "Chord between stations — not surveyed track centerline",
            },
        })
    return {"type": "FeatureCollection", "features": features}

@app.get("/map/network-lines")
def network_lines() -> list[dict]:
    """Provisional line polylines built from reference stations sharing a line id.
    Replace with PostGIS railway_segments.geometry when available.
    """
    by_line: dict[str, list] = {}
    for s in store.stations.values():
        for line in s.railway_line_ids:
            by_line.setdefault(line, []).append(s)
    out = []
    for line_id, sts in by_line.items():
        ordered = sorted(sts, key=lambda x: (x.longitude, -x.latitude))
        out.append({
            "id": line_id,
            "type": "LineString",
            "coordinates": [[s.longitude, s.latitude] for s in ordered],
            "source_kind": "REFERENCE_NETWORK_DERIVED",
        })
    return out

# Favorites stubs (local app is source of truth offline)
@app.get("/favorites")
def get_favorites() -> list:
    return []


@app.post("/favorites")
def add_favorite(body: dict[str, Any]) -> dict[str, Any]:
    return body


@app.delete("/favorites/{fav_id}")
def delete_favorite(fav_id: str) -> dict[str, str]:
    return {"status": "ok"}



# ─── Realtime (public train state only) ──────────────────────────────────────

@app.websocket("/ws/train-state")
async def ws_all_trains(websocket: WebSocket):
    """Subscribe to all public train_state updates."""
    await websocket.accept()
    q = await hub.subscribe(None)
    try:
        # snapshot current aggregates
        for a in list(store.aggregates.values()):
            if a.truth != "UNKNOWN":
                await websocket.send_json({"type": "train_state", "data": _live_from_aggregate(a)})
        while True:
            msg = await q.get()
            await websocket.send_text(msg)
    except WebSocketDisconnect:
        pass
    finally:
        await hub.unsubscribe(q)


@app.websocket("/ws/trips/{trip_id}")
async def ws_trip(websocket: WebSocket, trip_id: str):
    await websocket.accept()
    q = await hub.subscribe(trip_id)
    try:
        a = store.aggregates.get(trip_id)
        if a and a.truth != "UNKNOWN":
            await websocket.send_json({"type": "train_state", "data": _live_from_aggregate(a)})
        else:
            await websocket.send_json({"type": "train_state", "data": None})
        while True:
            msg = await q.get()
            await websocket.send_text(msg)
    except WebSocketDisconnect:
        pass
    finally:
        await hub.unsubscribe(q, trip_id)


@app.get("/stream/trips/{trip_id}")
async def sse_trip(trip_id: str):
    """Server-Sent Events fallback for public trip state."""
    async def gen():
        q = await hub.subscribe(trip_id)
        try:
            a = store.aggregates.get(trip_id)
            payload = _live_from_aggregate(a) if a and a.truth != "UNKNOWN" else None
            yield f"data: {json.dumps({'type': 'train_state', 'data': payload}, ensure_ascii=False)}\n\n"
            while True:
                msg = await q.get()
                yield f"data: {msg}\n\n"
        finally:
            await hub.unsubscribe(q, trip_id)

    return StreamingResponse(gen(), media_type="text/event-stream")
