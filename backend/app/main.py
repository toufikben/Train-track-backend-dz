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
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse, HTMLResponse
import asyncio
import json
import os
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
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

def haversine(a, b):
    import math as _m
    lat1, lon1 = _m.radians(a[1]), _m.radians(a[0])
    lat2, lon2 = _m.radians(b[1]), _m.radians(b[0])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    x = _m.sin(dlat / 2) ** 2 + _m.cos(lat1) * _m.cos(lat2) * _m.sin(dlon / 2) ** 2
    return 2 * 6371000 * _m.asin(_m.sqrt(x))


app = FastAPI(
    title="Train Tracking Algeria API",
    version="0.10.0",
    description="Community railway tracking — OBSERVED / ESTIMATED / UNKNOWN",
)

_cors_origins = [
    origin.strip().rstrip("/")
    for origin in os.environ.get("CORS_ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Accept", "Authorization", "Content-Type", "Origin", "X-Requested-With"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)


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


def _validate_db_uuid_fields(*fields: tuple[str, str | None]) -> None:
    """Reject non-UUID identifiers only when a UUID-backed DB adapter is active.

    MemoryStore remains compatible with local/text fixtures. PostgreSQL paths do
    not silently normalize or invent identifiers; callers receive a stable 422.
    """
    if not getattr(store, "active", False):
        return
    for field_name, value in fields:
        if value is None:
            continue
        try:
            UUID(str(value))
        except (AttributeError, TypeError, ValueError):
            raise HTTPException(status_code=422, detail=f"invalid_{field_name}_uuid")


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
        n_trip_stop_rows = store.load_trip_stops()
        print(
            f"Loaded {n_trip_stop_rows} trip-stop rows for "
            f"{len(store.trips)} registered trips from Postgres"
        )
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
    if getattr(store, "evict_stale_live", None):
        store.evict_stale_live()
    evict_stale(store)
    return health_snapshot(store)


def _sqlproxy_live_probe() -> str:
    """One-off runtime probe of the sql-proxy Edge Function (for diagnostics)."""
    try:
        import os as _os
        import requests as _r
        url = _os.environ.get("SQLPROXY_URL") or ""
        key = _os.environ.get("SQLPROXY_KEY") or ""
        if not url or not key:
            return "missing_env"
        resp = _r.post(url, headers={"Content-Type": "application/json", "X-Api-Key": key},
                       json={"query": "SELECT 1 AS one"}, timeout=15)
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:200]
        return f"http={resp.status_code} body={body!r}"
    except Exception as exc:  # noqa: BLE001
        return f"ERROR: {exc}"


@app.get("/admin/refresh-reference")
def admin_refresh_reference() -> dict:
    """Reload stations + railway segments from Postgres into the live cache."""
    n_stations = 0
    n_segments = 0
    if getattr(store, "active", False):
        n_stations = store.load_reference_stations()
        n_segments = store.load_railway_segments()
        n_trips = store.load_trip_stops()
    return {"stations": n_stations, "railway_segments": n_segments, "trip_stops": n_trips}


@app.get("/admin/diag")
def admin_diag() -> dict:
    """Boot diagnostics: env vars (keys only), psycopg status, store active."""
    import os
    return {
        "database_url_set": bool(os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")),
        "supabase_pooler_url_set": bool(os.environ.get("SUPABASE_POOLER_URL")),
        "psycopg_version": getattr(store, "_PSYCOPG_VERSION", "?"),
        "sqlproxy_url_set": bool(os.environ.get("SQLPROXY_URL")),
        "sqlproxy_active": bool(getattr(store, "_active", None) and type(store).__name__ == "PostgresStoreSqlproxy"),
        "sqlproxy_key_len": len(os.environ.get("SQLPROXY_KEY") or ""),
        "sqlproxy_live_probe": _sqlproxy_live_probe(),
        "store_active": bool(getattr(store, "active", False)),
        "python": os.sys.version,
        "env_keys": sorted(k for k in os.environ if "URL" in k.upper()),
    }


@app.get("/admin/dashboard", response_class=HTMLResponse)
def admin_dashboard() -> HTMLResponse:
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
def get_trips(line_id: str | None = Query(None)) -> list[dict[str, Any]]:
    trips = list(store.trips.values())
    if line_id:
        trips = [t for t in trips if t.get("line_id") == line_id]
    return trips


@app.get("/trips/{trip_id}")
def get_trip(trip_id: str) -> dict[str, Any]:
    t = store.trips.get(trip_id)
    if not t:
        raise HTTPException(404, "trip_not_found")
    return t


@app.get("/trips/{trip_id}/stops")
def get_trip_stops(trip_id: str) -> list[dict[str, Any]]:
    if trip_id not in store.trips:
        raise HTTPException(404, "trip_not_found")
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
    if getattr(store, "evict_stale_live", None):
        store.evict_stale_live()
    evict_stale(store)
    a = store.aggregates.get(trip_id)
    if not a or not should_publish(a):
        return None  # Android treats null as UNKNOWN — no fabricated train
    return _live_from_aggregate(a)


def _line_order_rank(aggregates: dict) -> dict[str, int]:
    """Step 25 — relative ordering of running trains on the same line.

    Trains on the same line share the same ordered stop sequence, so a train
    that has progressed farther (or arrives sooner at its next stop) is the
    *leader* on that line. Ranking is derived purely from real live data:
    trains with a real ETA first (ascending), then trains without ETA by
    stop-sequence progress (most advanced first). Result is per-line: rank 1
    means first/leader among same-line trains; None when no line is known.
    """
    indexed: list[tuple[str, dict]] = []
    for train_id, a in aggregates.items():
        if a.truth == "UNKNOWN":
            continue
        trip_meta = store.trips.get(a.train_id, {})
        line_id = trip_meta.get("line_id")
        eta_sec = a.eta_min_sec if a.eta_station_id and a.eta_min_sec is not None else None
        progress = 0
        stops = store.trip_stops.get(a.trip_id, [])
        if stops:
            seqs = [s.sequence for s in stops if s.station_id == a.next_station_id] if a.next_station_id else []
            progress = max(seqs) if seqs else 0
        indexed.append((train_id, {"line_id": line_id, "eta_sec": eta_sec, "progress": progress}))
    ranked: dict[str, int] = {}
    by_line: dict[str | None, list] = {}
    for tid, info in indexed:
        by_line.setdefault(info["line_id"], []).append((tid, info))
    for _line_id, members in by_line.items():
        with_eta = sorted([m for m in members if m[1]["eta_sec"] is not None], key=lambda m: m[1]["eta_sec"])
        without_eta = sorted([m for m in members if m[1]["eta_sec"] is None], key=lambda m: -m[1]["progress"])
        for i, (tid, _) in enumerate(with_eta + without_eta):
            ranked[tid] = i + 1
    return ranked


@app.get("/trains")
def get_trains() -> list[dict[str, Any]]:
    # Distinct train ids from active aggregates only
    if getattr(store, "evict_stale_db", None):
        store.evict_stale_db()
    if getattr(store, "evict_stale_live", None):
        store.evict_stale_live()
    removed = evict_stale(store)
    # Step 26b — tell WS map clients a train left public view instantly
    for trip_id in removed:
        # Aggregate already removed; get the train id from trip meta if known
        trip_meta = store.trips.get(trip_id, {})
        hub.publish_train_gone_threadsafe(trip_meta.get("train_id") or trip_id, trip_id)
    # Distinct train ids from active aggregates only — full live payload
    # (position, ETA, next station) restored via _live_from_aggregate so the
    # map and alerts see real data. Kept per-train (not per-aggregate) and
    # ordered by last observation.
    seen = {}
    for a in store.aggregates.values():
        if a.truth == "UNKNOWN":
            continue
        payload = _live_from_aggregate(a)
        payload["id"] = a.train_id
        # Keep the freshest aggregate per train id
        if a.train_id not in seen or a.last_observed_at > seen[a.train_id]["_at"]:
            seen[a.train_id] = (payload, a.last_observed_at)
    # Step 25 — same-line relative order (1 = leader on the line)
    ranks = _line_order_rank(store.aggregates)
    out = []
    for payload, _at in sorted(seen.values(), key=lambda v: v[1], reverse=True):
        payload["line_order"] = ranks.get(payload["id"])
        out.append(payload)
    return out


@app.get("/trains/{train_id}")
def get_train(train_id: str) -> dict[str, Any]:
    for a in store.aggregates.values():
        if a.train_id == train_id and a.truth != "UNKNOWN":
            payload = _live_from_aggregate(a)
            payload["id"] = train_id
            payload["line_order"] = _line_order_rank(store.aggregates).get(train_id)
            return payload
    raise HTTPException(404, "train_not_found")


@app.get("/trains/{train_id}/live")
def get_train_live(train_id: str) -> dict[str, Any] | None:
    if getattr(store, "evict_stale_db", None):
        store.evict_stale_db()
    if getattr(store, "evict_stale_live", None):
        store.evict_stale_live()
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
    if getattr(store, "evict_stale_live", None):
        store.evict_stale_live()
    removed = evict_stale(store)
    for trip_id in removed:
        trip_meta = store.trips.get(trip_id, {})
        hub.publish_train_gone_threadsafe(trip_meta.get("train_id") or trip_id, trip_id)
    out = []
    for a in store.aggregates.values():
        if not should_publish(a):
            continue
        d = dist_m(lat, lon, a.latitude, a.longitude)
        if d <= radius:
            entry = _live_from_aggregate(a)
            entry["id"] = a.train_id
            entry["line_id"] = store.trips.get(a.train_id, {}).get("line_id")
            out.append(entry)
    # Step 25 — same-line relative order among nearby trains
    ranks = _line_order_rank(store.aggregates)
    for entry in out:
        entry["line_order"] = ranks.get(entry["id"])
    return out


# ─── Monitor sessions ────────────────────────────────────────────────────────

@app.post("/monitor-sessions")
def create_monitor_session(body: CreateMonitorSessionIn) -> dict[str, Any]:
    _validate_db_uuid_fields(("trip_id", body.trip_id), ("train_id", body.train_id))
    _validate_trip_train_reference(body.trip_id, body.train_id)
    sid = str(uuid4())
    row = SessionRow(
        id=sid,
        trip_id=body.trip_id,
        train_id=body.train_id,
        anonymous_monitor_id=body.anonymous_monitor_id,
        status="STARTING",
        started_at=utcnow(),
    )
    # Persist first. A failed DB write must not leave a phantom cache row.
    try:
        if getattr(store, "upsert_session", None):
            store.upsert_session(sid, row.trip_id, row.train_id, row.status,
                                 row.started_at, row.anonymous_monitor_id)
    except Exception as exc:  # noqa: BLE001
        print(f"monitor session persistence failed: {type(exc).__name__}")
        raise HTTPException(status_code=503, detail="storage_unavailable") from None
    store.sessions[sid] = row
    # MemoryStore-only shell behavior is retained for local development. A
    # DB-backed adapter must never invent a trip absent from canonical tables.
    if not getattr(store, "active", False) and body.trip_id not in store.trips:
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

def _assert_observation_binding(body: ObservationIn) -> None:
    """Reject a known session when its trip/train identity is contradicted.

    Unknown sessions intentionally keep the existing orphan-session behavior in
    this narrow patch; a separate policy decision is required to reject them.
    """
    session = store.sessions.get(body.session_id)
    if session is None:
        return
    if session.trip_id != body.trip_id or session.train_id != body.train_id:
        raise HTTPException(status_code=409, detail="session_binding_mismatch")


def _validate_trip_train_reference(trip_id: str, train_id: str) -> None:
    """Validate DB-backed references before creating any session cache entry."""
    checker = getattr(store, "check_trip_train_reference", None)
    if not getattr(store, "active", False) or checker is None:
        return
    try:
        error_code = checker(trip_id, train_id)
    except Exception as exc:  # noqa: BLE001
        # Keep SQL/DSN details out of the public API and logs.
        print(f"monitor session reference check failed: {type(exc).__name__}")
        raise HTTPException(status_code=503, detail="storage_unavailable") from None
    if error_code:
        raise HTTPException(status_code=409, detail=error_code)


@app.post("/observations")
def post_observation(body: ObservationIn) -> dict[str, Any]:
    _validate_db_uuid_fields(
        ("session_id", body.session_id), ("trip_id", body.trip_id), ("train_id", body.train_id)
    )
    _validate_trip_train_reference(body.trip_id, body.train_id)
    _assert_observation_binding(body)
    if getattr(store, "active", False) and body.session_id not in store.sessions:
        raise HTTPException(status_code=409, detail="session_not_found")
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
    _validate_db_uuid_fields(
        ("trip_id", body.trip_id),
        *( ("station_id", sid) for sid in body.station_ids )
    )
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
    _validate_db_uuid_fields(
        ("train_id", body.train_id), ("trip_id", body.trip_id), ("station_id", body.station_id)
    )
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
    """LineString features per suburban line.
    Real surveyed OSM track geometry (railway_segments table) is used when
    available; otherwise falls back to chords between reference stations.
    """
    # real surveyed geometry from the database
    by_line: dict[str, list] = {}
    if getattr(store, "railway_segments", None):
        for coords, meta in zip(store.railway_segments, store.railway_segment_meta):
            if coords and len(coords) >= 2:
                by_line.setdefault(str(meta.get("line_id", "")), []).append(coords)
    if by_line:
        features = []
        for line_id, segs in by_line.items():
            if not line_id:
                continue
            coords: list = []
            for seg in segs:
                if coords and seg and haversine(coords[-1], seg[0]) < 3000:
                    coords = coords + seg[1:]
                elif coords:
                    coords = coords + seg
                else:
                    coords = list(seg)
            if len(coords) >= 2:
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": coords},
                    "properties": {
                        "id": f"seg-{line_id}",
                        "line_id": line_id,
                        "source_kind": "OSM_REVIEWED",
                        "distance_meters": round(sum(
                            haversine(coords[i], coords[i + 1]) for i in range(len(coords) - 1)), 0),
                    },
                })
        return {"type": "FeatureCollection", "features": features}
    # fallback: chord between reference stations (not surveyed track)
    by_station_line: dict[str, list] = {}
    for s in store.stations.values():
        for line in s.railway_line_ids:
            by_station_line.setdefault(line, []).append(s)
    features = []
    for line_id, sts in by_station_line.items():
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
    """Line polylines per suburban line — real OSM track geometry when
    available, otherwise chords between reference stations.
    """
    by_line: dict[str, list] = {}
    if getattr(store, "railway_segments", None):
        for coords, meta in zip(store.railway_segments, store.railway_segment_meta):
            if coords and len(coords) >= 2:
                by_line.setdefault(str(meta.get("line_id", "")), []).append(coords)
    if by_line:
        out = []
        for line_id, segs in by_line.items():
            if not line_id:
                continue
            coords: list = []
            for seg in segs:
                if coords and seg and haversine(coords[-1], seg[0]) < 3000:
                    coords = coords + seg[1:]
                elif coords:
                    coords = coords + seg
                else:
                    coords = list(seg)
            if len(coords) >= 2:
                out.append({
                    "id": line_id,
                    "type": "LineString",
                    "coordinates": coords,
                    "source_kind": "OSM_REVIEWED",
                })
        return out
    by_station_line: dict[str, list] = {}
    for s in store.stations.values():
        for line in s.railway_line_ids:
            by_station_line.setdefault(line, []).append(s)
    out = []
    for line_id, sts in by_station_line.items():
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
