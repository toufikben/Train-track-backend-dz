"""In-memory store for MVP pipeline testing without mandatory Postgres.
Swap to PostGIS later; public API shape stays the same.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any
from uuid import uuid4


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SessionRow:
    id: str
    trip_id: str | None
    train_id: str | None
    anonymous_monitor_id: str | None
    status: str
    started_at: datetime
    ended_at: datetime | None = None
    last_observation_at: datetime | None = None
    line_id: str | None = None
    direction: str | None = None


@dataclass
class ObservationRow:
    id: str
    session_id: str
    trip_id: str | None
    train_id: str | None
    latitude: float
    longitude: float
    accuracy: float
    speed: float | None
    heading: float | None
    observed_at: datetime
    accepted: bool
    rejection_reason: str | None
    validation_score: float
    line_id: str | None = None
    direction: str | None = None


@dataclass
class AggregateRow:
    trip_id: str
    train_id: str
    latitude: float
    longitude: float
    speed_mps: float | None
    heading_deg: float | None
    confidence: str
    confidence_score: float
    freshness: str
    source_count: int
    last_observed_at: datetime
    last_estimated_at: datetime
    truth: str  # OBSERVED / ESTIMATED / UNKNOWN
    next_station_id: str | None = None
    next_station_name_ar: str | None = None
    station_event: str | None = None
    eta_station_id: str | None = None
    eta_min_sec: int | None = None
    eta_max_sec: int | None = None
    eta_confidence: str | None = None
    wait_decision: str | None = None
    wait_reason_ar: str | None = None


@dataclass
class StationRow:
    id: str
    name_ar: str
    name_fr: str
    name_en: str
    latitude: float
    longitude: float
    railway_line_ids: list[str] = field(default_factory=list)


@dataclass
class TripStopRow:
    station_id: str
    station_name: str
    sequence: int
    latitude: float
    longitude: float


class MemoryStore:
    def __init__(self) -> None:
        self._lock = RLock()
        self.sessions: dict[str, SessionRow] = {}
        self.observations: list[ObservationRow] = []
        self.aggregates: dict[str, AggregateRow] = {}  # key: trip_id
        self.stations: dict[str, StationRow] = {}
        self.trip_stops: dict[str, list[TripStopRow]] = {}  # trip_id -> ordered stops
        self.trips: dict[str, dict[str, Any]] = {}
        self.reports: list[dict[str, Any]] = []
        self.railway_segments: list[list[list[float]]] = []  # [[lon,lat],...]
        self.railway_segment_meta: list[dict[str, Any]] = []

    def load_reference_stations(self, path: str) -> int:
        import json
        from pathlib import Path
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        assert data.get("not_live_tracking") is True
        n = 0
        with self._lock:
            for s in data["stations"]:
                self.stations[s["id"]] = StationRow(
                    id=s["id"],
                    name_ar=s["name_ar"],
                    name_fr=s["name_fr"],
                    name_en=s["name_en"],
                    latitude=s["lat"],
                    longitude=s["lon"],
                    railway_line_ids=s.get("lines", []),
                )
                n += 1
        return n

    def load_railway_segments_geojson(self, path: str) -> int:
        """Load LineString features. Coordinates are [lon, lat]."""
        import json
        from pathlib import Path
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        n = 0
        with self._lock:
            self.railway_segments.clear()
            self.railway_segment_meta.clear()
            for feat in data.get("features", []):
                geom = feat.get("geometry") or {}
                if geom.get("type") != "LineString":
                    continue
                coords = geom.get("coordinates") or []
                if len(coords) < 2:
                    continue
                self.railway_segments.append(coords)
                self.railway_segment_meta.append(feat.get("properties") or {})
                n += 1
        return n


store = MemoryStore()


# ---- Postgres/PostGIS switch: same public API as MemoryStore --------------
from .postgres_store import PostgresStore

_pg = PostgresStore()
if not _pg.active:
    # Fallback driver for runtimes where psycopg v3 has no wheels
    # (e.g. Python 3.14): psycopg2-binary provides cp314 wheels.
    try:
        from .postgres_store_psycopg2 import PostgresStorePsycopg2
        _pg2 = PostgresStorePsycopg2()
        if _pg2.active:
            _pg = _pg2
    except Exception:  # noqa: BLE001
        _pg2 = None
if _pg.active:
    store = _pg                    # write-through Postgres adapter
    store.load_reference_stations()
    print("postgres_store: ACTIVE (DATABASE_URL/SUPABASE_DB_URL set)")
else:
    # Third fallback: Supabase sql-proxy Edge Function (HTTPS-only path,
    # works even when direct Postgres TCP is unreachable, e.g. IPv6-only
    # Supabase endpoints from hosts like Render).
    try:
        from .postgres_store_sqlproxy import PostgresStoreSqlproxy
        _px = PostgresStoreSqlproxy()
        if _px.active:
            _pg = _px
            store = _px
            store.load_reference_stations()
            print("postgres_store: ACTIVE (sql-proxy Edge Function path)")
        else:
            _px = None
            print("postgres_store: INACTIVE — using in-memory store")
    except Exception:  # noqa: BLE001
        _px = None
        print("postgres_store: INACTIVE — using in-memory store")
