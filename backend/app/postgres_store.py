"""Postgres/PostGIS adapter implementing the same public API as MemoryStore.

Enabled when DATABASE_URL or SUPABASE_DB_URL is set. All writes go through
the service-role connection so public (anon) RLS policies never apply to the
backend. Reads for the public API use the same connection (Postgres adapter
reads the same tables the backend wrote; public RLS only affects the
Supabase REST/GraphQL gateway, not direct SQL).

Reference stations and railway segments are loaded from Postgres at startup
(via /stations /map endpoints) instead of the local JSON/GeoJSON files.
"""
from __future__ import annotations
import dataclasses
import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from .postgres_notes import database_url

try:
    import psycopg  # noqa: F401
    from psycopg_pool import ConnectionPool
    _PSYCOPG_VERSION = getattr(psycopg, "__version__", "?")
except Exception as exc:  # pragma: no cover
    psycopg = None  # adapter inactive without psycopg
    ConnectionPool = None
    _PSYCOPG_VERSION = f"IMPORT_FAILED ({exc})"
print(f"postgres_store: psycopg={_PSYCOPG_VERSION} DATABASE_URL={'set' if database_url() else 'NOT set'}")

from .store import SessionRow, StationRow, TripStopRow, AggregateRow, utcnow
from .ttl import DEFAULT_MAX_AGE_SECONDS


class PostgresStore:
    """Drop-in replacement surface for the parts of MemoryStore used by
    pipeline.py and main.py.

    MemoryStore public surface kept identical:
      store.sessions      dict[str, SessionRow]
      store.observations  list[ObservationRow]   (bounded in-memory window)
      store.aggregates    dict[str, AggregateRow] (write-through cache)
      store.stations      dict[str, StationRow]   (read-only reference)
      store.trip_stops    dict[str, list[TripStopRow]] (in-memory cache)
      store.trips         dict[str, dict]         (in-memory cache)
      store.reports       list[dict]              (write-through, bounded window)
      store.railway_segments    list[[lon,lat]]   (from PostGIS)
      store.railway_segment_meta list[dict]       (from PostGIS)
      store.load_reference_stations(path) -> int
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.sessions: dict[str, SessionRow] = {}
        self.observations: list = []
        self.aggregates: dict[str, AggregateRow] = {}
        self.stations: dict[str, StationRow] = {}
        self.trip_stops: dict[str, list[TripStopRow]] = {}
        self.trips: dict[str, dict] = {}
        self.reports: list[dict] = []
        self.railway_segments: list[list[list[float]]] = []
        self.railway_segment_meta: list[dict] = []
        self._obs_limit = 2000
        self._report_limit = 500
        self._pool: object | None = None
        self._conninfo = database_url()
        if self._conninfo and psycopg is not None:
            try:
                self._pool = ConnectionPool(
                    self._conninfo, min_size=2, max_size=10, open=False
                )
                self._pool.open()
                self._boot()
            except Exception as exc:  # noqa: BLE001
                print(f"postgres_store: connection failed ({exc}); falling back to memory store")
                self._pool = None

    @property
    def active(self) -> bool:
        return self._pool is not None

    @contextmanager
    def _conn(self):
        # pool.connection() returns a context-managed checked connection
        with self._pool.connection() as conn:
            yield conn

    # ---------- boot: load reference data from Postgres ----------

    def _boot(self) -> None:
        self.load_reference_stations()
        self.load_railway_segments()
        self._load_live_state()

    def load_reference_stations(self, _path: str | None = None) -> int:
        """Load stations from public.stations. _path ignored (kept for
        MemoryStore compatibility)."""
        if not self.active:
            return 0
        with self._lock:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, name_ar, name_fr, name_en, "
                        "ST_Y(location), ST_X(location), "
                        "COALESCE(railway_line_ids, '[]'::jsonb)::text "
                        "FROM public.stations WHERE deleted_at IS NULL"
                    )
                    self.stations.clear()
                    for (sid, ar, fr, en, lat, lon, lines_j) in cur.fetchall():
                        self.stations[str(sid)] = StationRow(
                            id=str(sid),
                            name_ar=ar,
                            name_fr=fr,
                            name_en=en,
                            latitude=lat,
                            longitude=lon,
                            railway_line_ids=json.loads(lines_j),
                        )
                    return len(self.stations)

    def load_railway_segments(self, _path: str | None = None) -> int:
        """Load PostGIS LineStrings from public.railway_segments.
        Falls back to local GeoJSON if the table is empty (OSM extraction
        not yet performed)."""
        with self._lock:
            if self.active:
                try:
                    with self._conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT id, line_id, ST_AsGeoJSON(geometry), distance_meters, direction, source_kind "
                                "FROM public.railway_segments WHERE deleted_at IS NULL"
                            )
                            rows = cur.fetchall()
                            if rows:
                                self.railway_segments.clear()
                                self.railway_segment_meta.clear()
                                for (sid, line_id, geojson, dist, direction, source_kind) in rows:
                                    geom = json.loads(geojson)
                                    coords = geom.get("coordinates") or []
                                    self.railway_segments.append(coords)
                                    self.railway_segment_meta.append({
                                        "id": str(sid),
                                        "line_id": str(line_id),
                                        "distance_meters": dist,
                                        "direction": direction,
                                        "source_kind": source_kind,
                                    })
                                return len(rows)
                except Exception:  # noqa: BLE001
                    pass
            return 0

    def _load_live_state(self) -> None:
        """Rehydrate session/trip/stop caches from Postgres so a restarted
        backend does not lose in-flight monitoring context."""
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, trip_id, train_id, anonymous_monitor_id, status, "
                    "started_at, last_observation_at, ended_at FROM public.monitor_sessions"
                )
                for (sid, trip_id, train_id, anon, status, started, last_obs, ended) in cur.fetchall():
                    self.sessions[str(sid)] = SessionRow(
                        id=str(sid), trip_id=str(trip_id), train_id=str(train_id),
                        anonymous_monitor_id=anon,
                        status=str(status), started_at=started,
                        ended_at=ended,
                        last_observation_at=last_obs,
                    )
                cur.execute(
                    "SELECT trip_id, station_id, station_name, sequence, latitude, longitude "
                    "FROM public.trip_stops ORDER BY trip_id, sequence"
                )
                for (trip_id, st_id, st_name, seq, lat, lon) in cur.fetchall():
                    self.trip_stops.setdefault(str(trip_id), []).append(
                        TripStopRow(
                            station_id=str(st_id), station_name=st_name,
                            sequence=int(seq), latitude=lat, longitude=lon,
                        )
                    )
                cur.execute(
                    "SELECT trip_id, train_id, ST_Y(location), ST_X(location), "
                    "truth, confidence, freshness, source_count, last_observed_at, updated_at "
                    "FROM public.aggregated_train_positions WHERE truth <> 'UNKNOWN'"
                )
                for (trip_id, train_id, lat, lon, truth, conf, fresh, sc, lob, upd) in cur.fetchall():
                    self.aggregates[str(trip_id)] = AggregateRow(
                        trip_id=str(trip_id), train_id=str(train_id),
                        latitude=lat, longitude=lon,
                        truth=str(truth), confidence=str(conf), freshness=str(fresh),
                        source_count=int(sc),
                        last_observed_at=lob, last_estimated_at=upd or lob,
                    )

    # ---------- pipeline writes ----------

    def upsert_session(self, session_id: str, trip_id: str, train_id: str,
                       status: str, started_at: datetime,
                       anonymous_monitor_id: str | None = None,
                       ended_at: datetime | None = None,
                       last_observation_at: datetime | None = None) -> None:
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.monitor_sessions "
                    "(id, trip_id, train_id, anonymous_monitor_id, status, started_at, "
                    "last_observation_at, ended_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "status=EXCLUDED.status, last_observation_at=EXCLUDED.last_observation_at, "
                    "ended_at=EXCLUDED.ended_at",
                    (session_id, trip_id, train_id, anonymous_monitor_id, status,
                     started_at, last_observation_at, ended_at),
                )

    def insert_observation(self, row: "object") -> None:
        """row: ObservationRow from pipeline. Write-through + bounded window."""
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.gps_observations "
                    "(id, session_id, trip_id, train_id, latitude, longitude, "
                    "accuracy_m, speed_mps, heading_deg, observed_at, "
                    "accepted, rejection_reason, validation_score) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (id) DO NOTHING",
                    (row.id, row.session_id, row.trip_id, row.train_id,
                     row.latitude, row.longitude, row.accuracy, row.speed,
                     row.heading, row.observed_at, row.accepted,
                     row.rejection_reason, row.validation_score),
                )

    def upsert_aggregate(self, row: AggregateRow | None, trip_id: str) -> None:
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                if row is None:
                    cur.execute(
                        "DELETE FROM public.aggregated_train_positions WHERE trip_id = %s",
                        (trip_id,),
                    )
                    return
                cur.execute(
                    "INSERT INTO public.aggregated_train_positions "
                    "(trip_id, train_id, latitude, longitude, truth, confidence, freshness, "
                    "source_count, next_station_id, next_station_name_ar, station_event, "
                    "eta_station_id, eta_min_sec, eta_max_sec, eta_confidence, "
                    "wait_decision, wait_reason_ar, last_observed_at, updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()) "
                    "ON CONFLICT (trip_id) DO UPDATE SET "
                    "train_id=EXCLUDED.train_id, latitude=EXCLUDED.latitude, "
                    "longitude=EXCLUDED.longitude, truth=EXCLUDED.truth, "
                    "confidence=EXCLUDED.confidence, freshness=EXCLUDED.freshness, "
                    "source_count=EXCLUDED.source_count, "
                    "next_station_id=EXCLUDED.next_station_id, "
                    "next_station_name_ar=EXCLUDED.next_station_name_ar, "
                    "station_event=EXCLUDED.station_event, "
                    "eta_station_id=EXCLUDED.eta_station_id, "
                    "eta_min_sec=EXCLUDED.eta_min_sec, eta_max_sec=EXCLUDED.eta_max_sec, "
                    "eta_confidence=EXCLUDED.eta_confidence, "
                    "wait_decision=EXCLUDED.wait_decision, "
                    "wait_reason_ar=EXCLUDED.wait_reason_ar, "
                    "last_observed_at=EXCLUDED.last_observed_at, updated_at=now()",
                    (row.trip_id, row.train_id, row.latitude, row.longitude,
                     row.truth, row.confidence, row.freshness, row.source_count,
                     row.next_station_id, row.next_station_name_ar, row.station_event,
                     row.eta_station_id, row.eta_min_sec, row.eta_max_sec,
                     row.eta_confidence, row.wait_decision, row.wait_reason_ar,
                     row.last_observed_at),
                )

    def evict_stale_db(self, max_age: float = DEFAULT_MAX_AGE_SECONDS) -> list[str]:
        """Delete publishable aggregates older than max_age from Postgres
        (mirrors ttl.evict_stale for the DB)."""
        removed: list[str] = []
        if not self.active:
            return removed
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.aggregated_train_positions "
                    "WHERE last_observed_at < now() - make_interval(secs => %s) "
                    "RETURNING trip_id",
                    (max_age,),
                )
                removed = [str(r[0]) for r in cur.fetchall()]
        with self._lock:
            for tid in removed:
                self.aggregates.pop(tid, None)
        return removed

    def save_trip_stops(self, trip_id: str, rows: list[TripStopRow]) -> None:
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.trip_stops WHERE trip_id = %s", (trip_id,))
                for r in rows:
                    cur.execute(
                        "INSERT INTO public.trip_stops "
                        "(trip_id, station_id, station_name, sequence, latitude, longitude) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (trip_id, r.station_id, r.station_name, r.sequence,
                         r.latitude, r.longitude),
                    )

    def save_report(self, rec: dict) -> None:
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.community_reports "
                    "(id, session_id, trip_id, train_id, event_kind, description_ar, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING",
                    (rec.get("id"), rec.get("session_id"), rec.get("trip_id"),
                     rec.get("train_id"), rec.get("report_type"),
                     rec.get("description"), rec.get("created_at")),
                )

    def trim_windows(self) -> None:
        """Bound in-memory windows to keep memory usage predictable."""
        with self._lock:
            if len(self.observations) > self._obs_limit:
                self.observations = self.observations[-self._obs_limit:]
            if len(self.reports) > self._report_limit:
                self.reports = self.reports[-self._report_limit:]
