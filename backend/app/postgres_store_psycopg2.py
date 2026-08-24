"""psycopg2-based Postgres/PostGIS adapter (drop-in for postgres_store.py).

Used on runtimes where psycopg (v3) lacks wheels (e.g. Python 3.14 on
Render) but psycopg2-binary provides cp314 wheels. Identical public
surface to PostgresStore: same __init__/active/load_* writes.
Enabled when DATABASE_URL or SUPABASE_DB_URL is set.
"""
from __future__ import annotations
import dataclasses
import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from .postgres_notes import database_url

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
except Exception as exc:  # pragma: no cover
    psycopg2 = None  # noqa: F401
    _PSYCOPG2_VERSION = f"IMPORT_FAILED ({exc})"
else:
    _PSYCOPG2_VERSION = getattr(psycopg2, "__version__", "?")
print(f"postgres_store_psycopg2: psycopg2={_PSYCOPG2_VERSION} DATABASE_URL={'set' if database_url() else 'NOT set'}")

from .store import SessionRow, StationRow, TripStopRow, AggregateRow, utcnow
from .trip_registry import build_trip_registry
from .ttl import DEFAULT_MAX_AGE_SECONDS


class PostgresStorePsycopg2:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.sessions: dict[str, SessionRow] = {}
        self.observations: list = []
        self.aggregates: dict[str, AggregateRow] = {}
        self.stations: dict[str, StationRow] = {}
        self.trip_stops: dict[str, list[TripStopRow]] = {}
        self.trip_metadata: dict[str, dict] = {}
        self.trips: dict[str, dict] = {}
        self.reports: list[dict] = []
        self.railway_segments: list[list[list[float]]] = []
        self.railway_segment_meta: list[dict] = []
        self._obs_limit = 2000
        self._report_limit = 500
        self._pool = None
        conninfo = database_url()
        if conninfo and psycopg2 is not None:
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(2, 10, conninfo)
                self._boot()
            except Exception as exc:  # noqa: BLE001
                print(f"postgres_store_psycopg2: connection failed ({exc}); falling back to memory store")
                self._pool = None

    @property
    def active(self) -> bool:
        return self._pool is not None

    @contextmanager
    def _conn(self):
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def _boot(self) -> None:
        self.load_reference_stations()
        self.load_railway_segments()
        self._load_live_state()

    def load_reference_stations(self, _path: str | None = None) -> int:
        if not self.active:
            return 0
        with self._lock:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, name_ar, name_fr, name_en, "
                        "ST_Y(location), ST_X(location), '[]'::text "
                        "FROM public.stations WHERE deleted_at IS NULL"
                    )
                    self.stations.clear()
                    for (sid, ar, fr, en, lat, lon, lines_j) in cur.fetchall():
                        self.stations[str(sid)] = StationRow(
                            id=str(sid), name_ar=ar, name_fr=fr, name_en=en,
                            latitude=lat, longitude=lon,
                            railway_line_ids=json.loads(lines_j),
                        )
                    return len(self.stations)

    def load_railway_segments(self, _path: str | None = None) -> int:
        with self._lock:
            if self.active:
                try:
                    with self._conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT id, line_id, ST_AsGeoJSON(geometry), "
                                "distance_meters, direction, 'UNKNOWN'::text AS source_kind "
                                "FROM public.railway_segments"
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
                                        "id": str(sid), "line_id": str(line_id),
                                        "distance_meters": dist,
                                        "direction": direction,
                                        "source_kind": source_kind,
                                    })
                                return len(rows)
                except Exception:  # noqa: BLE001
                    pass
            return 0

    def _load_live_state(self) -> None:
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
                        anonymous_monitor_id=anon, status=str(status),
                        started_at=started, ended_at=ended,
                        last_observation_at=last_obs,
                    )
                cur.execute(
                    "SELECT ts.trip_id, ts.station_id, s.name_ar, ts.sequence, "
                    "ST_Y(s.location), ST_X(s.location) "
                    "FROM public.trip_stops ts "
                    "JOIN public.stations s ON s.id = ts.station_id "
                    "ORDER BY ts.trip_id, ts.sequence"
                )
                for (trip_id, st_id, st_name, seq, lat, lon) in cur.fetchall():
                    self.trip_stops.setdefault(str(trip_id), []).append(
                        TripStopRow(
                            station_id=str(st_id), station_name=st_name,
                            sequence=int(seq), latitude=lat, longitude=lon,
                        )
                    )
                cur.execute(
                    "SELECT id, train_id, line_id, direction, scheduled_departure, "
                    "scheduled_arrival, status FROM public.trips "
                    "WHERE deleted_at IS NULL"
                )
                self.trip_metadata = {
                    str(trip_id): {
                        "train_id": train_id,
                        "line_id": line_id,
                        "direction": direction,
                        "scheduled_departure": scheduled_departure,
                        "scheduled_arrival": scheduled_arrival,
                        "status": status,
                    }
                    for (
                        trip_id, train_id, line_id, direction,
                        scheduled_departure, scheduled_arrival, status,
                    ) in cur.fetchall()
                }
                self.trips.clear()
                self.trips.update(build_trip_registry(self.trip_stops, self.trip_metadata))
                cur.execute(
                    "SELECT trip_id, train_id, ST_Y(location), ST_X(location), "
                    "estimated_speed_mps, heading_deg, confidence, confidence_score, "
                    "freshness, source_count, last_observed_at, last_estimated_at, truth, "
                    "next_station_id, next_station_name_ar, station_event, eta_station_id, "
                    "eta_min_sec, eta_max_sec, eta_confidence, wait_decision, wait_reason_ar "
                    "FROM public.aggregated_train_positions "
                    "WHERE truth IS NULL OR truth <> 'UNKNOWN'"
                )
                for (
                    trip_id, train_id, lat, lon, speed, heading, conf, conf_score,
                    fresh, sc, lob, last_est, truth, next_id, next_name, station_event,
                    eta_station, eta_min, eta_max, eta_conf, wait_dec, wait_reason,
                ) in cur.fetchall():
                    self.aggregates[str(trip_id)] = AggregateRow(
                        trip_id=str(trip_id), train_id=str(train_id),
                        latitude=lat, longitude=lon, speed_mps=speed,
                        heading_deg=heading, confidence=str(conf or "UNKNOWN"),
                        confidence_score=float(conf_score or 0.0),
                        freshness=str(fresh or "UNKNOWN"), source_count=int(sc or 0),
                        last_observed_at=lob or utcnow(),
                        last_estimated_at=last_est or lob or utcnow(),
                        truth=str(truth or "UNKNOWN"),
                        next_station_id=str(next_id) if next_id else None,
                        next_station_name_ar=next_name, station_event=station_event,
                        eta_station_id=str(eta_station) if eta_station else None,
                        eta_min_sec=eta_min, eta_max_sec=eta_max,
                        eta_confidence=eta_conf, wait_decision=wait_dec,
                        wait_reason_ar=wait_reason,
                    )

    def check_trip_train_reference(self, trip_id: str, train_id: str) -> str | None:
        """Read-only validation of canonical train/trip prerequisites."""
        if not self.active:
            return None
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM public.trains WHERE id = %s::uuid), "
                    "EXISTS (SELECT 1 FROM public.trips WHERE id = %s::uuid), "
                    "(SELECT train_id::text FROM public.trips WHERE id = %s::uuid)",
                    (train_id, trip_id, trip_id),
                )
                train_exists, trip_exists, trip_train_id = cur.fetchone()
        if not train_exists:
            return "unknown_train_reference"
        if not trip_exists:
            return "unknown_trip_reference"
        if str(trip_train_id) != str(train_id):
            return "trip_train_binding_mismatch"
        return None

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
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.gps_observations "
                    "(id, session_id, trip_id, train_id, location, accuracy_meters, "
                    "speed_mps, heading_deg, observed_at, is_valid, rejection_reason, "
                    "validation_score) "
                    "VALUES (%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (id) DO NOTHING",
                    (row.id, row.session_id, row.trip_id, row.train_id,
                     row.longitude, row.latitude, row.accuracy, row.speed,
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
                    "(trip_id, train_id, location, estimated_speed_mps, heading_deg, "
                    "confidence, confidence_score, freshness, truth, source_count, "
                    "next_station_id, next_station_name_ar, station_event, eta_station_id, "
                    "eta_min_sec, eta_max_sec, eta_confidence, wait_decision, wait_reason_ar, "
                    "last_observed_at, last_estimated_at, updated_at) "
                    "VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s,%s),4326), "
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, now()) "
                    "ON CONFLICT (trip_id) DO UPDATE SET "
                    "train_id=EXCLUDED.train_id, location=EXCLUDED.location, "
                    "estimated_speed_mps=EXCLUDED.estimated_speed_mps, heading_deg=EXCLUDED.heading_deg, "
                    "confidence=EXCLUDED.confidence, confidence_score=EXCLUDED.confidence_score, "
                    "freshness=EXCLUDED.freshness, truth=EXCLUDED.truth, "
                    "source_count=EXCLUDED.source_count, "
                    "next_station_id=EXCLUDED.next_station_id, "
                    "next_station_name_ar=EXCLUDED.next_station_name_ar, "
                    "station_event=EXCLUDED.station_event, "
                    "eta_station_id=EXCLUDED.eta_station_id, "
                    "eta_min_sec=EXCLUDED.eta_min_sec, eta_max_sec=EXCLUDED.eta_max_sec, "
                    "eta_confidence=EXCLUDED.eta_confidence, "
                    "wait_decision=EXCLUDED.wait_decision, "
                    "wait_reason_ar=EXCLUDED.wait_reason_ar, "
                    "last_observed_at=EXCLUDED.last_observed_at, last_estimated_at=EXCLUDED.last_estimated_at, "
                    "updated_at=now()",
                    (row.trip_id, row.train_id, row.longitude, row.latitude,
                     row.speed_mps, row.heading_deg, row.confidence, row.confidence_score,
                     row.freshness, row.truth, row.source_count, row.next_station_id,
                     row.next_station_name_ar, row.station_event, row.eta_station_id,
                     row.eta_min_sec, row.eta_max_sec, row.eta_confidence, row.wait_decision,
                     row.wait_reason_ar, row.last_observed_at, row.last_estimated_at),
                )

    def evict_stale_db(self, max_age: float = DEFAULT_MAX_AGE_SECONDS) -> list[str]:
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

    def load_trips_registry(self) -> int:
        """Rebuild public trip shells from canonical trip-stop rows."""
        if not self.active:
            return 0
        with self._lock:
            self.trips.clear()
            self.trips.update(build_trip_registry(self.trip_stops, self.trip_metadata))
            return len(self.trips)

    def load_trip_stops(self, _path: str | None = None) -> int:
        """Reload canonical trip stops and rebuild the public trip index."""
        if not self.active:
            return 0
        with self._lock:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT ts.trip_id, ts.station_id, s.name_ar, ts.sequence, "
                        "ST_Y(s.location), ST_X(s.location) "
                        "FROM public.trip_stops ts "
                        "JOIN public.stations s ON s.id = ts.station_id "
                        "ORDER BY ts.trip_id, ts.sequence"
                    )
                    self.trip_stops.clear()
                    for trip_id, station_id, name, sequence, lat, lon in cur.fetchall():
                        self.trip_stops.setdefault(str(trip_id), []).append(
                            TripStopRow(
                                station_id=str(station_id), station_name=name,
                                sequence=int(sequence), latitude=float(lat), longitude=float(lon),
                            )
                        )
                    cur.execute(
                        "SELECT id, train_id, line_id, direction, scheduled_departure, "
                        "scheduled_arrival, status FROM public.trips "
                        "WHERE deleted_at IS NULL"
                    )
                    self.trip_metadata = {
                        str(trip_id): {
                            "train_id": train_id,
                            "line_id": line_id,
                            "direction": direction,
                            "scheduled_departure": scheduled_departure,
                            "scheduled_arrival": scheduled_arrival,
                            "status": status,
                        }
                        for (
                            trip_id, train_id, line_id, direction,
                            scheduled_departure, scheduled_arrival, status,
                        ) in cur.fetchall()
                    }
                    total_stops = sum(len(rows) for rows in self.trip_stops.values())
                    self.trips.clear()
                    self.trips.update(build_trip_registry(self.trip_stops, self.trip_metadata))
                    return total_stops

    def save_trip_stops(self, trip_id: str, rows: list[TripStopRow]) -> None:
        if not self.active:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.trip_stops WHERE trip_id = %s", (trip_id,))
                for r in rows:
                    cur.execute(
                        "INSERT INTO public.trip_stops "
                        "(trip_id, station_id, sequence) VALUES (%s,%s,%s)",
                        (trip_id, r.station_id, r.sequence),
                    )

    def save_report(self, report: dict) -> bool:
        """Persist a community report using the canonical report contract."""
        if not self.active:
            return True
        with self._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        "INSERT INTO public.community_reports "
                        "(id, train_id, trip_id, station_id, report_type, description, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT (id) DO NOTHING",
                        (report.get("id"), report.get("train_id"),
                         report.get("trip_id"), report.get("station_id"),
                         report.get("report_type"), report.get("description"),
                         report.get("created_at")),
                    )
                    return True
                except Exception:  # noqa: BLE001
                    conn.rollback()
                    return False

    def trim_reports(self, limit: int = 500) -> int:
        if not self.active:
            return 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM public.community_reports")
                total = cur.fetchone()[0]
                if total <= limit:
                    return 0
                cur.execute(
                    "DELETE FROM public.community_reports WHERE id IN "
                    "(SELECT id FROM public.community_reports ORDER BY created_at ASC "
                    "LIMIT %s)",
                    (total - limit,),
                )
                return cur.rowcount
