"""Supabase sql-proxy (Edge Function) Postgres/PostGIS adapter.

Drop-in replacement surface for PostgresStore / PostgresStorePsycopg2.
Enabled when SQLPROXY_URL is set. All SQL goes over HTTPS to the
`sql-proxy` Edge Function deployed in the qitari-dz Supabase project,
which executes SELECT/INSERT/UPDATE/DELETE only (DDL is banned both in
the client and inside the DB function public.sql_proxy_execute, which
runs as SECURITY DEFINER so RLS does not block backend writes).

This bypasses the direct-TCP problem: Render (Oregon) cannot reach the
Supabase Postgres endpoint because it exposes IPv6 only, while the Edge
Function is reachable over normal HTTPS from anywhere.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone

from .store import SessionRow, StationRow, TripStopRow, AggregateRow, utcnow
from .ttl import DEFAULT_MAX_AGE_SECONDS

_SQLPROXY_URL = None
_SQLPROXY_KEY = None

try:
    import os
    _SQLPROXY_URL = (os.environ.get("SQLPROXY_URL") or "").strip()
    _SQLPROXY_KEY = (os.environ.get("SQLPROXY_KEY") or "").strip()
except Exception:  # pragma: no cover
    pass

try:
    import requests as _requests
except Exception:  # pragma: no cover
    _requests = None

_LINE_ID_MAP: dict[str, str] = {
    "zeralda-aga": "line-suburb-zeralda",
    "aga-zeralda": "line-suburb-zeralda",
    "thenia-aga": "line-suburb-thenia",
    "aga-thenia": "line-suburb-thenia",
    "aga-elaffroun": "line-suburb-elaffroun",
    "elaffroun-aga": "line-suburb-elaffroun",
}

_VERSION = "?"
if _requests is not None:
    _VERSION = getattr(_requests, "__version__", "?")
print(f"postgres_store_sqlproxy: requests={_VERSION} SQLPROXY_URL={'set' if _SQLPROXY_URL else 'NOT set'}")


def _run_sql(query: str, timeout: float = 15.0) -> list[dict]:
    """Execute one statement through the sql-proxy Edge Function."""
    resp = _requests.post(
        _SQLPROXY_URL,
        headers={"Content-Type": "application/json", "X-Api-Key": _SQLPROXY_KEY},
        json={"query": query},
        timeout=timeout,
    )
    data = resp.json()
    if resp.status_code != 200 or not isinstance(data.get("rows"), list):
        raise RuntimeError(f"sqlproxy {resp.status_code}: {data}")
    return data["rows"]


class PostgresStoreSqlproxy:
    """Write-through adapter backed by the sql-proxy Edge Function."""

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
        self._active = False
        if _SQLPROXY_URL and _SQLPROXY_KEY and _requests is not None:
            try:
                self._boot()
            except Exception as exc:  # noqa: BLE001
                print(f"postgres_store_sqlproxy: connection failed ({exc}); falling back to memory store")
                self._active = False

    @property
    def active(self) -> bool:
        return self._active

    def _boot(self) -> None:
        # Sanity probe: must be able to read stations from the real DB.
        rows = _run_sql("SELECT COUNT(*) AS n FROM public.stations")
        if not rows or rows[0].get("n") is None:
            raise RuntimeError("sqlproxy boot probe failed")
        self._active = True
        self.load_reference_stations()
        self.load_railway_segments()
        self._load_live_state()
        self.load_trip_stops()
        print(f"postgres_store_sqlproxy: ACTIVE (stations={len(self.stations)})")

    # ---------- reference data ----------

    def load_reference_stations(self, _path: str | None = None) -> int:
        if not self.active:
            return 0
        rows = _run_sql(
            "SELECT id, name_ar, name_fr, name_en, "
            "ST_Y(location) AS lat_y, ST_X(location) AS lon_x, "
            "COALESCE(railway_line_ids, '[]'::jsonb)::text AS station_lines "
            "FROM public.stations WHERE deleted_at IS NULL"
        )
        with self._lock:
            self.stations.clear()
            for r in rows:
                self.stations[str(r["id"])] = StationRow(
                    id=str(r["id"]),
                    name_ar=r["name_ar"], name_fr=r["name_fr"], name_en=r["name_en"],
                    latitude=float(r["lat_y"]), longitude=float(r["lon_x"]),
                    railway_line_ids=json.loads(r["station_lines"]),
                )
            return len(self.stations)

    def load_railway_segments(self, _path: str | None = None) -> int:
        with self._lock:
            if not self.active:
                return 0
            try:
                rows = _run_sql(
                    "SELECT id, line_id, ST_AsGeoJSON(geometry) AS geom_json, distance_meters, "
                    "direction, source_kind FROM public.railway_segments WHERE deleted_at IS NULL"
                )
                if not rows:
                    return 0
                self.railway_segments.clear()
                self.railway_segment_meta.clear()
                for r in rows:
                    geom = json.loads(r["geom_json"])
                    coords = geom.get("coordinates") or []
                    self.railway_segments.append(coords)
                    self.railway_segment_meta.append({
                        "id": str(r["id"]), "line_id": str(r["line_id"]),
                        "distance_meters": r["distance_meters"],
                        "direction": r["direction"], "source_kind": r["source_kind"],
                    })
                return len(rows)
            except Exception:  # noqa: BLE001
                return 0

    def _load_live_state(self) -> None:
        if not self.active:
            return
        rows = _run_sql(
            "SELECT id, trip_id, train_id, anonymous_monitor_id, status, "
            "started_at, last_observation_at, ended_at FROM public.monitor_sessions"
        )
        with self._lock:
            for r in rows:
                self.sessions[str(r["id"])] = SessionRow(
                    id=str(r["id"]), trip_id=str(r["trip_id"]), train_id=str(r["train_id"]),
                    anonymous_monitor_id=r["anonymous_monitor_id"],
                    status=str(r["status"]), started_at=r["started_at"],
                    ended_at=r["ended_at"], last_observation_at=r["last_observation_at"],
                )

    # ---------- live writes ----------

    def upsert_session(self, session_id: str, trip_id: str, train_id: str,
                       status: str, started_at: datetime,
                       anonymous_monitor_id: str | None = None,
                       ended_at: datetime | None = None,
                       last_observation_at: datetime | None = None) -> None:
        if not self.active:
            return
        _run_sql(
            "INSERT INTO public.monitor_sessions "
            "(id, trip_id, train_id, anonymous_monitor_id, status, started_at, "
            "last_observation_at, ended_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (id) DO UPDATE SET "
            "status=EXCLUDED.status, last_observation_at=EXCLUDED.last_observation_at, "
            "ended_at=EXCLUDED.ended_at" % (
                _esc(session_id), _esc(trip_id), _esc(train_id),
                _q(anonymous_monitor_id), _esc(status), _qt(started_at),
                _qt(last_observation_at), _qt(ended_at),
            ),
            timeout=10,
        )
        with self._lock:
            self.sessions[session_id] = SessionRow(
                id=session_id, trip_id=trip_id, train_id=train_id,
                anonymous_monitor_id=anonymous_monitor_id, status=status,
                started_at=started_at, ended_at=ended_at,
                last_observation_at=last_observation_at,
            )

    def insert_observation(self, row: object) -> None:
        if not self.active:
            return
        with self._lock:
            if len(self.observations) >= self._obs_limit:
                self.observations = self.observations[-(self._obs_limit // 2):]
        try:
            _run_sql(
                "INSERT INTO public.gps_observations "
                "(id, session_id, trip_id, train_id, latitude, longitude, "
                "accuracy_m, speed_mps, heading_deg, observed_at, "
                "accepted, rejection_reason, validation_score) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (id) DO NOTHING" % (
                    _esc(row.id), _esc(row.session_id), _esc(row.trip_id), _esc(row.train_id),
                    row.latitude, row.longitude, row.accuracy,
                    _q(row.speed), _q(row.heading), _qt(row.observed_at),
                    "true" if row.accepted else "false",
                    _q(row.rejection_reason), row.validation_score,
                ),
                timeout=10,
            )
        except Exception:  # noqa: BLE001
            pass  # observation loss is non-fatal
        with self._lock:
            self.observations.append(row)

    def upsert_aggregate(self, row: AggregateRow | None, trip_id: str) -> None:
        if not self.active:
            return
        if row is None:
            _run_sql(
                "DELETE FROM public.aggregated_train_positions WHERE trip_id = %s"
                % _esc(trip_id), timeout=10,
            )
            with self._lock:
                self.aggregates.pop(trip_id, None)
            return
        cols = ("trip_id, train_id, latitude, longitude, truth, confidence, freshness, "
                "source_count, next_station_id, next_station_name_ar, station_event, "
                "eta_station_id, eta_min_sec, eta_max_sec, eta_confidence, "
                "wait_decision, wait_reason_ar, last_observed_at, updated_at")
        values = ("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()" % (
            _esc(row.trip_id), _esc(row.train_id), row.latitude, row.longitude,
            _esc(row.truth), _esc(row.confidence), _esc(row.freshness), row.source_count,
            _q(row.next_station_id), _q(row.next_station_name_ar), _q(row.station_event),
            _q(row.eta_station_id), _q(row.eta_min_sec), _q(row.eta_max_sec),
            _q(row.eta_confidence),             _q(row.wait_decision), _q(row.wait_reason_ar),
            _qt(row.last_observed_at),
        ))
        update = ("train_id=EXCLUDED.train_id, latitude=EXCLUDED.latitude, "
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
                  "last_observed_at=EXCLUDED.last_observed_at, updated_at=now()")
        _run_sql(
            f"INSERT INTO public.aggregated_train_positions ({cols}) VALUES ({values}) "
            f"ON CONFLICT (trip_id) DO UPDATE SET {update}",
            timeout=10,
        )
        with self._lock:
            self.aggregates[row.trip_id] = row

    def evict_stale_db(self, max_age: float = DEFAULT_MAX_AGE_SECONDS) -> list[str]:
        removed: list[str] = []
        if not self.active:
            return removed
        try:
            rows = _run_sql(
                "DELETE FROM public.aggregated_train_positions "
                f"WHERE last_observed_at < now() - make_interval(secs => {max_age}) "
                "RETURNING trip_id",
                timeout=10,
            )
            removed = [str(r["trip_id"]) for r in rows]
        except Exception:  # noqa: BLE001
            pass
        with self._lock:
            for tid in removed:
                self.aggregates.pop(tid, None)
        return removed

    def load_trips_registry(self) -> int:
        """Build the public trips index from real trip_stops rows (no fake data).

        Each registered trip_id becomes a Trip shell carrying its line (derived
        from the trip id prefix), train_id (the trip id itself), OUTBOUND
        direction, and no scheduled times (the official SNTF local schedules we
        have do not publish them)."""
        if not self.active or not self.trip_stops:
            return 0
        with self._lock:
            self.trips.clear()
            for trip_id, stops in self.trip_stops.items():
                if not stops:
                    continue
                first, last = min(stops, key=lambda s: s.sequence), max(stops, key=lambda s: s.sequence)
                parts = trip_id.split("-")
                # trip ids look like "zeralda-aga-1501" / "aga-elaffroun-1025"
                raw_line = "-".join(parts[:-1]) if len(parts) >= 3 else trip_id
                # Canonical line ids expected by the Android app
                # (TrainRepository.LINE_ZERALDA etc.)
                line_id = _LINE_ID_MAP.get(raw_line, raw_line)
                self.trips[trip_id] = {
                    "id": trip_id,
                    "train_id": trip_id,
                    "line_id": line_id,
                    "direction": "OUTBOUND",
                    "scheduled_departure": None,
                    "scheduled_arrival": None,
                    "status": "SCHEDULED",
                    "stop_count": len(stops),
                    "first_station_id": first.station_id,
                    "last_station_id": last.station_id,
                }
        return len(self.trips)

    def load_trip_stops(self) -> int:
        """Restore all registered trip stops from Postgres into memory (survives restarts)."""
        if not self.active:
            return 0
        try:
            rows = _run_sql(
                "SELECT trip_id, station_id, station_name, sequence, "
                "latitude, longitude FROM public.trip_stops ORDER BY trip_id, sequence"
            )
        except RuntimeError:
            return 0
        if not rows:
            return 0
        with self._lock:
            self.trip_stops.clear()
            for r in rows:
                self.trip_stops.setdefault(str(r["trip_id"]), []).append(
                    TripStopRow(
                        station_id=str(r["station_id"]),
                        station_name=str(r["station_name"]),
                        sequence=int(r["sequence"]),
                        latitude=float(r["latitude"]),
                        longitude=float(r["longitude"]),
                    )
                )
        # Rebuild the public trips index so GET /trips exposes the real schedule.
        self.load_trips_registry()
        return len(self.trip_stops)

    def save_trip_stops(self, trip_id: str, rows: list[TripStopRow]) -> None:
        if not self.active:
            return
        _run_sql("DELETE FROM public.trip_stops WHERE trip_id = %s" % _esc(trip_id), timeout=15)
        for r in rows:
            _run_sql(
                "INSERT INTO public.trip_stops "
                "(trip_id, station_id, station_name, sequence, latitude, longitude) "
                "VALUES (%s,%s,%s,%s,%s,%s)" % (
                    _esc(trip_id), _esc(r.station_id), _esc(r.station_name),
                    r.sequence, r.latitude, r.longitude,
                ),
                timeout=10,
            )
        with self._lock:
            self.trip_stops[trip_id] = list(rows)

    def save_report(self, report: dict) -> bool:
        if not self.active:
            return True
        try:
            _run_sql(
                "INSERT INTO public.community_reports "
                "(report_id, trip_id, reporter_id, category, description, "
                "latitude, longitude, reported_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,now())" % (
                    _q(report.get("report_id")), _q(report.get("trip_id")),
                    _q(report.get("reporter_id")), _q(report.get("category")),
                    _q(report.get("description")), report.get("latitude"),
                    report.get("longitude"),
                ),
                timeout=10,
            )
            return True
        except Exception:  # noqa: BLE001
            return False

    def trim_reports(self, limit: int = 500) -> int:
        if not self.active:
            return 0
        try:
            total_rows = _run_sql("SELECT COUNT(*) FROM public.community_reports")
            total = int(total_rows[0]["count"]) if total_rows else 0
            if total <= limit:
                return 0
            rows = _run_sql(
                "DELETE FROM public.community_reports WHERE id IN "
                "(SELECT id FROM public.community_reports ORDER BY reported_at ASC "
                f"LIMIT {total - limit}) RETURNING id"
            )
            return len(rows)
        except Exception:  # noqa: BLE001
            return 0


def _esc(s: object) -> str:
    """Escape a value for embedding in a single-quoted SQL literal."""
    if s is None:
        return "NULL"
    return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"


def _q(s: object) -> str:
    if s is None:
        return "NULL"
    return _esc(s)


def _qt(dt: datetime | None) -> str:
    if dt is None:
        return "NULL"
    return "'" + dt.isoformat() + "'"
