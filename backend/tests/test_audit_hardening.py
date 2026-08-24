from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.main import ObservationIn, ReportIn, app, run_store_maintenance
from app import postgres_store_sqlproxy as sqlproxy
from app.store import TripStopRow
from app.trip_registry import build_trip_registry


def observation_payload(**overrides):
    payload = {
        "session_id": str(uuid4()),
        "trip_id": str(uuid4()),
        "train_id": str(uuid4()),
        "latitude": 36.7,
        "longitude": 3.1,
        "accuracy": 10.0,
        "speed": 12.0,
        "heading": 90.0,
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    }
    payload.update(overrides)
    return payload


def test_observation_bounds_and_non_finite_values_are_rejected():
    with pytest.raises(ValidationError):
        ObservationIn(**observation_payload(latitude=91.0))
    with pytest.raises(ValidationError):
        ObservationIn(**observation_payload(accuracy=-1.0))
    with pytest.raises(ValidationError):
        ObservationIn(**observation_payload(speed=float("nan")))
    with pytest.raises(ValidationError):
        ObservationIn(**observation_payload(heading=360.0))


def test_future_client_clock_is_clamped_without_changing_epoch_millis_contract():
    future = int(datetime.now(timezone.utc).timestamp() * 1000) + 3_600_000
    parsed = ObservationIn(**observation_payload(timestamp=future))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    assert parsed.timestamp <= now_ms + 120_000
    assert isinstance(parsed.timestamp, int)


def test_report_description_has_bounded_length():
    with pytest.raises(ValidationError):
        ReportIn(train_id=str(uuid4()), description="x" * 501)


def test_maintenance_prefers_adapter_report_retention_hook():
    calls: list[int] = []

    class Adapter:
        def trim_reports(self, limit: int = 500) -> int:
            calls.append(limit)
            return 2

        def trim_windows(self):
            raise AssertionError("trim_windows must not be selected when trim_reports exists")

    assert run_store_maintenance(Adapter(), limit=123) == 2
    assert calls == [123]


def test_batch_observation_limit_is_enforced():
    client = TestClient(app)
    response = client.post("/observations/batch", json=[observation_payload() for _ in range(51)])
    assert response.status_code == 413
    assert response.json()["detail"] == "observation_batch_too_large"


def test_registry_fallback_uses_schema_direction():
    stop = SimpleNamespace(sequence=1, station_id="station-1")
    result = build_trip_registry({"elafroun-aga-1000": [stop]})
    assert result["elafroun-aga-1000"]["direction"] == "INBOUND"


def test_sqlproxy_trip_stops_are_batched(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(sqlproxy, "_run_sql", lambda query, timeout=15.0: calls.append(query) or [])
    adapter = sqlproxy.PostgresStoreSqlproxy.__new__(sqlproxy.PostgresStoreSqlproxy)
    adapter._active = True
    adapter._lock = __import__("threading").RLock()
    adapter.trip_stops = {}
    trip_id = str(uuid4())
    rows = [
        TripStopRow("station-1", "Station 1", 1, 36.7, 3.1),
        TripStopRow("station-2", "Station 2", 2, 36.8, 3.2),
    ]
    adapter.save_trip_stops(trip_id, rows)
    assert len(calls) == 1
    assert "WITH deleted AS" in calls[0]
    assert calls[0].count("INSERT INTO public.trip_stops") == 1
    assert adapter.trip_stops[trip_id] == rows
