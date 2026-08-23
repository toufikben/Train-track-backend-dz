import os
import sys
import time

from fastapi.testclient import TestClient

# Keep this regression suite isolated from any configured external storage.
os.environ.pop("DATABASE_URL", None)
os.environ.pop("SUPABASE_DB_URL", None)
os.environ.pop("SUPABASE_POOLER_URL", None)
os.environ.pop("SQLPROXY_URL", None)
os.environ.pop("SQLPROXY_KEY", None)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from app.main import app, store  # noqa: E402


client = TestClient(app)


def setup_function() -> None:
    store.sessions.clear()
    store.observations.clear()
    store.aggregates.clear()
    store.reports.clear()
    store.trips.clear()
    store.trip_stops.clear()


def _observation(session_id: str, trip_id: str, train_id: str) -> dict:
    return {
        "session_id": session_id,
        "trip_id": trip_id,
        "train_id": train_id,
        "latitude": 36.7445,
        "longitude": 3.0905,
        "accuracy": 20.0,
        "speed": 8.0,
        "heading": 90.0,
        "timestamp": int(time.time() * 1000),
    }


def _create_session(trip_id: str = "binding-trip-a", train_id: str = "binding-train-a") -> str:
    response = client.post(
        "/monitor-sessions",
        json={
            "trip_id": trip_id,
            "train_id": train_id,
            "anonymous_monitor_id": "binding-test-monitor",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def test_matching_observation_keeps_existing_pipeline_behavior() -> None:
    session_id = _create_session()

    response = client.post(
        "/observations",
        json=_observation(session_id, "binding-trip-a", "binding-train-a"),
    )

    assert response.status_code == 200, response.text
    assert response.json()["accepted"] is True


def test_known_session_rejects_cross_binding_trip_and_train() -> None:
    session_id = _create_session()

    trip_mismatch = client.post(
        "/observations",
        json=_observation(session_id, "binding-trip-b", "binding-train-a"),
    )
    train_mismatch = client.post(
        "/observations",
        json=_observation(session_id, "binding-trip-a", "binding-train-b"),
    )

    assert trip_mismatch.status_code == 409
    assert trip_mismatch.json()["detail"] == "session_binding_mismatch"
    assert train_mismatch.status_code == 409
    assert train_mismatch.json()["detail"] == "session_binding_mismatch"
    assert store.observations == []
    assert store.aggregates == {}


def test_unknown_session_keeps_orphan_behavior_outside_this_patch_scope() -> None:
    response = client.post(
        "/observations",
        json=_observation("unknown-binding-session", "binding-trip-a", "binding-train-a"),
    )
    assert response.status_code == 200, response.text
    assert response.status_code != 409
    assert "unknown-binding-session" in store.sessions


def test_db_mode_rejects_non_uuid_observation_ids(monkeypatch) -> None:
    monkeypatch.setattr(store, "active", True, raising=False)
    response = client.post(
        "/observations",
        json=_observation("not-a-uuid", "binding-trip-a", "binding-train-a"),
    )
    assert response.status_code == 422
    assert response.json()["detail"] == "invalid_session_id_uuid"
