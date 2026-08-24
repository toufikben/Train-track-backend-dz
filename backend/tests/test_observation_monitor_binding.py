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


def test_active_store_denies_public_writes_by_default(monkeypatch) -> None:
    monkeypatch.setattr(store, "active", True, raising=False)
    monkeypatch.delenv("WINRAH_PUBLIC_WRITES_ENABLED", raising=False)
    response = client.post(
        "/monitor-sessions",
        json={
            "trip_id": "11111111-1111-4111-8111-111111111111",
            "train_id": "22222222-2222-4222-8222-222222222222",
            "anonymous_monitor_id": "write-gate-test",
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "public_writes_disabled"
    assert store.sessions == {}


def test_db_mode_rejects_unknown_reference_before_cache_mutation(monkeypatch) -> None:
    monkeypatch.setenv("WINRAH_PUBLIC_WRITES_ENABLED", "true")
    monkeypatch.setattr(store, "active", True, raising=False)
    monkeypatch.setattr(
        store, "check_trip_train_reference", lambda _trip, _train: "unknown_trip_reference", raising=False
    )
    response = client.post(
        "/monitor-sessions",
        json={
            "trip_id": "11111111-1111-4111-8111-111111111111",
            "train_id": "22222222-2222-4222-8222-222222222222",
            "anonymous_monitor_id": "db-reference-test",
        },
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "unknown_trip_reference"
    assert store.sessions == {}


def test_db_mode_does_not_cache_when_persistence_fails(monkeypatch) -> None:
    monkeypatch.setenv("WINRAH_PUBLIC_WRITES_ENABLED", "true")
    monkeypatch.setattr(store, "active", True, raising=False)
    monkeypatch.setattr(store, "check_trip_train_reference", lambda _trip, _train: None, raising=False)
    def fail_persist(*_args, **_kwargs):
        raise RuntimeError("simulated persistence failure")
    monkeypatch.setattr(store, "upsert_session", fail_persist, raising=False)
    response = client.post(
        "/monitor-sessions",
        json={
            "trip_id": "11111111-1111-4111-8111-111111111111",
            "train_id": "22222222-2222-4222-8222-222222222222",
            "anonymous_monitor_id": "db-persistence-test",
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "storage_unavailable"
    assert store.sessions == {}


def test_db_mode_rejects_orphan_observation(monkeypatch) -> None:
    monkeypatch.setenv("WINRAH_PUBLIC_WRITES_ENABLED", "true")
    monkeypatch.setattr(store, "active", True, raising=False)
    monkeypatch.setattr(store, "check_trip_train_reference", lambda _trip, _train: None, raising=False)
    response = client.post(
        "/observations",
        json=_observation(
            "33333333-3333-4333-8333-333333333333",
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        ),
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "session_not_found"
    assert store.observations == []
    assert store.aggregates == {}


def test_active_store_denies_favorite_writes_by_default(monkeypatch) -> None:
    monkeypatch.setattr(store, "active", True, raising=False)
    monkeypatch.delenv("WINRAH_PUBLIC_WRITES_ENABLED", raising=False)
    add_response = client.post("/favorites", json={"type": "STATION", "value": "any"})
    delete_response = client.delete("/favorites/any")
    assert add_response.status_code == 503
    assert delete_response.status_code == 503
    assert add_response.json()["detail"] == "public_writes_disabled"
    assert delete_response.json()["detail"] == "public_writes_disabled"


def test_report_aliases_are_normalized_to_schema_values() -> None:
    response = client.post(
        "/reports",
        json={"train_id": "legacy-train", "report_type": "DELAY", "description": "delay"},
    )
    assert response.status_code == 200, response.text
    assert store.reports[-1]["report_type"] == "DELAYED"

    crowding = client.post(
        "/reports",
        json={"train_id": "legacy-train", "report_type": "CROWDING", "description": "crowding"},
    )
    assert crowding.status_code == 200, crowding.text
    assert store.reports[-1]["report_type"] == "OTHER"


def test_report_rejects_unknown_type_before_mutation() -> None:
    response = client.post(
        "/reports",
        json={"train_id": "legacy-train", "report_type": "NOT_A_REPORT"},
    )
    assert response.status_code == 422
    assert store.reports == []


def test_report_does_not_cache_when_persistence_returns_false(monkeypatch) -> None:
    monkeypatch.setenv("WINRAH_PUBLIC_WRITES_ENABLED", "true")
    monkeypatch.setattr(store, "active", True, raising=False)
    monkeypatch.setattr(store, "save_report", lambda _record: False, raising=False)
    response = client.post(
        "/reports",
        json={
            "train_id": "22222222-2222-4222-8222-222222222222",
            "trip_id": "11111111-1111-4111-8111-111111111111",
            "station_id": "33333333-3333-4333-8333-333333333333",
            "report_type": "DELAYED",
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "report_persistence_failed"
    assert store.reports == []


def test_db_mode_rejects_non_uuid_observation_ids(monkeypatch) -> None:
    monkeypatch.setenv("WINRAH_PUBLIC_WRITES_ENABLED", "true")
    monkeypatch.setattr(store, "active", True, raising=False)
    response = client.post(
        "/observations",
        json=_observation("not-a-uuid", "binding-trip-a", "binding-train-a"),
    )
    assert response.status_code == 422
    assert response.json()["detail"] == "invalid_session_id_uuid"
