import os
import sys
import uuid
from datetime import datetime, timezone

import pytest

try:
    import psycopg
except ImportError:  # pragma: no cover - exercised in runtimes without psycopg3
    psycopg = None

DATABASE_URL = os.environ.get("WINRAH_TEST_DATABASE_URL", "").strip()
pytestmark = pytest.mark.skipif(
    not DATABASE_URL or psycopg is None,
    reason="set a local WINRAH_TEST_DATABASE_URL and install psycopg3 for this integration test",
)

# Never inherit a configured production/external storage URL during collection.
for key in ("DATABASE_URL", "SUPABASE_DB_URL", "SUPABASE_POOLER_URL", "SQLPROXY_URL", "SQLPROXY_KEY"):
    os.environ.pop(key, None)
if DATABASE_URL:
    assert "127.0.0.1" in DATABASE_URL or "localhost" in DATABASE_URL
    os.environ["DATABASE_URL"] = DATABASE_URL

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from fastapi.testclient import TestClient  # noqa: E402
from app.main import app, store  # noqa: E402


def _fixture_ids() -> tuple[str, str]:
    return str(uuid.uuid4()), str(uuid.uuid4())


def _reset_db(trip_id: str, train_id: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE public.gps_observations, public.monitor_sessions RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE public.aggregated_train_positions RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE public.trips, public.trains RESTART IDENTITY CASCADE")
            cur.execute(
                "INSERT INTO public.trains (id, train_number) VALUES (%s::uuid, %s)",
                (train_id, "local-persistence-train"),
            )
            cur.execute(
                "INSERT INTO public.trips (id, train_id, direction, status) "
                "VALUES (%s::uuid, %s::uuid, 'OUTBOUND', 'RUNNING')",
                (trip_id, train_id),
            )
        conn.commit()


def test_post_observation_writes_canonical_rows_and_rehydrates() -> None:
    trip_id, train_id = _fixture_ids()
    _reset_db(trip_id, train_id)
    client = TestClient(app)

    session_response = client.post(
        "/monitor-sessions",
        json={
            "trip_id": trip_id,
            "train_id": train_id,
            "anonymous_monitor_id": "pytest-local-persistence",
        },
    )
    assert session_response.status_code == 200, session_response.text
    session_id = session_response.json()["id"]

    observation_response = client.post(
        "/observations",
        json={
            "session_id": session_id,
            "trip_id": trip_id,
            "train_id": train_id,
            "latitude": 36.7445,
            "longitude": 3.0905,
            "accuracy": 20.0,
            "speed": 8.0,
            "heading": 90.0,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        },
    )
    assert observation_response.status_code == 200, observation_response.text
    assert observation_response.json()["accepted"] is True

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ST_AsText(location), accuracy_meters, speed_mps, heading_deg, "
                "is_valid, rejection_reason, validation_score "
                "FROM public.gps_observations WHERE session_id = %s::uuid",
                (session_id,),
            )
            observation_row = cur.fetchone()
            cur.execute(
                "SELECT ST_AsText(location), estimated_speed_mps, heading_deg, confidence, "
                "confidence_score, freshness, truth, source_count "
                "FROM public.aggregated_train_positions WHERE trip_id = %s::uuid",
                (trip_id,),
            )
            aggregate_row = cur.fetchone()

    assert observation_row == (
        "POINT(3.0905 36.7445)", 20.0, 8.0, 90.0, True, None, pytest.approx(0.7475)
    )
    assert aggregate_row[0] == "POINT(3.0905 36.7445)"
    assert aggregate_row[1] == pytest.approx(8.0)
    assert aggregate_row[2] == pytest.approx(90.0)
    assert aggregate_row[3] == "MEDIUM"
    assert aggregate_row[4] > 0.0
    assert aggregate_row[5:] == ("LIVE", "OBSERVED", 1)

    store.sessions.clear()
    store.aggregates.clear()
    store._load_live_state()
    assert session_id in store.sessions
    assert trip_id in store.aggregates
    rehydrated = store.aggregates[trip_id]
    assert rehydrated.speed_mps == pytest.approx(8.0)
    assert rehydrated.heading_deg == pytest.approx(90.0)
    assert rehydrated.confidence_score > 0.0
    assert rehydrated.truth == "OBSERVED"
    assert client.get(f"/trips/{trip_id}/live").status_code == 200
