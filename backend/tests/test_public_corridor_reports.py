import os
import sys
from fastapi.testclient import TestClient

# Keep this suite isolated from configured external storage.
for key in ("DATABASE_URL", "SUPABASE_DB_URL", "SUPABASE_POOLER_URL", "SQLPROXY_URL", "SQLPROXY_KEY"):
    os.environ.pop(key, None)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from app.main import app, store  # noqa: E402

client = TestClient(app)


def setup_function() -> None:
    store.sessions.clear()
    store.reports.clear()


def _create_public_session() -> str:
    response = client.post(
        "/monitor-sessions",
        json={
            "line_id": "thnia_algiers",
            "direction": "INBOUND",
            "anonymous_monitor_id": "public-report-test",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def test_public_corridor_report_accepts_without_train_id() -> None:
    session_id = _create_public_session()
    response = client.post(
        "/reports",
        json={
            "session_id": session_id,
            "station_id": None,
            "report_type": "OTHER",
            "description": "إفادة عامة عن اكتظاظ المسار",
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "accepted"
    assert store.reports[-1]["session_id"] == session_id
    assert store.reports[-1]["train_id"] is None


def test_public_corridor_report_rejects_bound_session() -> None:
    response = client.post(
        "/monitor-sessions",
        json={
            "line_id": "thnia_algiers",
            "direction": "INBOUND",
            "trip_id": "trip-bound",
            "train_id": "train-bound",
            "anonymous_monitor_id": "public-report-test",
        },
    )
    assert response.status_code == 200, response.text
    session_id = response.json()["id"]
    report = client.post(
        "/reports",
        json={"session_id": session_id, "report_type": "DELAYED"},
    )
    assert report.status_code == 409
    assert report.json()["detail"] == "public_report_requires_public_session"


def test_report_without_train_or_session_is_rejected() -> None:
    response = client.post("/reports", json={"report_type": "OTHER"})
    assert response.status_code == 422
    assert response.json()["detail"] == "public_report_session_required"
