import os
import sys
from pathlib import Path

import pytest

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

DATABASE_URL = os.environ.get("WINRAH_E2E_DATABASE_URL", "").strip()
pytestmark = pytest.mark.e2e

if not DATABASE_URL or psycopg is None:
    pytestmark = [
        pytest.mark.e2e,
        pytest.mark.skip(reason="set WINRAH_E2E_DATABASE_URL and install psycopg3"),
    ]
else:
    if not any(host in DATABASE_URL for host in ("127.0.0.1", "localhost")):
        raise RuntimeError("E2E database must be local; refusing non-local URL")
    for key in (
        "DATABASE_URL", "SUPABASE_DB_URL", "SUPABASE_POOLER_URL",
        "SQLPROXY_URL", "SQLPROXY_KEY",
    ):
        os.environ.pop(key, None)
    os.environ["DATABASE_URL"] = DATABASE_URL

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402
from app.main import app, store  # noqa: E402


def _reference_trip_id() -> str:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT t.id::text FROM public.trips t "
                "WHERE t.deleted_at IS NULL "
                "AND EXISTS (SELECT 1 FROM public.trip_stops ts WHERE ts.trip_id = t.id) "
                "ORDER BY t.id LIMIT 1"
            )
            row = cur.fetchone()
    assert row is not None
    return row[0]


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as test_client:
        assert getattr(store, "active", False) is True
        yield test_client


def test_trip_catalog_detail_and_stops_roundtrip(client):
    stations_response = client.get("/stations")
    trips_response = client.get("/trips")
    assert stations_response.status_code == 200, stations_response.text
    assert trips_response.status_code == 200, trips_response.text

    stations = stations_response.json()
    trips = trips_response.json()
    station_ids = {station["id"] for station in stations}
    assert len(stations) == 28
    assert len(trips) == 105
    assert len({trip["id"] for trip in trips}) == 105

    trip_id = _reference_trip_id()
    detail_response = client.get(f"/trips/{trip_id}")
    stops_response = client.get(f"/trips/{trip_id}/stops")
    assert detail_response.status_code == 200, detail_response.text
    assert stops_response.status_code == 200, stops_response.text

    detail = detail_response.json()
    stops = stops_response.json()
    assert detail["id"] == trip_id
    assert detail["stop_count"] == len(stops) > 0
    assert [stop["sequence"] for stop in stops] == sorted(stop["sequence"] for stop in stops)
    assert {stop["station_id"] for stop in stops} <= station_ids
    assert stops[0]["station_id"] == detail["first_station_id"]
    assert stops[-1]["station_id"] == detail["last_station_id"]


def test_line_filter_and_trip_metadata_are_consistent(client):
    trip_id = _reference_trip_id()
    detail = client.get(f"/trips/{trip_id}").json()
    line_id = detail["line_id"]
    filtered_response = client.get("/trips", params={"line_id": line_id})
    assert filtered_response.status_code == 200, filtered_response.text
    filtered = filtered_response.json()
    assert filtered
    assert all(trip["line_id"] == line_id for trip in filtered)
    assert any(trip["id"] == trip_id for trip in filtered)
    assert detail["train_id"]
    assert detail["direction"] in {"INBOUND", "OUTBOUND"}


def test_rehydration_is_idempotent_and_public_routes_remain_readable(client):
    expected_trip_ids = {trip["id"] for trip in client.get("/trips").json()}
    assert len(expected_trip_ids) == 105

    store.trips.clear()
    store.trip_stops.clear()
    loaded_stop_rows = store.load_trip_stops()
    assert loaded_stop_rows == 1476
    assert store.load_trips_registry() == 105
    first_rehydrated_ids = set(store.trips)

    store.trips.clear()
    store.trip_stops.clear()
    assert store.load_trip_stops() == 1476
    assert store.load_trips_registry() == 105
    second_rehydrated_ids = set(store.trips)

    assert first_rehydrated_ids == expected_trip_ids == second_rehydrated_ids
    trip_id = _reference_trip_id()
    assert client.get(f"/trips/{trip_id}").status_code == 200
    assert client.get(f"/trips/{trip_id}/stops").status_code == 200


def test_unknown_trip_is_not_exposed(client):
    unknown_trip_id = "00000000-0000-0000-0000-000000000000"
    assert client.get(f"/trips/{unknown_trip_id}").status_code == 404
    assert client.get(f"/trips/{unknown_trip_id}/stops").status_code == 404
