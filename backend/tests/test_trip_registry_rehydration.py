import os
import sys
from contextlib import contextmanager
from threading import RLock
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

# Import the application first; app.store initializes the selected adapter.
from app.main import store as _app_store  # noqa: F401
from app.postgres_store import PostgresStore
from app.postgres_store_psycopg2 import PostgresStorePsycopg2
from app.trip_registry import build_trip_registry


ROWS = [
    ("zeralda-aga-1501", "station-a", "A", 1, 36.70, 3.00),
    ("zeralda-aga-1501", "station-b", "B", 2, 36.71, 2.95),
    ("aga-elaffroun-1025", "station-c", "C", 1, 36.70, 3.00),
    ("aga-elaffroun-1025", "station-d", "D", 2, 36.60, 2.80),
    ("aga-elaffroun-1025", "station-e", "E", 3, 36.50, 2.70),
    ("elaffroun-aga-1058-a", "station-f", "F", 1, 36.50, 2.70),
]

META_ROWS = [
    ("zeralda-aga-1501", "train-a", "line-a", "INBOUND", None, None, "SCHEDULED"),
    ("aga-elaffroun-1025", "train-b", "line-b", "OUTBOUND", None, None, "SCHEDULED"),
    ("elaffroun-aga-1058-a", "train-c", "line-c", "INBOUND", None, None, "SCHEDULED"),
]


class FakeCursor:
    def __init__(self):
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql):
        self.sql = sql

    def fetchall(self):
        if "FROM public.trips" in self.sql:
            return META_ROWS
        return ROWS


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self):
        return FakeCursor()


def _fake_conn():
    @contextmanager
    def cm():
        yield FakeConnection()

    return cm


def _assert_registry(adapter):
    adapter._lock = RLock()
    adapter._pool = object()
    adapter.trip_stops = {"stale-trip": []}
    adapter.trips = {"stale-trip": {"id": "stale-trip"}}
    # The adapter method calls self._conn(), so install a callable context manager.
    adapter._conn = lambda: _fake_conn()()

    total_stops = adapter.load_trip_stops()

    assert total_stops == len(ROWS)
    assert set(adapter.trips) == {
        "zeralda-aga-1501", "aga-elaffroun-1025", "elaffroun-aga-1058-a"
    }
    assert adapter.trips["zeralda-aga-1501"] == {
        "id": "zeralda-aga-1501",
        "train_id": "train-a",
        "line_id": "line-a",
        "direction": "INBOUND",
        "scheduled_departure": None,
        "scheduled_arrival": None,
        "status": "SCHEDULED",
        "stop_count": 2,
        "first_station_id": "station-a",
        "last_station_id": "station-b",
    }
    assert adapter.trips["aga-elaffroun-1025"]["train_id"] == "train-b"
    assert adapter.trips["aga-elaffroun-1025"]["line_id"] == "line-b"
    assert adapter.trips["aga-elaffroun-1025"]["direction"] == "OUTBOUND"
    assert adapter.trips["aga-elaffroun-1025"]["stop_count"] == 3
    assert adapter.trips["elaffroun-aga-1058-a"]["train_id"] == "train-c"
    assert adapter.trips["elaffroun-aga-1058-a"]["line_id"] == "line-c"
    assert adapter.trips["elaffroun-aga-1058-a"]["direction"] == "INBOUND"

    adapter.trips["stale-trip"] = {"id": "stale-trip"}
    rebuilt_count = adapter.load_trips_registry()
    assert rebuilt_count == 3
    assert set(adapter.trips) == {
        "zeralda-aga-1501", "aga-elaffroun-1025", "elaffroun-aga-1058-a"
    }
    assert adapter.trips["zeralda-aga-1501"]["line_id"] == "line-a"


def test_trip_registry_ignores_empty_trips_and_orders_stops():
    trip_stops = {
        "uuid-trip": [
            SimpleNamespace(station_id="last", sequence=3),
            SimpleNamespace(station_id="first", sequence=1),
        ],
        "empty-trip": [],
    }
    metadata = {
        "uuid-trip": {
            "train_id": "uuid-train",
            "line_id": "uuid-line",
            "direction": "INBOUND",
            "status": "SCHEDULED",
        },
    }

    registry = build_trip_registry(trip_stops, metadata)

    assert set(registry) == {"uuid-trip"}
    assert registry["uuid-trip"]["first_station_id"] == "first"
    assert registry["uuid-trip"]["last_station_id"] == "last"
    assert registry["uuid-trip"]["stop_count"] == 2
    assert registry["uuid-trip"]["train_id"] == "uuid-train"
    assert registry["uuid-trip"]["line_id"] == "uuid-line"
    assert registry["uuid-trip"]["direction"] == "INBOUND"


def test_direct_postgres_rehydrates_public_trip_registry():
    adapter = object.__new__(PostgresStore)
    _assert_registry(adapter)


def test_psycopg2_fallback_rehydrates_public_trip_registry():
    adapter = object.__new__(PostgresStorePsycopg2)
    _assert_registry(adapter)
