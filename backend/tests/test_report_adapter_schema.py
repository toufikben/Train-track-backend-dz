import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from app import store as _store  # noqa: F401,E402
from app import postgres_store, postgres_store_psycopg2, postgres_store_sqlproxy  # noqa: E402


REPORT = {
    "id": "report-id",
    "train_id": "train-id",
    "trip_id": "trip-id",
    "station_id": "station-id",
    "report_type": "DELAYED",
    "description": "observed delay",
    "created_at": datetime.now(timezone.utc).isoformat(),
}


CANONICAL_COLUMNS = (
    "(id, train_id, trip_id, station_id, report_type, description, created_at)"
)
LEGACY_COLUMNS = (
    "report_id",
    "reporter_id",
    "category",
    "reported_at",
    "event_kind",
    "description_ar",
    "session_id",
)


class FakeCursor:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=None):
        self.calls.append((query, params))


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self.cursor_instance

    def rollback(self):
        pass


def assert_canonical_query(query: str) -> None:
    assert CANONICAL_COLUMNS in query
    for legacy in LEGACY_COLUMNS:
        assert legacy not in query


def test_psycopg3_save_report_uses_canonical_columns():
    store = object.__new__(postgres_store.PostgresStore)
    store._pool = object()
    connection = FakeConnection()
    store._conn = lambda: connection

    store.save_report(REPORT)

    query, params = connection.cursor_instance.calls[0]
    assert_canonical_query(query)
    assert params == tuple(REPORT[key] for key in (
        "id", "train_id", "trip_id", "station_id", "report_type", "description", "created_at"
    ))


def test_psycopg2_save_report_uses_canonical_columns():
    store = object.__new__(postgres_store_psycopg2.PostgresStorePsycopg2)
    store._pool = object()
    connection = FakeConnection()
    store._conn = lambda: connection

    assert store.save_report(REPORT) is True

    query, params = connection.cursor_instance.calls[0]
    assert_canonical_query(query)
    assert params == tuple(REPORT[key] for key in (
        "id", "train_id", "trip_id", "station_id", "report_type", "description", "created_at"
    ))


def test_sqlproxy_save_report_uses_canonical_columns(monkeypatch):
    calls = []

    def fake_run_sql(query: str, timeout: float = 15.0):
        calls.append((query, timeout))
        return []

    monkeypatch.setattr(postgres_store_sqlproxy, "_run_sql", fake_run_sql)
    store = object.__new__(postgres_store_sqlproxy.PostgresStoreSqlproxy)
    store._active = True

    assert store.save_report(REPORT) is True

    query, timeout = calls[0]
    assert_canonical_query(query)
    assert timeout == 10
