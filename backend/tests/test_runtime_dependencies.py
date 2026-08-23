"""Runtime dependency regression checks; no network or database connection is opened."""


def test_postgres_adapters_runtime_dependencies_are_importable():
    import psycopg
    import psycopg2
    from psycopg_pool import ConnectionPool

    assert callable(psycopg.connect)
    assert callable(ConnectionPool)
    assert callable(psycopg2.connect)
