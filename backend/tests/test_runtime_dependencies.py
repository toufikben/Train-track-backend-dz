"""Runtime dependency regression checks; no network or database connection is opened."""


def test_postgres_adapters_runtime_dependencies_are_importable():
    import psycopg
    import psycopg2

    assert callable(psycopg.connect)
    assert callable(psycopg2.connect)
