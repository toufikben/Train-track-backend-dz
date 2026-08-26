from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "migrations"
    / "008_rls_deferred_risks_server_only.sql"
)


def migration_text() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_deferred_risk_migration_is_local_review_only():
    sql = migration_text()
    assert "LOCAL REVIEW ONLY" in sql
    assert "BEGIN;" in sql
    assert "COMMIT;" in sql


def test_reports_and_sources_are_not_publicly_readable():
    sql = migration_text()
    assert "REVOKE SELECT ON TABLE public.community_reports, public.data_sources" in sql
    assert "DROP POLICY IF EXISTS community_reports_public_read" in sql
    assert "DROP POLICY IF EXISTS data_sources_public_read" in sql


def test_sensitive_tables_have_no_broad_grants_or_public_read_policies():
    sql = migration_text()
    sensitive = (
        "monitor_sessions",
        "gps_observations",
        "monitor_reputation",
        "favorites",
        "notifications",
        "audit_logs",
    )
    for table in sensitive:
        assert f"public.{table}" in sql
        assert f"{table}_public_read" in sql
        assert f"{table}_authenticated_read" in sql
    assert "auth.uid()" not in sql


def test_migration_does_not_modify_postgis_reference_table():
    sql = migration_text()
    assert "spatial_ref_sys" not in sql
