"""Optional Postgres/PostGIS.

Set environment DATABASE_URL=postgresql://user:pass@host:5432/dbname
and replace MemoryStore calls with SQL implementing the same public shapes.

This MVP keeps MemoryStore as default so the project runs without a database.
Schema: see supabase/phase04_postgis_schema.sql
"""
import os

def database_url() -> str | None:
    return os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")
