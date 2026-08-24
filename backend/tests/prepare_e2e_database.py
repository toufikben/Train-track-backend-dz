from pathlib import Path
import argparse
import os

import psycopg


ROOT = Path(__file__).resolve().parents[1]
DATABASE_URL = os.environ["WINRAH_E2E_DATABASE_URL"]
if not any(host in DATABASE_URL for host in ("127.0.0.1", "localhost")):
    raise RuntimeError("E2E database must be local")

parser = argparse.ArgumentParser(description="Prepare a local disposable E2E database")
parser.add_argument(
    "--reset",
    action="store_true",
    help="drop and recreate the public schema before applying schema and migrations",
)
args = parser.parse_args()

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        if args.reset:
            cur.execute(
                "DROP SCHEMA public CASCADE; "
                "CREATE SCHEMA public; "
                "CREATE EXTENSION IF NOT EXISTS postgis; "
                "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
            )
        cur.execute((ROOT / "schema.sql").read_text(encoding="utf-8"))
        migration_paths = sorted(
            path for path in (ROOT / "migrations").glob("*.sql")
            if not path.name.endswith("_rollback.sql")
        )
        for migration_path in migration_paths:
            cur.execute(migration_path.read_text(encoding="utf-8"))
        cur.execute(
            (ROOT / "data/references/generated/reference_seed.sql").read_text(
                encoding="utf-8"
            )
        )
