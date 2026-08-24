from pathlib import Path
import os

import psycopg


ROOT = Path(__file__).resolve().parents[1]
DATABASE_URL = os.environ["WINRAH_E2E_DATABASE_URL"]
if not any(host in DATABASE_URL for host in ("127.0.0.1", "localhost")):
    raise RuntimeError("E2E database must be local")

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        for relative_path in (
            "schema.sql",
            "migrations/001_observation_aggregate_canonical.sql",
            "data/references/generated/reference_seed.sql",
        ):
            cur.execute((ROOT / relative_path).read_text(encoding="utf-8"))
