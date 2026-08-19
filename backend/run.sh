#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m pip install -q -r requirements.txt
export PYTHONPATH="$(pwd)"
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
