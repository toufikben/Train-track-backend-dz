"""
PHASE 7.5 — Load provisional REFERENCE network stations.

Does NOT create live train positions.
Marks dataset as REFERENCE_NETWORK in metadata.
"""
from __future__ import annotations
import json
from pathlib import Path


def load_stations_json(path: str | Path) -> dict:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    assert data.get("not_live_tracking") is True
    assert data.get("source_kind") == "REFERENCE_NETWORK"
    return data


def to_insert_rows(data: dict) -> list[dict]:
    rows = []
    for s in data["stations"]:
        rows.append({
            "external_key": s["id"],
            "name_ar": s["name_ar"],
            "name_fr": s["name_fr"],
            "name_en": s["name_en"],
            "lon": s["lon"],
            "lat": s["lat"],
            "lines": s.get("lines", []),
            "source_kind": "REFERENCE_NETWORK",
            "as_of": data.get("as_of"),
        })
    return rows
