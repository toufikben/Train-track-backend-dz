#!/usr/bin/env python3
"""Generate a review-only canonical reference seed.

The source files contain development/reference timetable data, not live train
positions. UUIDs are deterministic so rerunning the generator preserves the
same identifiers. The generated SQL is never applied by this script.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REF_DIR = ROOT / "data" / "reference"
DATA_DIR = ROOT / "data" / "references"
OUT_DIR = DATA_DIR / "generated"
OUT_DIR.mkdir(parents=True, exist_ok=True)

NAMESPACE = uuid.UUID("baf9d9a1-71d7-4e9f-85a5-44f2aa6ec781")

def stable(kind: str, code: str) -> str:
    return str(uuid.uuid5(NAMESPACE, f"{kind}:{code}"))

def q(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"

def json_q(value: object) -> str:
    return q(json.dumps(value, ensure_ascii=False, separators=(",", ":")))

def route_for_trip(code: str) -> str:
    parts = code.split("-")
    if len(parts) >= 2 and parts[0] in {"zeralda", "aga", "thenia", "elaffroun"}:
        # Remove numeric train number and optional a/b suffix.
        if parts and (parts[-1].isdigit() or parts[-1].startswith("B") and parts[-1][1:].isdigit()):
            parts = parts[:-1]
        elif len(parts) >= 2 and parts[-1] in {"a", "b"}:
            parts = parts[:-1]
            if parts and (parts[-1].isdigit() or parts[-1].startswith("B") and parts[-1][1:].isdigit()):
                parts = parts[:-1]
        return "-".join(parts)
    raise ValueError(f"cannot derive route from trip code: {code}")

def line_for_route(route: str) -> str:
    mapping = {
        "zeralda-aga": "line-suburb-zeralda",
        "aga-zeralda": "line-suburb-zeralda",
        "aga-thenia": "line-suburb-thenia",
        "thenia-aga": "line-suburb-thenia",
        "aga-elaffroun": "line-suburb-elaffroun",
        "elaffroun-aga": "line-suburb-elaffroun",
    }
    try:
        return mapping[route]
    except KeyError as exc:
        raise ValueError(f"unsupported route: {route}") from exc

def sql_insert(table: str, columns: str, values: list[str]) -> str:
    return f"INSERT INTO public.{table} ({columns}) VALUES ({', '.join(values)}) ON CONFLICT (id) DO UPDATE SET " + \
        ", ".join(f"{column.strip()}=EXCLUDED.{column.strip()}" for column in columns.split(",")) + ";"

stations_doc = json.loads((REF_DIR / "stations_suburban_provisional.json").read_text(encoding="utf-8"))
trips_doc = json.loads((DATA_DIR / "trip_stops_data.json").read_text(encoding="utf-8"))
stations = stations_doc["stations"]
trips = trips_doc["trips"]
station_by_code = {s["id"]: s for s in stations}
if len(station_by_code) != len(stations):
    raise ValueError("duplicate station ids")

line_names = {
    "line-suburb-zeralda": ("الجزائر–زرالدة", "Alger–Zéralda"),
    "line-suburb-thenia": ("الجزائر–الثنية", "Alger–Thénia"),
    "line-suburb-elaffroun": ("الجزائر–العفرون", "Alger–El Affroun"),
}
route_names = {
    "zeralda-aga": ("INBOUND", "زرالدة–الجزائر"),
    "aga-zeralda": ("OUTBOUND", "الجزائر–زرالدة"),
    "aga-thenia": ("OUTBOUND", "الجزائر–الثنية"),
    "thenia-aga": ("INBOUND", "الثنية–الجزائر"),
    "aga-elaffroun": ("OUTBOUND", "الجزائر–العفرون"),
    "elaffroun-aga": ("INBOUND", "العفرون–الجزائر"),
}

lines_used = {line_for_route(route_for_trip(code)) for code in trips}
missing_by_trip = {
    code: [sid for sid in stop_codes if sid not in station_by_code]
    for code, stop_codes in trips.items()
}
missing_by_trip = {code: missing for code, missing in missing_by_trip.items() if missing}
missing_station_ids = sorted({sid for values in missing_by_trip.values() for sid in values})
(OUT_DIR / "reference_seed_audit.json").write_text(json.dumps({
    "status": "blocked" if missing_by_trip else "ready_for_review",
    "source": trips_doc["meta"],
    "stations_in_source": len(stations),
    "trips_in_source": len(trips),
    "missing_station_ids": missing_station_ids,
    "trips_with_missing_stations": missing_by_trip,
}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
if missing_by_trip:
    raise ValueError(
        "reference source is inconsistent: "
        f"{len(missing_station_ids)} station ids missing from {len(missing_by_trip)} trips; "
        f"see {OUT_DIR / 'reference_seed_audit.json'}"
    )

country_id = stable("country", "DZ")
region_id = stable("region", "algiers")
network_id = stable("network", "algiers-suburban")
source_id = stable("data-source", "sntf-timetable-2026-07-01")

lines_sql = []
for line_id in sorted(lines_used):
    ar, en = line_names[line_id]
    lines_sql.append(sql_insert("railway_lines", "id, network_id, name_ar, name_fr, name_en", [
        q(stable("line", line_id)), q(network_id), q(ar), q(en), q(en)
    ]))

station_sql = []
for s in stations:
    station_sql.append(sql_insert("stations", "id, name_ar, name_fr, name_en, location", [
        q(stable("station", s["id"])), q(s["name_ar"]), q(s["name_fr"]), q(s["name_en"]),
        f"ST_SetSRID(ST_MakePoint({float(s['lon'])},{float(s['lat'])}),4326)",
    ]))

train_sql = []
trip_sql = []
stop_sql = []
manifest = {}
for code, stop_codes in trips.items():
    route = route_for_trip(code)
    line_code = line_for_route(route)
    direction, route_ar = route_names[route]
    train_id = stable("train", code)
    trip_id = stable("trip", code)
    line_id = stable("line", line_code)
    train_sql.append(sql_insert("trains", "id, train_number, line_id", [q(train_id), q(code), q(line_id)]))
    trip_sql.append(sql_insert("trips", "id, train_id, line_id, direction, status", [
        q(trip_id), q(train_id), q(line_id), q(direction), q("SCHEDULED")
    ]))
    for sequence, station_code in enumerate(stop_codes, start=1):
        stop_sql.append(sql_insert("trip_stops", "id, trip_id, station_id, sequence", [
            q(stable("trip-stop", f"{code}:{sequence}")), q(trip_id),
            q(stable("station", station_code)), str(sequence)
        ]))
    manifest[code] = {
        "trip_id": trip_id,
        "train_id": train_id,
        "line_id": line_id,
        "direction": direction,
        "route_label_ar": route_ar,
        "station_count": len(stop_codes),
    }

sql = [
    "-- REVIEW-ONLY development/reference seed; NOT live positions.",
    f"-- Source: {trips_doc['meta']['source']}; source_date={trips_doc['meta']['source_date']}.",
    "-- This file is generated deterministically and has not been applied to Supabase.",
    "BEGIN;",
    sql_insert("countries", "id, name_ar, name_fr, name_en", [q(country_id), q("الجزائر"), q("Algérie"), q("Algeria")]),
    sql_insert("regions", "id, country_id, name_ar, name_fr, name_en", [q(region_id), q(country_id), q("الجزائر العاصمة"), q("Alger"), q("Algiers")]),
    sql_insert("railway_networks", "id, region_id, name_ar, name_fr, name_en", [q(network_id), q(region_id), q("شبكة ضواحي الجزائر"), q("Réseau suburbain d'Alger"), q("Algiers suburban network")]),
    sql_insert("data_sources", "id, source_type, reliability_score, metadata", [q(source_id), q("REFERENCE_TIMETABLE_PROVISIONAL"), "0.5", json_q(trips_doc["meta"])]),
    *lines_sql,
    *station_sql,
    *train_sql,
    *trip_sql,
    *stop_sql,
    "COMMIT;",
    "",
]

(OUT_DIR / "reference_seed.sql").write_text("\n".join(sql), encoding="utf-8")
(OUT_DIR / "reference_seed_manifest.json").write_text(json.dumps({
    "namespace": str(NAMESPACE),
    "source": trips_doc["meta"],
    "provisional": True,
    "stations": len(stations),
    "trips": len(trips),
    "trip_stops": sum(len(v) for v in trips.values()),
    "manifest": manifest,
}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(json.dumps({
    "sql": str(OUT_DIR / "reference_seed.sql"),
    "manifest": str(OUT_DIR / "reference_seed_manifest.json"),
    "stations": len(stations),
    "trips": len(trips),
    "trip_stops": sum(len(v) for v in trips.values()),
    "lines": len(lines_used),
}, ensure_ascii=False))
