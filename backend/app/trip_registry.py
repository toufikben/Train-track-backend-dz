"""Build the public trip index from canonical trip-stop rows and metadata."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


_LINE_ID_MAP: dict[str, str] = {
    "zeralda-aga": "line-suburb-zeralda",
    "aga-zeralda": "line-suburb-zeralda",
    "thenia-aga": "line-suburb-thenia",
    "aga-thenia": "line-suburb-thenia",
    "aga-elaffroun": "line-suburb-elaffroun",
    "elaffroun-aga": "line-suburb-elaffroun",
}


def _route_prefix(trip_id: str) -> str:
    parts = trip_id.split("-")
    # Return variants may end in a single alphabetic suffix (e.g. -a/-b).
    if len(parts) >= 4 and len(parts[-1]) == 1 and parts[-1].isalpha():
        parts = parts[:-1]
    # Drop the numeric or B-series service number.
    if parts and (parts[-1].isdigit() or (parts[-1].startswith("B") and parts[-1][1:].isdigit())):
        parts = parts[:-1]
    return "-".join(parts) if parts else trip_id


def build_trip_registry(
    trip_stops: Mapping[str, Sequence[Any]],
    trip_metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return public trip shells derived only from canonical database rows.

    ``trip_metadata`` comes from ``public.trips`` and is authoritative for
    UUID-based reference seeds.  The route-prefix fallback keeps the helper
    compatible with legacy human-readable IDs and isolated unit tests.
    """
    metadata = trip_metadata or {}
    trips: dict[str, dict[str, Any]] = {}
    for raw_trip_id, stops in trip_stops.items():
        trip_id = str(raw_trip_id)
        if not stops:
            continue
        ordered = sorted(stops, key=lambda stop: int(stop.sequence))
        first, last = ordered[0], ordered[-1]
        meta = metadata.get(trip_id, {})
        raw_line = _route_prefix(trip_id)
        derived = {
            "train_id": trip_id,
            "line_id": _LINE_ID_MAP.get(raw_line, raw_line),
            "direction": "OUTBOUND" if raw_line.startswith("aga-") else "RETURN",
            "scheduled_departure": None,
            "scheduled_arrival": None,
            "status": "SCHEDULED",
        }
        for key in derived:
            value = meta.get(key)
            if value is not None:
                derived[key] = str(value) if key in {"train_id", "line_id", "direction", "status"} else value
        trips[trip_id] = {
            "id": trip_id,
            **derived,
            "stop_count": len(ordered),
            "first_station_id": str(first.station_id),
            "last_station_id": str(last.station_id),
        }
    return trips
