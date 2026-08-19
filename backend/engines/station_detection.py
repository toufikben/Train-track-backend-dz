"""
PHASE 7 — Station Detection

Detect previous / current / next station and ARRIVING / AT_STATION / DEPARTED
from an aggregated train position + ordered trip stops.

Rules:
- Prefer trip stop sequence over global nearest station (avoids wrong branch).
- Hysteresis prevents flapping between states.
- Low-confidence aggregates must not publish public station events.
- Short dwell at station must not look like trip end.
"""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from math import radians, sin, cos, atan2, sqrt
from typing import Sequence


class StationEventType(str, Enum):
    ARRIVING = "ARRIVING"
    AT_STATION = "AT_STATION"
    DEPARTED = "DEPARTED"


@dataclass(frozen=True)
class TripStopRef:
    station_id: str
    sequence: int
    latitude: float
    longitude: float
    name_ar: str = ""


@dataclass(frozen=True)
class AggregatePosition:
    trip_id: str
    train_id: str
    latitude: float
    longitude: float
    speed_mps: float | None
    confidence: str  # HIGH / MEDIUM / LOW / UNKNOWN
    source_count: int


@dataclass(frozen=True)
class StationContext:
    previous: TripStopRef | None
    current_or_nearest: TripStopRef | None
    next: TripStopRef | None
    distance_to_nearest_m: float | None
    progress_index: int
    event: StationEventType | None
    publishable: bool


ARRIVING_RADIUS_M = 800.0
AT_STATION_RADIUS_M = 120.0
DEPARTED_MIN_SPEED_MPS = 2.5
MIN_CONFIDENCE_TO_PUBLISH = {"HIGH", "MEDIUM"}


def _haversine_m(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    r = 6371000.0
    p1, p2 = radians(a_lat), radians(b_lat)
    dp = radians(b_lat - a_lat)
    dl = radians(b_lon - a_lon)
    x = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r * atan2(sqrt(x), sqrt(max(0.0, 1.0 - x)))


def _nearest_along_trip(
    lat: float, lon: float, stops: Sequence[TripStopRef]
) -> tuple[TripStopRef | None, float | None, int]:
    if not stops:
        return None, None, -1
    ordered = sorted(stops, key=lambda s: s.sequence)
    best = ordered[0]
    best_d = _haversine_m(lat, lon, best.latitude, best.longitude)
    best_i = 0
    for i, s in enumerate(ordered):
        d = _haversine_m(lat, lon, s.latitude, s.longitude)
        if d < best_d:
            best, best_d, best_i = s, d, i
    return best, best_d, best_i


def detect(
    position: AggregatePosition,
    stops: Sequence[TripStopRef],
    previous_event: StationEventType | None = None,
    previous_station_id: str | None = None,
) -> StationContext:
    ordered = sorted(stops, key=lambda s: s.sequence)
    nearest, dist, idx = _nearest_along_trip(
        position.latitude, position.longitude, ordered
    )
    prev = ordered[idx - 1] if idx > 0 else None
    nxt = ordered[idx + 1] if 0 <= idx < len(ordered) - 1 else None
    publishable = position.confidence in MIN_CONFIDENCE_TO_PUBLISH and position.source_count >= 1

    event: StationEventType | None = None
    if nearest is not None and dist is not None and publishable:
        speed = position.speed_mps
        at_station = dist <= AT_STATION_RADIUS_M
        arriving = dist <= ARRIVING_RADIUS_M and not at_station
        moving = speed is not None and speed >= DEPARTED_MIN_SPEED_MPS

        if at_station:
            if previous_event == StationEventType.AT_STATION and previous_station_id == nearest.station_id:
                if moving and dist > AT_STATION_RADIUS_M * 0.6:
                    event = StationEventType.DEPARTED
                else:
                    event = StationEventType.AT_STATION
            else:
                event = StationEventType.AT_STATION
        elif arriving:
            event = StationEventType.ARRIVING
        elif previous_event == StationEventType.AT_STATION and previous_station_id == nearest.station_id and moving:
            event = StationEventType.DEPARTED
        elif previous_event == StationEventType.DEPARTED and previous_station_id == nearest.station_id:
            event = StationEventType.DEPARTED

    return StationContext(
        previous=prev,
        current_or_nearest=nearest,
        next=nxt,
        distance_to_nearest_m=dist,
        progress_index=ordered[idx].sequence if idx >= 0 else -1,
        event=event,
        publishable=publishable,
    )
