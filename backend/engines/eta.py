"""
PHASE 8 — ETA Engine (Step 25 improvements)

ETA is derived from:
- current aggregated position
- remaining trip stops (sequence after progress)
- segment distances (haversine fallback if no PostGIS length)
- validated speed (with floor/ceiling; Step 25 prefers the median of real
  observed consecutive speeds over the phone's often-stale reported speed)
- optional dwell at intermediate stations
- confidence / freshness degrade the reported range

Never invent ETA when position is UNKNOWN or confidence is UNKNOWN.
"""
from __future__ import annotations
from dataclasses import dataclass
from math import radians, sin, cos, atan2, sqrt
from statistics import median
from typing import Sequence


@dataclass(frozen=True)
class StopGeo:
    station_id: str
    sequence: int
    latitude: float
    longitude: float
    dwell_seconds: float = 60.0  # default dwell; tune per station later


@dataclass(frozen=True)
class EtaInput:
    latitude: float
    longitude: float
    speed_mps: float | None
    progress_sequence: int
    stops: Sequence[StopGeo]
    confidence: str  # HIGH/MEDIUM/LOW/UNKNOWN
    freshness: str   # LIVE/RECENT/AGING/STALE/UNKNOWN
    target_station_id: str | None = None  # None = next stop


@dataclass(frozen=True)
class EtaResult:
    station_id: str | None
    eta_min_seconds: int | None
    eta_max_seconds: int | None
    distance_m: float | None
    confidence: str
    reason: str


MIN_SPEED_MPS = 5.0    # floor when moving slowly / unknown
MAX_SPEED_MPS = 40.0   # ~144 km/h ceiling for suburban
DEFAULT_SPEED_MPS = 16.7  # ~60 km/h nominal


def _haversine_m(a_lat, a_lon, b_lat, b_lon) -> float:
    r = 6371000.0
    p1, p2 = radians(a_lat), radians(b_lat)
    dp = radians(b_lat - a_lat)
    dl = radians(b_lon - a_lon)
    x = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r * atan2(sqrt(x), sqrt(max(0.0, 1.0 - x)))


def _path_distance_m(lat: float, lon: float, remaining: Sequence[StopGeo]) -> float:
    if not remaining:
        return 0.0
    total = _haversine_m(lat, lon, remaining[0].latitude, remaining[0].longitude)
    for i in range(len(remaining) - 1):
        a, b = remaining[i], remaining[i + 1]
        total += _haversine_m(a.latitude, a.longitude, b.latitude, b.longitude)
    return total


def _dwell_seconds(remaining: Sequence[StopGeo], exclude_final: bool = True) -> float:
    if len(remaining) <= 1:
        return 0.0
    stops = remaining[:-1] if exclude_final else remaining
    return sum(s.dwell_seconds for s in stops)


def _effective_speed(x: EtaInput, observed_speeds_mps: Sequence[float] | None = None) -> float:
    """Step 25 — speed truth chain.

    Inside a moving train the phone's reported speed is frequently zero or
    stale (GPS throttling / indoor). A list of real consecutive observed
    speeds (derived by the pipeline from accepted observation pairs) is
    more truthful: we take its median when available, otherwise fall back
    to the aggregate speed or the nominal default.
    """
    if observed_speeds_mps:
        real = [s for s in observed_speeds_mps if s is not None and s > 1.0]
        if real:
            raw_speed = median(real)
        elif x.speed_mps is not None and x.speed_mps > 1.0:
            raw_speed = x.speed_mps
        else:
            raw_speed = DEFAULT_SPEED_MPS
    else:
        raw_speed = x.speed_mps if x.speed_mps and x.speed_mps > 1.0 else DEFAULT_SPEED_MPS
    return max(MIN_SPEED_MPS, min(MAX_SPEED_MPS, raw_speed))


def estimate(x: EtaInput, observed_speeds_mps: Sequence[float] | None = None) -> EtaResult:
    """`observed_speeds_mps` is optional and new in Step 25; callers that omit
    it keep the previous behaviour exactly."""
    if x.confidence == "UNKNOWN" or x.freshness == "UNKNOWN":
        return EtaResult(None, None, None, None, "UNKNOWN", "insufficient_truth")

    ordered = sorted(x.stops, key=lambda s: s.sequence)
    remaining = [s for s in ordered if s.sequence > x.progress_sequence]
    if x.target_station_id:
        remaining = [s for s in ordered if s.station_id == x.target_station_id or (
            remaining and s.sequence >= remaining[0].sequence and s.sequence <= next(
                (t.sequence for t in ordered if t.station_id == x.target_station_id), -1
            )
        )]
        # simplify: remaining from progress to target inclusive path
        target_seq = next((s.sequence for s in ordered if s.station_id == x.target_station_id), None)
        if target_seq is None:
            return EtaResult(None, None, None, None, "UNKNOWN", "unknown_target")
        remaining = [s for s in ordered if x.progress_sequence < s.sequence <= target_seq]

    if not remaining:
        # already at or past last stop
        last = ordered[-1] if ordered else None
        return EtaResult(
            last.station_id if last else None,
            0, 0, 0.0,
            x.confidence,
            "arrived_or_past"
        )

    target = remaining[-1] if x.target_station_id else remaining[0]
    distance = _path_distance_m(x.latitude, x.longitude, remaining)
    dwell = _dwell_seconds(remaining, exclude_final=True)

    speed = _effective_speed(x, observed_speeds_mps)

    base = distance / speed + dwell

    # Widen range when confidence/freshness weaker
    if x.confidence == "HIGH" and x.freshness in ("LIVE", "RECENT"):
        lo, hi = 0.85, 1.15
        conf = "HIGH"
    elif x.confidence == "MEDIUM":
        lo, hi = 0.70, 1.40
        conf = "MEDIUM"
    else:
        lo, hi = 0.55, 1.70
        conf = "LOW"

    if x.freshness in ("AGING", "STALE"):
        lo *= 0.9
        hi *= 1.25
        if conf == "HIGH":
            conf = "MEDIUM"

    return EtaResult(
        station_id=target.station_id,
        eta_min_seconds=max(0, int(base * lo)),
        eta_max_seconds=max(0, int(base * hi)),
        distance_m=distance,
        confidence=conf,
        reason="ok",
    )
