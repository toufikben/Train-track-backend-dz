"""Server-side observation validation. Client is not trusted."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from math import isfinite, radians, sin, cos, atan2, sqrt


@dataclass(frozen=True)
class Observation:
    observation_id: str
    session_id: str
    trip_id: str
    train_id: str
    latitude: float
    longitude: float
    accuracy_m: float
    speed_mps: float | None
    heading_deg: float | None
    observed_at: datetime


@dataclass(frozen=True)
class ValidationContext:
    railway_distance_m: float | None
    route_match_score: float
    trip_match_score: float
    expected_heading_deg: float | None
    previous: Observation | None


@dataclass(frozen=True)
class ValidationResult:
    accepted: bool
    reason: str
    score: float


def _distance_m(a_lat, a_lon, b_lat, b_lon):
    r = 6371000.0
    p1, p2 = radians(a_lat), radians(b_lat)
    dp = radians(b_lat - a_lat)
    dl = radians(b_lon - a_lon)
    x = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r * atan2(sqrt(x), sqrt(max(0.0, 1 - x)))


def validate(o: Observation, c: ValidationContext) -> ValidationResult:
    if not (-90 <= o.latitude <= 90 and -180 <= o.longitude <= 180):
        return ValidationResult(False, "invalid_coordinates", 0.0)
    if not isfinite(o.accuracy_m) or o.accuracy_m < 0 or o.accuracy_m > 250:
        return ValidationResult(False, "poor_accuracy", 0.0)
    if o.speed_mps is not None and (not isfinite(o.speed_mps) or o.speed_mps < 0 or o.speed_mps > 120):
        return ValidationResult(False, "impossible_speed", 0.0)
    if o.heading_deg is not None and not (0 <= o.heading_deg < 360):
        return ValidationResult(False, "invalid_heading", 0.0)
    if c.railway_distance_m is not None and c.railway_distance_m > 500:
        return ValidationResult(False, "far_from_railway", 0.0)
    if c.route_match_score < 0.20 or c.trip_match_score < 0.20:
        return ValidationResult(False, "route_or_trip_mismatch", 0.0)

    if c.previous is not None:
        dt = (o.observed_at - c.previous.observed_at).total_seconds()
        if dt <= 0:
            return ValidationResult(False, "replay_or_out_of_order", 0.0)
        d = _distance_m(c.previous.latitude, c.previous.longitude, o.latitude, o.longitude)
        if d / dt > 120:
            return ValidationResult(False, "teleportation", 0.0)

    accuracy_score = max(0.0, min(1.0, 1.0 - o.accuracy_m / 150.0))
    score = (
        0.30 * accuracy_score
        + 0.30 * max(0.0, min(1.0, c.route_match_score))
        + 0.25 * max(0.0, min(1.0, c.trip_match_score))
        + 0.15 * 1.0
    )
    return ValidationResult(True, "accepted_for_aggregation", score)
