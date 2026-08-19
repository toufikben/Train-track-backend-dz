"""Confidence + Freshness vocabulary for public train state."""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from math import exp


class ConfidenceLevel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"


class Freshness(str, Enum):
    LIVE = "LIVE"
    RECENT = "RECENT"
    AGING = "AGING"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ConfidenceInput:
    observation_quality: float
    monitor_agreement: float
    route_match: float
    trip_match: float
    speed_consistency: float
    heading_consistency: float
    source_count: int
    age_seconds: float
    report_support: float = 0.0
    conflict_penalty: float = 0.0


def freshness(age_seconds: float) -> Freshness:
    if age_seconds < 0:
        return Freshness.UNKNOWN
    if age_seconds <= 60:
        return Freshness.LIVE
    if age_seconds <= 300:
        return Freshness.RECENT
    if age_seconds <= 900:
        return Freshness.AGING
    if age_seconds <= 1800:
        return Freshness.STALE
    return Freshness.UNKNOWN


def calculate(x: ConfidenceInput) -> tuple[float, ConfidenceLevel, Freshness]:
    f = freshness(x.age_seconds)
    if f == Freshness.UNKNOWN:
        return 0.0, ConfidenceLevel.UNKNOWN, f

    quality = max(0.0, min(1.0, x.observation_quality))
    agreement = max(0.0, min(1.0, x.monitor_agreement))
    route = max(0.0, min(1.0, x.route_match))
    trip = max(0.0, min(1.0, x.trip_match))
    speed = max(0.0, min(1.0, x.speed_consistency))
    heading = max(0.0, min(1.0, x.heading_consistency))
    reports = max(0.0, min(1.0, x.report_support))
    conflict = max(0.0, min(1.0, x.conflict_penalty))

    score = (
        0.22 * quality
        + 0.20 * agreement
        + 0.16 * route
        + 0.12 * trip
        + 0.10 * speed
        + 0.08 * heading
        + 0.07 * reports
        + 0.05 * min(1.0, x.source_count / 3.0)
    )
    score *= exp(-x.age_seconds / 1200.0)
    score *= (1.0 - 0.65 * conflict)
    score = max(0.0, min(1.0, score))

    # Single source cannot claim HIGH
    if x.source_count < 2:
        score = min(score, 0.69)

    if score >= 0.75 and f in (Freshness.LIVE, Freshness.RECENT):
        level = ConfidenceLevel.HIGH
    elif score >= 0.45 and f != Freshness.UNKNOWN:
        level = ConfidenceLevel.MEDIUM
    elif score > 0:
        level = ConfidenceLevel.LOW
    else:
        level = ConfidenceLevel.UNKNOWN
    return score, level, f
