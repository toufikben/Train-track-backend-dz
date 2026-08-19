"""Multi-monitor spatial consensus. Outliers do not move the public marker."""
from __future__ import annotations
from dataclasses import dataclass
from math import hypot
from statistics import median
from typing import Iterable


@dataclass(frozen=True)
class ValidatedObservation:
    observation_id: str
    trip_id: str
    latitude: float
    longitude: float
    accuracy_m: float
    speed_mps: float | None
    heading_deg: float | None
    observed_at_epoch: float
    validation_score: float
    monitor_reliability: float


@dataclass(frozen=True)
class Aggregate:
    trip_id: str
    latitude: float
    longitude: float
    estimated_speed_mps: float | None
    heading_deg: float | None
    confidence_score: float
    source_count: int
    last_observed_at_epoch: float


def _weight(o: ValidatedObservation) -> float:
    accuracy = max(0.05, min(1.0, 1.0 - o.accuracy_m / 150.0))
    reliability = max(0.05, min(1.0, o.monitor_reliability))
    validation = max(0.05, min(1.0, o.validation_score))
    return accuracy * reliability * validation


def _weighted_mean(values, weights):
    total = sum(weights)
    return sum(v * w for v, w in zip(values, weights)) / total if total else 0.0


def aggregate(
    observations: Iterable[ValidatedObservation],
    max_cluster_distance_deg: float = 0.0015,
) -> Aggregate | None:
    obs = list(observations)
    if not obs:
        return None
    obs.sort(key=lambda x: x.observed_at_epoch, reverse=True)
    anchor = obs[0]
    cluster = [
        o for o in obs
        if hypot(o.latitude - anchor.latitude, o.longitude - anchor.longitude)
        <= max_cluster_distance_deg
    ]
    if not cluster:
        return None
    weights = [_weight(o) for o in cluster]
    lat = _weighted_mean([o.latitude for o in cluster], weights)
    lon = _weighted_mean([o.longitude for o in cluster], weights)
    speeds = [o.speed_mps for o in cluster if o.speed_mps is not None]
    headings = [o.heading_deg for o in cluster if o.heading_deg is not None]
    agreement = len(cluster) / max(1, len(obs))
    mean_quality = sum(weights) / len(weights)
    confidence = max(0.0, min(1.0, 0.55 * agreement + 0.45 * mean_quality))
    return Aggregate(
        trip_id=anchor.trip_id,
        latitude=lat,
        longitude=lon,
        estimated_speed_mps=median(speeds) if speeds else None,
        heading_deg=median(headings) if headings else None,
        confidence_score=confidence,
        source_count=len(cluster),
        last_observed_at_epoch=max(o.observed_at_epoch for o in cluster),
    )
