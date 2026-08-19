"""Passenger API never exposes raw GPS — only aggregated public state."""
from __future__ import annotations
from dataclasses import dataclass
from .confidence import ConfidenceLevel, Freshness


@dataclass(frozen=True)
class PublicTrainState:
    train_id: str
    trip_id: str
    latitude: float | None
    longitude: float | None
    state: str  # OBSERVED / ESTIMATED / UNKNOWN
    confidence: ConfidenceLevel
    freshness: Freshness
    source_count: int
    last_observed_at: str | None


def safe_public_state(
    train_id: str,
    trip_id: str,
    lat: float | None,
    lon: float | None,
    confidence: ConfidenceLevel,
    freshness: Freshness,
    source_count: int,
    last_observed_at: str | None,
    observed: bool,
) -> PublicTrainState:
    if lat is None or lon is None or confidence == ConfidenceLevel.UNKNOWN:
        return PublicTrainState(
            train_id=train_id,
            trip_id=trip_id,
            latitude=None,
            longitude=None,
            state="UNKNOWN",
            confidence=ConfidenceLevel.UNKNOWN,
            freshness=Freshness.UNKNOWN,
            source_count=0,
            last_observed_at=last_observed_at,
        )
    return PublicTrainState(
        train_id=train_id,
        trip_id=trip_id,
        latitude=lat,
        longitude=lon,
        state="OBSERVED" if observed else "ESTIMATED",
        confidence=confidence,
        freshness=freshness,
        source_count=source_count,
        last_observed_at=last_observed_at,
    )
