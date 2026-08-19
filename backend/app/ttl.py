"""Evict public aggregates that are no longer fresh enough to publish."""
from __future__ import annotations
from datetime import datetime, timezone

from .store import MemoryStore, AggregateRow

# After this age (seconds) public state becomes UNKNOWN (removed)
DEFAULT_MAX_AGE_SECONDS = 15 * 60  # 15 minutes


def age_seconds(row: AggregateRow, now: datetime | None = None) -> float:
    now = now or datetime.now(timezone.utc)
    return max(0.0, (now - row.last_observed_at).total_seconds())


def should_publish(row: AggregateRow, max_age: float = DEFAULT_MAX_AGE_SECONDS) -> bool:
    if row.truth == "UNKNOWN":
        return False
    if row.confidence == "UNKNOWN":
        return False
    if age_seconds(row) > max_age:
        return False
    return True


def evict_stale(store: MemoryStore, max_age: float = DEFAULT_MAX_AGE_SECONDS) -> list[str]:
    removed: list[str] = []
    for trip_id, row in list(store.aggregates.items()):
        if not should_publish(row, max_age):
            del store.aggregates[trip_id]
            removed.append(trip_id)
    return removed
