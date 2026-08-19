from __future__ import annotations
from datetime import datetime, timezone
from typing import Any
from .store import MemoryStore
from .ttl import age_seconds, should_publish

def health_snapshot(store: MemoryStore) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    aggregates = list(store.aggregates.values())
    publishable = [a for a in aggregates if should_publish(a)]
    return {
        "generated_at": now.isoformat(),
        "stations_reference_count": len(store.stations),
        "active_sessions": sum(1 for s in store.sessions.values() if s.status in ("STARTING", "ACTIVE")),
        "ended_sessions": sum(1 for s in store.sessions.values() if s.status == "ENDED"),
        "observations_total": len(store.observations),
        "observations_accepted": sum(1 for o in store.observations if o.accepted),
        "observations_rejected": sum(1 for o in store.observations if not o.accepted),
        "aggregates_total": len(aggregates),
        "aggregates_publishable": len(publishable),
        "reports_total": len(store.reports),
        "trip_stops_registered": len(store.trip_stops),
        "publishable_trips": [
            {
                "trip_id": a.trip_id,
                "train_id": a.train_id,
                "truth": a.truth,
                "confidence": a.confidence,
                "freshness": a.freshness,
                "age_seconds": int(age_seconds(a, now)),
                "source_count": a.source_count,
            }
            for a in publishable
        ],
    }
