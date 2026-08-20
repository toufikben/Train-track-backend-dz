"""Observation → Validation → Aggregation → Confidence → Station → ETA → Wait."""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path

# engines on path
ENG = Path(__file__).resolve().parent.parent / "engines"
if str(ENG) not in sys.path:
    sys.path.insert(0, str(ENG))

from observation_validation import Observation, ValidationContext, validate
from aggregation import ValidatedObservation, aggregate
from confidence import ConfidenceInput, calculate as calc_confidence, freshness as calc_freshness
from station_detection import (
    AggregatePosition as SDPos,
    TripStopRef,
    detect as detect_station,
)
import math as _math
from eta import EtaInput, StopGeo as EtaStop, estimate as estimate_eta
from wait_decision import WaitInput, decide as decide_wait
from route_match import min_distance_to_segments_m, route_match_score

from .store import MemoryStore, ObservationRow, AggregateRow, utcnow


def _parse_ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def process_observation(
    store: MemoryStore,
    *,
    observation_id: str,
    session_id: str,
    trip_id: str,
    train_id: str,
    latitude: float,
    longitude: float,
    accuracy: float,
    speed: float | None,
    heading: float | None,
    timestamp_ms: int,
) -> dict:
    observed_at = _parse_ts(timestamp_ms)

    # Previous accepted observation for this session
    prev = None
    for o in reversed(store.observations):
        if o.session_id == session_id and o.accepted:
            prev = Observation(
                observation_id=o.id,
                session_id=o.session_id,
                trip_id=o.trip_id,
                train_id=o.train_id,
                latitude=o.latitude,
                longitude=o.longitude,
                accuracy_m=o.accuracy,
                speed_mps=o.speed,
                heading_deg=o.heading,
                observed_at=o.observed_at,
            )
            break

    obs = Observation(
        observation_id=observation_id,
        session_id=session_id,
        trip_id=trip_id,
        train_id=train_id,
        latitude=latitude,
        longitude=longitude,
        accuracy_m=accuracy,
        speed_mps=speed,
        heading_deg=heading,
        observed_at=observed_at,
    )
    rail_dist = min_distance_to_segments_m(latitude, longitude, store.railway_segments)
    r_score = route_match_score(rail_dist)
    ctx = ValidationContext(
        railway_distance_m=rail_dist if rail_dist is not None else 80.0,
        route_match_score=r_score if rail_dist is not None else 0.5,
        trip_match_score=0.75,
        expected_heading_deg=None,
        previous=prev,
    )
    result = validate(obs, ctx)

    row = ObservationRow(
        id=observation_id,
        session_id=session_id,
        trip_id=trip_id,
        train_id=train_id,
        latitude=latitude,
        longitude=longitude,
        accuracy=accuracy,
        speed=speed,
        heading=heading,
        observed_at=observed_at,
        accepted=result.accepted,
        rejection_reason=None if result.accepted else result.reason,
        validation_score=result.score,
    )
    store.observations.append(row)
    if getattr(store, "insert_observation", None):
        store.insert_observation(row)

    if session_id in store.sessions:
        sess = store.sessions[session_id]
        sess.last_observation_at = observed_at
        if sess.status == "STARTING":
            sess.status = "ACTIVE"

    if not result.accepted:
        return {
            "accepted": False,
            "reason": result.reason,
            "aggregate": None,
        }

    # Aggregate recent accepted observations for this trip (last 10 min)
    cutoff = observed_at.timestamp() - 600
    validated = []
    for o in store.observations:
        if not o.accepted or o.trip_id != trip_id:
            continue
        if o.observed_at.timestamp() < cutoff:
            continue
        validated.append(
            ValidatedObservation(
                observation_id=o.id,
                trip_id=o.trip_id,
                latitude=o.latitude,
                longitude=o.longitude,
                accuracy_m=o.accuracy,
                speed_mps=o.speed,
                heading_deg=o.heading,
                observed_at_epoch=o.observed_at.timestamp(),
                validation_score=o.validation_score,
                monitor_reliability=0.8,
            )
        )

    agg = aggregate(validated)
    if agg is None:
        return {"accepted": True, "reason": "accepted_no_aggregate", "aggregate": None}

    age = max(0.0, utcnow().timestamp() - agg.last_observed_at_epoch)
    conf_score, conf_level, fresh = calc_confidence(
        ConfidenceInput(
            observation_quality=agg.confidence_score,
            monitor_agreement=min(1.0, agg.source_count / 3.0),
            route_match=r_score if rail_dist is not None else 0.5,
            trip_match=0.75,
            speed_consistency=0.7,
            heading_consistency=0.7,
            source_count=agg.source_count,
            age_seconds=age,
        )
    )

    # Station detection if we have stops for trip
    stops = store.trip_stops.get(trip_id, [])
    next_id = next_name = station_event = None
    progress_seq = 0
    if stops:
        refs = [
            TripStopRef(s.station_id, s.sequence, s.latitude, s.longitude, s.station_name)
            for s in stops
        ]
        sd = detect_station(
            SDPos(
                trip_id=trip_id,
                train_id=train_id,
                latitude=agg.latitude,
                longitude=agg.longitude,
                speed_mps=agg.estimated_speed_mps,
                confidence=conf_level.value,
                source_count=agg.source_count,
            ),
            refs,
        )
        if sd.current_or_nearest:
            progress_seq = sd.progress_index
        if sd.next:
            next_id = sd.next.station_id
            next_name = sd.next.name_ar
        if sd.publishable and sd.event:
            station_event = sd.event.value

    eta_station = eta_min = eta_max = eta_conf = None
    wait_dec = wait_reason = None
    if conf_level.value != "UNKNOWN" and fresh.value != "UNKNOWN":
        eta_stops = [
            EtaStop(s.station_id, s.sequence, s.latitude, s.longitude)
            for s in stops
        ] if stops else []
        if eta_stops:
            # Step 25 — real observed speeds from consecutive accepted
            # observations beat the phone's often-stale reported speed.
            observed_speeds: list[float] = []
            for i in range(len(validated) - 1):
                newer, older = validated[i], validated[i + 1]
                dt = newer.observed_at_epoch - older.observed_at_epoch
                if dt <= 0:
                    continue
                d_lat = (newer.latitude - older.latitude) * 111_320.0
                d_lon = (newer.longitude - older.longitude) * 111_320.0 * _math.cos(
                    _math.radians((newer.latitude + older.latitude) / 2.0)
                )
                s = (d_lat * d_lat + d_lon * d_lon) ** 0.5 / dt
                if 1.0 < s <= 40.0:
                    observed_speeds.append(s)
            er = estimate_eta(
                EtaInput(
                    latitude=agg.latitude,
                    longitude=agg.longitude,
                    speed_mps=agg.estimated_speed_mps,
                    progress_sequence=progress_seq,
                    stops=eta_stops,
                    confidence=conf_level.value,
                    freshness=fresh.value,
                ),
                observed_speeds_mps=observed_speeds,
            )
            eta_station = er.station_id
            eta_min = er.eta_min_seconds
            eta_max = er.eta_max_seconds
            eta_conf = er.confidence
            wr = decide_wait(
                WaitInput(
                    eta_min_seconds=eta_min,
                    eta_max_seconds=eta_max,
                    confidence=conf_level.value,
                    freshness=fresh.value,
                    station_event=station_event,
                )
            )
            wait_dec = wr.decision.value
            wait_reason = wr.reason_ar

    truth = "OBSERVED" if agg.source_count >= 1 and conf_level.value != "UNKNOWN" else "UNKNOWN"
    if truth == "UNKNOWN":
        # Do not publish coordinates when unknown
        if trip_id in store.aggregates:
            del store.aggregates[trip_id]
        if getattr(store, "upsert_aggregate", None):
            store.upsert_aggregate(None, trip_id)
        return {"accepted": True, "reason": "accepted_but_unknown_public", "aggregate": None}

    public = AggregateRow(
        trip_id=trip_id,
        train_id=train_id,
        latitude=agg.latitude,
        longitude=agg.longitude,
        speed_mps=agg.estimated_speed_mps,
        heading_deg=agg.heading_deg,
        confidence=conf_level.value,
        confidence_score=conf_score,
        freshness=fresh.value,
        source_count=agg.source_count,
        last_observed_at=datetime.fromtimestamp(agg.last_observed_at_epoch, tz=timezone.utc),
        last_estimated_at=utcnow(),
        truth=truth,
        next_station_id=next_id,
        next_station_name_ar=next_name,
        station_event=station_event,
        eta_station_id=eta_station,
        eta_min_sec=eta_min,
        eta_max_sec=eta_max,
        eta_confidence=eta_conf,
        wait_decision=wait_dec,
        wait_reason_ar=wait_reason,
    )
    store.aggregates[trip_id] = public
    if getattr(store, "upsert_aggregate", None):
        store.upsert_aggregate(public, trip_id)
    return {
        "accepted": True,
        "reason": "accepted_and_aggregated",
        "aggregate": {
            "trip_id": trip_id,
            "train_id": train_id,
            "latitude": public.latitude,
            "longitude": public.longitude,
            "truth": public.truth,
            "confidence": public.confidence,
            "freshness": public.freshness,
            "source_count": public.source_count,
            "eta_min_sec": public.eta_min_sec,
            "eta_max_sec": public.eta_max_sec,
            "wait_decision": public.wait_decision,
        },
    }
