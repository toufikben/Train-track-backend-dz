"""Step 25 — same-line relative train ordering.

Rules (derived only from real aggregates, never fabricated):
- Trains on the same line are ranked per-line: 1 = leader (first to arrive
  at its next stop).
- A train with a real ETA beats a train without one on the same line.
- Unknown-truth aggregates are excluded from ranking.
- Different lines are ranked independently.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
from main import _line_order_rank


def _agg(trip_id, train_id, truth, next_station_id, eta_min, line_id):
    a = MagicMock()
    a.trip_id = trip_id
    a.train_id = train_id
    a.truth = truth
    a.next_station_id = next_station_id
    a.eta_min_sec = eta_min
    a.eta_station_id = next_station_id if eta_min is not None else None
    a.latitude = a.longitude = 0.0
    return (train_id, a, line_id)


def make_store(pairs):
    """pairs: (train_id, line_id, trip_id, stops_for_trip)."""
    store = MagicMock()
    store.trips = {}
    store.trip_stops = {}
    for train_id, line_id, trip_id, stops in pairs:
        store.trips[train_id] = {"id": train_id, "line_id": line_id}
        store.trip_stops[trip_id] = stops
    return store


def run():
    import main as m

    # Line A: 3 trains; two with ETA (120s, 300s), one without (progress 3)
    agg_a1 = MagicMock()
    agg_a1.trip_id = "aga-zer-1"
    agg_a1.train_id = "aga-zer-1"
    agg_a1.truth = "OBSERVED"
    agg_a1.next_station_id = "st2"
    agg_a1.eta_min_sec = 300
    agg_a1.eta_station_id = "st2"
    agg_a2 = MagicMock()
    agg_a2.trip_id = "aga-zer-2"
    agg_a2.train_id = "aga-zer-2"
    agg_a2.truth = "OBSERVED"
    agg_a2.next_station_id = "st2"
    agg_a2.eta_min_sec = 120
    agg_a2.eta_station_id = "st2"
    agg_a3 = MagicMock()
    agg_a3.trip_id = "aga-zer-3"
    agg_a3.train_id = "aga-zer-3"
    agg_a3.truth = "OBSERVED"
    agg_a3.next_station_id = "st3"
    agg_a3.eta_min_sec = None
    agg_a3.eta_station_id = None

    # Line B: single train
    agg_b = MagicMock()
    agg_b.trip_id = "aga-ala-1"
    agg_b.train_id = "aga-ala-1"
    agg_b.truth = "ESTIMATED"
    agg_b.next_station_id = "st1"
    agg_b.eta_min_sec = 60
    agg_b.eta_station_id = "st1"

    # UNKNOWN train on line A must be excluded
    agg_unk = MagicMock()
    agg_unk.trip_id = "aga-zer-9"
    agg_unk.train_id = "aga-zer-9"
    agg_unk.truth = "UNKNOWN"
    agg_unk.next_station_id = None
    agg_unk.eta_min_sec = None
    agg_unk.eta_station_id = None

    aggregates = {
        "aga-zer-1": agg_a1, "aga-zer-2": agg_a2, "aga-zer-3": agg_a3,
        "aga-ala-1": agg_b, "aga-zer-9": agg_unk,
    }

    stops3 = [MagicMock(sequence=1, station_id="st1"),
              MagicMock(sequence=2, station_id="st2"),
              MagicMock(sequence=3, station_id="st3")]
    store = make_store([
        ("aga-zer-1", "line_zeralda", "aga-zer-1", stops3),
        ("aga-zer-2", "line_zeralda", "aga-zer-2", stops3),
        ("aga-zer-3", "line_zeralda", "aga-zer-3", stops3),
        ("aga-ala-1", "line_thania", "aga-ala-1", [MagicMock(sequence=1, station_id="st1")]),
        ("aga-zer-9", "line_zeralda", "aga-zer-9", stops3),
    ])

    # Monkey-patch module-level store used by _line_order_rank
    orig = m.store
    m.store = store
    try:
        ranks = _line_order_rank(aggregates)
    finally:
        m.store = orig

    # Line A: a2 (120s) leader, a1 (300s) second, a3 (no ETA) last
    assert ranks["aga-zer-2"] == 1, ranks
    assert ranks["aga-zer-1"] == 2, ranks
    assert ranks["aga-zer-3"] == 3, ranks
    # Line B: alone = leader of its line
    assert ranks["aga-ala-1"] == 1, ranks
    # Unknown must be absent
    assert "aga-zer-9" not in ranks, ranks
    print("  ranks:", ranks)
    print("line_order tests OK")


if __name__ == "__main__":
    run()
