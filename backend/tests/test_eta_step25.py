"""Step 25 — ETA engine: real observed speeds preference.

Rules (never fabricated):
- With real consecutive observed speeds inside the plausible band,
  the ETA engine must use their median, not the stale phone speed.
- When all observed speeds are implausible, fall back to the aggregate
  speed then the nominal default.
- When no observed speeds are provided, behaviour must be identical to
  the pre-Step-25 implementation.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "engines"))
from eta import EtaInput, StopGeo, estimate, MIN_SPEED_MPS, MAX_SPEED_MPS

# A simple two-stop trip: train is 1000 m from the next station.
STOPS = [
    StopGeo("st1", 1, 36.75, 3.05),
    StopGeo("st2", 2, 36.759, 3.05),  # ~1000 m north of st1
]

BASE = EtaInput(
    latitude=36.75,
    longitude=3.05,
    speed_mps=None,        # phone reports nothing (stale)
    progress_sequence=1,
    stops=STOPS,
    confidence="HIGH",
    freshness="LIVE",
)


def run():
    results = []

    # 1. Without observed speeds: nominal default (~16.7 m/s) -> ~60-71 s
    r0 = estimate(BASE)
    assert r0.eta_min_seconds is not None and 40 <= r0.eta_min_seconds <= 90, r0
    results.append(("default_speed", r0.eta_min_seconds, r0.eta_max_seconds))

    # 2. Real consecutive speeds (train actually at 25 m/s = 90 km/h):
    #    base time = 1000/25 = 40 s -> min ~34, max ~46
    r1 = estimate(BASE, observed_speeds_mps=[24.0, 26.0, 25.0])
    assert r1.eta_min_seconds is not None and 30 <= r1.eta_min_seconds <= 60, r1
    assert r1.eta_min_seconds < r0.eta_min_seconds, "real speed must shorten ETA"
    results.append(("real_90kmh", r1.eta_min_seconds, r1.eta_max_seconds))

    # 3. Slow real speeds (10 m/s = 36 km/h): ETA longer than default
    r2 = estimate(BASE, observed_speeds_mps=[9.5, 10.5, 10.0])
    assert r2.eta_min_seconds > r0.eta_min_seconds, "slower real speed must lengthen ETA"
    results.append(("real_36kmh", r2.eta_min_seconds, r2.eta_max_seconds))

    # 4. Implausible observed speeds (zero / near-zero) must be ignored -> default.
    # (speeds > MAX are still clamped to 40 m/s by the engine, so use only
    # sub-1.0 values to test the pure fallback path.)
    r3 = estimate(BASE, observed_speeds_mps=[0.0, 0.5, 0.9])
    assert r3.eta_min_seconds == r0.eta_min_seconds, "implausible speeds must fall back"
    results.append(("implausible", r3.eta_min_seconds, r3.eta_max_seconds))

    # 5. UNKNOWN confidence must never invent an ETA
    unknown = BASE.replace(confidence="UNKNOWN") if hasattr(BASE, "replace") else EtaInput(
        latitude=36.75, longitude=3.05, speed_mps=None, progress_sequence=1,
        stops=STOPS, confidence="UNKNOWN", freshness="LIVE",
    )
    r4 = estimate(unknown, observed_speeds_mps=[25.0])
    assert r4.eta_min_seconds is None and r4.confidence == "UNKNOWN", r4
    results.append(("unknown_no_eta", "NONE", "NONE"))

    # 6. Already past last stop returns arrived
    past = EtaInput(latitude=36.75, longitude=3.05, speed_mps=None,
                    progress_sequence=2, stops=STOPS, confidence="HIGH", freshness="LIVE")
    r5 = estimate(past, observed_speeds_mps=[25.0])
    assert r5.eta_min_seconds == 0 and r5.reason == "arrived_or_past", r5
    results.append(("arrived", "0", "0"))

    for name, lo, hi in results:
        print(f"  {name}: min={lo}s max={hi}s")
    print("step25 ETA tests OK")


if __name__ == "__main__":
    run()
