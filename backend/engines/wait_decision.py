"""
PHASE 9 — Wait Decision

WAIT | UNCERTAIN | CONSIDER_ALTERNATIVE

Inputs:
- ETA range width
- confidence
- freshness
- station event (optional)
- optional schedule deviation (seconds late); None if no schedule truth
"""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum


class DecisionType(str, Enum):
    WAIT = "WAIT"
    UNCERTAIN = "UNCERTAIN"
    CONSIDER_ALTERNATIVE = "CONSIDER_ALTERNATIVE"


@dataclass(frozen=True)
class WaitInput:
    eta_min_seconds: int | None
    eta_max_seconds: int | None
    confidence: str
    freshness: str
    station_event: str | None  # ARRIVING / AT_STATION / DEPARTED / None
    schedule_delay_seconds: int | None = None


@dataclass(frozen=True)
class WaitResult:
    decision: DecisionType
    reason_ar: str
    reason_en: str


def decide(x: WaitInput) -> WaitResult:
    if (
        x.confidence == "UNKNOWN"
        or x.freshness == "UNKNOWN"
        or x.eta_min_seconds is None
        or x.eta_max_seconds is None
    ):
        return WaitResult(
            DecisionType.UNCERTAIN,
            "لا توجد بيانات كافية لاتخاذ قرار.",
            "Insufficient data for a wait decision.",
        )

    if x.freshness == "STALE":
        return WaitResult(
            DecisionType.UNCERTAIN,
            "البيانات قديمة؛ انتظر تحديثاً أحدث.",
            "Data is stale; wait for a fresher update.",
        )

    span = x.eta_max_seconds - x.eta_min_seconds
    mid = (x.eta_min_seconds + x.eta_max_seconds) / 2

    # Already at / arriving soon with solid confidence
    if x.station_event == "AT_STATION":
        return WaitResult(
            DecisionType.WAIT,
            "القطار في المحطة أو عندها.",
            "Train is at the station.",
        )

    if x.station_event == "ARRIVING" and x.confidence in ("HIGH", "MEDIUM") and mid <= 180:
        return WaitResult(
            DecisionType.WAIT,
            "القطار يقترب؛ الانتظار معقول.",
            "Train is arriving; waiting is reasonable.",
        )

    # Very wide uncertainty band
    if span > 900 or x.confidence == "LOW":
        return WaitResult(
            DecisionType.UNCERTAIN,
            "نطاق الوصول واسع أو الثقة منخفضة.",
            "Arrival window is wide or confidence is low.",
        )

    # Long wait
    if mid > 1200:  # > 20 min
        if x.schedule_delay_seconds is not None and x.schedule_delay_seconds > 900:
            return WaitResult(
                DecisionType.CONSIDER_ALTERNATIVE,
                "تأخير كبير متوقع؛ فكّر في بديل.",
                "Large delay expected; consider an alternative.",
            )
        return WaitResult(
            DecisionType.CONSIDER_ALTERNATIVE,
            "الوصول بعيد نسبياً؛ قد تفضّل بديلاً.",
            "Arrival is relatively far; an alternative may be better.",
        )

    # Moderate wait with decent confidence
    if mid <= 600 and x.confidence in ("HIGH", "MEDIUM") and x.freshness in ("LIVE", "RECENT"):
        return WaitResult(
            DecisionType.WAIT,
            "الوصول خلال وقت معقول؛ يمكن الانتظار.",
            "Arrival within a reasonable time; waiting is fine.",
        )

    if mid <= 900 and x.confidence == "HIGH":
        return WaitResult(
            DecisionType.WAIT,
            "الثقة عالية والوصول قريب نسبياً.",
            "High confidence and relatively near arrival.",
        )

    return WaitResult(
        DecisionType.UNCERTAIN,
        "المعلومات غير حاسمة بما يكفي.",
        "Information is not decisive enough.",
    )
