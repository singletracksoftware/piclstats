"""Rider performance forecasting engine.

Predicts where a rider would place in a different division based on
their min/mile pace, adjusted for fatigue (extra laps), loop transition
(MS→HS), and seasonal improvement trends.

The model is configurable via DEFAULT_CONFIG — all tunable parameters
are drawn from this dict rather than hardcoded in the algorithm.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from statistics import mean, median, stdev


@dataclass(frozen=True)
class RaceObservation:
    """One race's pace data for a rider."""

    event_name: str
    course_id: int | None
    season: int
    event_order: int
    min_per_mile: float
    division: str
    loop_type: str | None
    lap_count: int | None


@dataclass(frozen=True)
class ForecastInput:
    """All inputs needed for a prediction."""

    rider_id: int
    rider_name: str
    rider_gender: str
    source_division: str
    target_division: str
    observations: list[RaceObservation]
    target_paces: list[float]  # min/mile values from target division
    target_field_sizes: list[int]  # field sizes per event in target
    source_laps: int
    target_laps: int
    source_loop_type: str
    target_loop_type: str
    source_loop_miles: float
    target_loop_miles: float


@dataclass(frozen=True)
class ForecastResult:
    """Prediction output."""

    predicted_min_per_mile: float
    predicted_place_low: int
    predicted_place_mid: int
    predicted_place_high: int
    predicted_percentile: float
    typical_field_size: int
    readiness: str  # "Ready", "Competitive", "Developing"
    readiness_color: str  # CSS color class
    readiness_detail: str
    confidence: str  # "High", "Medium", "Low"
    inputs_summary: dict


# ── Configuration ────────────────────────────────────────────────────

DEFAULT_CONFIG: dict = {
    # How many recent races to weight
    "recent_race_count": 5,
    # Exponential decay: most recent race = 1.0, previous = decay, etc.
    "recency_decay": 0.8,
    # Fatigue: % slower per additional lap beyond source division
    "fatigue_per_extra_lap": 0.03,
    # Loop transition: % penalty when switching MS loop → HS loop
    "ms_to_hs_loop_penalty": 1.05,
    # How much of the seasonal improvement trend to credit (0-1)
    "improvement_weight": 0.5,
    # Percentile thresholds for readiness labels
    "readiness_thresholds": {
        "ready": 50,
        "competitive": 25,
    },
    # Minimum races needed to produce a forecast
    "min_races_for_forecast": 2,
}


# ── Model Implementation ────────────────────────────────────────────

class StatisticalForecastModel:
    """V1 forecast model using statistical pace comparison."""

    def __init__(self, config: dict | None = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}

    def predict(self, inp: ForecastInput) -> ForecastResult | None:
        cfg = self.config

        if len(inp.observations) < cfg["min_races_for_forecast"]:
            return None

        if not inp.target_paces:
            return None

        # Step 1: Rider's current pace (recency-weighted average)
        recent = inp.observations[-cfg["recent_race_count"]:]
        decay = cfg["recency_decay"]
        weights = [decay ** (len(recent) - 1 - i) for i in range(len(recent))]
        total_w = sum(weights)
        rider_pace = sum(o.min_per_mile * w for o, w in zip(recent, weights)) / total_w

        # Step 2: Fatigue adjustment
        extra_laps = max(0, inp.target_laps - inp.source_laps)
        fatigue_pct = cfg["fatigue_per_extra_lap"] * extra_laps
        fatigue_multiplier = 1.0 + fatigue_pct

        # Step 3: Loop transition (MS → HS only)
        loop_transition = 1.0
        if inp.source_loop_type == "MS" and inp.target_loop_type == "HS":
            loop_transition = cfg["ms_to_hs_loop_penalty"]
        # No bonus for HS → MS (conservative)

        # Step 4: Seasonal improvement credit
        improvement_credit = 0.0
        if len(recent) >= 3:
            paces = [o.min_per_mile for o in recent]
            n = len(paces)
            x_mean = (n - 1) / 2
            y_mean = mean(paces)
            numerator = sum((i - x_mean) * (p - y_mean) for i, p in enumerate(paces))
            denominator = sum((i - x_mean) ** 2 for i in range(n))
            if denominator > 0:
                slope = numerator / denominator  # negative slope = improving
                if slope < 0:
                    improvement_credit = abs(slope) * cfg["improvement_weight"]

        # Final adjusted pace
        adjusted_pace = (rider_pace * fatigue_multiplier * loop_transition) - improvement_credit
        adjusted_pace = max(adjusted_pace, 1.0)  # sanity floor

        # Step 5: Place in target distribution
        sorted_paces = sorted(inp.target_paces)
        position = bisect.bisect_left(sorted_paces, adjusted_pace)
        field_size = len(sorted_paces)
        typical_field = round(mean(inp.target_field_sizes)) if inp.target_field_sizes else field_size

        predicted_place = position + 1
        percentile = round((1 - position / max(field_size, 1)) * 100, 1)

        # Confidence band based on rider's variance
        rider_paces = [o.min_per_mile for o in inp.observations]
        rider_std = stdev(rider_paces) if len(rider_paces) >= 2 else 0
        # Map stdev to place range: find how many places ±1 stdev covers
        low_pace = adjusted_pace - rider_std
        high_pace = adjusted_pace + rider_std
        place_low = max(1, bisect.bisect_left(sorted_paces, low_pace) + 1)
        place_high = min(typical_field, bisect.bisect_left(sorted_paces, high_pace) + 1)
        place_mid = max(1, min(typical_field, predicted_place))

        # Ensure low <= mid <= high
        place_low = min(place_low, place_mid)
        place_high = max(place_high, place_mid)

        # Step 6: Readiness
        thresholds = cfg["readiness_thresholds"]
        if percentile >= thresholds["ready"]:
            readiness = "Ready"
            readiness_color = "green"
            readiness_detail = f"Projected top half — would likely place {place_low}-{place_high} out of ~{typical_field}"
        elif percentile >= thresholds["competitive"]:
            readiness = "Competitive"
            readiness_color = "amber"
            readiness_detail = f"Would be competitive but likely in the bottom half — place ~{place_mid} out of ~{typical_field}"
        else:
            readiness = "Developing"
            readiness_color = "red"
            readiness_detail = f"May find it challenging — projected place ~{place_mid} out of ~{typical_field}"

        # Confidence level
        if len(inp.observations) >= 5 and len(sorted_paces) >= 20:
            confidence = "High"
        elif len(inp.observations) >= 3 and len(sorted_paces) >= 10:
            confidence = "Medium"
        else:
            confidence = "Low"

        # Step 7: Transparency
        target_avg = mean(sorted_paces) if sorted_paces else 0
        target_med = median(sorted_paces) if sorted_paces else 0

        inputs_summary = {
            "rider_raw_pace": round(rider_pace, 1),
            "recent_races_used": len(recent),
            "recency_decay": decay,
            "fatigue_extra_laps": extra_laps,
            "fatigue_pct": round(fatigue_pct * 100, 1),
            "fatigue_multiplier": round(fatigue_multiplier, 3),
            "loop_transition": "MS → HS" if loop_transition > 1 else "Same loop",
            "loop_transition_factor": round(loop_transition, 3),
            "improvement_trend": round(-improvement_credit / cfg["improvement_weight"], 2) if improvement_credit else 0,
            "improvement_credit": round(improvement_credit, 2),
            "adjusted_pace": round(adjusted_pace, 1),
            "rider_consistency_stdev": round(rider_std, 2),
            "source_division": inp.source_division,
            "source_laps": inp.source_laps,
            "source_loop": f"{inp.source_loop_type} ({inp.source_loop_miles} mi)",
            "target_division": inp.target_division,
            "target_laps": inp.target_laps,
            "target_loop": f"{inp.target_loop_type} ({inp.target_loop_miles} mi)",
            "target_avg_pace": round(target_avg, 1),
            "target_median_pace": round(target_med, 1),
            "target_sample_size": field_size,
            "typical_field_size": typical_field,
        }

        return ForecastResult(
            predicted_min_per_mile=round(adjusted_pace, 1),
            predicted_place_low=place_low,
            predicted_place_mid=place_mid,
            predicted_place_high=place_high,
            predicted_percentile=percentile,
            typical_field_size=typical_field,
            readiness=readiness,
            readiness_color=readiness_color,
            readiness_detail=readiness_detail,
            confidence=confidence,
            inputs_summary=inputs_summary,
        )
