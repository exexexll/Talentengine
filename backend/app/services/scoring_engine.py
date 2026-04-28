from datetime import datetime, timezone

from backend.app.models.schemas import ScoreComponent, ScoreResult, ScenarioDefinition


# --- Completeness-factor constants (PRD §13) -------------------------------
# A component score is multiplied by two completeness factors:
#   coverage_factor = COVERAGE_BASE + COVERAGE_SLOPE * coverage_ratio
#   direct_factor   = DIRECT_BASE   + DIRECT_SLOPE   * direct_ratio
# At coverage=1, direct=1 the factor is 1.0.  At coverage=0, direct=0 the
# factor is COVERAGE_BASE * DIRECT_BASE ≈ 0.51, so sparse-data metrics
# don't get zeroed out but do penalize meaningfully.
COVERAGE_BASE = 0.78
COVERAGE_SLOPE = 0.22
DIRECT_BASE = 0.65
DIRECT_SLOPE = 0.35

# Active-weight floor below which we linearly damp the raw score.  A
# geography with less than half its features populated can't earn a full
# score; e.g. coverage=0.25 → scores get multiplied by 0.25/0.5 = 0.5.
MIN_FULL_COVERAGE = 0.5


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_regional_opportunity_score(
    geography_id: str,
    scenario: ScenarioDefinition,
    features: dict[str, float],
    confidence: float,
    version: str = "v1",
    pct: dict[str, float] | None = None,
) -> ScoreResult:
    """PRD §13 — composite regional opportunity score.

    Pipeline:
      1. Normalize each feature (inverted metrics flipped).
      2. Skip metrics with no data, scale metrics with partial data.
      3. Re-normalize by the *active* weights so missing features don't
         artificially depress the score.
      4. Damp the score if fewer than MIN_FULL_COVERAGE of weights were
         active (`coverage < 0.5` → score *= coverage / 0.5`).
      5. Apply the US-city direct-data gate.
      6. Apply the US contrast curve so map colors diverge usefully.
    """
    from backend.app.services.metrics_engine import (
        COMPONENT_SOURCE_METRICS,  # noqa: F401 — re-exported for callers
        INVERTED_COMPONENTS,
        _component_data_factors,
        _city_direct_data_gate_factor,
        apply_score_contrast,
    )

    weights = scenario.weights.model_dump()
    components: list[ScoreComponent] = []
    weighted_sum = 0.0
    active_weight = 0.0

    for metric_name, weight in weights.items():
        raw_value = features.get(metric_name, 0.0)
        scoring_value = (1.0 - raw_value) if metric_name in INVERTED_COMPONENTS else raw_value
        components.append(
            ScoreComponent(metric_name=metric_name, weight=weight, value=scoring_value)
        )
        if pct is None:
            has_data, coverage_ratio, direct_ratio = True, 1.0, 1.0
        else:
            has_data, coverage_ratio, direct_ratio = _component_data_factors(metric_name, pct)
        if has_data:
            completeness_factor = (
                (COVERAGE_BASE + COVERAGE_SLOPE * coverage_ratio)
                * (DIRECT_BASE + DIRECT_SLOPE * direct_ratio)
            )
            weighted_sum += scoring_value * weight * completeness_factor
            active_weight += weight

    if active_weight > 0:
        total_possible = sum(weights.values())
        raw = (weighted_sum / active_weight) * 100.0
        coverage = active_weight / total_possible
        if coverage < MIN_FULL_COVERAGE:
            raw *= coverage / MIN_FULL_COVERAGE
        if pct is not None:
            raw *= _city_direct_data_gate_factor(pct, geography_id)
        normalized = apply_score_contrast(_clamp_score(raw), geography_id=geography_id)
    else:
        normalized = 0.0
    return ScoreResult(
        geography_id=geography_id,
        scenario_id=scenario.scenario_id,
        score_name="regional_opportunity_score",
        score_value=normalized,
        components=components,
        confidence=confidence,
        version=version,
        updated_at=datetime.now(timezone.utc),
    )


def compute_industry_fit_score(
    geography_id: str,
    features: dict[str, float],
    confidence: float,
    version: str = "v1",
) -> ScoreResult:
    """PRD Section 13: Local Core Industry Fit Score."""
    value = features.get("industry_fit", 0.0)
    return ScoreResult(
        geography_id=geography_id,
        scenario_id="composite",
        score_name="local_core_industry_fit_score",
        score_value=_clamp_score(value * 100.0),
        components=[ScoreComponent(metric_name="industry_fit", weight=1.0, value=value)],
        confidence=confidence,
        version=version,
        updated_at=datetime.now(timezone.utc),
    )


def compute_talent_conversion_score(
    geography_id: str,
    features: dict[str, float],
    confidence: float,
    version: str = "v1",
) -> ScoreResult:
    """PRD Section 13: Talent Conversion Score."""
    value = features.get("talent_conversion", 0.0)
    return ScoreResult(
        geography_id=geography_id,
        scenario_id="composite",
        score_name="talent_conversion_score",
        score_value=_clamp_score(value * 100.0),
        components=[ScoreComponent(metric_name="talent_conversion", weight=1.0, value=value)],
        confidence=confidence,
        version=version,
        updated_at=datetime.now(timezone.utc),
    )


def compute_demand_capture_score(
    geography_id: str,
    features: dict[str, float],
    confidence: float,
    version: str = "v1",
) -> ScoreResult:
    """PRD Section 13: Demand Capture Score."""
    value = features.get("demand_capture", 0.0)
    return ScoreResult(
        geography_id=geography_id,
        scenario_id="composite",
        score_name="demand_capture_score",
        score_value=_clamp_score(value * 100.0),
        components=[ScoreComponent(metric_name="demand_capture", weight=1.0, value=value)],
        confidence=confidence,
        version=version,
        updated_at=datetime.now(timezone.utc),
    )


def compute_confidence_score(
    geography_id: str,
    metrics_confidence: float,
    version: str = "v1",
) -> ScoreResult:
    """PRD Section 13: Confidence Score - freshness and completeness of underlying data."""
    return ScoreResult(
        geography_id=geography_id,
        scenario_id="composite",
        score_name="confidence_score",
        score_value=_clamp_score(metrics_confidence * 100.0),
        components=[ScoreComponent(metric_name="data_confidence", weight=1.0, value=metrics_confidence)],
        confidence=metrics_confidence,
        version=version,
        updated_at=datetime.now(timezone.utc),
    )
