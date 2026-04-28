from fastapi import APIRouter, HTTPException, Response

from backend.app.models.schemas import (
    RecommendationDistributionResponse,
    RecommendationExplainResponse,
    RecommendationResult,
)
from backend.app.services.analysis_engine import (
    recommendation_distribution,
    recommendation_for_geography,
    score_for_geography,
)
from backend.app.services.metrics_engine import ArtifactDataUnavailableError

router = APIRouter()

_SCENARIO_CACHE_HEADER = "public, max-age=600"


@router.get("/distribution", response_model=RecommendationDistributionResponse)
def get_recommendation_distribution(
    response: Response,
    scenario_id: str = "default-opportunity",
) -> RecommendationDistributionResponse:
    response.headers["Cache-Control"] = _SCENARIO_CACHE_HEADER
    try:
        return recommendation_distribution(scenario_id=scenario_id)
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{geography_id}/explain", response_model=RecommendationExplainResponse)
def explain_recommendation(
    response: Response,
    geography_id: str,
    scenario_id: str = "default-opportunity",
) -> RecommendationExplainResponse:
    response.headers["Cache-Control"] = _SCENARIO_CACHE_HEADER
    try:
        recommendation = recommendation_for_geography(
            geography_id=geography_id,
            scenario_id=scenario_id,
        )
        score, _features = score_for_geography(
            geography_id=geography_id,
            scenario_id=scenario_id,
        )
    except (ArtifactDataUnavailableError, KeyError):
        return RecommendationExplainResponse(
            geography_id=geography_id,
            scenario_id=scenario_id,
            recommendation="Monitor",
            score=0.0,
            confidence=0.0,
            components=[],
            key_drivers=["No metric data available for this geography."],
        )

    sorted_components = sorted(
        score.components,
        key=lambda comp: abs(comp.weight * comp.value),
        reverse=True,
    )
    key_drivers = [
        f"{comp.metric_name} ({(comp.weight * comp.value):.3f} contribution)"
        for comp in sorted_components[:3]
    ]
    return RecommendationExplainResponse(
        geography_id=geography_id,
        scenario_id=scenario_id,
        recommendation=recommendation.label,
        score=score.score_value,
        confidence=score.confidence,
        components=score.components,
        key_drivers=key_drivers,
    )


@router.get("/{geography_id}", response_model=RecommendationResult)
def get_recommendation(
    response: Response,
    geography_id: str,
    scenario_id: str = "default-opportunity",
) -> RecommendationResult:
    response.headers["Cache-Control"] = _SCENARIO_CACHE_HEADER
    try:
        return recommendation_for_geography(
            geography_id=geography_id,
            scenario_id=scenario_id,
        )
    except (ArtifactDataUnavailableError, KeyError):
        return RecommendationResult(
            geography_id=geography_id,
            scenario_id=scenario_id,
            label="Monitor",
            rationale=["Insufficient data to produce a recommendation for this geography."],
            risk_flags=["No metric data available."],
            confidence=0.0,
        )
