from fastapi import APIRouter, HTTPException, Response

from backend.app.models.schemas import RankedScore, ScoreDeltaResponse, ScoreResult

_SCENARIO_CACHE_HEADER = "public, max-age=600"
from backend.app.services.analysis_engine import ranked_scores, score_delta, score_for_geography
from backend.app.services.cache import api_cache
from backend.app.services.metrics_engine import (
    ArtifactDataUnavailableError,
    _get_or_build_percentiles,
    all_metrics_grouped_by_geography,
    compute_opportunity_score,
    data_quality_summary,
    derive_score_features,
)
from backend.app.services.scenario_engine import get_scenario

router = APIRouter()

CT_PLANNING_TO_OLD_COUNTY: dict[str, list[str]] = {
    "09110": ["09003", "09013"],  # Capitol → Hartford, Tolland
    "09120": ["09001"],           # Greater Bridgeport → Fairfield
    "09130": ["09007"],           # Lower CT River Valley → Middlesex
    "09140": ["09009"],           # Naugatuck Valley → New Haven
    "09150": ["09015"],           # Northeastern CT → Windham
    "09160": ["09005"],           # Northwest Hills → Litchfield
    "09170": ["09009"],           # South Central CT → New Haven
    "09180": ["09011"],           # Southeastern CT → New London
    "09190": ["09001"],           # Western CT → Fairfield
}


def _backfill_ct_old_fips(result: dict[str, dict[str, float]]) -> None:
    """Copy CT planning-region scores onto old county FIPS for GeoJSON compat."""
    for new_fips, old_fips_list in CT_PLANNING_TO_OLD_COUNTY.items():
        if new_fips not in result:
            continue
        for old_fips in old_fips_list:
            if old_fips not in result:
                result[old_fips] = result[new_fips]


@router.get("/_ranked", response_model=list[RankedScore])
def get_ranked_scores(
    response: Response,
    scenario_id: str = "default-opportunity",
    limit: int = 200,
) -> list[RankedScore]:
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    response.headers["Cache-Control"] = _SCENARIO_CACHE_HEADER
    cache_key = f"ranked:{scenario_id}:{limit}"
    cached = api_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        ranked_raw = ranked_scores(scenario_id=scenario_id, limit=limit)
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    ranked = [
        RankedScore(rank=idx, geography_id=geo_id, score_value=score, confidence=confidence)
        for idx, (geo_id, score, confidence) in enumerate(ranked_raw, start=1)
    ]
    api_cache.set(cache_key, ranked)
    return ranked


@router.get("/_features_bulk")
def get_features_bulk(
    response: Response,
    scenario_id: str = "default-opportunity",
) -> dict[str, dict[str, float]]:
    """Bulk feature values for every geography, keyed by geography_id."""
    response.headers["Cache-Control"] = _SCENARIO_CACHE_HEADER
    cache_key = f"features_bulk:{scenario_id}"
    cached = api_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        grouped = all_metrics_grouped_by_geography()
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    scenario = get_scenario(scenario_id)
    weights = scenario.weights.model_dump()
    pct_tables = _get_or_build_percentiles(grouped)
    result: dict[str, dict[str, float]] = {}
    for geo_id, metrics in grouped.items():
        geo_pct = pct_tables.get(geo_id, {})
        features = derive_score_features(metrics, geo_pct)
        features["opportunity_score"] = compute_opportunity_score(
            features,
            weights,
            geo_pct,
            geography_id=geo_id,
        )
        features.update(data_quality_summary(geo_pct, geo_id))
        result[geo_id] = features
    _backfill_ct_old_fips(result)
    _backfill_place_scores(result)
    api_cache.set(cache_key, result)
    return result


def _backfill_place_scores(result: dict[str, dict[str, float]]) -> None:
    """Propagate county scores to places that lack their own features."""
    from backend.app.services.analysis_engine import _load_place_county_map

    place_map = _load_place_county_map()
    if not place_map:
        return

    county_features = {
        gid: feats for gid, feats in result.items()
        if len(gid) == 5 and gid.isdigit()
    }
    if not county_features:
        return

    state_best: dict[str, dict[str, float]] = {}
    for cid, feats in county_features.items():
        st = cid[:2]
        if st not in state_best or feats.get("opportunity_score", 0) > state_best[st].get("opportunity_score", 0):
            state_best[st] = feats

    for place_id, county_id in place_map.items():
        if place_id in result:
            continue
        if county_id and county_id in county_features:
            result[place_id] = county_features[county_id]
        else:
            st = place_id[:2]
            if st in state_best:
                result[place_id] = state_best[st]


@router.get("/_delta", response_model=ScoreDeltaResponse)
def get_score_delta(
    response: Response,
    scenario_id: str,
    baseline_scenario_id: str = "default-opportunity",
    limit: int = 25,
) -> ScoreDeltaResponse:
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    response.headers["Cache-Control"] = _SCENARIO_CACHE_HEADER
    cache_key = f"delta:{baseline_scenario_id}:{scenario_id}:{limit}"
    cached = api_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        result = score_delta(
            baseline_scenario_id=baseline_scenario_id,
            scenario_id=scenario_id,
            limit=limit,
        )
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    api_cache.set(cache_key, result)
    return result


@router.get("/{geography_id}", response_model=ScoreResult)
def get_score(
    response: Response,
    geography_id: str,
    scenario_id: str = "default-opportunity",
) -> ScoreResult:
    response.headers["Cache-Control"] = _SCENARIO_CACHE_HEADER
    try:
        score, _features = score_for_geography(geography_id=geography_id, scenario_id=scenario_id)
    except (ArtifactDataUnavailableError, KeyError):
        from datetime import datetime, timezone
        return ScoreResult(
            geography_id=geography_id,
            scenario_id=scenario_id,
            score_name="opportunity",
            score_value=0.0,
            components=[],
            confidence=0.0,
            version="v1-fallback",
            updated_at=datetime.now(timezone.utc),
        )
    return score
