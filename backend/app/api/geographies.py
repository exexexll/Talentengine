from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi.responses import JSONResponse

from backend.app.models.schemas import GeographyProfile, GeographyProfileInsightsResponse, GeographyProfileTabsResponse
from backend.app.services.analysis_engine import geography_profile_insights, geography_profile_tabs
from backend.app.services.metrics_engine import (
    ArtifactDataUnavailableError,
    _build_geo_name_cache,
    list_geographies_from_artifacts,
    search_geographies,
)

router = APIRouter()

_STATIC_CACHE_HEADERS = {"Cache-Control": "public, max-age=3600"}


@router.get("/search", response_model=list[GeographyProfile])
def search_geographies_endpoint(
    q: str = Query(..., min_length=1),
    geography_types: str | None = None,
) -> list[GeographyProfile]:
    try:
        type_set = (
            {item.strip() for item in geography_types.split(",") if item.strip()}
            if geography_types
            else None
        )
        return search_geographies(query=q, geography_types=type_set)
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/{geography_id}/profile", response_model=GeographyProfileInsightsResponse)
def geography_profile(geography_id: str, scenario_id: str = "default-opportunity") -> GeographyProfileInsightsResponse:
    try:
        return geography_profile_insights(geography_id=geography_id, scenario_id=scenario_id)
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{geography_id}/profile/tabs", response_model=GeographyProfileTabsResponse)
def geography_profile_tabs_endpoint(
    geography_id: str, scenario_id: str = "default-opportunity"
) -> GeographyProfileTabsResponse:
    try:
        return geography_profile_tabs(geography_id=geography_id, scenario_id=scenario_id)
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/names")
def geography_names() -> JSONResponse:
    """Return {fips: human_name} for all known geographies."""
    return JSONResponse(content=_build_geo_name_cache([]), headers=_STATIC_CACHE_HEADERS)


@router.get("", response_model=list[GeographyProfile])
def list_geographies() -> list[GeographyProfile]:
    try:
        return list_geographies_from_artifacts()
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
