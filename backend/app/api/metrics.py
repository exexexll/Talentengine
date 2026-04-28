from fastapi import APIRouter, HTTPException

from backend.app.models.schemas import MetricValue
from backend.app.services.metrics_engine import (
    ArtifactDataUnavailableError,
    metric_bundle_from_artifacts,
)

router = APIRouter()


@router.get("/{geography_id}", response_model=list[MetricValue])
def get_metrics(geography_id: str) -> list[MetricValue]:
    try:
        return metric_bundle_from_artifacts(geography_id)
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
