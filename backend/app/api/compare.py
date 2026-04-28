import csv
import io

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

from backend.app.models.schemas import CompareRequest, CompareResponse
from backend.app.services.analysis_engine import compare_geographies
from backend.app.services.metrics_engine import ArtifactDataUnavailableError

router = APIRouter()


@router.get("/csv", response_class=PlainTextResponse)
def compare_csv(
    geography_ids: str = Query(..., description="Comma-separated geography IDs"),
    scenario_id: str = "default-opportunity",
) -> str:
    geo_list = [item.strip() for item in geography_ids.split(",") if item.strip()]
    if len(geo_list) < 2:
        raise HTTPException(status_code=400, detail="At least 2 geography IDs required")
    try:
        comparison = compare_geographies(geography_ids=geo_list, scenario_id=scenario_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        ["scenario_id", "geography_id", "score", "recommendation", "demand_score", "supply_score", "confidence"]
    )
    for row in comparison.rows:
        writer.writerow(
            [
                comparison.scenario_id,
                row.geography_id,
                f"{row.score:.4f}",
                row.recommendation,
                f"{row.demand_score:.4f}",
                f"{row.supply_score:.4f}",
                f"{row.confidence:.4f}",
            ]
        )
    return buffer.getvalue()


@router.post("", response_model=CompareResponse)
def compare(payload: CompareRequest) -> CompareResponse:
    try:
        return compare_geographies(
            geography_ids=payload.geography_ids,
            scenario_id=payload.scenario_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
