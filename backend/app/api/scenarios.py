from fastapi import APIRouter, HTTPException, Response

from backend.app.models.schemas import (
    ScenarioCloneRequest,
    ScenarioDefinition,
    ScenarioSimulationRequest,
    ScenarioSimulationResponse,
    ScenarioUpsert,
)
from backend.app.services.analysis_engine import simulate_scores
from backend.app.services.metrics_engine import ArtifactDataUnavailableError
from backend.app.services.scenario_engine import (
    clone_scenario,
    delete_scenario,
    get_scenario,
    list_scenarios,
    upsert_scenario,
)

router = APIRouter()


@router.get("", response_model=list[ScenarioDefinition])
def scenarios(response: Response) -> list[ScenarioDefinition]:
    response.headers["Cache-Control"] = "public, max-age=3600"
    return list_scenarios()


@router.post("/simulate", response_model=ScenarioSimulationResponse)
def scenario_simulate(payload: ScenarioSimulationRequest) -> ScenarioSimulationResponse:
    try:
        return simulate_scores(
            weights=payload.weights,
            limit=payload.limit,
            geography_ids=payload.geography_ids,
        )
    except ArtifactDataUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{scenario_id}/clone", response_model=ScenarioDefinition)
def scenario_clone(scenario_id: str, payload: ScenarioCloneRequest) -> ScenarioDefinition:
    try:
        return clone_scenario(
            source_scenario_id=scenario_id,
            target_scenario_id=payload.target_scenario_id,
            target_name=payload.target_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{scenario_id}", response_model=ScenarioDefinition)
def scenario_by_id(scenario_id: str) -> ScenarioDefinition:
    try:
        return get_scenario(scenario_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.put("/{scenario_id}", response_model=ScenarioDefinition)
def scenario_upsert(scenario_id: str, payload: ScenarioUpsert) -> ScenarioDefinition:
    if payload.scenario_id != scenario_id:
        raise HTTPException(status_code=400, detail="scenario_id mismatch")
    try:
        return upsert_scenario(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/{scenario_id}")
def scenario_delete(scenario_id: str) -> dict[str, str]:
    try:
        delete_scenario(scenario_id)
        return {"status": "deleted", "scenario_id": scenario_id}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
