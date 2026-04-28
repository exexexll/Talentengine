import json
from pathlib import Path

from backend.app.models.schemas import ScenarioDefinition, ScenarioUpsert, ScenarioWeights
from backend.app.services.cache import api_cache


DEFAULT_SCENARIO = ScenarioDefinition(
    scenario_id="default-opportunity",
    name="Default Opportunity",
    description="Demand/supply-heavy weights for Figwork opportunity with secondary cost/execution checks.",
    weights=ScenarioWeights(),
    filters={},
)

SCENARIO_FILE = Path("backend/data/scenarios.json")


def _load_user_scenarios() -> list[ScenarioDefinition]:
    if not SCENARIO_FILE.exists():
        return []
    with SCENARIO_FILE.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return [ScenarioDefinition(**item) for item in payload]


def _save_user_scenarios(scenarios: list[ScenarioDefinition]) -> None:
    SCENARIO_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SCENARIO_FILE.open("w", encoding="utf-8") as handle:
        json.dump([item.model_dump() for item in scenarios], handle, indent=2, sort_keys=True)


def list_scenarios() -> list[ScenarioDefinition]:
    user_scenarios = _load_user_scenarios()
    return [DEFAULT_SCENARIO, *user_scenarios]


def get_scenario(scenario_id: str) -> ScenarioDefinition:
    for scenario in list_scenarios():
        if scenario.scenario_id == scenario_id:
            return scenario
    raise KeyError(f"Unknown scenario_id={scenario_id}")


def upsert_scenario(payload: ScenarioUpsert) -> ScenarioDefinition:
    if payload.scenario_id == DEFAULT_SCENARIO.scenario_id:
        raise ValueError("default-opportunity is reserved")
    scenarios = _load_user_scenarios()
    updated = ScenarioDefinition(**payload.model_dump())
    replaced = False
    for idx, item in enumerate(scenarios):
        if item.scenario_id == payload.scenario_id:
            scenarios[idx] = updated
            replaced = True
            break
    if not replaced:
        scenarios.append(updated)
    _save_user_scenarios(scenarios)
    api_cache.clear()
    return updated


def delete_scenario(scenario_id: str) -> None:
    if scenario_id == DEFAULT_SCENARIO.scenario_id:
        raise ValueError("default-opportunity cannot be deleted")
    scenarios = _load_user_scenarios()
    kept = [item for item in scenarios if item.scenario_id != scenario_id]
    if len(kept) == len(scenarios):
        raise KeyError(f"Unknown scenario_id={scenario_id}")
    _save_user_scenarios(kept)
    api_cache.clear()


def clone_scenario(source_scenario_id: str, target_scenario_id: str, target_name: str) -> ScenarioDefinition:
    if target_scenario_id == DEFAULT_SCENARIO.scenario_id:
        raise ValueError("default-opportunity is reserved")
    source = get_scenario(source_scenario_id)
    existing_ids = {scenario.scenario_id for scenario in list_scenarios()}
    if target_scenario_id in existing_ids:
        raise ValueError(f"Target scenario already exists: {target_scenario_id}")
    clone = ScenarioDefinition(
        scenario_id=target_scenario_id,
        name=target_name,
        description=f"Clone of {source_scenario_id}",
        weights=source.weights,
        filters=source.filters,
    )
    scenarios = _load_user_scenarios()
    scenarios.append(clone)
    _save_user_scenarios(scenarios)
    api_cache.clear()
    return clone
