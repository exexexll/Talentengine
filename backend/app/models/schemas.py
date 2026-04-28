from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field
from pydantic import model_validator


GeographyType = Literal["nation", "state", "metro", "county", "place", "zcta", "tract", "block"]


class GeographyProfile(BaseModel):
    geography_id: str
    geography_type: GeographyType
    name: str
    parent_id: str | None = None
    population: int | None = None


class MetricValue(BaseModel):
    geography_id: str
    period: str
    metric_name: str
    raw_value: float
    normalized_value: float | None = None
    units: str
    source: str
    source_period: str
    last_refresh: date
    formula: str
    confidence: float = Field(ge=0.0, le=1.0)


class ScoreComponent(BaseModel):
    metric_name: str
    weight: float
    value: float


class ScoreResult(BaseModel):
    geography_id: str
    scenario_id: str
    score_name: str
    score_value: float
    components: list[ScoreComponent]
    confidence: float = Field(ge=0.0, le=1.0)
    version: str
    updated_at: datetime


class ScenarioWeights(BaseModel):
    # Figwork-first baseline:
    # strongly prioritize demand + scarcity, while retaining
    # market gap/cost/execution as secondary constraints.
    business_demand: float = 0.38
    talent_supply: float = 0.34
    market_gap: float = 0.16
    cost_efficiency: float = 0.04
    execution_feasibility: float = 0.08

    @model_validator(mode="after")
    def validate_weight_sum(self) -> "ScenarioWeights":
        total = (
            self.business_demand
            + self.talent_supply
            + self.market_gap
            + self.cost_efficiency
            + self.execution_feasibility
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Scenario weights must sum to 1.0, got {total:.6f}")
        return self


class ScenarioDefinition(BaseModel):
    scenario_id: str
    name: str
    description: str
    weights: ScenarioWeights
    filters: dict[str, Any] = Field(default_factory=dict)


class RecommendationResult(BaseModel):
    geography_id: str
    scenario_id: str
    label: Literal[
        "Enter now",
        "Pilot first",
        "Supply-rich market",
        "Demand-first market",
        "Partnership-led market",
        "Monitor",
        "Avoid for now",
    ]
    rationale: list[str]
    risk_flags: list[str]
    confidence: float = Field(ge=0.0, le=1.0)


class RankedScore(BaseModel):
    rank: int
    geography_id: str
    score_value: float
    confidence: float = Field(ge=0.0, le=1.0)


class ScenarioUpsert(BaseModel):
    scenario_id: str
    name: str
    description: str
    weights: ScenarioWeights
    filters: dict[str, Any] = Field(default_factory=dict)


class ScenarioCloneRequest(BaseModel):
    target_scenario_id: str
    target_name: str


class CompareRequest(BaseModel):
    geography_ids: list[str] = Field(min_length=2)
    scenario_id: str = "default-opportunity"


class CompareRow(BaseModel):
    geography_id: str
    score: float
    recommendation: str
    demand_score: float
    supply_score: float
    confidence: float


class CompareResponse(BaseModel):
    scenario_id: str
    rows: list[CompareRow]


class ScenarioSimulationRequest(BaseModel):
    weights: ScenarioWeights
    limit: int = Field(default=10, ge=1, le=200)
    geography_ids: list[str] = Field(default_factory=list)


class ScenarioSimulationRow(BaseModel):
    rank: int
    geography_id: str
    score: float
    confidence: float


class ScenarioSimulationResponse(BaseModel):
    rows: list[ScenarioSimulationRow]


class RecommendationExplainResponse(BaseModel):
    geography_id: str
    scenario_id: str
    recommendation: str
    score: float
    confidence: float
    components: list[ScoreComponent]
    key_drivers: list[str]


class ScoreDeltaRow(BaseModel):
    geography_id: str
    baseline_rank: int
    scenario_rank: int
    rank_change: int
    baseline_score: float
    scenario_score: float
    score_change: float
    top_component_shift: str | None = None


class ScoreDeltaResponse(BaseModel):
    baseline_scenario_id: str
    scenario_id: str
    rows: list[ScoreDeltaRow]


class RecommendationDistributionRow(BaseModel):
    label: str
    count: int


class RecommendationDistributionResponse(BaseModel):
    scenario_id: str
    total_geographies: int
    rows: list[RecommendationDistributionRow]


class GeographyProfileInsightsResponse(BaseModel):
    geography_id: str
    scenario_id: str
    score: float
    recommendation: str
    strengths: list[str]
    risks: list[str]
    key_metrics: dict[str, float]


class ProfileTabOverview(BaseModel):
    geography_id: str
    geography_type: str
    population: float | None = None
    opportunity_score: float
    recommendation: str
    confidence: float


class ProfileTabTalent(BaseModel):
    target_occupation_employment: float | None = None
    talent_density: float | None = None
    adjacent_skill_pool_index: float | None = None
    labor_force: float | None = None
    unemployment_rate: float | None = None
    educational_attainment_bachelors_plus: float | None = None
    talent_conversion_score: float | None = None


class ProfileTabIndustries(BaseModel):
    industry_employment: float | None = None
    business_establishments: float | None = None
    industry_specialization_lq: float | None = None
    gdp_current_dollars: float | None = None
    gdp_growth_rate: float | None = None
    job_creation_rate: float | None = None
    net_job_dynamism: float | None = None
    industry_fit_score: float | None = None
    demand_capture_score: float | None = None


class ProfileTabEducation(BaseModel):
    relevant_completions: float | None = None
    institutions_reporting: float | None = None
    graduate_pipeline_intensity: float | None = None
    median_earnings_4yr: float | None = None
    completion_rate: float | None = None


class ProfileTabMovement(BaseModel):
    net_migrants: float | None = None
    inbound_returns: float | None = None
    outbound_returns: float | None = None
    workplace_jobs: float | None = None
    residence_workers: float | None = None
    commute_inflow_ratio: float | None = None
    population_growth_rate: float | None = None


class ProfileTabAccess(BaseModel):
    served_household_ratio: float | None = None
    high_speed_ratio: float | None = None
    rurality_index: float | None = None
    metro_linkage_index: float | None = None
    internet_access_rate: float | None = None
    work_from_home_rate: float | None = None
    commute_mean_minutes: float | None = None
    cost_adjusted_wage: float | None = None
    regional_price_parity: float | None = None
    housing_cost_burden_ratio: float | None = None


class ProfileTabRecommendation(BaseModel):
    label: str
    rationale: list[str]
    risk_flags: list[str]
    key_drivers: list[str]
    demand_supply_gap: float | None = None


class GeographyProfileTabsResponse(BaseModel):
    geography_id: str
    scenario_id: str
    overview: ProfileTabOverview
    talent: ProfileTabTalent
    industries: ProfileTabIndustries
    education: ProfileTabEducation
    movement: ProfileTabMovement
    access: ProfileTabAccess
    recommendation: ProfileTabRecommendation
