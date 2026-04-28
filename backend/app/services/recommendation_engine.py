from backend.app.models.schemas import RecommendationResult, ScoreResult


def build_recommendation(
    geography_id: str,
    scenario_id: str,
    score: ScoreResult,
    demand_score: float,
    supply_score: float,
    broadband_score: float,
) -> RecommendationResult:
    rationale: list[str] = []
    risks: list[str] = []

    label = "Monitor"
    if score.score_value >= 72 and score.confidence >= 0.7:
        label = "Enter now"
        rationale.append("Top-tier opportunity with strong data confidence.")
    elif score.score_value >= 60 and demand_score >= 0.55 and broadband_score >= 0.5:
        label = "Partnership-led market"
        rationale.append("Demand and infrastructure support partner-led entry.")
    elif score.score_value >= 55 and demand_score > supply_score:
        label = "Demand-first market"
        rationale.append("Demand exceeds local talent supply -- sourcing opportunity.")
    elif score.score_value >= 55 and supply_score >= demand_score:
        label = "Supply-rich market"
        rationale.append("Strong talent pool relative to demand -- competitive sourcing.")
    elif score.score_value >= 42:
        label = "Pilot first"
        rationale.append("Moderate signals; validate with a pilot before scaling.")
    elif score.score_value < 30:
        label = "Avoid for now"
        rationale.append("Bottom-quartile composite signal under current scenario.")

    if broadband_score < 0.35:
        risks.append("Broadband readiness is weak for distributed operations.")
    if score.confidence < 0.55:
        risks.append("Data confidence is low; verify with manual review.")

    if not rationale:
        rationale.append("Mixed signals; continue monitoring before committing.")

    return RecommendationResult(
        geography_id=geography_id,
        scenario_id=scenario_id,
        label=label,
        rationale=rationale,
        risk_flags=risks,
        confidence=score.confidence,
    )
