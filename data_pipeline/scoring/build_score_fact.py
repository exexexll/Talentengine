import json
from datetime import datetime, timezone
from pathlib import Path

from backend.app.services.artifact_store import load_latest_artifact_bundle


def _write_ndjson(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in records:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


DEFAULT_SCENARIO = {
    "scenario_id": "default-opportunity",
    "weights": {
        "business_demand": 0.25,
        "talent_supply": 0.2,
        "market_gap": 0.2,
        "cost_efficiency": 0.15,
        "execution_feasibility": 0.2,
    },
}

NATIONAL_INDUSTRY_SHARE = 0.08


INVERSE_METRICS = {
    "regional_price_parity",
    "occupation_median_wage",
    "commute_mean_minutes",
    "housing_cost_burden_ratio",
    "unemployment_rate",
    "job_destruction_rate",
    "establishment_death_rate",
    "rurality_index",
}

COMPONENT_SOURCE_METRICS: dict[str, list[str]] = {
    "business_demand": [
        "industry_employment", "business_establishments",
        "business_employment", "gdp_current_dollars", "job_creation_rate",
    ],
    "talent_supply": [
        "target_occupation_employment", "relevant_completions",
        "adjacent_skill_pool_index", "educational_attainment_bachelors_plus",
        "labor_force",
    ],
    "market_gap": [
        "unemployment_rate", "median_household_income",
        "business_employment", "business_establishments",
    ],
    "cost_efficiency": [
        "regional_price_parity", "housing_cost_burden_ratio",
        "commute_mean_minutes", "median_household_income",
    ],
    "execution_feasibility": [
        "served_household_ratio", "high_speed_ratio",
        "internet_access_rate", "work_from_home_rate",
    ],
}


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _avg_present(pct: dict[str, float], keys: list[str]) -> float:
    """Average only metrics that exist in the percentile table."""
    vals = [pct[k] for k in keys if k in pct]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _safe_div(num: float, denom: float, fallback: float = 0.0) -> float:
    return num / denom if denom != 0 else fallback


def _build_percentile_tables(
    grouped: dict[str, list[dict]],
) -> dict[str, dict[str, float]]:
    """Build percentile-based normalization tables across all geographies.
    Returns {geography_id: {metric_name: 0.0-1.0}}."""

    metric_values: dict[str, list[tuple[str, float]]] = {}
    for geo_id, rows in grouped.items():
        seen: dict[str, float] = {}
        for row in rows:
            name = row["metric_name"]
            val = float(row["raw_value"])
            if name not in seen or val > seen[name]:
                seen[name] = val
        for name, val in seen.items():
            metric_values.setdefault(name, []).append((geo_id, val))

    result: dict[str, dict[str, float]] = {}
    for metric_name, pairs in metric_values.items():
        values = [v for _, v in pairs]
        n = len(values)
        if n <= 1:
            for geo_id, _ in pairs:
                result.setdefault(geo_id, {})[metric_name] = 0.5
            continue

        sorted_vals = sorted(set(values))
        rank_map: dict[float, float] = {}
        for i, v in enumerate(sorted_vals):
            rank_map[v] = i / (len(sorted_vals) - 1) if len(sorted_vals) > 1 else 0.5

        for geo_id, val in pairs:
            pct = rank_map.get(val, 0.5)
            if metric_name in INVERSE_METRICS:
                pct = 1.0 - pct
            result.setdefault(geo_id, {})[metric_name] = pct

    return result


def _features(
    rows: list[dict],
    pct: dict[str, float],
) -> dict[str, float]:
    raw = {row["metric_name"]: float(row["raw_value"]) for row in rows}

    population = raw.get("population", 0.0)
    target_emp = raw.get("target_occupation_employment", 0.0)
    rpp = raw.get("regional_price_parity", 100.0)
    median_wage = raw.get("occupation_median_wage", 0.0)
    relevant_completions = raw.get("relevant_completions", 0.0)
    industry_emp = raw.get("industry_employment", 0.0)

    talent_density = _safe_div(target_emp, population / 10_000) if population > 0 else 0.0
    cost_adjusted_wage = _safe_div(median_wage, rpp / 100.0) if rpp > 0 else 0.0
    graduate_pipeline = _safe_div(relevant_completions, target_emp) if target_emp > 0 else 0.0
    local_share = _safe_div(industry_emp, population) if population > 0 else 0.0
    industry_lq = _safe_div(local_share, NATIONAL_INDUSTRY_SHARE)

    talent = _avg_present(pct, [
        "target_occupation_employment", "relevant_completions",
        "adjacent_skill_pool_index", "educational_attainment_bachelors_plus",
        "labor_force",
    ])
    demand = _avg_present(pct, [
        "industry_employment", "business_establishments",
        "business_employment", "gdp_current_dollars",
        "job_creation_rate",
    ])
    cost_signals: list[float] = []
    if "regional_price_parity" in pct:
        cost_signals.append(pct["regional_price_parity"])
    if "housing_cost_burden_ratio" in pct:
        cost_signals.append(pct["housing_cost_burden_ratio"])
    if "commute_mean_minutes" in pct:
        cost_signals.append(pct["commute_mean_minutes"])
    if "median_household_income" in pct:
        cost_signals.append(1.0 - pct["median_household_income"])
    cost_efficiency = _avg(cost_signals) if cost_signals else 0.0
    execution = _avg_present(pct, [
        "served_household_ratio", "high_speed_ratio",
        "internet_access_rate", "work_from_home_rate",
    ])

    industry_fit_vals = [v for v in [
        pct.get("industry_employment"),
        pct.get("business_establishments"),
        pct.get("business_employment"),
        pct.get("gdp_current_dollars"),
    ] if v is not None]
    if industry_lq > 0:
        industry_fit_vals.append(_clamp01(industry_lq / 3.0))
    industry_fit = _avg(industry_fit_vals) if industry_fit_vals else 0.0

    talent_conversion_vals = [v for v in [
        pct.get("adjacent_skill_pool_index"),
        pct.get("remote_compatibility_index"),
        pct.get("educational_attainment_bachelors_plus"),
    ] if v is not None]
    if graduate_pipeline > 0:
        talent_conversion_vals.append(_clamp01(graduate_pipeline))
    talent_conversion = _avg(talent_conversion_vals) if talent_conversion_vals else 0.0

    demand_capture = _avg_present(pct, [
        "industry_employment", "business_establishments",
        "business_employment", "job_creation_rate", "firm_startup_rate",
    ])

    tightness_signals: list[float] = []
    if "unemployment_rate" in pct:
        tightness_signals.append(pct["unemployment_rate"])
    if "median_household_income" in pct:
        tightness_signals.append(pct["median_household_income"])
    if "business_employment" in pct:
        tightness_signals.append(pct["business_employment"])
    if "business_establishments" in pct:
        tightness_signals.append(pct["business_establishments"])
    market_gap = _avg(tightness_signals) if tightness_signals else 0.0

    return {
        "business_demand": demand,
        "talent_supply": talent,
        "market_gap": market_gap,
        "cost_efficiency": cost_efficiency,
        "execution_feasibility": execution,
        "industry_fit": industry_fit,
        "talent_conversion": talent_conversion,
        "demand_capture": demand_capture,
        "talent_density": talent_density,
        "cost_adjusted_wage": cost_adjusted_wage,
        "graduate_pipeline_intensity": graduate_pipeline,
        "industry_specialization_lq": industry_lq,
    }


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def _score(
    geography_id: str,
    features: dict[str, float],
    confidence: float,
    pct: dict[str, float],
) -> list[dict]:
    ts = datetime.now(timezone.utc).isoformat()
    scores: list[dict] = []

    components: list[dict] = []
    weighted_sum = 0.0
    INVERTED_COMPONENTS = {"talent_supply"}
    active_weight = 0.0
    for metric_name, weight in DEFAULT_SCENARIO["weights"].items():
        value = features.get(metric_name, 0.0)
        components.append({"metric_name": metric_name, "weight": weight, "value": value})
        source_keys = COMPONENT_SOURCE_METRICS.get(metric_name, [])
        if any(k in pct for k in source_keys):
            score_value = (1.0 - value) if metric_name in INVERTED_COMPONENTS else value
            weighted_sum += score_value * weight
            active_weight += weight
    if active_weight > 0:
        total_possible = sum(DEFAULT_SCENARIO["weights"].values())
        raw_score = (weighted_sum / active_weight) * 100.0
        coverage = active_weight / total_possible
        if coverage < 0.5:
            raw_score *= coverage / 0.5
        normalized = _clamp_score(raw_score)
    else:
        normalized = 0.0
    scores.append({
        "geography_id": geography_id,
        "scenario_id": DEFAULT_SCENARIO["scenario_id"],
        "period": "latest",
        "score_name": "regional_opportunity_score",
        "score_value": normalized,
        "component_json": components,
        "confidence": confidence,
        "score_version": "v1",
        "updated_at": ts,
    })

    for score_name, feature_key in [
        ("local_core_industry_fit_score", "industry_fit"),
        ("talent_conversion_score", "talent_conversion"),
        ("demand_capture_score", "demand_capture"),
    ]:
        value = features.get(feature_key, 0.0)
        scores.append({
            "geography_id": geography_id,
            "scenario_id": "composite",
            "period": "latest",
            "score_name": score_name,
            "score_value": _clamp_score(value * 100.0),
            "component_json": [{"metric_name": feature_key, "weight": 1.0, "value": value}],
            "confidence": confidence,
            "score_version": "v1",
            "updated_at": ts,
        })

    scores.append({
        "geography_id": geography_id,
        "scenario_id": "composite",
        "period": "latest",
        "score_name": "confidence_score",
        "score_value": _clamp_score(confidence * 100.0),
        "component_json": [{"metric_name": "data_confidence", "weight": 1.0, "value": confidence}],
        "confidence": confidence,
        "score_version": "v1",
        "updated_at": ts,
    })

    return scores


def _recommendation(score_row: dict, features: dict[str, float]) -> dict:
    score_value = float(score_row["score_value"])
    confidence = float(score_row["confidence"])
    demand_score = float(features["business_demand"])
    supply_score = float(features["talent_supply"])
    broadband_score = float(features["execution_feasibility"])

    label = "Monitor"
    rationale = []
    risk_flags = []

    if score_value >= 72 and confidence >= 0.7:
        label = "Enter now"
        rationale.append("Top-tier opportunity with strong data confidence.")
    elif score_value >= 60 and demand_score >= 0.55 and broadband_score >= 0.5:
        label = "Partnership-led market"
        rationale.append("Demand and infrastructure support partner-led entry.")
    elif score_value >= 55 and demand_score > supply_score:
        label = "Demand-first market"
        rationale.append("Demand exceeds local talent supply -- sourcing opportunity.")
    elif score_value >= 55 and supply_score >= demand_score:
        label = "Supply-rich market"
        rationale.append("Strong talent pool relative to demand -- competitive sourcing.")
    elif score_value >= 42:
        label = "Pilot first"
        rationale.append("Moderate signals; validate with a pilot before scaling.")
    elif score_value < 30:
        label = "Avoid for now"
        rationale.append("Bottom-quartile composite signal under current scenario.")
    else:
        rationale.append("Mixed signals; continue monitoring before committing.")

    if broadband_score < 0.4:
        risk_flags.append("Broadband readiness is weak for distributed operations.")
    if confidence < 0.55:
        risk_flags.append("Data confidence is low; verify with manual review.")

    return {
        "geography_id": score_row["geography_id"],
        "scenario_id": score_row["scenario_id"],
        "period": "latest",
        "recommendation_label": label,
        "rationale": rationale,
        "risk_flags": risk_flags,
        "supporting_score_refs": [
            {
                "score_name": score_row["score_name"],
                "score_value": score_value,
                "confidence": confidence,
            }
        ],
        "confidence": confidence,
    }


def run() -> None:
    bundle = load_latest_artifact_bundle("all")
    rows = bundle["metrics"]
    if not rows:
        raise RuntimeError(
            "No all-phase artifacts available. Run `python -m data_pipeline.ingestion.build_all_dataset` first."
        )
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["geography_id"], []).append(row)

    pct_tables = _build_percentile_tables(grouped)

    score_rows: list[dict] = []
    recommendation_rows: list[dict] = []

    for geography_id, geo_rows in grouped.items():
        geo_pct = pct_tables.get(geography_id, {})
        features = _features(geo_rows, geo_pct)
        confidence = min(float(row["confidence"]) for row in geo_rows)
        geo_score_rows = _score(geography_id, features, confidence, geo_pct)
        score_rows.extend(geo_score_rows)
        opportunity_row = geo_score_rows[0]
        recommendation_row = _recommendation(opportunity_row, features)
        recommendation_rows.append(recommendation_row)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path("data_pipeline/artifacts/phase4") / run_id
    _write_ndjson(out_dir / "score_fact.ndjson", score_rows)
    _write_ndjson(out_dir / "recommendation_fact.ndjson", recommendation_rows)

    print(f"phase4_scores={len(score_rows)}")
    print(f"phase4_recommendations={len(recommendation_rows)}")
    print(f"artifacts={out_dir}")


if __name__ == "__main__":
    run()
