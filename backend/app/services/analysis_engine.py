from backend.app.models.schemas import (
    CompareResponse,
    CompareRow,
    GeographyProfileInsightsResponse,
    GeographyProfileTabsResponse,
    ProfileTabAccess,
    ProfileTabEducation,
    ProfileTabIndustries,
    ProfileTabMovement,
    ProfileTabOverview,
    ProfileTabRecommendation,
    ProfileTabTalent,
    RecommendationDistributionResponse,
    RecommendationDistributionRow,
    RecommendationResult,
    ScoreDeltaResponse,
    ScoreDeltaRow,
    ScenarioDefinition,
    ScenarioSimulationResponse,
    ScenarioSimulationRow,
    ScenarioWeights,
    ScoreResult,
)
from backend.app.services.metrics_engine import (
    _get_or_build_percentiles,
    all_metrics_grouped_by_geography,
    derive_score_features,
    metric_bundle_from_artifacts,
)
from backend.app.services.recommendation_engine import build_recommendation
from backend.app.services.scenario_engine import get_scenario
from backend.app.services.scoring_engine import (
    compute_demand_capture_score,
    compute_industry_fit_score,
    compute_regional_opportunity_score,
    compute_talent_conversion_score,
)


def _pct_for_geography(geography_id: str) -> dict[str, float]:
    grouped = all_metrics_grouped_by_geography()
    tables = _get_or_build_percentiles(grouped)
    return tables.get(geography_id, {})


def _score_for_geography_with_scenario(
    geography_id: str,
    scenario_id: str,
    scenario: ScenarioDefinition,
) -> tuple[ScoreResult, dict[str, float]]:
    metrics = metric_bundle_from_artifacts(geography_id)
    pct = _pct_for_geography(geography_id)
    features = derive_score_features(metrics, pct)
    confidence = min(m.confidence for m in metrics)
    score = compute_regional_opportunity_score(
        geography_id=geography_id,
        scenario=scenario,
        features=features,
        confidence=confidence,
        version="v1",
        pct=pct,
    )
    return score, features


def _resolve_parent_to_child(geography_id: str, scenario: ScenarioDefinition | None = None) -> str:
    """If geography_id is a parent (e.g. IN-MP) with no direct metrics,
    find the best-scoring child geography that starts with that prefix."""
    grouped = all_metrics_grouped_by_geography()
    if geography_id in grouped:
        return geography_id
    prefix = geography_id + "-"
    children = [gid for gid in grouped if gid.startswith(prefix)]
    if not children:
        return geography_id
    if len(children) == 1:
        return children[0]
    tables = _get_or_build_percentiles(grouped)
    best_id = children[0]
    best_opp = -1.0
    from backend.app.services.metrics_engine import compute_opportunity_score, derive_score_features
    active_scenario = scenario or get_scenario("default-opportunity")
    weights = active_scenario.weights.model_dump()
    for cid in children:
        pct = tables.get(cid, {})
        features = derive_score_features(grouped[cid], pct)
        opp = compute_opportunity_score(features, weights, pct, geography_id=cid)
        if opp > best_opp:
            best_opp = opp
            best_id = cid
    return best_id


def score_for_geography(geography_id: str, scenario_id: str) -> tuple[ScoreResult, dict[str, float]]:
    scenario = get_scenario(scenario_id)
    resolved_id = _resolve_place_to_county(geography_id)
    resolved_id = _resolve_parent_to_child(resolved_id, scenario=scenario)
    return _score_for_geography_with_scenario(
        geography_id=resolved_id,
        scenario_id=scenario_id,
        scenario=scenario,
    )


def _resolve_place_to_county(geography_id: str) -> str:
    """If geography_id is a US place (7-digit FIPS), resolve to its parent county."""
    if not (len(geography_id) == 7 and geography_id.isdigit()):
        return geography_id
    place_map = _load_place_county_map()
    county_id = place_map.get(geography_id, "")
    return county_id if county_id else geography_id


def recommendation_for_geography(geography_id: str, scenario_id: str) -> RecommendationResult:
    scenario = get_scenario(scenario_id)
    resolved_id = _resolve_parent_to_child(geography_id, scenario=scenario)
    score, features = score_for_geography(geography_id=resolved_id, scenario_id=scenario_id)
    return build_recommendation(
        geography_id=resolved_id,
        scenario_id=scenario_id,
        score=score,
        demand_score=features["business_demand"],
        supply_score=features["talent_supply"],
        broadband_score=features["execution_feasibility"],
    )


def compare_geographies(geography_ids: list[str], scenario_id: str) -> CompareResponse:
    from backend.app.services.metrics_engine import ArtifactDataUnavailableError as _ADUE

    unique_geo_ids: list[str] = list(dict.fromkeys(geography_ids))
    if len(unique_geo_ids) < 2:
        raise ValueError("compare_geographies requires at least 2 unique geographies")
    if len(unique_geo_ids) > 25:
        raise ValueError("compare_geographies supports up to 25 geographies per request")
    scenario = get_scenario(scenario_id)
    rows: list[CompareRow] = []
    for geography_id in unique_geo_ids:
        resolved_id = _resolve_parent_to_child(geography_id, scenario=scenario)
        try:
            score, features = _score_for_geography_with_scenario(
                geography_id=resolved_id,
                scenario_id=scenario_id,
                scenario=scenario,
            )
            recommendation = build_recommendation(
                geography_id=resolved_id,
                scenario_id=scenario_id,
                score=score,
                demand_score=features["business_demand"],
                supply_score=features["talent_supply"],
                broadband_score=features["execution_feasibility"],
            )
            rows.append(
                CompareRow(
                    geography_id=resolved_id,
                    score=score.score_value,
                    recommendation=recommendation.label,
                    demand_score=features["business_demand"],
                    supply_score=features["talent_supply"],
                    confidence=score.confidence,
                )
            )
        except (_ADUE, KeyError):
            rows.append(
                CompareRow(
                    geography_id=resolved_id,
                    score=0.0,
                    recommendation="Monitor",
                    demand_score=0.0,
                    supply_score=0.0,
                    confidence=0.0,
                )
            )
    rows.sort(key=lambda row: row.score, reverse=True)
    return CompareResponse(scenario_id=scenario_id, rows=rows)


def ranked_scores(scenario_id: str, limit: int) -> list[tuple[str, float, float]]:
    if limit < 1:
        return []
    limit = min(limit, 10000)
    scenario = get_scenario(scenario_id)
    out = _ranked_rows_for_scenario(scenario)
    return out[:limit]


def _ranked_rows_for_scenario(
    scenario: ScenarioDefinition,
    grouped: dict[str, list] | None = None,
) -> list[tuple[str, float, float]]:
    grouped_metrics = grouped if grouped is not None else all_metrics_grouped_by_geography()
    pct_tables = _get_or_build_percentiles(grouped_metrics)
    out: list[tuple[str, float, float]] = []
    county_scores: dict[str, tuple[float, float]] = {}
    for geography_id, metrics in grouped_metrics.items():
        pct = pct_tables.get(geography_id, {})
        features = derive_score_features(metrics, pct)
        confidence = min(m.confidence for m in metrics)
        score = compute_regional_opportunity_score(
            geography_id=geography_id,
            scenario=scenario,
            features=features,
            confidence=confidence,
            version="v1",
            pct=pct,
        )
        out.append((geography_id, score.score_value, score.confidence))
        if len(geography_id) == 5 and geography_id.isdigit():
            county_scores[geography_id] = (score.score_value, score.confidence)

    existing_geo_ids = set(grouped_metrics.keys())
    place_rows = _propagate_place_scores(county_scores, existing_geo_ids)
    out.extend(place_rows)

    out.sort(key=lambda item: item[1], reverse=True)
    return out


_place_county_map: dict[str, str] | None = None


def _load_place_county_map() -> dict[str, str]:
    """Build place_geoid → county_geoid mapping from Census API + cache."""
    global _place_county_map
    if _place_county_map is not None:
        return _place_county_map

    import json as _json, os, urllib.request, urllib.error
    from pathlib import Path

    cache_file = Path("backend/data/place_county_crosswalk.json")
    if cache_file.exists():
        try:
            _place_county_map = _json.loads(cache_file.read_text(encoding="utf-8"))
            if _place_county_map:
                return _place_county_map
        except (ValueError, OSError):
            pass

    mapping: dict[str, str] = {}
    key = os.getenv("CENSUS_API_KEY", "").strip()
    if key:
        try:
            url = (
                f"https://api.census.gov/data/2020/dec/pl"
                f"?get=NAME&for=place:*&in=state:*+county:*&key={key}"
            )
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                rows = _json.loads(resp.read())
                for row in rows[1:]:
                    state = row[1]
                    county = row[2]
                    place = row[3]
                    place_geoid = f"{state}{place}"
                    county_geoid = f"{state}{county}"
                    if place_geoid not in mapping:
                        mapping[place_geoid] = county_geoid
            print(f"[PlaceCounty] Loaded {len(mapping)} place→county mappings from Census")
        except Exception as exc:
            print(f"[PlaceCounty] Census crosswalk fetch failed: {exc}")

    if not mapping:
        boundary_file = Path("backend/data/boundary_cache/us_places_v1.json")
        if boundary_file.exists():
            try:
                bd = _json.loads(boundary_file.read_text(encoding="utf-8"))
                for feat in bd.get("features", []):
                    gid = feat.get("properties", {}).get("GEOID", "")
                    if len(gid) == 7 and gid.isdigit():
                        mapping[gid] = ""
            except (ValueError, OSError):
                pass

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(_json.dumps(mapping), encoding="utf-8")
    _place_county_map = mapping
    return mapping


def _propagate_place_scores(
    county_scores: dict[str, tuple[float, float]],
    existing_geo_ids: set[str] | None = None,
) -> list[tuple[str, float, float]]:
    """Assign each US place its parent county's score, skipping places that already have direct data."""
    place_map = _load_place_county_map()
    if not place_map or not county_scores:
        return []

    skip = existing_geo_ids or set()

    state_best: dict[str, tuple[float, float]] = {}
    for county_id, (score, conf) in county_scores.items():
        st = county_id[:2]
        if st not in state_best or score > state_best[st][0]:
            state_best[st] = (score, conf)

    out: list[tuple[str, float, float]] = []
    for place_id, county_id in place_map.items():
        if place_id in skip:
            continue
        if county_id and county_id in county_scores:
            score, conf = county_scores[county_id]
            out.append((place_id, score, max(0.0, conf - 0.1)))
        else:
            st = place_id[:2]
            if st in state_best:
                score, conf = state_best[st]
                out.append((place_id, score * 0.85, max(0.0, conf - 0.2)))
    return out


def score_delta(
    baseline_scenario_id: str,
    scenario_id: str,
    limit: int,
) -> ScoreDeltaResponse:
    baseline = get_scenario(baseline_scenario_id)
    scenario = get_scenario(scenario_id)
    grouped = all_metrics_grouped_by_geography()
    baseline_rows = _ranked_rows_for_scenario(baseline, grouped=grouped)
    scenario_rows = _ranked_rows_for_scenario(scenario, grouped=grouped)

    baseline_rank = {geo_id: idx for idx, (geo_id, _score, _conf) in enumerate(baseline_rows, start=1)}
    baseline_score = {geo_id: score for geo_id, score, _conf in baseline_rows}
    scenario_rank = {geo_id: idx for idx, (geo_id, _score, _conf) in enumerate(scenario_rows, start=1)}
    scenario_score = {geo_id: score for geo_id, score, _conf in scenario_rows}

    pct_tables = _get_or_build_percentiles(grouped)
    rows: list[ScoreDeltaRow] = []
    for geo_id in scenario_rank.keys():
        b_rank = baseline_rank[geo_id]
        s_rank = scenario_rank[geo_id]
        b_score = baseline_score[geo_id]
        s_score = scenario_score[geo_id]
        geo_pct = pct_tables.get(geo_id, {})
        features = derive_score_features(grouped[geo_id], geo_pct)
        confidence = min(m.confidence for m in grouped[geo_id])
        baseline_score_result = compute_regional_opportunity_score(
            geography_id=geo_id,
            scenario=baseline,
            features=features,
            confidence=confidence,
            version="v1",
            pct=geo_pct,
        )
        scenario_score_result = compute_regional_opportunity_score(
            geography_id=geo_id,
            scenario=scenario,
            features=features,
            confidence=confidence,
            version="v1",
            pct=geo_pct,
        )
        baseline_components = {
            item.metric_name: item.weight * item.value for item in baseline_score_result.components
        }
        scenario_components = {
            item.metric_name: item.weight * item.value for item in scenario_score_result.components
        }
        component_names = sorted(set(baseline_components.keys()).union(scenario_components.keys()))
        component_deltas = {
            name: scenario_components.get(name, 0.0) - baseline_components.get(name, 0.0)
            for name in component_names
        }
        top_component = (
            max(component_deltas.items(), key=lambda pair: abs(pair[1])) if component_deltas else None
        )

        rows.append(
            ScoreDeltaRow(
                geography_id=geo_id,
                baseline_rank=b_rank,
                scenario_rank=s_rank,
                rank_change=b_rank - s_rank,
                baseline_score=b_score,
                scenario_score=s_score,
                score_change=s_score - b_score,
                top_component_shift=(
                    f"{top_component[0]} ({top_component[1]:+.3f})" if top_component else None
                ),
            )
        )
    rows.sort(key=lambda row: abs(row.rank_change), reverse=True)
    return ScoreDeltaResponse(
        baseline_scenario_id=baseline_scenario_id,
        scenario_id=scenario_id,
        rows=rows[: min(max(limit, 1), 200)],
    )


def simulate_scores(
    weights: ScenarioWeights,
    limit: int,
    geography_ids: list[str] | None = None,
) -> ScenarioSimulationResponse:
    full_grouped = all_metrics_grouped_by_geography()
    grouped = full_grouped
    if geography_ids:
        allowed = set(geography_ids)
        grouped = {geo_id: metrics for geo_id, metrics in grouped.items() if geo_id in allowed}

    scenario = ScenarioDefinition(
        scenario_id="simulation",
        name="Simulation",
        description="Temporary what-if simulation.",
        weights=weights,
        filters={},
    )
    # Keep percentile normalization consistent with global corpus even when
    # simulation is constrained to a subset of geographies.
    pct_tables = _get_or_build_percentiles(full_grouped)
    rows: list[tuple[str, float, float]] = []
    for geography_id, metrics in grouped.items():
        geo_pct = pct_tables.get(geography_id, {})
        features = derive_score_features(metrics, geo_pct)
        confidence = min(m.confidence for m in metrics)
        score = compute_regional_opportunity_score(
            geography_id=geography_id,
            scenario=scenario,
            features=features,
            confidence=confidence,
            version="v1",
            pct=geo_pct,
        )
        rows.append((geography_id, score.score_value, score.confidence))
    rows.sort(key=lambda item: item[1], reverse=True)
    ranked = [
        ScenarioSimulationRow(rank=idx, geography_id=geo_id, score=score, confidence=confidence)
        for idx, (geo_id, score, confidence) in enumerate(rows[:limit], start=1)
    ]
    return ScenarioSimulationResponse(rows=ranked)


def recommendation_distribution(scenario_id: str) -> RecommendationDistributionResponse:
    scenario = get_scenario(scenario_id)
    grouped = all_metrics_grouped_by_geography()
    pct_tables = _get_or_build_percentiles(grouped)
    counts: dict[str, int] = {}
    for geography_id, metrics in grouped.items():
        geo_pct = pct_tables.get(geography_id, {})
        features = derive_score_features(metrics, geo_pct)
        confidence = min(m.confidence for m in metrics)
        score = compute_regional_opportunity_score(
            geography_id=geography_id,
            scenario=scenario,
            features=features,
            confidence=confidence,
            version="v1",
            pct=geo_pct,
        )
        recommendation = build_recommendation(
            geography_id=geography_id,
            scenario_id=scenario_id,
            score=score,
            demand_score=features["business_demand"],
            supply_score=features["talent_supply"],
            broadband_score=features["execution_feasibility"],
        )
        counts[recommendation.label] = counts.get(recommendation.label, 0) + 1

    rows = [
        RecommendationDistributionRow(label=label, count=count)
        for label, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
    ]
    return RecommendationDistributionResponse(
        scenario_id=scenario_id,
        total_geographies=sum(counts.values()),
        rows=rows,
    )


def geography_profile_insights(
    geography_id: str,
    scenario_id: str,
) -> GeographyProfileInsightsResponse:
    score, features = score_for_geography(geography_id=geography_id, scenario_id=scenario_id)
    recommendation = build_recommendation(
        geography_id=geography_id,
        scenario_id=scenario_id,
        score=score,
        demand_score=features["business_demand"],
        supply_score=features["talent_supply"],
        broadband_score=features["execution_feasibility"],
    )

    sorted_features = sorted(features.items(), key=lambda item: item[1], reverse=True)
    strengths = [f"{name} ({value:.3f})" for name, value in sorted_features[:3]]
    risks = [f"{name} ({value:.3f})" for name, value in sorted_features[-2:]]
    return GeographyProfileInsightsResponse(
        geography_id=geography_id,
        scenario_id=scenario_id,
        score=score.score_value,
        recommendation=recommendation.label,
        strengths=strengths,
        risks=risks,
        key_metrics=features,
    )


def geography_profile_tabs(
    geography_id: str,
    scenario_id: str,
) -> GeographyProfileTabsResponse:
    """PRD Section 14: assemble profile drawer tabs."""
    metrics = metric_bundle_from_artifacts(geography_id)
    raw = {m.metric_name: m.raw_value for m in metrics}
    pct = _pct_for_geography(geography_id)
    features = derive_score_features(metrics, pct)
    confidence = min(m.confidence for m in metrics)

    scenario = get_scenario(scenario_id)
    score = compute_regional_opportunity_score(
        geography_id=geography_id,
        scenario=scenario,
        features=features,
        confidence=confidence,
        version="v1",
        pct=pct,
    )
    recommendation = build_recommendation(
        geography_id=geography_id,
        scenario_id=scenario_id,
        score=score,
        demand_score=features["business_demand"],
        supply_score=features["talent_supply"],
        broadband_score=features["execution_feasibility"],
    )
    ind_fit = compute_industry_fit_score(geography_id, features, confidence)
    tal_conv = compute_talent_conversion_score(geography_id, features, confidence)
    dem_cap = compute_demand_capture_score(geography_id, features, confidence)

    sorted_components = sorted(
        score.components,
        key=lambda c: abs(c.weight * c.value),
        reverse=True,
    )
    key_drivers = [
        f"{c.metric_name} ({(c.weight * c.value):.3f} contribution)"
        for c in sorted_components[:3]
    ]

    from backend.app.services.metrics_engine import _infer_geography_type

    return GeographyProfileTabsResponse(
        geography_id=geography_id,
        scenario_id=scenario_id,
        overview=ProfileTabOverview(
            geography_id=geography_id,
            geography_type=_infer_geography_type(geography_id),
            population=raw.get("population"),
            opportunity_score=score.score_value,
            recommendation=recommendation.label,
            confidence=confidence,
        ),
        talent=ProfileTabTalent(
            target_occupation_employment=raw.get("target_occupation_employment"),
            talent_density=features.get("talent_density"),
            adjacent_skill_pool_index=raw.get("adjacent_skill_pool_index"),
            labor_force=raw.get("labor_force"),
            unemployment_rate=raw.get("unemployment_rate"),
            educational_attainment_bachelors_plus=raw.get("educational_attainment_bachelors_plus"),
            talent_conversion_score=tal_conv.score_value,
        ),
        industries=ProfileTabIndustries(
            industry_employment=raw.get("industry_employment"),
            business_establishments=raw.get("business_establishments"),
            industry_specialization_lq=features.get("industry_specialization_lq"),
            gdp_current_dollars=raw.get("gdp_current_dollars"),
            gdp_growth_rate=raw.get("gdp_growth_rate"),
            job_creation_rate=raw.get("job_creation_rate"),
            net_job_dynamism=raw.get("net_job_dynamism"),
            industry_fit_score=ind_fit.score_value,
            demand_capture_score=dem_cap.score_value,
        ),
        education=ProfileTabEducation(
            relevant_completions=raw.get("relevant_completions"),
            institutions_reporting=raw.get("institutions_reporting"),
            graduate_pipeline_intensity=features.get("graduate_pipeline_intensity"),
            median_earnings_4yr=raw.get("median_earnings_4yr"),
            completion_rate=raw.get("completion_rate"),
        ),
        movement=ProfileTabMovement(
            net_migrants=raw.get("net_migrants"),
            inbound_returns=raw.get("inbound_returns"),
            outbound_returns=raw.get("outbound_returns"),
            workplace_jobs=raw.get("workplace_jobs"),
            residence_workers=raw.get("residence_workers"),
            commute_inflow_ratio=raw.get("commute_inflow_ratio"),
            population_growth_rate=raw.get("population_growth_rate"),
        ),
        access=ProfileTabAccess(
            served_household_ratio=raw.get("served_household_ratio"),
            high_speed_ratio=raw.get("high_speed_ratio"),
            rurality_index=raw.get("rurality_index"),
            metro_linkage_index=raw.get("metro_linkage_index"),
            internet_access_rate=raw.get("internet_access_rate"),
            work_from_home_rate=raw.get("work_from_home_rate"),
            commute_mean_minutes=raw.get("commute_mean_minutes"),
            cost_adjusted_wage=features.get("cost_adjusted_wage"),
            regional_price_parity=raw.get("regional_price_parity"),
            housing_cost_burden_ratio=raw.get("housing_cost_burden_ratio"),
        ),
        recommendation=ProfileTabRecommendation(
            label=recommendation.label,
            rationale=recommendation.rationale,
            risk_flags=recommendation.risk_flags,
            key_drivers=key_drivers,
            demand_supply_gap=features.get("demand_supply_gap"),
        ),
    )
