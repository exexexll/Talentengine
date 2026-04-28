from datetime import date, datetime
from functools import lru_cache
import math

from backend.app.models.schemas import GeographyProfile, MetricValue
from backend.app.services.artifact_store import load_latest_artifact_bundle
from backend.app.services.confidence_engine import (
    apply_quality_penalties,
    confidence_from_freshness,
)


class ArtifactDataUnavailableError(RuntimeError):
    pass


_VALID_STATE_FIPS = {str(i).zfill(2) for i in list(range(1, 57)) + [60, 66, 69, 72, 78]}


def _infer_geography_type(geography_id: str) -> str:
    if geography_id in ("00", "AU", "IN"):
        return "nation"
    if geography_id.startswith("EU-") and len(geography_id) == 5:
        return "nation"
    if geography_id.startswith("AU-") and not geography_id.startswith("AU-SA4"):
        return "state"
    if geography_id.startswith("AU-SA4"):
        return "county"
    if geography_id.startswith("IN-"):
        parts = geography_id.split("-")
        if len(parts) >= 3:
            return "county"
        return "state"
    if geography_id.startswith("EU-"):
        code = geography_id[3:]
        if len(code) == 2:
            return "nation"
        if len(code) == 3:
            return "state"
        return "county"
    if len(geography_id) == 2 and geography_id.isdigit():
        return "state"
    if len(geography_id) == 5 and geography_id.isdigit():
        state_prefix = geography_id[:2]
        county_suffix = int(geography_id[2:])
        if state_prefix in _VALID_STATE_FIPS and county_suffix <= 840:
            return "county"
        return "metro"
    if len(geography_id) == 11 and geography_id.isdigit():
        return "tract"
    if len(geography_id) == 15 and geography_id.isdigit():
        return "block"
    if len(geography_id) == 7 and geography_id.isdigit():
        return "place"
    if len(geography_id) == 8 and geography_id.isdigit():
        return "metro"
    return "place"


_FIPS_TO_STATE: dict[str, str] = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa", "20": "Kansas",
    "21": "Kentucky", "22": "Louisiana", "23": "Maine", "24": "Maryland",
    "25": "Massachusetts", "26": "Michigan", "27": "Minnesota", "28": "Mississippi",
    "29": "Missouri", "30": "Montana", "31": "Nebraska", "32": "Nevada",
    "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico", "36": "New York",
    "37": "North Carolina", "38": "North Dakota", "39": "Ohio", "40": "Oklahoma",
    "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island", "45": "South Carolina",
    "46": "South Dakota", "47": "Tennessee", "48": "Texas", "49": "Utah",
    "50": "Vermont", "51": "Virginia", "53": "Washington", "54": "West Virginia",
    "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico",
}

_geo_name_cache: dict[str, str] | None = None


def _build_geo_name_cache(geo_ids: list[str]) -> dict[str, str]:
    """Batch-resolve geography names from Census API. Cached after first call."""
    global _geo_name_cache
    if _geo_name_cache is not None:
        return _geo_name_cache

    import json as _json
    import os
    import urllib.request
    import urllib.error
    from pathlib import Path

    cache_file = Path("backend/data/geography_names.json")
    if cache_file.exists():
        try:
            _geo_name_cache = _json.loads(cache_file.read_text(encoding="utf-8"))
            if len(_geo_name_cache) > 100:
                return _geo_name_cache
        except (ValueError, OSError):
            pass

    names: dict[str, str] = _geo_name_cache or {}

    for fips, state_name in _FIPS_TO_STATE.items():
        names[fips] = state_name

    _CBSA_NAMES: dict[str, str] = {
        "12060": "Atlanta, GA Metro", "12420": "Austin, TX Metro",
        "13820": "Birmingham, AL Metro", "14460": "Boston, MA Metro",
        "16740": "Charlotte, NC Metro", "16980": "Chicago, IL Metro",
        "17460": "Cleveland, OH Metro", "19100": "Dallas-Fort Worth, TX Metro",
        "19740": "Denver, CO Metro", "19820": "Detroit, MI Metro",
        "26420": "Houston, TX Metro", "29820": "Las Vegas, NV Metro",
        "31080": "Los Angeles, CA Metro", "33100": "Miami, FL Metro",
        "33460": "Minneapolis, MN Metro", "35380": "New Orleans, LA Metro",
        "35620": "New York, NY Metro", "36740": "Orlando, FL Metro",
        "37980": "Philadelphia, PA Metro", "38060": "Phoenix, AZ Metro",
        "38300": "Pittsburgh, PA Metro", "38900": "Portland, OR Metro",
        "40060": "Richmond, VA Metro", "40140": "Riverside, CA Metro",
        "41180": "St. Louis, MO Metro", "41620": "Salt Lake City, UT Metro",
        "41700": "San Antonio, TX Metro", "41740": "San Diego, CA Metro",
        "41860": "San Francisco, CA Metro", "42660": "Seattle, WA Metro",
        "45300": "Tampa, FL Metro", "47900": "Washington, DC Metro",
    }
    names.update(_CBSA_NAMES)

    _GLOBAL_NAMES: dict[str, str] = {
        "AU": "Australia", "AU-NSW": "New South Wales", "AU-VIC": "Victoria",
        "AU-QLD": "Queensland", "AU-SA": "South Australia", "AU-WA": "Western Australia",
        "AU-TAS": "Tasmania", "AU-NT": "Northern Territory", "AU-ACT": "Australian Capital Territory",
        "IN": "India", "IN-AP": "Andhra Pradesh", "IN-AR": "Arunachal Pradesh",
        "IN-AS": "Assam", "IN-BR": "Bihar", "IN-CT": "Chhattisgarh", "IN-GA": "Goa",
        "IN-GJ": "Gujarat", "IN-HR": "Haryana", "IN-HP": "Himachal Pradesh",
        "IN-JH": "Jharkhand", "IN-KA": "Karnataka", "IN-KL": "Kerala",
        "IN-MP": "Madhya Pradesh", "IN-MH": "Maharashtra", "IN-MN": "Manipur",
        "IN-ML": "Meghalaya", "IN-MZ": "Mizoram", "IN-NL": "Nagaland",
        "IN-OR": "Odisha", "IN-PB": "Punjab", "IN-RJ": "Rajasthan",
        "IN-SK": "Sikkim", "IN-TN": "Tamil Nadu", "IN-TG": "Telangana",
        "IN-TR": "Tripura", "IN-UP": "Uttar Pradesh", "IN-UK": "Uttarakhand",
        "IN-WB": "West Bengal", "IN-DL": "Delhi",
        "AUS": "Australia", "IND": "India",
    }
    names.update(_GLOBAL_NAMES)

    _EU_COUNTRY_NAMES: dict[str, str] = {
        "EU-AT": "Austria", "EU-BE": "Belgium", "EU-BG": "Bulgaria", "EU-HR": "Croatia",
        "EU-CY": "Cyprus", "EU-CZ": "Czechia", "EU-DK": "Denmark", "EU-EE": "Estonia",
        "EU-FI": "Finland", "EU-FR": "France", "EU-DE": "Germany", "EU-EL": "Greece",
        "EU-HU": "Hungary", "EU-IE": "Ireland", "EU-IT": "Italy", "EU-LV": "Latvia",
        "EU-LT": "Lithuania", "EU-LU": "Luxembourg", "EU-MT": "Malta",
        "EU-NL": "Netherlands", "EU-PL": "Poland", "EU-PT": "Portugal",
        "EU-RO": "Romania", "EU-SK": "Slovakia", "EU-SI": "Slovenia",
        "EU-ES": "Spain", "EU-SE": "Sweden", "EU-NO": "Norway",
        "EU-CH": "Switzerland", "EU-UK": "United Kingdom", "EU-IS": "Iceland",
        "EU-AL": "Albania", "EU-RS": "Serbia", "EU-ME": "Montenegro",
        "EU-MK": "North Macedonia", "EU-TR": "Turkey", "EU-BA": "Bosnia and Herzegovina",
    }
    names.update(_EU_COUNTRY_NAMES)

    census_key = os.getenv("CENSUS_API_KEY", "").strip()
    if census_key:
        try:
            url = (
                f"https://api.census.gov/data/2022/acs/acs5"
                f"?get=NAME&for=county:*&in=state:*&key={census_key}"
            )
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                if "application/json" in resp.headers.get("Content-Type", ""):
                    rows = _json.loads(resp.read())
                    for row in rows[1:]:
                        name_val, state_fips, county_fips = row[0], row[1], row[2]
                        geo_id = f"{state_fips}{county_fips}"
                        names[geo_id] = name_val
        except Exception as exc:
            print(f"[NameCache] County name fetch failed: {exc}")

        try:
            url = (
                f"https://api.census.gov/data/2022/acs/acs5"
                f"?get=NAME&for=place:*&in=state:*&key={census_key}"
            )
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                if "application/json" in resp.headers.get("Content-Type", ""):
                    rows = _json.loads(resp.read())
                    for row in rows[1:]:
                        name_val, state_fips, place_fips = row[0], row[1], row[2]
                        geo_id = f"{state_fips}{place_fips}"
                        names[geo_id] = name_val
        except Exception as exc:
            print(f"[NameCache] Place name fetch failed: {exc}")

    boundary_cache_dir = Path("backend/data/boundary_cache")
    for cache_name in ("au_sa4_v2", "in_districts_v2", "eu_nuts2_v2", "us_places_v1"):
        bf = boundary_cache_dir / f"{cache_name}.json"
        if not bf.exists():
            continue
        try:
            bd = _json.loads(bf.read_text(encoding="utf-8"))
            for feat in bd.get("features", []):
                gid = feat.get("properties", {}).get("GEOID", "")
                nm = feat.get("properties", {}).get("name", "")
                if gid and nm:
                    names[gid] = nm
        except (ValueError, OSError):
            pass

    for gid in geo_ids:
        if gid in names:
            continue
        if gid.startswith("AU-SA4"):
            names[gid] = f"SA4 {gid[6:]}, Australia"
        elif gid.startswith("IN-") and gid.count("-") >= 2:
            state_code = gid.split("-")[1]
            state_name = names.get(f"IN-{state_code}", state_code)
            names[gid] = f"District in {state_name}, India"
        elif gid.startswith("EU-"):
            names[gid] = gid[3:]

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(_json.dumps(names, indent=2), encoding="utf-8")
    _geo_name_cache = names
    return names


def _resolve_geo_name(geo_id: str, names: dict[str, str]) -> str:
    if geo_id in names:
        return names[geo_id]
    if len(geo_id) == 2:
        return _FIPS_TO_STATE.get(geo_id, geo_id)
    if len(geo_id) == 5:
        state = _FIPS_TO_STATE.get(geo_id[:2], "")
        return f"County {geo_id}, {state}" if state else geo_id
    return geo_id


def list_geographies_from_artifacts() -> list[GeographyProfile]:
    bundle = load_latest_artifact_bundle("all")
    metrics = bundle["metrics"]
    geo_ids = sorted({row["geography_id"] for row in metrics})
    if not geo_ids:
        raise ArtifactDataUnavailableError(
            "No artifact metrics found. Run `python -m data_pipeline.ingestion.build_all_dataset`."
        )
    names = _build_geo_name_cache(geo_ids)
    return [
        GeographyProfile(
            geography_id=geo_id,
            geography_type=_infer_geography_type(geo_id),
            name=_resolve_geo_name(geo_id, names),
        )
        for geo_id in geo_ids
    ]


def search_geographies(query: str, geography_types: set[str] | None = None) -> list[GeographyProfile]:
    normalized_query = query.strip().lower()
    if not normalized_query:
        return []
    matches: list[GeographyProfile] = []
    for item in list_geographies_from_artifacts():
        if geography_types and item.geography_type not in geography_types:
            continue
        if normalized_query in item.geography_id.lower() or normalized_query in item.name.lower():
            matches.append(item)
    return matches[:50]


@lru_cache(maxsize=8)
def _metric_values_by_geography_for_run(run_name: str) -> dict[str, list[MetricValue]]:
    bundle = load_latest_artifact_bundle("all")
    if bundle["run_name"] != run_name:
        return {}
    rows = bundle["metrics"]
    if not rows:
        return {}
    grouped_rows: dict[str, list[dict]] = bundle["metrics_by_geography"]
    snapshots_by_id = bundle["snapshots_by_id"]
    output: dict[str, list[MetricValue]] = {}
    for geo_id, geo_rows in grouped_rows.items():
        metrics: list[MetricValue] = []
        for row in geo_rows:
            snapshot = snapshots_by_id.get(row["source_snapshot_id"])
            last_refresh = (
                datetime.fromisoformat(snapshot["extracted_at"]).date()
                if snapshot and snapshot.get("extracted_at")
                else date.today()
            )
            base_conf = confidence_from_freshness(last_refresh)
            confidence = apply_quality_penalties(
                base_confidence=min(base_conf, float(row.get("confidence", 0.8))),
                is_imputed_from_coarser_geo=False,
                has_known_source_noise=False,
            )
            metrics.append(
                MetricValue(
                    geography_id=row["geography_id"],
                    period=row["period"],
                    metric_name=row["metric_name"],
                    raw_value=float(row["raw_value"]),
                    normalized_value=row.get("normalized_value"),
                    units=row["units"],
                    source=row["source_snapshot_id"].rsplit("-", 1)[0],
                    source_period=row["period"],
                    last_refresh=last_refresh,
                    formula=row.get("formula", ""),
                    confidence=confidence,
                )
            )
        output[geo_id] = metrics
    return output


def metric_bundle_from_artifacts(geography_id: str) -> list[MetricValue]:
    bundle = load_latest_artifact_bundle("all")
    run_name = bundle["run_name"]
    if not run_name:
        raise ArtifactDataUnavailableError(
            "No artifact metrics found. Run `python -m data_pipeline.ingestion.build_all_dataset`."
        )
    grouped = _metric_values_by_geography_for_run(run_name)
    rows = grouped.get(geography_id, [])
    if not rows:
        raise ArtifactDataUnavailableError(
            f"No metrics found for geography_id={geography_id} in latest artifacts."
        )
    return rows


def all_metrics_grouped_by_geography() -> dict[str, list[MetricValue]]:
    bundle = load_latest_artifact_bundle("all")
    run_name = bundle["run_name"]
    if not run_name or not bundle["metrics"]:
        raise ArtifactDataUnavailableError(
            "No artifact metrics found. Run `python -m data_pipeline.ingestion.build_all_dataset`."
        )
    return _metric_values_by_geography_for_run(run_name)


def refresh_metric_cache() -> None:
    global _percentile_cache
    _metric_values_by_geography_for_run.cache_clear()
    _percentile_cache = None


INVERSE_METRICS = {
    "regional_price_parity", "occupation_median_wage", "commute_mean_minutes",
    "housing_cost_burden_ratio", "unemployment_rate", "job_destruction_rate",
    "establishment_death_rate", "rurality_index",
    # Global fallback affordability proxies: higher GDP/capita usually implies
    # higher labor and operating costs for staffing operations.
    "gdp_per_capita", "gdp_per_capita_ppp",
}

COMPONENT_SOURCE_METRICS: dict[str, list[str]] = {
    "business_demand": [
        "industry_employment", "business_establishments",
        "business_employment", "gdp_current_dollars", "job_creation_rate",
        # Fallback proxies (incl. place/city where firm counts are sparse)
        "gdp_per_capita", "gdp_per_capita_ppp", "employment_to_pop_ratio",
        "population", "labor_force", "median_household_income", "unemployment_rate",
    ],
    "talent_supply": [
        "target_occupation_employment", "relevant_completions",
        "adjacent_skill_pool_index", "educational_attainment_bachelors_plus",
        "labor_force",
        # Global fallback proxies
        "population", "employment_to_pop_ratio",
    ],
    "market_gap": [
        "unemployment_rate", "median_household_income",
        "job_creation_rate", "firm_startup_rate",
        "target_occupation_employment", "relevant_completions",
    ],
    "cost_efficiency": [
        "regional_price_parity", "housing_cost_burden_ratio",
        "commute_mean_minutes", "median_household_income",
        # Global fallback proxies
        "gdp_per_capita", "gdp_per_capita_ppp",
    ],
    "execution_feasibility": [
        "internet_access_rate", "work_from_home_rate",
        # Global fallback
        "employment_to_pop_ratio",
    ],
}

INVERTED_COMPONENTS = {"talent_supply"}
OPPORTUNITY_SCORE_CONTRAST = 1.65

# Value keys actually used by derive_score_features per component.
# Keep this aligned with feature formulas to avoid source/value drift.
COMPONENT_VALUE_METRICS: dict[str, list[str]] = {
    "business_demand": [
        "industry_employment", "business_establishments",
        "business_employment", "gdp_current_dollars", "job_creation_rate",
        "gdp_per_capita", "gdp_per_capita_ppp", "employment_to_pop_ratio",
        "population", "labor_force", "median_household_income", "unemployment_rate",
    ],
    "talent_supply": [
        "target_occupation_employment", "relevant_completions",
        "adjacent_skill_pool_index", "educational_attainment_bachelors_plus",
        "labor_force",
    ],
    "market_gap": [
        "unemployment_rate", "median_household_income",
        "job_creation_rate", "firm_startup_rate",
        "target_occupation_employment", "relevant_completions",
    ],
    "cost_efficiency": [
        "regional_price_parity", "housing_cost_burden_ratio",
        "commute_mean_minutes", "median_household_income",
        "gdp_per_capita", "gdp_per_capita_ppp",
    ],
    "execution_feasibility": [
        "internet_access_rate", "work_from_home_rate",
        "employment_to_pop_ratio",
    ],
}

# Metrics treated as proxy-like for city-level reliability controls.
COMPONENT_PROXY_METRICS: dict[str, set[str]] = {
    "business_demand": {"gdp_per_capita", "gdp_per_capita_ppp", "employment_to_pop_ratio"},
    "talent_supply": {"population", "employment_to_pop_ratio"},
    "market_gap": set(),
    "cost_efficiency": {"gdp_per_capita", "gdp_per_capita_ppp"},
    "execution_feasibility": {"employment_to_pop_ratio"},
}

# Direct, reputable, open US city metrics (Census/BLS-style local signals).
US_CITY_DIRECT_METRICS: set[str] = {
    "population",
    "labor_force",
    "median_household_income",
    "unemployment_rate",
    "educational_attainment_bachelors_plus",
    "internet_access_rate",
    "work_from_home_rate",
    "business_establishments",
    "business_employment",
    "target_occupation_employment",
    "relevant_completions",
    "job_creation_rate",
}


def _is_us_geography_id(geography_id: str | None) -> bool:
    if not geography_id:
        return False
    if not geography_id.isdigit():
        return False
    return len(geography_id) in {2, 5, 7, 8, 11, 15}


def _is_us_city_geography_id(geography_id: str | None) -> bool:
    return bool(geography_id and geography_id.isdigit() and len(geography_id) == 7)


def _component_data_factors(
    component: str,
    pct: dict[str, float],
) -> tuple[bool, float, float]:
    """Return (has_data, coverage_ratio, direct_ratio) for a score component."""
    value_keys = COMPONENT_VALUE_METRICS.get(component, COMPONENT_SOURCE_METRICS.get(component, []))
    if not value_keys:
        return False, 0.0, 0.0
    present_keys = [k for k in value_keys if k in pct]
    present = len(present_keys)
    if present == 0:
        return False, 0.0, 0.0
    coverage_ratio = present / len(value_keys)
    proxy_keys = COMPONENT_PROXY_METRICS.get(component, set())
    proxy_present = sum(1 for k in present_keys if k in proxy_keys)
    direct_present = present - proxy_present
    direct_ratio = direct_present / present if present else 0.0
    return True, coverage_ratio, direct_ratio


def _city_direct_data_gate_factor(pct: dict[str, float], geography_id: str | None) -> float:
    """Hard reliability gate for US cities: require direct local open-data signals."""
    if not _is_us_city_geography_id(geography_id):
        return 1.0
    direct_present = sum(1 for k in US_CITY_DIRECT_METRICS if k in pct)
    if direct_present >= 6:
        return 1.0
    if direct_present == 5:
        return 0.88
    if direct_present == 4:
        return 0.75
    if direct_present == 3:
        return 0.62
    if direct_present == 2:
        return 0.5
    return 0.4


def data_quality_summary(pct: dict[str, float], geography_id: str | None) -> dict[str, float]:
    direct_present = float(sum(1 for k in US_CITY_DIRECT_METRICS if k in pct))
    direct_required = float(len(US_CITY_DIRECT_METRICS))
    direct_share = (direct_present / direct_required) if direct_required else 0.0
    score = (0.6 * direct_share + 0.4 * min(1.0, direct_present / 6.0)) * 100.0
    if _is_us_city_geography_id(geography_id):
        score *= _city_direct_data_gate_factor(pct, geography_id)
    return {
        "data_quality_score": max(0.0, min(100.0, score)),
        "direct_metrics_present": direct_present,
        "direct_metrics_required": direct_required,
        "direct_metrics_share": max(0.0, min(1.0, direct_share)),
    }


def apply_score_contrast(score: float, geography_id: str | None = None) -> float:
    """Increase mid-range score separation while preserving ordering.

    Raw opportunity scores tend to cluster near the middle due to percentile
    averaging across many components. This tanh stretch expands differences in
    the mid-band (where most US counties/cities sit) and keeps endpoints
    bounded to [0, 100].
    """
    clamped = max(0.0, min(100.0, score))
    if not _is_us_geography_id(geography_id):
        return clamped
    centered = (clamped - 50.0) / 50.0
    stretched = math.tanh(OPPORTUNITY_SCORE_CONTRAST * centered) / math.tanh(OPPORTUNITY_SCORE_CONTRAST)
    return max(0.0, min(100.0, 50.0 + 50.0 * stretched))


def compute_opportunity_score(
    features: dict[str, float],
    weights: dict[str, float],
    pct: dict[str, float],
    geography_id: str | None = None,
) -> float:
    """Weighted opportunity score for a staffing firm.  talent_supply is
    INVERTED: scarcity of talent = high opportunity for Figwork because
    employers need external help to fill roles.  Re-normalizes weights
    when a component has no underlying data and applies a coverage penalty
    when fewer than half the components are backed by metrics."""
    total_possible = sum(weights.values())
    active_weight = 0.0
    weighted_sum = 0.0
    for component, weight in weights.items():
        has_data, coverage_ratio, direct_ratio = _component_data_factors(component, pct)
        if has_data:
            value = features.get(component, 0.0)
            if component in INVERTED_COMPONENTS:
                value = 1.0 - value
            # Penalize proxy-heavy components and sparsely observed components.
            completeness_factor = (0.78 + 0.22 * coverage_ratio) * (0.65 + 0.35 * direct_ratio)
            weighted_sum += value * weight * completeness_factor
            active_weight += weight
    if active_weight <= 0:
        return 0.0
    score = (weighted_sum / active_weight) * 100.0
    coverage = active_weight / total_possible
    if coverage < 0.5:
        score *= coverage / 0.5
    score *= _city_direct_data_gate_factor(pct, geography_id)
    return apply_score_contrast(score, geography_id=geography_id)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _avg_present(pct: dict[str, float], keys: list[str]) -> float:
    """Average only metrics that exist in the percentile table for this geography.
    Avoids penalizing geographies that simply lack a data source."""
    vals = [pct[k] for k in keys if k in pct]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _safe_div(numerator: float, denominator: float, fallback: float = 0.0) -> float:
    if denominator == 0:
        return fallback
    return numerator / denominator


def _build_percentile_tables_from_metrics(
    all_grouped: dict[str, list[MetricValue]],
) -> dict[str, dict[str, float]]:
    def _geo_jurisdiction(geo_id: str) -> str:
        if geo_id.startswith("AU-") or geo_id == "AU":
            return "AU"
        if geo_id.startswith("IN-") or geo_id == "IN":
            return "IN"
        if geo_id.startswith("EU-"):
            return "EU"
        if geo_id.isdigit():
            return "US"
        return "OTHER"

    def _percentile_bucket(geo_id: str) -> str:
        gtype = _infer_geography_type(geo_id)
        jurisdiction = _geo_jurisdiction(geo_id)
        if gtype in {"county", "place", "state", "nation", "metro", "tract", "block"}:
            return f"{jurisdiction}:{gtype}"
        return f"{jurisdiction}:other"

    metric_values: dict[tuple[str, str], list[tuple[str, float]]] = {}
    for geo_id, metrics in all_grouped.items():
        seen: dict[str, float] = {}
        for m in metrics:
            if m.metric_name not in seen or m.raw_value > seen[m.metric_name]:
                seen[m.metric_name] = m.raw_value
        bucket = _percentile_bucket(geo_id)
        for name, val in seen.items():
            metric_values.setdefault((name, bucket), []).append((geo_id, val))

    result: dict[str, dict[str, float]] = {}
    for (metric_name, _bucket), pairs in metric_values.items():
        n = len(pairs)
        if n <= 1:
            for geo_id, _ in pairs:
                result.setdefault(geo_id, {})[metric_name] = 0.5
            continue
        sorted_vals = sorted(set(v for _, v in pairs))
        rank_map: dict[float, float] = {}
        for i, v in enumerate(sorted_vals):
            rank_map[v] = i / (len(sorted_vals) - 1) if len(sorted_vals) > 1 else 0.5
        for geo_id, val in pairs:
            pct = rank_map.get(val, 0.5)
            if metric_name in INVERSE_METRICS:
                pct = 1.0 - pct
            result.setdefault(geo_id, {})[metric_name] = pct
    return result


_percentile_cache: dict[str, dict[str, dict[str, float]]] | None = None


def _get_or_build_percentiles(
    all_grouped: dict[str, list[MetricValue]],
) -> dict[str, dict[str, float]]:
    global _percentile_cache
    try:
        bundle = load_latest_artifact_bundle("all")
        run_name = bundle.get("run_name", "unknown")
    except Exception:
        run_name = "unknown"
    cache_key = f"{run_name}:{len(all_grouped)}"
    if _percentile_cache is not None and cache_key in _percentile_cache:
        return _percentile_cache[cache_key]
    tables = _build_percentile_tables_from_metrics(all_grouped)
    _percentile_cache = {cache_key: tables}
    return tables


def compute_derived_metrics(raw: dict[str, float]) -> dict[str, float]:
    population = raw.get("population", 0.0)
    target_emp = raw.get("target_occupation_employment", 0.0)
    rpp = raw.get("regional_price_parity", 100.0)
    median_wage = raw.get("occupation_median_wage", 0.0)
    relevant_completions = raw.get("relevant_completions", 0.0)
    industry_emp = raw.get("industry_employment", 0.0)

    talent_density = _safe_div(target_emp, population / 10_000) if population > 0 else 0.0
    cost_adjusted_wage = _safe_div(median_wage, rpp / 100.0) if rpp > 0 else 0.0
    graduate_pipeline_intensity = _safe_div(relevant_completions, target_emp) if target_emp > 0 else 0.0

    national_industry_share = 0.08
    local_industry_share = _safe_div(industry_emp, population) if population > 0 else 0.0
    industry_specialization = _safe_div(local_industry_share, national_industry_share)

    demand_proxy = industry_emp + raw.get("business_establishments", 0.0) * 10
    supply_proxy = target_emp + relevant_completions
    demand_supply_gap = demand_proxy - supply_proxy

    return {
        "talent_density": talent_density,
        "cost_adjusted_wage": cost_adjusted_wage,
        "graduate_pipeline_intensity": graduate_pipeline_intensity,
        "industry_specialization_lq": industry_specialization,
        "demand_supply_gap": demand_supply_gap,
    }


def derive_score_features(
    metrics: list[MetricValue],
    pct: dict[str, float] | None = None,
) -> dict[str, float]:
    raw = {m.metric_name: m.raw_value for m in metrics}
    derived = compute_derived_metrics(raw)

    if pct is None:
        pct = {}

    talent = _avg_present(pct, [
        "target_occupation_employment", "relevant_completions",
        "adjacent_skill_pool_index", "educational_attainment_bachelors_plus",
        "labor_force",
    ])
    demand = _avg_present(pct, [
        "industry_employment", "business_establishments",
        "business_employment", "gdp_current_dollars",
        "job_creation_rate",
        "gdp_per_capita", "gdp_per_capita_ppp",
        "employment_to_pop_ratio", "population", "labor_force",
        "median_household_income", "unemployment_rate",
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
    if "gdp_per_capita" in pct:
        cost_signals.append(pct["gdp_per_capita"])
    if "gdp_per_capita_ppp" in pct:
        cost_signals.append(pct["gdp_per_capita_ppp"])
    cost_efficiency = _avg(cost_signals) if cost_signals else 0.0
    execution = _avg_present(pct, [
        "internet_access_rate", "work_from_home_rate",
        "employment_to_pop_ratio",
    ])

    industry_fit_vals = [v for v in [
        pct.get("industry_employment"),
        pct.get("business_establishments"),
        pct.get("business_employment"),
        pct.get("gdp_current_dollars"),
        pct.get("gdp_per_capita"),
        pct.get("gdp_per_capita_ppp"),
    ] if v is not None]
    if derived["industry_specialization_lq"] > 0:
        industry_fit_vals.append(_clamp01(derived["industry_specialization_lq"] / 3.0))
    industry_fit = _avg(industry_fit_vals) if industry_fit_vals else 0.0

    talent_conversion_vals = [v for v in [
        pct.get("adjacent_skill_pool_index"),
        pct.get("remote_compatibility_index"),
        pct.get("educational_attainment_bachelors_plus"),
    ] if v is not None]
    if derived["graduate_pipeline_intensity"] > 0:
        talent_conversion_vals.append(_clamp01(derived["graduate_pipeline_intensity"]))
    talent_conversion = _avg(talent_conversion_vals) if talent_conversion_vals else 0.0

    demand_capture = _avg_present(pct, [
        "industry_employment", "business_establishments",
        "business_employment", "job_creation_rate", "firm_startup_rate",
        "gdp_per_capita", "gdp_per_capita_ppp", "employment_to_pop_ratio",
    ])

    tightness_signals: list[float] = []
    if "unemployment_rate" in pct:
        tightness_signals.append(pct["unemployment_rate"])
    if "median_household_income" in pct:
        tightness_signals.append(pct["median_household_income"])
    if "job_creation_rate" in pct:
        tightness_signals.append(pct["job_creation_rate"])
    if "firm_startup_rate" in pct:
        tightness_signals.append(pct["firm_startup_rate"])
    if "target_occupation_employment" in pct:
        tightness_signals.append(pct["target_occupation_employment"])
    if "relevant_completions" in pct:
        # More completions generally reduce scarcity pressure.
        tightness_signals.append(1.0 - pct["relevant_completions"])
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
        "talent_density": derived["talent_density"],
        "cost_adjusted_wage": derived["cost_adjusted_wage"],
        "graduate_pipeline_intensity": derived["graduate_pipeline_intensity"],
        "industry_specialization_lq": derived["industry_specialization_lq"],
        "demand_supply_gap": derived["demand_supply_gap"],
    }
