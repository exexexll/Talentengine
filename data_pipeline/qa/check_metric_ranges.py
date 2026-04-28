from typing import Any


RATIO_METRICS = {
    "unemployment_rate",
    "internet_access_rate",
    "work_from_home_rate",
    "educational_attainment_bachelors_plus",
    "housing_cost_burden_ratio",
    "job_creation_rate",
    "job_destruction_rate",
    "establishment_birth_rate",
    "establishment_death_rate",
    "firm_startup_rate",
    "served_household_ratio",
    "high_speed_ratio",
    "adjacent_skill_pool_index",
    "remote_compatibility_index",
    "completion_rate",
    "rurality_index",
    "metro_linkage_index",
    "inflow_ratio",
    "commute_inflow_ratio",
}

ALLOW_NEGATIVE_METRICS = {
    "net_migrants",
    "net_job_dynamism",
    "population_growth_rate",
    "gdp_growth_rate",
}


def run(rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for row in rows:
        metric = row["metric_name"]
        value = float(row["raw_value"])
        if metric in RATIO_METRICS and not (0.0 <= value <= 1.0):
            errors.append(f"{metric} out of ratio bounds: {value}")
        if value < 0 and metric not in ALLOW_NEGATIVE_METRICS:
            errors.append(f"{metric} has unexpected negative value: {value}")
    return errors
