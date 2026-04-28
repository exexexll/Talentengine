from collections import defaultdict
from typing import Any


def aggregate_rows_by_parent(
    rows: list[dict[str, Any]],
    crosswalk: dict[str, str],
) -> list[dict[str, Any]]:
    """
    Aggregate raw metrics from child geography to parent geography.
    crosswalk maps child geography_id -> parent geography_id.
    """
    grouped: dict[tuple[str, str, str, str], float] = defaultdict(float)
    exemplar: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for row in rows:
        child_id = row["geography_id"]
        parent_id = crosswalk.get(child_id)
        if not parent_id:
            continue
        key = (parent_id, row["period"], row["metric_name"], row["units"])
        grouped[key] += float(row["raw_value"])
        exemplar[key] = row

    output: list[dict[str, Any]] = []
    for key, total in grouped.items():
        parent_id, period, metric_name, units = key
        sample = exemplar[key]
        output.append(
            {
                **sample,
                "geography_id": parent_id,
                "period": period,
                "metric_name": metric_name,
                "raw_value": total,
                "units": units,
            }
        )
    return output
