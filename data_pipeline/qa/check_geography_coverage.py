from collections import defaultdict
from typing import Any


def run(rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    by_metric: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        by_metric[row["metric_name"]].add(row["geography_id"])

    for metric_name, geos in by_metric.items():
        if len(geos) < 1:
            errors.append(f"{metric_name} has no geography coverage")
    return errors
