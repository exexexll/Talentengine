from typing import Any


def run(rows: list[dict[str, Any]], max_freshness_days: int = 540) -> list[str]:
    errors: list[str] = []
    for row in rows:
        freshness = int(row.get("freshness_days", 0))
        if freshness > max_freshness_days:
            errors.append(
                f"{row['metric_name']} for {row['geography_id']} too stale: {freshness} days"
            )
    return errors
