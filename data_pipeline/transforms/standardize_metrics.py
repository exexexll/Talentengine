from datetime import date
from typing import Any


def normalize_ratio(value: float, floor: float = 0.0, ceiling: float = 1.0) -> float:
    if ceiling <= floor:
        return 0.0
    bounded = min(max(value, floor), ceiling)
    return (bounded - floor) / (ceiling - floor)


def standardize_metric_row(
    row: dict[str, Any],
    source_snapshot_id: str,
    formula: str,
    confidence: float = 0.8,
) -> dict[str, Any]:
    return {
        "source_snapshot_id": source_snapshot_id,
        "geography_id": row["geography_id"],
        "period": row["period"],
        "metric_name": row["metric_name"],
        "raw_value": float(row["raw_value"]),
        "normalized_value": row.get("normalized_value"),
        "units": row["units"],
        "formula": formula,
        "freshness_days": row.get("freshness_days", 0),
        "confidence": confidence,
        "updated_at": date.today().isoformat(),
    }
