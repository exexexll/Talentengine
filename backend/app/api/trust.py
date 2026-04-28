from collections import defaultdict

from fastapi import APIRouter

from backend.app.services.artifact_store import load_latest_artifact_bundle

router = APIRouter()


@router.get("/coverage")
def coverage_matrix() -> dict[str, dict[str, int]]:
    bundle = load_latest_artifact_bundle("all")
    matrix: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in bundle["metrics"]:
        metric_name = row["metric_name"]
        period = row["period"]
        matrix[metric_name][period] += 1
    return {metric: dict(periods) for metric, periods in matrix.items()}


@router.get("/geography/{geography_id}")
def geography_trust(geography_id: str) -> dict[str, object]:
    bundle = load_latest_artifact_bundle("all")
    rows = [row for row in bundle["metrics"] if row["geography_id"] == geography_id]
    snapshots = {row["snapshot_id"]: row for row in bundle["snapshots"]}
    details = []
    for row in rows:
        snapshot = snapshots.get(row["source_snapshot_id"], {})
        details.append(
            {
                "metric_name": row["metric_name"],
                "period": row["period"],
                "formula": row.get("formula"),
                "confidence": row.get("confidence"),
                "source_snapshot_id": row["source_snapshot_id"],
                "source_name": snapshot.get("source_name"),
                "extracted_at": snapshot.get("extracted_at"),
            }
        )
    return {
        "geography_id": geography_id,
        "metric_count": len(details),
        "metrics": details,
    }
