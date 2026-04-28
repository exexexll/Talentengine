import json
from functools import lru_cache
from pathlib import Path
from typing import Any


ARTIFACT_ROOT = Path("data_pipeline/artifacts")


def _latest_phase_dir(phase_name: str = "all") -> Path | None:
    phase_dir = ARTIFACT_ROOT / phase_name
    if not phase_dir.exists():
        return None
    runs = sorted([p for p in phase_dir.iterdir() if p.is_dir()])
    if not runs:
        return None
    return runs[-1]


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


@lru_cache(maxsize=16)
def _load_bundle_for_run(
    phase_name: str,
    run_name: str,
) -> dict[str, Any]:
    run_dir = ARTIFACT_ROOT / phase_name / run_name
    metric_file = run_dir / "metric_fact.ndjson"
    snapshot_file = run_dir / "source_snapshot.ndjson"
    if not metric_file.exists():
        metric_file = run_dir / "score_fact.ndjson"
    if not snapshot_file.exists():
        snapshot_file = run_dir / "recommendation_fact.ndjson"

    metrics = _read_ndjson(metric_file)
    snapshots = _read_ndjson(snapshot_file)
    by_geography: dict[str, list[dict[str, Any]]] = {}
    for row in metrics:
        by_geography.setdefault(row["geography_id"], []).append(row)
    snapshot_by_id: dict[str, dict[str, Any]] = {}
    for row in snapshots:
        snapshot_id = row.get("snapshot_id")
        if snapshot_id:
            snapshot_by_id[snapshot_id] = row
    return {
        "phase": phase_name,
        "run_name": run_name,
        "metrics": metrics,
        "snapshots": snapshots,
        "metrics_by_geography": by_geography,
        "snapshots_by_id": snapshot_by_id,
    }


def load_latest_artifact_bundle(phase_name: str = "all") -> dict[str, Any]:
    latest = _latest_phase_dir(phase_name=phase_name)
    if latest is None:
        return {
            "phase": phase_name,
            "run_name": None,
            "metrics": [],
            "snapshots": [],
            "metrics_by_geography": {},
            "snapshots_by_id": {},
        }
    return _load_bundle_for_run(phase_name=phase_name, run_name=latest.name)


def refresh_cache() -> None:
    _load_bundle_for_run.cache_clear()


def cleanup_old_runs(keep_last_n: int = 20) -> dict[str, int]:
    if keep_last_n < 1:
        raise ValueError("keep_last_n must be >= 1")
    deleted: dict[str, int] = {}
    if not ARTIFACT_ROOT.exists():
        return deleted
    for phase_dir in ARTIFACT_ROOT.iterdir():
        if not phase_dir.is_dir():
            continue
        runs = sorted([p for p in phase_dir.iterdir() if p.is_dir()])
        stale = runs[:-keep_last_n]
        for run_dir in stale:
            for child in run_dir.iterdir():
                if child.is_file():
                    child.unlink()
            run_dir.rmdir()
        if stale:
            deleted[phase_dir.name] = len(stale)
    if deleted:
        refresh_cache()
    return deleted
