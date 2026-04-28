from pathlib import Path

from backend.app.services.artifact_store import load_latest_artifact_bundle
from data_pipeline.ingestion.build_dataset_common import build_standardized_rows, validate_rows
from data_pipeline.ingestion.sources import (
    PHASE2_CORE_CONNECTORS,
    PHASE3_EXPANSION_CONNECTORS,
)
from data_pipeline.qa import check_source_catalog
from data_pipeline.qa import check_staleness_flags


def run() -> None:
    catalog_errors = check_source_catalog.run()
    if catalog_errors:
        raise ValueError("\n".join(catalog_errors))

    rows, snapshots = build_standardized_rows(PHASE2_CORE_CONNECTORS + PHASE3_EXPANSION_CONNECTORS)
    validate_rows(rows, snapshots)
    stale_errors = check_staleness_flags.run(rows)
    if stale_errors:
        raise ValueError("\n".join(stale_errors))

    required_paths = [
        Path("infra/sql/010_geography_dim.sql"),
        Path("infra/sql/020_core_dims.sql"),
        Path("infra/sql/030_metric_score_fact.sql"),
        Path("backend/app/main.py"),
        Path("frontend/src/pages/map-dashboard.tsx"),
    ]
    missing = [str(path) for path in required_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required release assets: {missing}")

    all_bundle = load_latest_artifact_bundle("all")
    score_bundle = load_latest_artifact_bundle("phase4")
    if not all_bundle["metrics"]:
        raise ValueError("No all-phase metrics artifacts found.")
    if not score_bundle["metrics"]:
        raise ValueError("No phase4 score artifacts found.")

    all_geo_ids = {row["geography_id"] for row in all_bundle["metrics"]}
    score_geo_ids = {row["geography_id"] for row in score_bundle["metrics"]}
    missing_scores = sorted(all_geo_ids.difference(score_geo_ids))
    if missing_scores:
        raise ValueError(f"Missing score rows for geographies: {missing_scores}")

    print("release_acceptance=pass")
    print(f"rows_validated={len(rows)}")
    print(f"snapshots_validated={len(snapshots)}")
    print(f"phase4_geographies={len(score_geo_ids)}")


if __name__ == "__main__":
    run()
