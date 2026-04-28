# Data Pipeline

## Phase 2 coverage

Priority source connectors implemented for:

- OEWS
- LAUS
- QCEW
- ACS 5-year
- Population Estimates
- CBP/ZBP
- BEA RPP
- BEA GDP

- Build standardized metric rows from all phase-2 connectors:
  - `python -m data_pipeline.ingestion.build_phase2_dataset`
  - writes NDJSON artifacts to `data_pipeline/artifacts/phase2/<run_id>/`

## Phase 3 expansion

Expansion connector set includes:

- LEHD/LODES
- IRS migration
- IPEDS
- College Scorecard
- O*NET
- RUCA/RUCC

Run phase 3 builder:

- `python -m data_pipeline.ingestion.build_phase3_dataset`
- writes NDJSON artifacts to `data_pipeline/artifacts/phase3/<run_id>/`

Build all phases together:

- `python -m data_pipeline.ingestion.build_all_dataset`
- writes NDJSON artifacts to `data_pipeline/artifacts/all/<run_id>/`

## Phase 4 scoring outputs

Generate `score_fact` and `recommendation_fact` artifacts from latest ingested data:

- `python -m data_pipeline.scoring.build_score_fact`
- writes NDJSON artifacts to `data_pipeline/artifacts/phase4/<run_id>/`

## Quality gates

`build_phase2_dataset` runs three QA checks before writing artifacts:

- `qa/check_metric_ranges.py`
- `qa/check_geography_coverage.py`
- `qa/check_freshness.py`
- `qa/check_duplicate_metric_keys.py`

## Transforms

- `transforms/standardize_metrics.py`: metric row normalization shape.
- `transforms/geography_aggregation.py`: child-to-parent geography aggregation helper.

## Source catalog configuration

Connectors read raw source rows from:

- default: `data_pipeline/source_snapshots/local_source_records.json`
- override: set `FIGWORK_SOURCE_RECORDS_FILE=/absolute/or/relative/path/to/catalog.json`

The catalog must contain one top-level key per source name (for example `BLS_OEWS`, `CENSUS_ACS5`).

## Live source mode

Set `FIGWORK_SOURCE_MODE=live` to pull directly from upstream endpoints/files instead of the local catalog.

Each connector reads from a source-specific URL variable:

- pattern: `FIGWORK_LIVE_<SOURCE_NAME>_URL`
- example: `FIGWORK_LIVE_BLS_OEWS_URL=https://...`
- optional auth token: `FIGWORK_LIVE_<SOURCE_NAME>_TOKEN`

Supported payload formats for live mode:

- JSON list
- JSON object containing `rows`, `data`, or `results`
- CSV
- NDJSON

Optional fallback if some live URLs are not ready yet:

- `FIGWORK_LIVE_ALLOW_CATALOG_FALLBACK=1`

To inspect live readiness for all connectors:

- `GET /api/system/status` and review `live_source_status`

Release acceptance validates source-catalog completeness using:

- `qa/check_source_catalog.py`
