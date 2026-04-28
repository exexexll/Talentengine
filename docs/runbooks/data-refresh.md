# Data Refresh Runbook

## Purpose

Operate ingestion and artifact refresh for v1 source stack with predictable recovery behavior.

## Daily/regular commands

- Build all ingestion artifacts:
  - `python -m data_pipeline.ingestion.build_all_dataset`
- Build phase-4 score artifacts:
  - `python -m data_pipeline.scoring.build_score_fact`
- Run release acceptance checks:
  - `python -m data_pipeline.qa.release_acceptance`
- Refresh API runtime caches after publishing new artifacts:
  - `POST /api/system/refresh-caches`

## Failure handling

- If a source connector fails:
  - Keep last known good artifact set.
  - Log failing source and retry individually.
  - Mark impacted metrics stale in UI trust panel.
- If QA checks fail:
  - Do not publish new artifacts.
  - Investigate metric range/freshness/coverage errors first.

## Publish checklist

- `artifacts/all/<run_id>` exists with `metric_fact.ndjson` and `source_snapshot.ndjson`.
- `artifacts/phase4/<run_id>` exists with `score_fact.ndjson` and `recommendation_fact.ndjson`.
- `/api/trust/coverage` reflects expected metric families.
