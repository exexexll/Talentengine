# Score Validation Runbook

## Goal

Validate composite outputs and recommendations are explainable and stable before release.

## Validation steps

1. Generate fresh artifacts:
   - `python -m data_pipeline.ingestion.build_all_dataset`
   - `python -m data_pipeline.scoring.build_score_fact`
2. Inspect top-ranked geographies:
   - `GET /api/scores/_ranked?limit=10`
3. Inspect component transparency:
   - Open `artifacts/phase4/<run_id>/score_fact.ndjson`
4. Verify recommendation rationale:
   - Open `artifacts/phase4/<run_id>/recommendation_fact.ndjson`
5. Confirm trust lineage:
   - `GET /api/trust/geography/{geography_id}`

## Drift checks

- Score delta by geography should be explainable by source refresh or scenario changes.
- Any large rank changes should be reviewed against component deltas.
- Recommendation label changes should map to explicit threshold transitions.
