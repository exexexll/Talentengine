# Architecture (v1 foundation)

## Layers

1. Raw ingestion layer: source pulls and immutable snapshots.
2. Canonical modeling layer: normalized geography/occupation/industry/institution dimensions.
3. Metric layer: atomic metrics keyed by geography and period.
4. Score layer: derived composite and recommendation outputs.
5. Serving layer: FastAPI endpoints and cached payloads.
6. Application layer: map UI, compare mode, profiles, and scenarios.

## Data flow

- Pull official sources by cadence.
- Validate schema and geography keys.
- Publish normalized metrics into `metric_fact`.
- Compute composite scores and recommendation labels.
- Serve map/profile/ranking payloads via API.

## Trust requirements

- Every metric and score must carry source, period, formula, freshness, and confidence.
- Degraded mode must serve last known good data with stale warnings.
