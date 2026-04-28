# Scoring and Recommendations

## Composite structure

`Regional Opportunity Score` uses weighted families:

- `business_demand`
- `talent_supply`
- `market_gap`
- `cost_efficiency`
- `execution_feasibility`

Weights are scenario-driven and default to:

- demand: 0.25
- supply: 0.20
- gap: 0.20
- cost: 0.15
- execution: 0.20

## Recommendation policy (rule-based)

Labels are assigned from thresholds and comparative patterns:

- `Enter now`
- `Partnership-led market`
- `Demand-first market`
- `Supply-first market`
- `Pilot first`
- `Monitor`
- `Avoid for now`

## Explainability outputs

Each score persists:

- component metric values
- component weights
- score confidence
- score version

Each recommendation persists:

- label
- rationale bullets
- risk flags
- supporting score references
