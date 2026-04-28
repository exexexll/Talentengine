# Metric Definitions (starter)

## Talent density

- Formula: `target_occupation_employment / population * 10,000`
- Primary source: OEWS (numerator), Population Estimates or ACS (denominator)

## Local core industry specialization

- Formula: `local_industry_share / national_industry_share`
- Primary source: QCEW or CBP

## Cost-adjusted wage

- Formula: `occupation_median_wage / regional_price_parity`
- Primary source: OEWS + BEA RPP

## Demand-supply gap

- Formula: `weighted_demand_proxy - weighted_supply_proxy`
- Nature: derived score input (not native source field)

## Confidence

- Inputs: freshness, geography imputation level, source noise flags
- Scale: 0 to 1
