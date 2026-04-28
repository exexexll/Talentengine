# Open Datasets For Coverage Gaps

This list focuses on open datasets that directly fill current scoring gaps,
especially missing demand/supply depth for US county/place and global districts.

## Current data integrity notes (important)

- Modeled/proxy sources currently in active pipeline:
  - `ABS_AUSTRALIA` (national WB indicators distributed to SA4 with shares + jitter)
  - `INDIA_WORLDBANK` (national WB indicators distributed to districts with shares + jitter)
- These are not fabricated random data, but they are syntheticized regional estimates
  rather than direct observed district-level measurements.
- FCC broadband connector has been removed from active ingestion/scoring path.
- US city/place scoring now enforces a direct-data gate:
  - city scores are down-weighted when direct local open metrics are sparse.
  - direct metrics currently accepted: population, labor force, unemployment,
    median household income, bachelors attainment, internet access, WFH rate,
    and other local labor/business indicators when present.
  - proxy-only signals (for example PPP/GDP per capita fallbacks) cannot by
    themselves produce a top city rank.

## Priority 1 (directly actionable now)

- Eurostat Business Demography database (enterprise births/deaths, high-growth firms):  
  https://ec.europa.eu/eurostat/web/business-demography/database  
  - Use for `business_establishments`, startup/death proxies, demand pressure.

- Eurostat Job Vacancies (quarterly):  
  https://ec.europa.eu/eurostat/web/labour-market/information-data/job-vacancies  
  - Use for direct demand signal and market tightness.

- OECD Regional economy + labor datasets (TL2/TL3):  
  https://stats.oecd.org/Index.aspx?DataSetCode=REGION_ECONOM  
  - Use for regional GDP, employment growth, labor pressure.

- Australia Labour Market Data for SA4 (data.gov.au):  
  https://data.gov.au/data/dataset/labour-market-data-for-australian-bureau-of-statistics-statistical-area-4-sa4-regions  
  - Use for SA4-level demand/supply and unemployment consistency.

- India OGD BharatNet district coverage:  
  https://www.data.gov.in/resource/district-wise-servise-ready-gram-panchayat-status-under-bharatnet-31-10-2023  
  - Use for district execution feasibility / broadband readiness.

- India OGD UDISE Plus district education:  
  https://www.data.gov.in/catalog/unified-district-information-system-education-plus-udise-plus  
  - Use for education pipeline and talent readiness.

- US Census Nonemployer Statistics (NES) API:  
  https://www.census.gov/data/developers/data-sets/cbp-nonemp-zbp/nonemp-api.html  
  - Use for county demand depth where employer counts are sparse (nonemployer receipts + establishments).

- BLS JOLTS state estimates (open API):  
  https://www.bls.gov/jlt/jlt_statedata.htm  
  https://www.bls.gov/developers/  
  - Use for explicit openings/hires/separations demand pressure.

- HUD USPS ZIP Crosswalk API/files:  
  https://www.huduser.gov/portal/dataset/uspszip-api.html  
  - Use to map ZIP-level demand datasets into place/city geographies.

- LEHD LODES block-level WAC/RAC/OD + crosswalks:  
  https://lehd.ces.census.gov/data/lodes/  
  - Aggregate to place/city for direct local demand/supply intensity.

## Priority 2 (broad enhancement)

- World Bank Enterprise Surveys (firm-level + employment indicators):  
  https://www.enterprisesurveys.org/en/survey-datasets  
  - Use for business dynamics and formal-sector demand proxies.

- OECD Registered Unemployed and Job Vacancies (LAB_REG_VAC):  
  https://stats.oecd.org/Index.aspx?DataSetCode=LAB_REG_VAC  
  - Use for comparable labor-tightness signals.

- ITU DataHub ICT indicators:  
  https://datahub.itu.int/  
  - Use for connectivity quality and digital access proxies.

- UNESCO UIS education data browser:  
  http://databrowser.uis.unesco.org/  
  - Use for tertiary attainment and education trend consistency checks.

## Component mapping guidance

- `business_demand`: job vacancies, enterprise births, business stock, regional GDP growth.
- `talent_supply`: labor force, tertiary attainment, vocational completions, employment ratio.
- `market_gap`: unemployment + vacancies + wage pressure + employment growth.
- `cost_efficiency`: regional income/cost proxies, PPP, housing burden (where available).
- `execution_feasibility`: internet access, broadband rollout, service readiness, remote-work proxy.

## Integration sequence

1. Add US NES connector (county-level nonemployer demand) and wire into demand features.
2. Add HUD ZIP crosswalk helper to aggregate ZIP/block demand to US places.
3. Add BLS JOLTS state demand pressure (openings/hires/separations) and blend into market gap.
4. Add Eurostat vacancies + business demography for EU NUTS.
5. Add Australia SA4 labor market dataset from data.gov.au.
6. Add India district BharatNet + UDISE OGD feeds.
7. Recompute percentile tables and rerun score validation (`docs/runbooks/score-validation.md`).

