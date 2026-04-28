# Figwork Geographic Intelligence Engine

Geographic intelligence platform for market prioritization, talent supply analysis, and local industry fit across U.S. geographies.

## What is included

- `backend/` - FastAPI service for geographies, metrics, scores, scenarios, recommendations, and tile serving.
- `frontend/` - React + Vite + MapLibre GL map-first dashboard with zoom-adaptive layers.
- `data_pipeline/` - Source ingestion (16 connectors), QA, scoring artifact pipeline, and Prefect orchestration flows.
- `infra/sql/` - Postgres/PostGIS schema bootstrap scripts.
- `infra/tiles/` - Tippecanoe + PMTiles vector tile build pipeline and layer manifest.
- `docs/` - Technical architecture and implementation notes.

## Quick start

```bash
# 1. Clone and set up Python backend
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 2. Build data artifacts
python -m data_pipeline.ingestion.build_all_dataset
python -m data_pipeline.scoring.build_score_fact

# 3. Start backend API (port 8000)
uvicorn backend.app.main:app --reload --port 8000

# 4. Set up and start frontend (port 3000)
cd frontend && npm install && npm run dev

# 5. Open in browser
open http://localhost:3000
```

The frontend dev server proxies `/api/*` requests to the backend on port 8000.

## API keys

**Default mode requires NO API keys.** The system runs in `catalog` mode using bundled sample data.

To switch to live government APIs, copy `.env.example` to `.env` and register for free keys:

| Key | Registration Link | Covers |
|-----|------------------|--------|
| `CENSUS_API_KEY` | https://api.census.gov/data/key_signup.html | ACS5, CBP/ZBP, BDS, Population Estimates |
| `BLS_API_KEY` | https://data.bls.gov/registrationEngine/ | OEWS, LAUS, QCEW |
| `BEA_API_KEY` | https://apps.bea.gov/API/signup/index.cfm | RPP, GDP |
| `ONET_USERNAME` / `ONET_PASSWORD` | https://services.onetcenter.org/developer/signup | O*NET skill/occupation data |
| `COLLEGE_SCORECARD_API_KEY` | https://collegescorecard.ed.gov/data/api/ | College Scorecard |
| `FCC_API_TOKEN` | https://broadbandmap.fcc.gov (account -> Manage API Access) | FCC Broadband |

IRS Migration, IPEDS, LEHD/LODES, and RUCA/RUCC data are published as bulk downloads -- no API key needed.

Check runtime key presence: `GET /api/system/status` (booleans only, no secrets exposed).

## Live ingestion setup

```bash
cp .env.example .env
# Edit .env: set FIGWORK_SOURCE_MODE=live and fill in API keys
# Optional: set FIGWORK_LIVE_ALLOW_CATALOG_FALLBACK=1 for mixed mode
```

System readiness: `GET /api/system/status` -> `live_source_status` shows configured vs missing sources.

## Pipeline

```bash
# Build ingestion artifacts (all 16 sources)
python -m data_pipeline.ingestion.build_all_dataset

# Build score/recommendation artifacts (5 composite scores per geography)
python -m data_pipeline.scoring.build_score_fact

# Run QA release acceptance
python -m data_pipeline.qa.release_acceptance

# Run tests
python -m unittest discover -v tests/
```

## Orchestration (Prefect)

Scheduled flows in `data_pipeline/orchestration/flows.py`:

- `nightly_full_refresh` - full ingest + score + QA + cache refresh + cleanup
- `daily_fast_refresh` - recompute scores + refresh API caches
- `monthly_laus_refresh` / `quarterly_qcew_refresh` / `annual_structural_refresh`

```bash
python -m data_pipeline.orchestration.flows  # run locally
```

## Vector tiles

```bash
python -m infra.tiles.build_geojson  # export scored GeoJSON
python -m infra.tiles.build_tiles    # convert to PMTiles
```

Tile manifest: `GET /api/tiles/manifest` | Serve: `GET /api/tiles/{layer_id}.pmtiles`

## Redis caching

Set `REDIS_URL=redis://localhost:6379/0` in `.env`. Falls back to in-memory TTL cache if unavailable.

## Database bootstrap

Run SQL scripts in order against Postgres with PostGIS:

```bash
psql -f infra/sql/001_extensions.sql
psql -f infra/sql/010_geography_dim.sql
psql -f infra/sql/020_core_dims.sql
psql -f infra/sql/030_metric_score_fact.sql
```

## API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/geographies/` | GET | List all geographies |
| `/api/geographies/search?q=` | GET | Search geographies |
| `/api/geographies/{id}/profile` | GET | Geography profile insights |
| `/api/geographies/{id}/profile/tabs` | GET | Full 7-tab profile (PRD Section 14) |
| `/api/metrics/{id}` | GET | Metric bundle for geography |
| `/api/scores/{id}` | GET | Score for geography |
| `/api/scores/_ranked` | GET | Ranked geographies by score |
| `/api/scores/_features_bulk` | GET | Bulk feature values for all geographies |
| `/api/scores/_delta` | GET | Score delta between scenarios |
| `/api/recommendations/{id}` | GET | Recommendation for geography |
| `/api/recommendations/{id}/explain` | GET | Recommendation explainability |
| `/api/recommendations/distribution` | GET | Recommendation distribution analytics |
| `/api/scenarios` | GET | List scenarios |
| `/api/scenarios/simulate` | POST | What-if scenario simulation |
| `/api/compare` | POST | Compare geographies |
| `/api/compare/csv` | GET | CSV export of comparison |
| `/api/trust/coverage` | GET | Data coverage matrix |
| `/api/system/status` | GET | System status and diagnostics |
| `/api/tiles/manifest` | GET | Available tile layers |
| `/api/worktrigger/status` | GET | WorkTrigger module health/config flags |
| `/api/worktrigger/signals/ingest` | POST | Ingest external account signal |
| `/api/worktrigger/accounts/{id}/score` | POST | Recompute ICP/signal/work-fit/priority |
| `/api/worktrigger/accounts/{id}/work-hypothesis` | POST | Generate structured work hypothesis |
| `/api/worktrigger/accounts/{id}/contacts/enrich` | POST | Upsert contacts and pick best buyer |
| `/api/worktrigger/drafts/generate` | POST | Generate outreach draft |
| `/api/worktrigger/drafts/{id}/review` | POST | Human review action |
| `/api/worktrigger/drafts/{id}/send` | POST | Send approved draft via Resend |
| `/api/worktrigger/crm/sync/opportunity` | POST | Sync opportunity to HubSpot |
| `/api/worktrigger/replies/classify` | POST | Classify inbound reply |
| `/api/worktrigger/opportunities/{id}/scoping-brief` | POST | Generate first-call scoping brief |
| `/api/worktrigger/queue` | GET | SDR review queue with account/contact context |
| `/api/worktrigger/accounts/{id}/detail` | GET | Account signal timeline, geo attribution, contacts, hypotheses, drafts |
| `/api/worktrigger/jobs/enqueue` | POST | Enqueue deterministic background job |
| `/api/worktrigger/jobs/claim` | POST | Worker claims next queued job |
| `/api/worktrigger/jobs/{id}/complete` | POST | Mark job completed |
| `/api/worktrigger/jobs/{id}/fail` | POST | Mark job failed and retry/DLQ |
| `/api/worktrigger/jobs/dead-letter` | GET | Inspect dead-letter jobs |
| `/api/worktrigger/jobs/dead-letter/{id}/requeue` | POST | Requeue failed dead-letter job |
| `/api/worktrigger/worker/run-once` | POST | Execute one queued job (operator/cron utility) |
| `/api/worktrigger/worker/heartbeats` | GET | Worker runtime heartbeat/last outcome snapshots |
| `/api/worktrigger/compliance/suppress` | POST | Add recipient suppression |
| `/api/worktrigger/compliance/suppressions` | GET | List suppressions |
| `/api/worktrigger/compliance/consent` | POST/GET | Upsert or fetch channel consent record |
| `/api/worktrigger/compliance/delete` | POST | Request subject deletion workflow |
| `/api/worktrigger/compliance/delete/{id}/complete` | POST | Complete deletion request and purge PII |
| `/api/worktrigger/compliance/retention/policy` | POST | Upsert retention policy by entity |
| `/api/worktrigger/compliance/retention/apply` | POST | Execute retention purge sweep |
| `/api/worktrigger/crm/reconcile` | GET | CRM sync reconciliation report |
| `/api/worktrigger/crm/conflicts/detect` | POST | Detect CRM/app drift conflicts for account |
| `/api/worktrigger/crm/conflicts` | GET | List CRM conflicts by status |
| `/api/worktrigger/crm/conflicts/{id}/resolve` | POST | Resolve conflict with operator override |
| `/api/worktrigger/analytics/summary` | GET | Funnel counts, score averages, CRM drift summary |
| `/api/worktrigger/llm/evals` | GET | LLM schema/cache/eval rollup report |
| `/api/worktrigger/llm/runs` | GET | Recent grounded model run metadata |
| `/api/worktrigger/feedback/events` | POST/GET | Capture and inspect learning-loop feedback |
| `/api/worktrigger/execution/quotes` | POST | Generate draft quote package |
| `/api/worktrigger/execution/shortlists` | POST | Generate talent shortlist artifact |
| `/api/worktrigger/execution/staffing` | POST | Upsert staffing workflow state/checklist |
| `/api/worktrigger/accounts/all` | GET | List all accounts with geo/signal data for map pins |
| `/api/worktrigger/vendors/clay/webhook` | POST | Clay table-row webhook receiver |
| `/api/worktrigger/vendors/clay/pull` | POST | Batch-pull rows from a Clay table |
| `/api/worktrigger/vendors/commonroom/webhook` | POST | Common Room activity webhook receiver |
| `/api/worktrigger/vendors/commonroom/pull` | POST | Batch-pull recent Common Room signals |
| `/api/worktrigger/vendors/crunchbase/pull-funding` | POST | Pull recent Crunchbase funding rounds |
| `/api/worktrigger/vendors/crunchbase/enrich` | POST | Enrich company from Crunchbase and update account |
| `/api/worktrigger/vendors/linkedin/search` | POST | Search LinkedIn Sales Navigator accounts |
| `/api/worktrigger/vendors/linkedin/enrich` | POST | Fetch LinkedIn account insights and ingest as signal |
| `/api/worktrigger/vendors/contacts/enrich-waterfall` | POST | Run Apollo -> Findymail -> Hunter contact waterfall |
| `/api/worktrigger/vendors/status` | GET | Report which vendor API keys are configured |

### WorkTrigger environment variables

**Core (minimum to get started):**
- `OPENAI_API_KEY` — required for hypothesis/draft/classification/scoping generation

**Sending and CRM:**
- `RESEND_API_KEY` + `WORKTRIGGER_FROM_EMAIL` — required for send endpoint
- `HUBSPOT_PRIVATE_APP_TOKEN` — required for CRM sync endpoint

**Vendor integrations (signal enrichment):**
- `CLAY_API_KEY` — Clay signal orchestration
- `COMMONROOM_API_TOKEN` — Common Room website visitor / job-change signals
- `CRUNCHBASE_API_KEY` — Crunchbase funding / company intelligence
- `LINKEDIN_SALES_NAV_TOKEN` — LinkedIn Sales Navigator buyer intent

**Contact enrichment waterfall:**
- `APOLLO_API_KEY` — Apollo ([create keys](https://docs.apollo.io/docs/create-api-key)): use a **master key** or ensure the key can call **People API Search** (`mixed_people/api_search`) and **People Enrichment** (`people/match`). Auth uses the `x-api-key` header per [Apollo OpenAPI](https://docs.apollo.io/reference/people-api-search).
- `APOLLO_REVEAL_PERSONAL_EMAILS` — default `true`. Search alone does not return emails ([docs](https://docs.apollo.io/reference/people-api-search)); when true, the backend calls [People Enrichment](https://docs.apollo.io/reference/people-enrichment) with `reveal_personal_emails=true`, which **consumes Apollo credits**.
- `FINDYMAIL_API_KEY` — Findymail (verification + secondary finder)
- `HUNTER_API_KEY` — Hunter.io (tertiary fallback)

**Tuning:**
- `WORKTRIGGER_DB_PATH` (default: `backend/data/worktrigger.sqlite3`)
- `OPENAI_MODEL` (default: `gpt-5.4`) — primary model for chat co-pilot, hypothesis, research briefings, social-signal analysis
- `WORKTRIGGER_OPENAI_MODEL` (default: `gpt-5.4-mini`) — cheap model for WorkTrigger structured generation
- `SEARCH_LLM_MODEL` (default: `gpt-5.4-mini`) — cheap model for universal-search query normalization
- `WORKTRIGGER_DAILY_SEND_CAP_PER_DOMAIN` (default: `50`)
- `WORKTRIGGER_LLM_TOKEN_BUDGET` (default: `2000`)
- `WORKTRIGGER_LLM_CACHE_TTL_SECONDS` (default: `1800`)

**Observability (optional):**
- `SENTRY_DSN` + `SENTRY_TRACES_SAMPLE_RATE` — backend error/perf telemetry
- `POSTHOG_API_KEY` + `POSTHOG_HOST` — backend product telemetry

### WorkTrigger frontend

- Open `http://localhost:3000/sdr` for the unified SDR workspace (queue + ingest + analytics + geo intelligence).
- Open `http://localhost:3000/` for the talent intelligence map with company pin overlay.
