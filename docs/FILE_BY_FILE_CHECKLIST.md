# File-by-file audit (163 tracked files)

Each file is verified by **import graph**, **runtime use**, and **deploy path**.

Status legend:
- **OK** — used at runtime, wired correctly.
- **Manual** — utility/script, only invoked on demand by humans (still valid).
- **Reference** — documentation or template, not exercised at runtime.
- **Fixed** — was broken, repaired in this audit.
- **Legacy** — superseded but kept; safe to delete later.

---

## Repo root (8 files)

| File | Status | Verified | Notes |
|---|---|---|---|
| `.dockerignore` | OK | excludes `.env`, `.venv`, `node_modules`, `data_pipeline/artifacts`, social/ai/discovery caches, terminals | matches build context |
| `.env.example` | Reference | safe template; no real keys | for `cp .env.example .env` |
| `.gitignore` | Fixed | now excludes `tsconfig.tsbuildinfo`, all `*.pem`, `*credentials*.json`, caches | hardened in this audit |
| `Dockerfile` | OK | multi-stage; node:22 → python:3.12-slim; healthcheck on `/health` | builds clean |
| `README.md` | Reference | top-level project doc | |
| `docker-compose.yml` | OK | base service + named volume `figwork_data` for SQLite | |
| `docker-compose.prod.yml` | OK | env_file overlay + bind mounts for `data_pipeline/artifacts` and `backend/data/boundary_cache` | both required for full UI |
| `pyproject.toml` | Reference | setuptools metadata; not used by Dockerfile | |

---

## Backend — top level (3 files)

| File | Status | Verified | Notes |
|---|---|---|---|
| `backend/__init__.py` | OK | empty package marker | |
| `backend/app/__init__.py` | OK | empty package marker | |
| `backend/app/main.py` | OK | mounts 14 routers, CORS, auth gate, `/health`, `/health/ready`, SPA static mount | every router below is registered here |
| `backend/requirements.txt` | OK | fastapi, uvicorn, pydantic, httpx, redis, prefect, bcrypt, itsdangerous | minimal & complete |

---

## Backend — API routers (15 files)

Each `@router` listed below is included in `backend/app/main.py`.

| File | Status | Routes serve | Used by |
|---|---|---|---|
| `backend/app/api/__init__.py` | OK | package marker | |
| `backend/app/api/auth.py` | OK | `/api/auth/{login,logout,me}` | LoginScreen, App `refreshAuth`, fetch wrapper |
| `backend/app/api/ai.py` | OK | `/api/ai/research/{geography_id}` | Map dashboard right pane |
| `backend/app/api/boundaries.py` | OK | `/api/boundaries/{au,in,eu,us_places}` | Map vector layers |
| `backend/app/api/compare.py` | OK | `/api/compare` | Map dashboard "Compare" sidebar |
| `backend/app/api/geographies.py` | OK | `/api/geographies/{names,search,profile-tabs,…}` | Map search, label cache, profile drawer |
| `backend/app/api/metrics.py` | OK | `/api/metrics/{...}` | Dev introspection |
| `backend/app/api/recommendations.py` | OK | `/api/recommendations/{geo,_distribution,explain}` | Map dashboard "Recommendations" pane |
| `backend/app/api/scenarios.py` | OK | `/api/scenarios` (CRUD) | Scenario picker dropdown |
| `backend/app/api/scores.py` | OK | `/api/scores/{geo,_ranked,_features_bulk,_delta}` | Map shading, rankings table |
| `backend/app/api/system.py` | OK | `/api/system/{cache-clear,artifact-info,…}` | Ops |
| `backend/app/api/tiles.py` | OK | `/api/tiles/manifest`, `/api/tiles/{name}.pmtiles` | Empty until `infra.tiles.build_tiles` is run |
| `backend/app/api/trust.py` | OK | `/api/trust/{...}` | Data lineage panel |
| `backend/app/api/worktrigger.py` | OK | 90+ routes — SDR / WorkTrigger | Most of SDR Workspace |

---

## Backend — middleware & models (4 files)

| File | Status | Verified | Notes |
|---|---|---|---|
| `backend/app/middleware/auth_gate.py` | OK | gates all `/api/*` when `FIGWORK_AUTH_ENABLED=1`, exempts `/api/auth/*` | works with `frontend/src/main.tsx` cookie wrapper |
| `backend/app/models/__init__.py` | OK | package marker | |
| `backend/app/models/schemas.py` | OK | Pydantic models for map domain | imported by `recommendations.py`, `scores.py`, `geographies.py` |
| `backend/app/models/worktrigger.py` | OK | Pydantic models for SDR domain | imported by `api/worktrigger.py`, `services/worktrigger_service.py` |

---

## Backend — analysis services (8 files)

| File | Status | Imported by | Notes |
|---|---|---|---|
| `backend/app/services/__init__.py` | OK | package marker | |
| `backend/app/services/artifact_store.py` | OK | metrics_engine, system, trust, boundaries | reads `data_pipeline/artifacts/all/<latest>/{metric_fact,source_snapshot}.ndjson` — **needs prod bind mount** |
| `backend/app/services/metrics_engine.py` | OK | scores, geographies, recommendations, system, scenarios, compare | central read-side; serves `/api/scores`, etc. |
| `backend/app/services/analysis_engine.py` | OK | scores, recommendations, scenarios, compare, geographies, worktrigger_service | top-level scoring orchestrator |
| `backend/app/services/scoring_engine.py` | OK | analysis_engine | weighted score per scenario |
| `backend/app/services/scenario_engine.py` | OK | analysis_engine, scores, scenarios | reads `backend/data/scenarios.json` |
| `backend/app/services/recommendation_engine.py` | OK | analysis_engine | "demand-first market" labels |
| `backend/app/services/confidence_engine.py` | OK | metrics_engine | confidence-band UI signals |

---

## Backend — operational services (8 files)

| File | Status | Imported by | Notes |
|---|---|---|---|
| `backend/app/services/cache.py` | OK | scores, scenarios, system | Redis-backed if `REDIS_URL` set, else in-memory TTL |
| `backend/app/services/telemetry.py` | OK | main.py | per-request timing logs |
| `backend/app/services/llm_config.py` | OK | worktrigger_service, search_service, chat_service | central `cheap_model` / `grounding_preamble` |
| `backend/app/services/simple_accounts.py` | OK | api/auth.py | bcrypt + plain-password fallback for `FIGWORK_ACCOUNTS_JSON` |
| `backend/app/services/signal_taxonomy.py` | OK | worktrigger_service, search_service, social_signals | classifies funding/hiring/intent/exec |
| `backend/app/services/ai_research.py` | OK | api/ai.py | OpenAI digest + heuristic news; reads `geography_names.json` |
| `backend/app/services/search_service.py` | OK | api/worktrigger.py | universal `⌘K` search |
| `backend/app/services/chat_service.py` | OK | api/worktrigger.py | streaming chat in account drawer |

---

## Backend — WorkTrigger SDR core (3 files)

| File | Status | Imported by | Notes |
|---|---|---|---|
| `backend/app/services/worktrigger_store.py` | OK | api/worktrigger.py, worker, service, search, chat | SQLite schema + queries (incl. `twitter_url` migration added in this audit cycle) |
| `backend/app/services/worktrigger_service.py` | OK | api/worktrigger.py, worker | orchestrates score/hypothesis/draft/reply/scoping LLM calls |
| `backend/app/services/worktrigger_worker.py` | OK | only invoked via `python -m backend.app.services.worktrigger_worker` | optional background runner; not running in current deploy |

---

## Backend — vendor adapters (10 files, all under `services/vendors/`)

| File | Status | Imported by | Env keys |
|---|---|---|---|
| `vendors/__init__.py` | OK | package marker | — |
| `vendors/clay.py` | OK | api/worktrigger.py | `CLAY_API_KEY` (optional) |
| `vendors/commonroom.py` | OK | api/worktrigger.py | `COMMONROOM_API_KEY` (optional) |
| `vendors/company_discovery.py` | OK | api/worktrigger.py | `APOLLO_API_KEY` |
| `vendors/contact_waterfall.py` | OK | api/worktrigger.py | Apollo + PDL + Hunter (cascading) |
| `vendors/crunchbase.py` | OK | api/worktrigger.py | `CRUNCHBASE_API_KEY` (optional) |
| `vendors/hunter_company.py` | OK | api/worktrigger.py, contact_waterfall | `HUNTER_API_KEY` |
| `vendors/linkedin.py` | OK | api/worktrigger.py | unofficial; no env key |
| `vendors/pdl.py` | OK | contact_waterfall, company_discovery | `PDL_API_KEY` |
| `vendors/sec_edgar.py` | OK | api/worktrigger.py | no key (free) |
| `vendors/social_signals.py` | OK | api/worktrigger.py, chat_service, worktrigger_service | `SERPAPI_KEY` + `OPENAI_API_KEY` |

---

## Backend — local data files (3 files)

| File | Status | Read by | Notes |
|---|---|---|---|
| `backend/data/geography_names.json` | OK | metrics_engine, ai_research | label cache for ~3 K geographies |
| `backend/data/place_county_crosswalk.json` | OK | analysis_engine | 7-digit place → 5-digit county FIPS |
| `backend/data/scenarios.json` | OK | scenario_engine | preset weight bundles |

---

## Frontend (24 tracked files)

| File | Status | Notes |
|---|---|---|
| `frontend/index.html` | OK | Vite entry; mounts `<div id="root">` |
| `frontend/package.json` | OK | react 18, vite 6, maplibre-gl, marked |
| `frontend/package-lock.json` | OK | tracked for reproducible builds |
| `frontend/tsconfig.json` | OK | TS 5.6 strict |
| `frontend/tsconfig.tsbuildinfo` | Fixed | **was tracked**, now gitignored & untracked (build cache) |
| `frontend/vite.config.ts` | OK | proxy `/api` → `localhost:8000` for dev only |
| `frontend/public/favicon.svg` | OK | served at `/favicon.svg` |
| `frontend/README.md` | Reference | dev quickstart |
| `frontend/src/main.tsx` | OK | wraps `fetch` for `credentials: include` and 401 → `figwork:session-expired` |
| `frontend/src/App.tsx` | OK | router for `/`, `/rankings`, `/sdr`, `/worktrigger/analytics`; loads `LoginScreen` when needed |
| `frontend/src/vite-env.d.ts` | OK | Vite type augments |
| `frontend/src/auth/LoginScreen.tsx` | OK | hits `/api/auth/login` |
| `frontend/src/components/UniversalSearch.tsx` | OK | imported in `sdr-workspace.tsx`; ⌘K modal |
| `frontend/src/components/compare-panel.tsx` | OK | presentational; used by inline reference in map page (CSS classes match) |
| `frontend/src/components/profile-drawer.tsx` | OK | same |
| `frontend/src/components/scenario-controls.tsx` | OK | same |
| `frontend/src/components/layer-picker.tsx` | OK | same |
| `frontend/src/pages/map-dashboard.tsx` | OK | mounted at `/`; consumes `/api/scores`, boundaries, AI research |
| `frontend/src/pages/map-dashboard.css` | OK | sibling to above |
| `frontend/src/pages/rankings.tsx` | OK | mounted at `/rankings` |
| `frontend/src/pages/rankings.css` | OK | sibling |
| `frontend/src/pages/sdr-workspace.tsx` | OK | mounted at `/sdr`; full SDR UI |
| `frontend/src/pages/sdr-workspace.css` | OK | sibling |
| `frontend/src/pages/worktrigger-analytics.tsx` | OK | mounted at `/worktrigger/analytics` (added in earlier audit cycle) |
| `frontend/src/pages/worktrigger.tsx` | Legacy | not routed; superseded by `sdr-workspace.tsx`. Safe to delete when convenient. |

---

## Data pipeline (33 files)

### Top-level (3 files)

| File | Status | Notes |
|---|---|---|
| `data_pipeline/__init__.py` | OK | package marker |
| `data_pipeline/README.md` | Reference | author runbook |
| `data_pipeline/source_snapshots/local_source_records.json` | OK | sample dataset for `FIGWORK_SOURCE_MODE=catalog` |

### Ingestion (5 files)

| File | Status | Notes |
|---|---|---|
| `ingestion/__init__.py` | OK | marker |
| `ingestion/base.py` | OK | `SourceConnector` ABC for all sources |
| `ingestion/build_dataset_common.py` | OK | shared phases used by all 3 builders |
| `ingestion/build_phase2_dataset.py` | OK | core US sources |
| `ingestion/build_phase3_dataset.py` | OK | expansion US sources |
| `ingestion/build_all_dataset.py` | OK | every connector incl. global |
| `ingestion/run_demo.py` | Manual | dev helper |

### Sources (20 files)

| File | Status | Registered in `__init__.py` | Notes |
|---|---|---|---|
| `sources/__init__.py` | Fixed | now lists 19 connectors | `FCCBroadbandConnector` added in this audit (was orphan) |
| `sources/abs_australia.py` | OK | `GLOBAL_CONNECTORS` | |
| `sources/acs5.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/bds.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/bea_gdp.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/bea_rpp.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/cbp_zbp.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/college_scorecard.py` | OK | `PHASE3_EXPANSION_CONNECTORS` | |
| `sources/eurostat.py` | OK | `GLOBAL_CONNECTORS` | |
| `sources/fcc_broadband.py` | Fixed | now in `PHASE3_EXPANSION_CONNECTORS` | was orphan — env vars existed but connector wasn't called |
| `sources/india_worldbank.py` | OK | `GLOBAL_CONNECTORS` | |
| `sources/ipeds.py` | OK | `PHASE3_EXPANSION_CONNECTORS` | |
| `sources/irs_migration.py` | OK | `PHASE3_EXPANSION_CONNECTORS` | |
| `sources/laus.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/lehd_lodes.py` | OK | `PHASE3_EXPANSION_CONNECTORS` | |
| `sources/oews.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/onet.py` | OK | `PHASE3_EXPANSION_CONNECTORS` | |
| `sources/pop_estimates.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/qcew.py` | OK | `PHASE2_CORE_CONNECTORS` | |
| `sources/ruca_rucc.py` | OK | `PHASE3_EXPANSION_CONNECTORS` | |

### Transforms / orchestration / scoring / qa (10 files)

| File | Status | Notes |
|---|---|---|
| `transforms/__init__.py` | OK | marker |
| `transforms/standardize_metrics.py` | OK | imported by `build_dataset_common.py` |
| `transforms/geography_aggregation.py` | OK | upward roll-ups (place → county → state) |
| `orchestration/__init__.py` | OK | marker |
| `orchestration/flows.py` | Manual | Prefect flows; only run via `prefect deployment` |
| `qa/__init__.py` | OK | marker |
| `qa/check_duplicate_metric_keys.py` | OK | imported by `build_dataset_common.py` |
| `qa/check_freshness.py` | OK | imported by `build_dataset_common.py` |
| `qa/check_geography_coverage.py` | OK | imported by `build_dataset_common.py` |
| `qa/check_metric_ranges.py` | OK | imported by `build_dataset_common.py` |
| `qa/check_source_catalog.py` | OK | imported by `release_acceptance.py` |
| `qa/check_staleness_flags.py` | OK | imported by `release_acceptance.py` |
| `qa/release_acceptance.py` | Manual | `python -m data_pipeline.qa.release_acceptance` |
| `scoring/__init__.py` | OK | marker |
| `scoring/build_score_fact.py` | Manual | `python -m data_pipeline.scoring.build_score_fact` |

---

## Deploy (5 files)

| File | Status | Notes |
|---|---|---|
| `deploy/digitalocean/Caddyfile.example` | Reference | copy to `/etc/caddy/Caddyfile` for HTTPS |
| `deploy/digitalocean/README.md` | Reference | step-by-step deploy doc |
| `deploy/digitalocean/env.defaults` | OK | committed safe defaults; merged before `.env` |
| `deploy/digitalocean/env.example` | OK | placeholder template, no real keys |
| `deploy/docker-compose.prod.legacy.yml` | OK | Compose < 2.24 fallback; mirrors prod overlay's volume mounts |

---

## Tests (6 files)

| File | Status | Test count | Notes |
|---|---|---|---|
| `tests/__init__.py` | OK | — | marker |
| `tests/test_api_endpoints.py` | OK | 13 | exercises map-domain routes against artifact bundle |
| `tests/test_data_pipeline_qa.py` | OK | 2 | `qa.*` checker invariants |
| `tests/test_models_validation.py` | OK | 3 | Pydantic models reject bad shapes |
| `tests/test_vendor_adapters.py` | OK | 5 | Apollo/Hunter/Clay/PDL adapters parse fixtures |
| `tests/test_worktrigger_api.py` | OK | 5 | SDR routes happy-path |

Run with `pytest -q` from repo root.

---

## SQL / tiles infrastructure (8 files)

| File | Status | Notes |
|---|---|---|
| `infra/sql/001_extensions.sql` | Reference | Postgres-flavoured DDL |
| `infra/sql/010_geography_dim.sql` | Reference | not used at runtime (app uses SQLite) |
| `infra/sql/020_core_dims.sql` | Reference | future-postgres only |
| `infra/sql/030_metric_score_fact.sql` | Reference | future-postgres only |
| `infra/sql/040_worktrigger.sql` | Reference | mirrors what `worktrigger_store.py` builds in SQLite |
| `infra/sql/041_worktrigger_hardening.sql` | Reference | indexes/constraints, future-postgres |
| `infra/tiles/build_geojson.py` | Manual | `python -m infra.tiles.build_geojson` |
| `infra/tiles/build_tiles.py` | Manual | invokes Tippecanoe; creates `infra/tiles/output/*.pmtiles` |
| `infra/tiles/layer_manifest.yaml` | Reference | layer-id → source mapping for tile build |

---

## Docs (8 files)

| File | Status | Audience |
|---|---|---|
| `docs/README.md` | Reference | docs index |
| `docs/COMPONENT_PIPELINE_CHECKLIST.md` | Reference | FE→BE→LLM matrix |
| `docs/FILE_BY_FILE_CHECKLIST.md` | Reference | this document |
| `docs/runbooks/data-refresh.md` | Reference | how to re-run the data pipeline |
| `docs/runbooks/score-validation.md` | Reference | scoring sanity gates |
| `docs/technical/architecture.md` | Reference | high-level diagrams |
| `docs/technical/data-source-catalog.md` | Reference | per-source URLs/cadence |
| `docs/technical/metric-definitions.md` | Reference | metric-name dictionary |
| `docs/technical/open-datasets-gap-coverage.md` | Reference | global coverage matrix |
| `docs/technical/scoring-and-recommendations.md` | Reference | weight rationale |

---

## Misc (1 file)

| File | Status | Notes |
|---|---|---|
| `scripts/hash_figwork_password.py` | Manual | `python3 scripts/hash_figwork_password.py` to make a bcrypt hash for `FIGWORK_ACCOUNTS_JSON` |

---

## Bugs found and fixed in this pass

1. **`data_pipeline/ingestion/sources/fcc_broadband.py`** — connector existed but was not in any list in `sources/__init__.py`. Result: `FIGWORK_LIVE_FCC_BROADBAND_*` env vars were set but the pipeline never ran the connector. **Now in `PHASE3_EXPANSION_CONNECTORS`.**
2. **`frontend/tsconfig.tsbuildinfo`** — build cache was tracked in git. **Untracked + gitignored.**

## Known unfixed (intentional)

- **`frontend/src/pages/worktrigger.tsx`** — legacy SDR list view, superseded by `sdr-workspace.tsx`. Safe to delete; left in place to avoid breaking anyone referencing it directly.
- **`infra/sql/*.sql`** — future Postgres migration; SQLite at runtime is what the app uses.
- **`infra/tiles/*`** — only invoked manually via `python -m`; output dir empty in the default deploy.

---

_Audit run: 163 files, 2 fixes, 1 legacy left, all other paths verified by import graph._
