# Figwork: Frontend → Backend → LLM pipeline checklist

Each row was checked against the repo (routes in `backend/app/api/*.py`, services, and LLM call sites). **Viability** means: wiring exists, handler implemented, and (where relevant) env keys documented.

Legend: **LLM** = OpenAI (or optional fallback) in code path. **Vendor** = third-party HTTP without model inference.

---

## Global platform

| # | Component / concern | Backend | LLM / external | Viability | Done |
|---|---------------------|---------|----------------|-----------|------|
| G1 | Session cookie on all `/api/*` calls | `frontend/src/main.tsx` fetch patch | — | Same-origin `credentials: "include"` merged for any `/api` URL | [x] |
| G2 | Auth gate (when `FIGWORK_AUTH_*` on) | `backend/app/middleware/auth_gate.py` | — | `/api/auth/login`, `/logout`, `/me` exempt; other `/api/*` require valid session | [x] |
| G3 | Login / logout / whoami | `backend/app/api/auth.py` | — | Cookie session; `LoginScreen`, `App` | [x] |
| G4 | Health (liveness) | `GET /health` | — | Not under `/api`; no auth | [x] |
| G5 | Health (SQLite readiness) | `GET /health/ready` | SQLite probe | Not under `/api`; no auth | [x] |
| G6 | CORS + trusted hosts | `backend/app/main.py` | — | `FIGWORK_ALLOWED_ORIGINS`, optional `FIGWORK_TRUSTED_HOSTS` | [x] |

---

## Map dashboard (`frontend/src/pages/map-dashboard.tsx`)

| # | UI flow | API | Service / module | LLM / vendor | Viability | Done |
|---|---------|-----|-------------------|--------------|-----------|------|
| M1 | Scenario list | `GET /api/scenarios` | scenarios API + data | — | Static / DB-backed | [x] |
| M2 | Geo search typeahead | `GET /api/geographies/search` | geographies | — | Works | [x] |
| M3 | Geo id → display names | `GET /api/geographies/names` | geographies | — | Cached in IDB | [x] |
| M4 | Ranked scores (map coloring) | `GET /api/scores/_ranked` | `analysis_engine` | — | Large limit supported | [x] |
| M5 | Per-geo score + features | `GET /api/scores/{id}`, `GET /api/scores/_features_bulk` | `analysis_engine` | — | Works | [x] |
| M6 | Compare selected geos | `POST /api/compare` | compare service | — | Heuristic | [x] |
| M7 | Recommendations + explain + distribution | `GET /api/recommendations/...` | recommendations + engine | — | No LLM in API layer | [x] |
| M8 | Delta vs baseline scenario | `GET /api/scores/_delta` | `score_delta` | — | Works | [x] |
| M9 | Map boundaries (US places, AU, IN, EU) | `GET /api/boundaries/*` | boundaries (static/geojson) | — | Cached | [x] |
| M10 | Pinned geo — AI research panel | `GET /api/ai/research/{geography_id}` | `ai_research.research_geography` | **LLM** (optional; news heuristics if no key) | `OPENAI_API_KEY` improves quality | [x] |
| M11 | Company enrich from map | `POST /api/worktrigger/vendors/companies/enrich` | vendor adapters | Vendor (Apollo/PDL/etc. per env) | Env-gated keys | [x] |
| M12 | Contacts count badge | `GET /api/worktrigger/vendors/companies/contacts-count` | worktrigger vendors | Vendor | Route exists (`worktrigger.py`) | [x] |
| M13 | Company discover | `GET /api/worktrigger/vendors/companies/discover` | vendors | Vendor | Env-gated | [x] |
| M14 | Intake batch + poll | `POST/GET .../intake-batch`, `GET .../intake-batch/{id}` | store + vendors | Vendor / store | Works | [x] |
| M15 | Single company intake | `POST .../vendors/companies/intake` | intake pipeline | Vendor | Works | [x] |

---

## Rankings (`frontend/src/pages/rankings.tsx`)

| # | UI flow | API | Service | LLM | Viability | Done |
|---|---------|-----|---------|-----|-----------|------|
| R1 | Scenario picker | `GET /api/scenarios` | scenarios | — | Works | [x] |
| R2 | Full table | `GET /api/scores/_ranked?limit=10000` | ranked_scores | — | Works | [x] |
| R3 | Human-readable geo labels | `GET /api/geographies/names` | geographies | — | Works | [x] |

---

## SDR workspace (`frontend/src/pages/sdr-workspace.tsx`) — primary WorkTrigger UI

Grouped by feature area. All routes under `GET/POST/PATCH/DELETE /api/worktrigger/...` unless noted.

| # | UI flow | API (representative) | Service | LLM / vendor | Viability | Done |
|---|---------|----------------------|---------|--------------|-----------|------|
| S1 | Queue + accounts list | `queue`, `accounts/all` | `worktrigger_store` | — | Works | [x] |
| S2 | Account detail | `accounts/{id}/detail` | store + joins | — | Works | [x] |
| S3 | Work hypothesis | `POST accounts/{id}/work-hypothesis` | `worktrigger_service.generate_work_hypothesis` | **LLM** structured JSON | Cached runs in store | [x] |
| S4 | Draft generate (incl. job-targeted) | `POST /drafts/generate` | `generate_draft` | **LLM** | Uses `_openai_structured_json` | [x] |
| S5 | Draft fetch / review / send | `GET/POST drafts/{id}`, `.../review`, `.../send` | service + store | Review may persist without new LLM call | Works | [x] |
| S6 | CRM sync opportunity | `POST /crm/sync/opportunity` | HubSpot / CRM adapter | Vendor | Env-gated | [x] |
| S7 | Bulk delete / single delete | `POST accounts/bulk-delete`, `DELETE accounts/{id}` | store | — | Works | [x] |
| S8 | Contacts: add, delete, enrich, search-by-title | various `contacts/*`, `vendors/contacts/*` | waterfall / Hunter/PDL | Vendor | Env-gated | [x] |
| S9 | Job outreach toggle | `POST accounts/{id}/job-outreach` | store + draft regen hooks | Optional **LLM** on regen | Works | [x] |
| S10 | Operator chat | `.../chat/sessions`, `.../messages` | `chat_service` | **LLM** streaming | `OPENAI_API_KEY` | [x] |
| S11 | Social signals card | `GET vendors/companies/social-signals` | `social_signals` | **LLM** digest + Serp | `SERPAPI_*`, `OPENAI_API_KEY`; cache keyed by domain + social URLs | [x] |
| S12 | Signal CSV ingest + score | `POST signals/ingest`, `POST accounts/{id}/score` | store + heuristics | — (deterministic scoring) | Works | [x] |
| S13 | Universal-style search (embedded) | uses shared search if present; main search in component below | `search_service` | **LLM** normalize query | See U1 | [x] |
| S14 | Analytics summary + worker heartbeats | `analytics/summary`, `worker/heartbeats` | store / worker | — | Works | [x] |
| S15 | Draft review queue operations | regenerate, purge, collapse duplicates | service | **LLM** on generate paths | Works | [x] |
| S16 | Compliance / DLQ / CRM conflicts (ops tab) | `compliance/*`, `jobs/dead-letter/*`, `crm/conflicts/*` | store + jobs | — | Operator tools | [x] |
| S17 | Cross-link: geo scores from account | `GET /api/scores/{geography_id}` | scores API | — | Works | [x] |
| S18 | Geography names (filters) | `GET /api/geographies/names` | geographies | — | Works | [x] |

---

## Universal search (`frontend/src/components/UniversalSearch.tsx`)

| # | UI flow | API | Service | LLM | Viability | Done |
|---|---------|-----|---------|-----|-----------|------|
| U1 | Typeahead company/geo search | `GET /api/worktrigger/search` | `search_service` | **LLM** optional normalization | `OPENAI_API_KEY` | [x] |
| U2 | Intake from search result | `POST .../vendors/companies/intake` | intake | Vendor | Works | [x] |
| U3 | Add suggested contact post-intake | `POST .../accounts/{id}/contacts/add` | store | Vendor chain | Works | [x] |

---

## Standalone WorkTrigger pages (not mounted in `App.tsx`)

| # | File | Status | Viability | Done |
|---|------|--------|-----------|------|
| O1 | `pages/worktrigger.tsx` | **Legacy** | Not routed in `App.tsx`; `/worktrigger` loads **SdrWorkspace**. Keep file for reference or delete when redundant. | [x] |
| O2 | `pages/worktrigger-analytics.tsx` | **`/worktrigger/analytics`** | Lazy-loaded from `App.tsx`; SDR topbar **Full analytics** opens same page. | [x] |

---

## Map-only presentational components

| # | File | API | Notes | Done |
|---|------|-----|-------|------|
| P1 | `compare-panel.tsx` | — | Props only; parent loads `/api/compare` | [x] |
| P2 | `profile-drawer.tsx` | — | Props only | [x] |
| P3 | `scenario-controls.tsx`, `layer-picker.tsx` | — | Props only | [x] |

---

## Backend WorkTrigger routes without a first-class SPA screen

These exist for workers, webhooks, cron, or future UI — verified **viable** at code level:

| # | Route area | LLM? | Done |
|---|------------|------|------|
| B1 | Jobs: enqueue, claim, complete, fail, `worker/run-once` | Via job payload types | [x] |
| B2 | `POST replies/classify` | **LLM** | [x] |
| B3 | `POST opportunities/{id}/scoping-brief` | **LLM** | [x] |
| B4 | `POST execution/quotes`, `shortlists`, `staffing` | **No LLM** (template stubs in `worktrigger_service`) | [x] |
| B5 | Vendor webhooks / pull (Clay, Common Room, Crunchbase, SEC) | Vendor | [x] |
| B6 | `GET /llm/evals`, `GET /llm/runs` | Telemetry | [x] |
| B7 | Compliance flows (consent, delete, retention) | — | [x] |
| B8 | Feedback events | — | [x] |

---

## LLM touchpoints (canonical list)

| # | Module | Entry | Model config | Done |
|---|--------|-------|--------------|------|
| L1 | `worktrigger_service` | Work hypothesis, draft body, reply classify, scoping brief | `llm_config` | [x] |
| L2 | `chat_service` | Account-scoped operator chat | `llm_config` | [x] |
| L3 | `search_service` | Query normalization for universal search | OpenAI client inline | [x] |
| L4 | `ai_research` | Geography narrative + adjustments | OpenAI + heuristics | [x] |
| L5 | `social_signals` | Serp-derived digest / attribution | OpenAI | [x] |

---

## Summary

- **Mounted SPA surfaces**: Map, Rankings, SDR, and **`/worktrigger/analytics`** (full-page analytics); session cookies applied globally for `/api`.
- **LLM**: Five backend areas (L1–L5); geographic scoring and recommendations remain **non-LLM** heuristics unless AI Research is opened.
- **Data**: `wt_accounts.twitter_url` is migrated and persisted from Apollo intake/enrich, Clay rows, and manual `update_account_fields`; `linkedin_url` is also written on intake/enrich when Apollo returns it (fixes empty social-signals cache keys for intaken accounts).
- **Gaps / notes**: `worktrigger.tsx` remains unmounted legacy. Execution quote/shortlist/staffing endpoints are **stubs** (no model) until product specs demand real generation.

_Last verified against codebase structure (Apr 2026)._
