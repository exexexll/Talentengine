-- WorkTrigger hardening schema: governance, conflicts, observability, execution OS

CREATE TABLE IF NOT EXISTS wt_worker_heartbeats (
  worker_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_result_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS wt_identity_events (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  identity_type TEXT NOT NULL,
  identity_value TEXT NOT NULL,
  source TEXT NOT NULL,
  details_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_identity_events_account_created ON wt_identity_events(account_id, created_at DESC);

CREATE TABLE IF NOT EXISTS wt_crm_conflicts (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL,
  contact_id UUID,
  opportunity_id UUID,
  field_name TEXT NOT NULL,
  app_value TEXT,
  crm_value TEXT,
  policy TEXT NOT NULL,
  resolution_status TEXT NOT NULL DEFAULT 'open',
  resolved_by TEXT,
  resolved_value TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_wt_crm_conflicts_open ON wt_crm_conflicts(resolution_status, created_at DESC);

CREATE TABLE IF NOT EXISTS wt_consent_records (
  id UUID PRIMARY KEY,
  email TEXT NOT NULL,
  channel TEXT NOT NULL,
  legal_basis TEXT NOT NULL,
  status TEXT NOT NULL,
  source TEXT NOT NULL,
  metadata_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(email, channel)
);

CREATE TABLE IF NOT EXISTS wt_deletion_requests (
  id UUID PRIMARY KEY,
  email TEXT,
  account_id UUID,
  reason TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'requested',
  requested_by TEXT NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS wt_retention_policies (
  entity_type TEXT PRIMARY KEY,
  retention_days INT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_llm_runs (
  id UUID PRIMARY KEY,
  task_name TEXT NOT NULL,
  model_name TEXT NOT NULL,
  prompt_hash TEXT NOT NULL,
  token_budget INT NOT NULL,
  prompt_tokens INT,
  completion_tokens INT,
  cached_hit BOOLEAN NOT NULL DEFAULT FALSE,
  evidence_json JSONB NOT NULL,
  response_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_llm_runs_task_created ON wt_llm_runs(task_name, created_at DESC);

CREATE TABLE IF NOT EXISTS wt_llm_cache (
  cache_key TEXT PRIMARY KEY,
  response_json JSONB NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_feedback_events (
  id UUID PRIMARY KEY,
  account_id UUID,
  draft_id UUID,
  event_type TEXT NOT NULL,
  value_num NUMERIC,
  value_text TEXT,
  metadata_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_quotes (
  id UUID PRIMARY KEY,
  opportunity_id UUID NOT NULL,
  quote_json JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_talent_shortlists (
  id UUID PRIMARY KEY,
  opportunity_id UUID NOT NULL,
  geography_id TEXT,
  candidates_json JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_staffing_workflows (
  id UUID PRIMARY KEY,
  opportunity_id UUID NOT NULL,
  state TEXT NOT NULL,
  owner_user_id TEXT,
  checklist_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
