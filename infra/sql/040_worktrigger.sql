-- WorkTrigger operational schema (signal-driven outbound + scoping engine)

CREATE TABLE IF NOT EXISTS wt_accounts (
  id UUID PRIMARY KEY,
  domain TEXT UNIQUE NOT NULL,
  name TEXT,
  linkedin_url TEXT,
  crunchbase_id TEXT,
  industry TEXT,
  employee_count INT,
  funding_stage TEXT,
  total_funding NUMERIC,
  country TEXT,
  icp_status TEXT NOT NULL DEFAULT 'unknown',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_contacts (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  full_name TEXT,
  title TEXT,
  linkedin_url TEXT,
  email TEXT,
  email_status TEXT NOT NULL DEFAULT 'unknown',
  persona_type TEXT,
  confidence_score NUMERIC NOT NULL DEFAULT 0,
  source TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_contacts_account_id ON wt_contacts(account_id);
CREATE INDEX IF NOT EXISTS idx_wt_contacts_email ON wt_contacts(email);

CREATE TABLE IF NOT EXISTS wt_signals (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  signal_type TEXT NOT NULL,
  source TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  raw_payload_json JSONB NOT NULL,
  normalized_payload_json JSONB NOT NULL,
  confidence_score NUMERIC NOT NULL,
  dedupe_hash TEXT UNIQUE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_signals_account_occurred ON wt_signals(account_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS wt_signal_stacks (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  stack_window_start TIMESTAMPTZ NOT NULL,
  stack_window_end TIMESTAMPTZ NOT NULL,
  funding_score NUMERIC NOT NULL DEFAULT 0,
  buyer_intent_score NUMERIC NOT NULL DEFAULT 0,
  hiring_score NUMERIC NOT NULL DEFAULT 0,
  web_intent_score NUMERIC NOT NULL DEFAULT 0,
  exec_change_score NUMERIC NOT NULL DEFAULT 0,
  total_signal_score NUMERIC NOT NULL,
  explanation_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_work_hypotheses (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  signal_stack_id UUID NOT NULL REFERENCES wt_signal_stacks(id) ON DELETE CASCADE,
  probable_problem TEXT NOT NULL,
  probable_deliverable TEXT NOT NULL,
  talent_archetype TEXT NOT NULL,
  urgency_score NUMERIC NOT NULL,
  taskability_score NUMERIC NOT NULL,
  fit_score NUMERIC NOT NULL,
  confidence_score NUMERIC NOT NULL,
  rationale_json JSONB NOT NULL,
  generated_by_model TEXT NOT NULL,
  model_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_outreach_drafts (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  contact_id UUID NOT NULL REFERENCES wt_contacts(id) ON DELETE CASCADE,
  work_hypothesis_id UUID NOT NULL REFERENCES wt_work_hypotheses(id) ON DELETE CASCADE,
  channel TEXT NOT NULL,
  subject_a TEXT,
  subject_b TEXT,
  email_body TEXT,
  followup_body TEXT,
  linkedin_dm TEXT,
  status TEXT NOT NULL DEFAULT 'draft_ready',
  generation_metadata_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_outreach_drafts_status ON wt_outreach_drafts(status);

CREATE TABLE IF NOT EXISTS wt_review_decisions (
  id UUID PRIMARY KEY,
  draft_id UUID NOT NULL REFERENCES wt_outreach_drafts(id) ON DELETE CASCADE,
  reviewer_user_id UUID NOT NULL,
  action TEXT NOT NULL,
  edited_body TEXT,
  edited_subject TEXT,
  reason_code TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_opportunities (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  contact_id UUID NOT NULL REFERENCES wt_contacts(id) ON DELETE CASCADE,
  source_draft_id UUID REFERENCES wt_outreach_drafts(id),
  crm_id TEXT,
  stage TEXT NOT NULL DEFAULT 'new',
  positive_reply_at TIMESTAMPTZ,
  owner_user_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_scoping_briefs (
  id UUID PRIMARY KEY,
  opportunity_id UUID NOT NULL REFERENCES wt_opportunities(id) ON DELETE CASCADE,
  summary TEXT NOT NULL,
  likely_pain_points_json JSONB NOT NULL,
  proposed_work_packages_json JSONB NOT NULL,
  suggested_talent_archetypes_json JSONB NOT NULL,
  discovery_questions_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_idempotency (
  id UUID PRIMARY KEY,
  endpoint TEXT NOT NULL,
  key TEXT NOT NULL,
  response_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(endpoint, key)
);

CREATE TABLE IF NOT EXISTS wt_account_identity (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  identity_type TEXT NOT NULL,
  identity_value TEXT NOT NULL,
  confidence_score NUMERIC NOT NULL DEFAULT 1.0,
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(identity_type, identity_value)
);
CREATE INDEX IF NOT EXISTS idx_wt_account_identity_account ON wt_account_identity(account_id);

CREATE TABLE IF NOT EXISTS wt_account_geo_attribution (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
  geography_id TEXT NOT NULL,
  weight NUMERIC NOT NULL,
  evidence TEXT NOT NULL,
  confidence_score NUMERIC NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_account_geo_attr_account ON wt_account_geo_attribution(account_id);

CREATE TABLE IF NOT EXISTS wt_jobs (
  id UUID PRIMARY KEY,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,
  run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_jobs_status_run_after ON wt_jobs(status, run_after);

CREATE TABLE IF NOT EXISTS wt_dead_letters (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL,
  job_type TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  error_message TEXT NOT NULL,
  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_suppressions (
  id UUID PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  reason TEXT NOT NULL,
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wt_crm_sync_events (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL,
  contact_id UUID,
  opportunity_id UUID,
  direction TEXT NOT NULL,
  status TEXT NOT NULL,
  details_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wt_crm_sync_events_account ON wt_crm_sync_events(account_id, created_at DESC);
