CREATE TABLE IF NOT EXISTS metric_fact (
    metric_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_snapshot_id TEXT REFERENCES source_snapshot(snapshot_id),
    geography_id TEXT NOT NULL REFERENCES geography_dim(geography_id),
    period TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    raw_value NUMERIC NOT NULL,
    normalized_value NUMERIC,
    units TEXT NOT NULL,
    formula TEXT NOT NULL,
    freshness_days INTEGER,
    confidence NUMERIC NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_snapshot_id, geography_id, period, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_metric_geo_period ON metric_fact(geography_id, period);
CREATE INDEX IF NOT EXISTS idx_metric_name ON metric_fact(metric_name);

CREATE TABLE IF NOT EXISTS scenario_dim (
    scenario_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    weight_profile JSONB NOT NULL,
    filter_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS score_fact (
    score_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    geography_id TEXT NOT NULL REFERENCES geography_dim(geography_id),
    scenario_id TEXT NOT NULL REFERENCES scenario_dim(scenario_id),
    period TEXT NOT NULL,
    score_name TEXT NOT NULL,
    score_value NUMERIC NOT NULL,
    component_json JSONB NOT NULL,
    confidence NUMERIC NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    score_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (geography_id, scenario_id, period, score_name, score_version)
);

CREATE TABLE IF NOT EXISTS recommendation_fact (
    recommendation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    geography_id TEXT NOT NULL REFERENCES geography_dim(geography_id),
    scenario_id TEXT NOT NULL REFERENCES scenario_dim(scenario_id),
    period TEXT NOT NULL,
    recommendation_label TEXT NOT NULL,
    rationale JSONB NOT NULL,
    risk_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    supporting_score_refs JSONB NOT NULL,
    confidence NUMERIC NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (geography_id, scenario_id, period)
);
