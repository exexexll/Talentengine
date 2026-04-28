CREATE TABLE IF NOT EXISTS industry_dim (
    naics_code TEXT PRIMARY KEY,
    parent_naics_code TEXT REFERENCES industry_dim(naics_code),
    level SMALLINT NOT NULL,
    label TEXT NOT NULL,
    sector_group TEXT,
    figwork_work_category TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS occupation_dim (
    soc_code TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    oews_group TEXT,
    onet_code TEXT,
    onet_title TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS institution_dim (
    institution_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    ipeds_unit_id TEXT,
    scorecard_unit_id TEXT,
    geography_id TEXT REFERENCES geography_dim(geography_id),
    latitude NUMERIC,
    longitude NUMERIC,
    carnegie_classification TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS source_snapshot (
    snapshot_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_release_date DATE,
    extracted_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL,
    cadence TEXT NOT NULL,
    qa_status TEXT NOT NULL DEFAULT 'pending',
    qa_notes TEXT
);
