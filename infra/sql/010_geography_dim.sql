CREATE TABLE IF NOT EXISTS geography_dim (
    geography_id TEXT PRIMARY KEY,
    geoid TEXT,
    name TEXT NOT NULL,
    geography_type TEXT NOT NULL,
    parent_geography_id TEXT REFERENCES geography_dim(geography_id),
    boundary_version TEXT NOT NULL,
    centroid GEOMETRY(Point, 4326),
    geometry GEOMETRY(MultiPolygon, 4326),
    area_sq_km NUMERIC,
    population BIGINT,
    denominator_source TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_geography_type ON geography_dim(geography_type);
CREATE INDEX IF NOT EXISTS idx_geography_parent ON geography_dim(parent_geography_id);
CREATE INDEX IF NOT EXISTS idx_geography_geom ON geography_dim USING GIST(geometry);

CREATE TABLE IF NOT EXISTS geography_crosswalk (
    crosswalk_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_system TEXT NOT NULL,
    from_code TEXT NOT NULL,
    to_system TEXT NOT NULL,
    to_code TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    weight NUMERIC,
    effective_start DATE,
    effective_end DATE,
    source_snapshot_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (from_system, from_code, to_system, to_code, relation_type)
);
