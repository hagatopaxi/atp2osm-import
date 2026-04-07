CREATE TABLE IF NOT EXISTS todo_brands (
    id SERIAL PRIMARY KEY,
    brand_wikidata TEXT NOT NULL UNIQUE,
    brand_name TEXT NOT NULL,
    osm_user_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    estimation INTEGER
);
