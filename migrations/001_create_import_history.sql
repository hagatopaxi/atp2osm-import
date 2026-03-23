CREATE TABLE IF NOT EXISTS import_history (
    id              SERIAL PRIMARY KEY,
    brand_wikidata  TEXT NOT NULL,
    osm_user_id     INTEGER NOT NULL,
    import_date     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT NOT NULL CHECK (status IN ('success', 'error')),
    comment         TEXT,
    items_count     INTEGER,
    changeset_ids   INTEGER[]
);

CREATE INDEX IF NOT EXISTS import_history_brand_wikidata_idx ON import_history (brand_wikidata);
CREATE INDEX IF NOT EXISTS import_history_status_idx ON import_history (status);
CREATE INDEX IF NOT EXISTS import_history_import_date_idx ON import_history (import_date DESC);
