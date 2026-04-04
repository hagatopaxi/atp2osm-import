CREATE TABLE IF NOT EXISTS data_imports (
    id      SERIAL PRIMARY KEY,
    type    TEXT NOT NULL CHECK (type IN ('osm', 'atp')),
    date    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS data_imports_type_date_idx ON data_imports (type, date DESC);
