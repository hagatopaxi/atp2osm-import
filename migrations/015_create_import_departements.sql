-- One status per changeset: an import_history row stays a human integration
-- action, and every integrated département now gets its own child row.
-- Older rows are not backfilled (nothing to rebuild them from: changeset_ids
-- does not say which département each id belonged to); they keep their
-- overall status.

CREATE TABLE IF NOT EXISTS import_departements (
    id                 SERIAL PRIMARY KEY,
    import_id          INTEGER NOT NULL REFERENCES import_history(id) ON DELETE CASCADE,
    departement_number TEXT NOT NULL,
    items_count        INTEGER NOT NULL,
    osm_changeset_id   INTEGER,
    status             TEXT NOT NULL CHECK (status IN ('success', 'error_osm_api', 'error_unknown')),
    comment            TEXT
);

CREATE INDEX IF NOT EXISTS import_departements_import_id_idx ON import_departements (import_id);
CREATE INDEX IF NOT EXISTS import_departements_dpt_idx ON import_departements (departement_number);
