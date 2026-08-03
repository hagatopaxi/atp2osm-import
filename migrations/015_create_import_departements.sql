-- Un statut par changeset : une ligne d'import_history reste une action humaine
-- d'intégration, chaque département intégré a désormais sa ligne fille.
-- Les lignes antérieures ne sont pas reprises (rien à reconstituer depuis
-- changeset_ids, qui ne dit pas à quel département chaque identifiant
-- correspondait) : elles gardent leur statut d'ensemble.

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
