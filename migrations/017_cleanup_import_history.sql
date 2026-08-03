-- Ménage d'après reprise. À appliquer après la migration 016, qui lit encore
-- changeset_ids et les suffixes de statut.

-- 1. import_departements porte le détail par département, reconstruit pour
--    l'existant : changeset_ids n'a plus de lecteur.
ALTER TABLE import_history DROP COLUMN IF EXISTS changeset_ids;

-- 2. Le type d'erreur (API OSM ou inattendue) ne qualifie qu'un changeset, et
--    vit désormais sur la ligne du département. Le statut de l'intégration se
--    contente de dire ce qui s'est passé dans son ensemble.
--    La contrainte tombe d'abord : elle interdit encore les valeurs sans suffixe.
ALTER TABLE import_history DROP CONSTRAINT IF EXISTS import_history_status_check;

UPDATE import_history SET status = 'partial' WHERE status LIKE 'partial\_%';
UPDATE import_history SET status = 'error'   WHERE status LIKE 'error\_%';

ALTER TABLE import_history ADD CONSTRAINT import_history_status_check
    CHECK (status IN ('success', 'partial', 'cancelled', 'error'));
