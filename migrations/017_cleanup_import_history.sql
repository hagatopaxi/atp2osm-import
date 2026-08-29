-- Cleanup after the backfill. To be applied after migration 016, which still
-- reads changeset_ids and the status suffixes.

-- 1. import_departements carries the per-département detail, rebuilt for the
--    existing rows: changeset_ids has no reader left.
ALTER TABLE import_history DROP COLUMN IF EXISTS changeset_ids;

-- 2. The error type (OSM API or unexpected) only qualifies a changeset, and
--    now lives on the département row. The integration status just says what
--    happened as a whole.
--    The constraint goes first: it still forbids the suffix-less values.
ALTER TABLE import_history DROP CONSTRAINT IF EXISTS import_history_status_check;

UPDATE import_history SET status = 'partial' WHERE status LIKE 'partial\_%';
UPDATE import_history SET status = 'error'   WHERE status LIKE 'error\_%';

ALTER TABLE import_history ADD CONSTRAINT import_history_status_check
    CHECK (status IN ('success', 'partial', 'cancelled', 'error'));
