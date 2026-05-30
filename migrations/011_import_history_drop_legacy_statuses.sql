-- Remove legacy 'partial' and 'error' statuses from the constraint.
-- All existing rows with these values should have been migrated before applying this migration.

ALTER TABLE import_history DROP CONSTRAINT IF EXISTS import_history_status_check;
ALTER TABLE import_history ADD CONSTRAINT import_history_status_check
    CHECK (status IN (
        'success',
        'partial_osm_api',
        'partial_unknown',
        'cancelled',
        'error_osm_api',
        'error_unknown'
    ));
