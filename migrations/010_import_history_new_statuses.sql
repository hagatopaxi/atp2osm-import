ALTER TABLE import_history DROP CONSTRAINT IF EXISTS import_history_status_check;
ALTER TABLE import_history ADD CONSTRAINT import_history_status_check
    CHECK (status IN (
        'success',
        'partial', 'partial_osm_api', 'partial_unknown',
        'cancelled',
        'error', 'error_osm_api', 'error_unknown'
    ));
