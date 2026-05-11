ALTER TABLE import_history DROP CONSTRAINT IF EXISTS import_history_status_check;
ALTER TABLE import_history ADD CONSTRAINT import_history_status_check
    CHECK (status IN ('success', 'error', 'partial'));
