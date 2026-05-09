ALTER TABLE data_imports DROP CONSTRAINT IF EXISTS data_imports_status_check;
ALTER TABLE data_imports ADD CONSTRAINT data_imports_status_check
    CHECK (status IN ('success', 'error', 'skipped'));
