-- Maintenance mode moves from its own table into data_imports: the site is in
-- maintenance as long as at least one row is 'pending'. A datasource posts a
-- pending row when it starts syncing and resolves it (success/skipped/error)
-- when it ends — a crashed or interrupted run leaves the row pending, keeping
-- the site in maintenance until an admin relaunches the pipeline.
--   Force on:  INSERT INTO data_imports (type, status) VALUES ('pipeline', 'pending');
--   Force off: DELETE FROM data_imports WHERE status = 'pending';
ALTER TABLE data_imports DROP CONSTRAINT IF EXISTS data_imports_status_check;
ALTER TABLE data_imports ADD CONSTRAINT data_imports_status_check
    CHECK (status IN ('success', 'error', 'skipped', 'pending'));

DROP TABLE IF EXISTS maintenance;
