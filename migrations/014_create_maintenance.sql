-- Marker set by the pipeline while it rebuilds the tables, cleared only when
-- a run completes successfully: a failed run keeps the site in maintenance
-- mode until an admin fixes it and relaunches the pipeline.
-- Maintenance is active as soon as the table holds a row.
--   Force on:  INSERT INTO maintenance (started_at) VALUES (NOW());
--   Force off: DELETE FROM maintenance;
CREATE TABLE IF NOT EXISTS maintenance (
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
