ALTER TABLE data_imports ADD COLUMN IF NOT EXISTS comment TEXT;

-- Pipeline-step failures outside the osm/atp branches (mv-brand, cleanup…)
-- are recorded under a generic 'pipeline' type.
ALTER TABLE data_imports DROP CONSTRAINT IF EXISTS data_imports_type_check;
ALTER TABLE data_imports ADD CONSTRAINT data_imports_type_check
    CHECK (type IN ('osm', 'atp', 'pipeline'));
