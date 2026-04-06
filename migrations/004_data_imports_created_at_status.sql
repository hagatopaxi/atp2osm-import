ALTER TABLE data_imports
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'success'
        CHECK (status IN ('success', 'error')),
    ALTER COLUMN date DROP NOT NULL;

ALTER TABLE data_imports ALTER COLUMN status DROP DEFAULT;
