CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS governance;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    layer TEXT NOT NULL,
    status TEXT NOT NULL,
    window_start DATE,
    window_end DATE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    manifest_path TEXT,
    row_count BIGINT,
    file_count INTEGER,
    size_bytes BIGINT,
    error_details TEXT
);

COMMENT ON SCHEMA bronze IS 'Relational source-shaped tables loaded from bronze Parquet in later stages.';
COMMENT ON SCHEMA governance IS 'Data quality and governance outputs derived from bronze/silver data.';
COMMENT ON SCHEMA audit IS 'Pipeline run metadata and operational audit tables.';
