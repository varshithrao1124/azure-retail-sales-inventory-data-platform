-- Control table for the ADF copy watermark.
-- The Databricks job keeps its own Delta commit table (audit.batch_commit)
-- and a per-row CDC sequence. This table only answers "which files were copied".

CREATE TABLE ctl.ingest_watermark (
    source_name VARCHAR(100) NOT NULL,
    last_batch_ts VARCHAR(32) NULL,
    last_sequence BIGINT NULL,
    load_type VARCHAR(32) NULL,
    updated_at DATETIME2 NOT NULL
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED INDEX (source_name)
);
