-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create metrics table
CREATE TABLE metrics (
    time TIMESTAMPTZ NOT NULL,
    container_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    tags JSONB
);

-- Convert to hypertable (TimescaleDB's time-series optimized table)
SELECT create_hypertable('metrics', 'time');

-- Create indexes for faster queries
CREATE INDEX idx_metrics_time ON metrics (container_name, metric_name, time DESC);
CREATE INDEX idx_metrics_name ON metrics (metric_name);
CREATE INDEX idx_metrics_tags ON metrics USING GIN (tags);

-- Add compression policy (compress data older than 7 days to save space)
ALTER TABLE metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'container_name, metric_name'
);

SELECT add_compression_policy('metrics', INTERVAL '7 days');

-- Retention policy: automatically drop data older than 30 days
SELECT add_retention_policy('metrics', INTERVAL '30 days');
