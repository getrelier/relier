-- Relier Core Database Initialization
-- This script sets up the necessary extensions and schemas for Relier.

-- Ensure uuid-ossp is available for high-entropy task IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Task audit log (resurrection history, DLQ records, timeouts)
CREATE TABLE IF NOT EXISTS relier_task_events (
    id          BIGSERIAL PRIMARY KEY,
    task_id     VARCHAR(128) NOT NULL,
    task_name   VARCHAR(256) NOT NULL,
    event_type  VARCHAR(64)  NOT NULL, -- completed|failed|resurrected|dlq|timeout
    queue_name  VARCHAR(128),
    worker_id   VARCHAR(128),
    schema_version INT DEFAULT 1,
    payload     JSONB,
    error_msg   TEXT,
    duration_ms INT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON relier_task_events(task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_event_type ON relier_task_events(event_type);
CREATE INDEX IF NOT EXISTS idx_task_events_created_at ON relier_task_events(created_at DESC);
