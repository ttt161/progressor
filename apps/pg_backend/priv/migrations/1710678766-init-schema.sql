-- migrations/1710678766-init-schema.sql
-- :up
-- Up migration
CREATE TYPE process_status AS ENUM ('waiting', 'running', 'error', 'finished');

CREATE TABLE IF NOT EXISTS default_processes(
    "process_id" VARCHAR(80) PRIMARY KEY,
    "status" process_status NOT NULL,
    "detail" TEXT,
    "metadata" JSONB
);

CREATE TABLE IF NOT EXISTS default_events(
    "process_id" VARCHAR(80) NOT NULL,
    "event_id" SMALLINT NOT NULL,
    "timestamp" TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    "payload" JSONB NOT NULL,
    PRIMARY KEY ("process_id", "event_id"),
    FOREIGN KEY ("process_id") REFERENCES default_processes ("process_id")
);

CREATE INDEX "process_idx" on default_events USING HASH ("process_id");

CREATE TABLE IF NOT EXISTS default_timers(
    "process_id" VARCHAR(80) PRIMARY KEY,
    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    "args" JSONB NOT NULL,
    "retry_policy" JSONB,
    "last_retry_interval" INTEGER NOT NULL,
    "attempts_count" SMALLINT NOT NULL,
    FOREIGN KEY ("process_id") REFERENCES default_processes ("process_id")
);

-- :down
-- Down migration
DROP TABLE default_timers;
DROP TABLE default_events;
DROP TABLE default_processes;
DROP TYPE process_status;
