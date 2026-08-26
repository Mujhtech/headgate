-- Destructive/offline rollback for migration 1. The migration-history table is retained
-- so the runner can delete version 1 atomically and a later UP starts from version zero.
DROP TABLE IF EXISTS headgate_operation;
DROP TABLE IF EXISTS headgate_effect;
DROP TABLE IF EXISTS headgate_worker;
DROP TABLE IF EXISTS headgate_schedule;
DROP TABLE IF EXISTS headgate_duty;
DROP TABLE IF EXISTS headgate_queue_state;
DROP TABLE IF EXISTS headgate_partition_counter;
DROP TABLE IF EXISTS headgate_queue_counter;
DROP TABLE IF EXISTS headgate_concurrency_limit;
DROP TABLE IF EXISTS headgate_inflight;
DROP TABLE IF EXISTS headgate_active_partition;
DROP TABLE IF EXISTS headgate_partition_deficit;
DROP TABLE IF EXISTS headgate_quarantine;
DROP TABLE IF EXISTS headgate_rate_bucket;
DROP TABLE IF EXISTS headgate_job;
DROP TYPE IF EXISTS headgate_state;
