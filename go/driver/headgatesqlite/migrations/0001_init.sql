PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS headgate_job (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 2147483647),
    payload BLOB NOT NULL,
    queue TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    rate_class TEXT NOT NULL,
    weight INTEGER NOT NULL CHECK (weight > 0),
    fingerprint TEXT NOT NULL,
    priority INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    crash_attempt INTEGER NOT NULL DEFAULT 0 CHECK (crash_attempt >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    enqueued_at_ms INTEGER NOT NULL,
    scheduled_at_ms INTEGER NOT NULL,
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms >= 0),
    deadline_ms INTEGER NOT NULL CHECK (deadline_ms >= 0),
    retention_ms INTEGER NOT NULL CHECK (retention_ms >= 0),
    state TEXT NOT NULL CHECK (state IN ('pending','scheduled','available','running','retryable','completed','archived','cancelled','quarantined','undecodable')),
    lease_id TEXT,
    fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
    lease_expires_at_ms INTEGER,
    claimed_at_ms INTEGER,
    claimed_by TEXT,
    checkpoint_json TEXT,
    checkpoint_cursor BLOB,
    result_schema_version INTEGER CHECK (result_schema_version IS NULL OR result_schema_version > 0),
    result_bytes BLOB,
    output_schema_version INTEGER CHECK (output_schema_version IS NULL OR output_schema_version > 0),
    output_bytes BLOB,
    output_fence INTEGER,
    output_updated_at_ms INTEGER,
    progress_current INTEGER,
    progress_total INTEGER,
    progress_message TEXT,
    progress_fence INTEGER,
    progress_updated_at_ms INTEGER,
    errors_json TEXT NOT NULL DEFAULT '[]',
    unique_key BLOB,
    unique_states INTEGER NOT NULL DEFAULT 0 CHECK (unique_states >= 0),
    unique_window_ms INTEGER NOT NULL DEFAULT 0 CHECK (unique_window_ms >= 0),
    unique_expires_at_ms INTEGER,
    headers_json TEXT,
    tags_json TEXT NOT NULL DEFAULT '[]',
    periodic_schedule_id TEXT NOT NULL DEFAULT '',
    periodic_tick_ms INTEGER NOT NULL DEFAULT 0 CHECK (periodic_tick_ms >= 0),
    sticky_worker TEXT NOT NULL DEFAULT '',
    rate_charge INTEGER NOT NULL DEFAULT 0 CHECK (rate_charge >= 0),
    finalized_at_ms INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS headgate_job_admit
    ON headgate_job (queue, state, scheduled_at_ms, partition_key, priority DESC, id);
CREATE INDEX IF NOT EXISTS headgate_job_lease
    ON headgate_job (state, lease_expires_at_ms, id);
CREATE INDEX IF NOT EXISTS headgate_job_unique_lookup
    ON headgate_job (unique_key, unique_expires_at_ms, state);

CREATE TABLE IF NOT EXISTS headgate_quarantine (
    fingerprint TEXT PRIMARY KEY,
    crash_count INTEGER NOT NULL CHECK (crash_count >= 0),
    quarantined_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_enqueue_policy (
    queue TEXT PRIMARY KEY,
    max_unfinished_jobs INTEGER CHECK (max_unfinished_jobs IS NULL OR max_unfinished_jobs >= 0)
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_duty (
    name TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_effect (
    effect_key TEXT PRIMARY KEY,
    claimed_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_queue_state (
    queue TEXT PRIMARY KEY,
    weight INTEGER NOT NULL DEFAULT 1 CHECK (weight > 0),
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1)),
    dispatch_count INTEGER NOT NULL DEFAULT 0 CHECK (dispatch_count >= 0),
    memory_bytes INTEGER CHECK (memory_bytes IS NULL OR memory_bytes >= 0)
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_rate_bucket (
    name TEXT PRIMARY KEY,
    tokens INTEGER NOT NULL,
    burst INTEGER NOT NULL CHECK (burst >= 1),
    limit_per_window INTEGER NOT NULL CHECK (limit_per_window >= 0),
    window_ms INTEGER NOT NULL CHECK (window_ms >= 1),
    refilled_at_ms INTEGER NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1))
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_concurrency_limit (
    name TEXT PRIMARY KEY,
    queue TEXT NOT NULL UNIQUE,
    max_concurrent INTEGER NOT NULL CHECK (max_concurrent >= 1),
    on_saturated TEXT NOT NULL CHECK (on_saturated IN ('queue','discard','cancel_running','cancel_incoming'))
) STRICT;

CREATE INDEX IF NOT EXISTS headgate_job_running_partition
    ON headgate_job(queue, partition_key, state, claimed_at_ms, id);

CREATE TABLE IF NOT EXISTS headgate_queue_counter (
    queue TEXT NOT NULL, bucket_ms INTEGER NOT NULL, arrived INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(queue,bucket_ms)
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_schedule (
    id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload BLOB NOT NULL DEFAULT X'', queue TEXT NOT NULL DEFAULT 'default',
    partition_key TEXT NOT NULL DEFAULT '', rate_class TEXT NOT NULL DEFAULT '', priority INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 25, retention_ms INTEGER NOT NULL DEFAULT 0, spec TEXT NOT NULL,
    next_run_ms INTEGER NOT NULL, last_enqueued_ms INTEGER, on_missed TEXT NOT NULL DEFAULT 'skip',
    backfill_limit INTEGER NOT NULL DEFAULT 0, paused INTEGER NOT NULL DEFAULT 0, updated_at_ms INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE INDEX IF NOT EXISTS headgate_schedule_due ON headgate_schedule(paused,next_run_ms);

CREATE TABLE IF NOT EXISTS headgate_schedule_event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_id TEXT NOT NULL, tick_ms INTEGER NOT NULL,
    job_id TEXT NOT NULL, outcome TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', recorded_at_ms INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS headgate_schedule_event_recent ON headgate_schedule_event(schedule_id,event_id DESC);

CREATE TABLE IF NOT EXISTS headgate_worker (
    worker_id TEXT PRIMARY KEY, host TEXT NOT NULL DEFAULT '', pid INTEGER NOT NULL DEFAULT 0,
    queues_json TEXT NOT NULL DEFAULT '[]', concurrency INTEGER NOT NULL DEFAULT 0,
    started_at_ms INTEGER NOT NULL DEFAULT 0, heartbeat_at_ms INTEGER NOT NULL DEFAULT 0,
    command TEXT, inflight INTEGER NOT NULL DEFAULT 0, polls INTEGER NOT NULL DEFAULT 0,
    empty_polls INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'running', duties_active INTEGER NOT NULL DEFAULT 1
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_operation (
    id TEXT PRIMARY KEY, action TEXT NOT NULL, selector_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    affected INTEGER NOT NULL DEFAULT 0, total_estimated INTEGER NOT NULL DEFAULT 0,
    dry_run INTEGER NOT NULL DEFAULT 0, error TEXT, created_at_ms INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE IF NOT EXISTS headgate_durable_event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, topic TEXT NOT NULL,
    idempotency_key TEXT NOT NULL, payload BLOB NOT NULL, source BLOB NOT NULL,
    recorded_at_ms INTEGER NOT NULL, UNIQUE(scope,idempotency_key)
) STRICT;
CREATE INDEX IF NOT EXISTS headgate_durable_event_recent ON headgate_durable_event(scope,event_id DESC);
