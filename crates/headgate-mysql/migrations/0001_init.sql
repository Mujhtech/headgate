-- headgate MySQL schema (backend wakeup contract) — the translation of headgate-postgres/migrations/0001,
-- dialect differences only, loudly:
--   * no partial indexes -> backend wakeup contract's GENERATED columns that are NULL when the job is not
--     in a unique-eligible state (MySQL unique indexes treat NULLs as distinct);
--   * jsonb -> JSON, bytea -> LONGBLOB/VARBINARY, text[] -> JSON;
--   * the admission gate is a READ COMMITTED transaction (no data-modifying CTEs, no
--     RETURNING), so indexes here serve the same clauses the PG CTEs use.

CREATE TABLE IF NOT EXISTS headgate_job (
  id                  BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ulid                VARCHAR(64)  NOT NULL,
  kind                VARCHAR(255) NOT NULL,
  schema_version      INT          NOT NULL DEFAULT 1,
  payload             LONGBLOB     NOT NULL,
  queue               VARCHAR(255) NOT NULL DEFAULT 'default',
  partition_key       VARCHAR(255) NOT NULL DEFAULT '',
  rate_class          VARCHAR(255) NOT NULL DEFAULT '',
  weight              INT UNSIGNED NOT NULL DEFAULT 1,
  -- Zero means this attempt was admitted fail-open and spent no configured bucket.
  rate_charge         INT UNSIGNED NOT NULL DEFAULT 0,
  fingerprint         VARCHAR(64)  NOT NULL DEFAULT '',
  priority            INT          NOT NULL DEFAULT 0,
  attempt             INT          NOT NULL DEFAULT 0,
  crash_attempt       INT          NOT NULL DEFAULT 0,
  max_attempts        INT          NOT NULL DEFAULT 25,
  enqueued_at_ms      BIGINT       NOT NULL,
  scheduled_at_ms     BIGINT       NOT NULL,
  timeout_ms          BIGINT       NOT NULL DEFAULT 0,
  deadline_ms         BIGINT       NOT NULL DEFAULT 0,
  retention_ms        BIGINT       NOT NULL DEFAULT 0,
  state               ENUM('available','scheduled','retryable','running','completed',
                           'archived','cancelled','undecodable','quarantined') NOT NULL,
  lease_id            VARCHAR(255) NULL,
  lease_expires_at_ms BIGINT       NULL,
  claimed_at_ms       BIGINT       NULL,
  claimed_by          VARCHAR(255) NULL,
  fence               BIGINT       NOT NULL DEFAULT 0,
  finalized_at_ms     BIGINT       NULL,
  errors              JSON         NOT NULL,
  -- telemetry and trace context opaque caller metadata (proto field 20). NULL is the header-less case; the
  -- store never interprets these bytes, it round-trips them. `traceparent` and
  -- `tracestate` are RESERVED keys, meaningful to the RUNTIME and to nothing here.
  headers             JSON         NULL,
  checkpoint          JSON         NULL,
  cp_cursor           LONGBLOB     NULL,
  unique_key          VARBINARY(255) NULL,
  unique_states       INT          NOT NULL DEFAULT 0,
  unique_window_ms    BIGINT       NOT NULL DEFAULT 0,
  unique_expires_at_ms BIGINT      NULL,

  -- job uniqueness LIFECYCLE uniqueness, backend wakeup contract's generated-column form: the column IS the key
  -- while the job is live, NULL otherwise — release by state change, crash-proof.
  unique_active   VARBINARY(255) GENERATED ALWAYS AS (
    CASE WHEN unique_key IS NOT NULL AND unique_expires_at_ms IS NULL
          AND state IN ('scheduled','available','running','retryable')
         THEN unique_key ELSE NULL END) STORED,
  -- job uniqueness THROTTLE uniqueness: held while the window row exists; released lazily by
  -- the conflicting enqueue when unique_expires_at_ms has passed.
  unique_throttle VARBINARY(255) GENERATED ALWAYS AS (
    CASE WHEN unique_key IS NOT NULL AND unique_expires_at_ms IS NOT NULL
         THEN unique_key ELSE NULL END) STORED,

  UNIQUE KEY headgate_job_ulid (ulid),
  UNIQUE KEY headgate_job_unique (unique_active),
  UNIQUE KEY headgate_job_unique_throttle (unique_throttle),
  -- admission policy the admission scan (no partial indexes: state leads the key)
  KEY headgate_job_admit (queue, state, priority DESC, scheduled_at_ms, id),
  -- tenant fairness the per-partition LATERAL draw and the active-partition pruner's emptiness
  -- re-check. headgate_job_admit does not carry partition_key, so without this a probe
  -- of one partition walks the whole queue's available backlog in priority order.
  KEY headgate_job_avail_partition (state, queue, partition_key, priority DESC, scheduled_at_ms, id),
  -- backlog metrics bounded age-of-oldest head lookup; priority must not mask an older low-priority job.
  KEY headgate_job_oldest_available (state, queue, scheduled_at_ms, id),
  KEY headgate_job_oldest_available_partition (state, queue, partition_key, scheduled_at_ms, id),
  -- lease reclaim sweep (lease fencing)
  KEY headgate_job_lease (state, lease_expires_at_ms),
  KEY headgate_job_running_oldest (state, queue, partition_key, claimed_at_ms, id),
  -- crash quarantine fingerprint lookups (quarantine sweeper, admission explain)
  KEY headgate_job_fp (state, fingerprint),
  -- retention and eviction contract retention sweep: functional due-time key, state leading
  KEY headgate_job_retention (state, ((finalized_at_ms + retention_ms)))
) ENGINE=InnoDB;

-- admission policy fleet-wide token buckets — shared, not per-process.
CREATE TABLE IF NOT EXISTS headgate_rate_bucket (
  name             VARCHAR(255) NOT NULL PRIMARY KEY,
  tokens           BIGINT NOT NULL,
  burst            BIGINT NOT NULL,
  limit_per_window BIGINT NOT NULL,
  window_ms        BIGINT NOT NULL,
  refilled_at_ms   BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS headgate_quarantine (
  fingerprint       VARCHAR(64) NOT NULL PRIMARY KEY,
  kind              VARCHAR(255) NOT NULL,
  crash_count       INT NOT NULL,
  quarantined_at_ms BIGINT NOT NULL,
  sample_payload    LONGBLOB NULL,
  reason            VARCHAR(255) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS headgate_partition_deficit (
  queue         VARCHAR(255) NOT NULL,
  partition_key VARCHAR(255) NOT NULL,
  deficit       BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (queue, partition_key)
) ENGINE=InnoDB;

-- tenant fairness/adaptive admission THE ACTIVE-PARTITION SET — maintained incrementally, never derived.
-- eligible.sql used to compute DISTINCT (queue, partition_key) over the available rows on
-- every admission, a full scan of the backlog per call. This is the SQL twin of the Redis
-- gate's `parts:{queue}` set. See the Postgres migration's comment for the full argument.
-- STALENESS IS ONE-DIRECTIONAL: a listed partition with no available job costs one
-- LATERAL probe and is pruned by promote_due; a partition holding an available job that is
-- MISSING here is starvation, so every producer inserts in its OWN transaction with
-- ON DUPLICATE KEY UPDATE (not INSERT IGNORE) — the no-op update takes the row lock that
-- serializes the producer against the pruner. Producers lock these route rows BEFORE
-- inserting jobs; the pruner and inflight reconciler use the same route -> job order.
-- Reversing it creates an InnoDB cycle under concurrent enqueue and pruning.
CREATE TABLE IF NOT EXISTS headgate_active_partition (
  queue         VARCHAR(255) NOT NULL,
  partition_key VARCHAR(255) NOT NULL,
  PRIMARY KEY (queue, partition_key)
) ENGINE=InnoDB;

-- admission policy/adaptive admission THE INFLIGHT COUNT — maintained incrementally, never derived. Second
-- application of the headgate_active_partition trick, to the gate's other full scan.
-- eligible.sql's concurrency clause used to read
--   SELECT queue, partition_key, COUNT(*) FROM headgate_job WHERE state = 'running' GROUP BY ..
-- on every admission — and MySQL has no partial index to narrow it, so it scanned the
-- whole running prefix of headgate_job_avail_partition. See the Postgres migration's
-- comment for the measurements and the full argument.
--
-- GRANULARITY is (queue, partition_key): headgate_concurrency_limit is keyed per QUEUE and
-- the ceiling it names is enforced per PARTITION of that queue (admission policy, "is this job's
-- partition under its ceiling?"). A per-queue counter would be a different policy.
--
-- Unlike headgate_active_partition, staleness is tolerated in NEITHER direction: too low
-- admits past a ceiling, too high stalls a partition permanently. +1 rides the claim in
-- the gate's transaction; −1 rides every running → * transition; `reconcile_inflight`, a
-- bounded sweep in the promote_due duty, recomputes the least-recently-verified rows so
-- drift heals instead of accumulating. Rows are never deleted, so a missing row
-- unambiguously means zero.
CREATE TABLE IF NOT EXISTS headgate_inflight (
  queue            VARCHAR(255) NOT NULL,
  partition_key    VARCHAR(255) NOT NULL,
  n                BIGINT NOT NULL DEFAULT 0,
  reconciled_at_ms BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, partition_key),
  -- the reconciliation sweep picks the least-recently-verified rows; without this it
  -- would sort the whole table every pass instead of reading a bounded prefix.
  KEY headgate_inflight_stale (reconciled_at_ms)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS headgate_concurrency_limit (
  name           VARCHAR(255) NOT NULL PRIMARY KEY,
  queue          VARCHAR(255) NOT NULL,
  max_concurrent BIGINT UNSIGNED NOT NULL,
  on_saturated   ENUM('queue','discard','cancel_running','cancel_incoming')
                 NOT NULL DEFAULT 'queue',
  UNIQUE KEY headgate_concurrency_limit_queue (queue),
  CONSTRAINT headgate_concurrency_limit_positive CHECK (max_concurrent > 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS headgate_queue_state (
  queue          VARCHAR(255) NOT NULL PRIMARY KEY,
  paused         BOOLEAN NOT NULL DEFAULT FALSE,
  weight         INT UNSIGNED NOT NULL DEFAULT 1,
  dispatch_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  CONSTRAINT headgate_queue_weight_positive CHECK (weight > 0)
) ENGINE=InnoDB;

-- backlog metrics incrementally-maintained counters (history + rates), per minute bucket.
CREATE TABLE IF NOT EXISTS headgate_queue_counter (
  queue     VARCHAR(255) NOT NULL,
  bucket_ms BIGINT NOT NULL,
  arrived   BIGINT NOT NULL DEFAULT 0,
  completed BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, bucket_ms)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS headgate_partition_counter (
  queue         VARCHAR(255) NOT NULL,
  partition_key VARCHAR(255) NOT NULL,
  bucket_ms     BIGINT NOT NULL,
  arrived       BIGINT NOT NULL DEFAULT 0,
  completed     BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, partition_key, bucket_ms),
  KEY headgate_partition_counter_recent (queue, bucket_ms, partition_key)
) ENGINE=InnoDB;

-- singleton duties singleton duty leases, compare-and-set on store time.
CREATE TABLE IF NOT EXISTS headgate_duty (
  name          VARCHAR(255) NOT NULL PRIMARY KEY,
  holder        VARCHAR(255) NOT NULL,
  expires_at_ms BIGINT NOT NULL
) ENGINE=InnoDB;

-- surveyed policy behavior durable periodic schedules (leaderless).
CREATE TABLE IF NOT EXISTS headgate_schedule (
  id               VARCHAR(255) NOT NULL PRIMARY KEY,
  kind             VARCHAR(255) NOT NULL,
  payload          LONGBLOB NOT NULL,
  queue            VARCHAR(255) NOT NULL DEFAULT 'default',
  partition_key    VARCHAR(255) NOT NULL DEFAULT '',
  rate_class       VARCHAR(255) NOT NULL DEFAULT '',
  priority         INT NOT NULL DEFAULT 0,
  max_attempts     INT NOT NULL DEFAULT 25,
  retention_ms     BIGINT NOT NULL DEFAULT 0,
  spec             VARCHAR(255) NOT NULL,
  next_run_ms      BIGINT NOT NULL,
  last_enqueued_ms BIGINT NULL,
  on_missed        VARCHAR(16) NOT NULL DEFAULT 'skip',
  backfill_limit   INT NOT NULL DEFAULT 0,
  paused           BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at_ms    BIGINT NOT NULL,
  KEY headgate_schedule_due (paused, next_run_ms)
) ENGINE=InnoDB;

-- worker registry + surveyed policy behavior server->worker control channel.
-- regression revision grew the beat's payload additively — see the Postgres migration's comment
-- for why (the registry knew what a worker was FOR, not what it was DOING). The three
-- new columns are LEVELS reported by the worker and default to 0, so a worker running
-- older code reports nothing rather than reporting wrong.
CREATE TABLE IF NOT EXISTS headgate_worker (
  worker_id       VARCHAR(255) NOT NULL PRIMARY KEY,
  host            VARCHAR(255) NOT NULL DEFAULT '',
  pid             INT NOT NULL DEFAULT 0,
  queues          JSON NOT NULL,
  concurrency     INT NOT NULL DEFAULT 0,
  started_at_ms   BIGINT NOT NULL DEFAULT 0,
  heartbeat_at_ms BIGINT NOT NULL,
  command         VARCHAR(32) NULL,
  inflight        INT NOT NULL DEFAULT 0,
  polls           BIGINT NOT NULL DEFAULT 0,
  empty_polls     BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB;

-- control API contract async bulk operations.
CREATE TABLE IF NOT EXISTS headgate_operation (
  id              VARCHAR(255) NOT NULL PRIMARY KEY,
  action          VARCHAR(32) NOT NULL,
  selector        JSON NOT NULL,
  status          VARCHAR(16) NOT NULL,
  affected        BIGINT NOT NULL DEFAULT 0,
  total_estimated BIGINT NOT NULL DEFAULT 0,
  dry_run         BOOLEAN NOT NULL DEFAULT FALSE,
  error           VARCHAR(1024) NULL,
  created_at_ms   BIGINT NOT NULL
) ENGINE=InnoDB;

-- transactional effects effect keys for once/step_once, claimed inside the caller's transaction.
CREATE TABLE IF NOT EXISTS headgate_effect (
  effect_key    VARCHAR(255) NOT NULL PRIMARY KEY,
  job_ulid      VARCHAR(64) NOT NULL,
  claimed_at_ms BIGINT NOT NULL
) ENGINE=InnoDB;
