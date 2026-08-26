-- headgate Postgres schema. Reference implementation (backend wakeup contract).
-- Every instant and duration is milliseconds (wire-time contract).

CREATE TYPE headgate_state AS ENUM (
  'scheduled','available','running','retryable',
  'completed','archived','cancelled','quarantined','undecodable'
);

CREATE TABLE headgate_job (
  id               bigserial PRIMARY KEY,
  ulid             text        NOT NULL UNIQUE,
  kind             text        NOT NULL,
  schema_version   int         NOT NULL DEFAULT 1,
  payload          bytea       NOT NULL,
  queue            text        NOT NULL DEFAULT 'default',
  state            headgate_state NOT NULL DEFAULT 'available',

  partition_key    text        NOT NULL DEFAULT '',   -- tenant fairness
  rate_class       text        NOT NULL DEFAULT '',   -- admission policy
  weight           int         NOT NULL DEFAULT 1 CHECK (weight > 0), -- surveyed policy behavior estimated rate cost
  -- What THIS attempt actually charged. Zero means its class was fail-open/unconfigured,
  -- so an ack cannot debit a bucket an operator happened to create after admission.
  rate_charge      int         NOT NULL DEFAULT 0 CHECK (rate_charge >= 0),
  fingerprint      text        NOT NULL,              -- crash quarantine

  priority         int         NOT NULL DEFAULT 0,
  attempt          int         NOT NULL DEFAULT 0,       -- handler returned an error
  crash_attempt    int         NOT NULL DEFAULT 0,       -- crash quarantine worker died. counted apart.
  max_attempts     int         NOT NULL DEFAULT 25,

  enqueued_at_ms   bigint      NOT NULL,
  scheduled_at_ms  bigint      NOT NULL,
  timeout_ms       bigint      NOT NULL DEFAULT 0,
  deadline_ms      bigint      NOT NULL DEFAULT 0,
  retention_ms     bigint      NOT NULL DEFAULT 0,
  finalized_at_ms  bigint,

  -- lease fencing lease is written by the SAME statement that claims. Never a separate step.
  lease_id             text,
  lease_expires_at_ms  bigint,
  claimed_at_ms        bigint,
  fence                bigint  NOT NULL DEFAULT 0,   -- rejects a superseded holder's writes
  claimed_by           text,

  unique_key       bytea,
  unique_states    int         NOT NULL DEFAULT 0,
  -- job uniqueness THROTTLE-mode uniqueness: "at most one per window", released by the clock.
  -- NULL means lifecycle mode: "one live job with this key", released by terminal state.
  unique_expires_at_ms bigint,
  headers          jsonb       NOT NULL DEFAULT '{}',
  errors           jsonb       NOT NULL DEFAULT '[]',

  -- step replay step replay. Durable BEFORE the step's side effects; the fence-verified write
  -- that persists it doubles as the step boundary's lease check.
  checkpoint       jsonb       NOT NULL DEFAULT '{}',
  cp_cursor        bytea,

  CONSTRAINT lease_iff_running CHECK (
    (state = 'running') = (lease_id IS NOT NULL)
  )
);

-- the admission hot path
CREATE INDEX headgate_job_admit ON headgate_job (queue, state, priority DESC, scheduled_at_ms, id)
  WHERE state = 'available';
-- lease reclaim sweep (lease fencing)
CREATE INDEX headgate_job_lease ON headgate_job (lease_expires_at_ms) WHERE state = 'running';
-- concurrency accounting per partition
CREATE INDEX headgate_job_running_partition ON headgate_job (queue, partition_key) WHERE state = 'running';
-- surveyed policy behavior cancel_running selects only the oldest victims it needs; never scan the running set.
CREATE INDEX headgate_job_running_oldest
  ON headgate_job (queue, partition_key, claimed_at_ms, id) WHERE state = 'running';
-- tenant fairness the per-partition LATERAL draw, and the active-partition pruner's emptiness
-- re-check. Without it a LATERAL probe of one partition walks the whole queue's
-- available backlog in priority order (headgate_job_admit does not carry partition_key),
-- which is the O(backlog) cost the maintained partition set exists to remove.
CREATE INDEX headgate_job_avail_partition
  ON headgate_job (queue, partition_key, priority DESC, scheduled_at_ms, id)
  WHERE state = 'available';
-- backlog metrics age-of-oldest is an indexed head lookup, never a scan hidden behind MIN().
-- Priority deliberately does not lead this index: queue priority controls admission,
-- while this metric asks for elapsed waiting time across every priority.
CREATE INDEX headgate_job_oldest_available
  ON headgate_job (queue, scheduled_at_ms, id)
  WHERE state = 'available';
CREATE INDEX headgate_job_oldest_available_partition
  ON headgate_job (queue, partition_key, scheduled_at_ms, id)
  WHERE state = 'available';
CREATE INDEX headgate_job_live_partition_metric
  ON headgate_job (queue, partition_key, state)
  WHERE state = ANY(ARRAY['scheduled','available','running','retryable']::headgate_state[]);
-- crash quarantine fingerprint lookups on waiting jobs: the quarantine sweeper and admission-explain
CREATE INDEX headgate_job_fp_waiting ON headgate_job (fingerprint)
  WHERE state = ANY(ARRAY['available','scheduled','retryable']::headgate_state[]);
-- retention and eviction contract the retention sweep: expression index on the eviction due-time so the sweep
-- reads only lapsed rows, never a scan of everything retained. quarantined exempt.
CREATE INDEX headgate_job_retention ON headgate_job ((finalized_at_ms + retention_ms))
  WHERE state = ANY(ARRAY['completed','archived','cancelled','undecodable']::headgate_state[])
    AND retention_ms > 0;
-- job uniqueness LIFECYCLE uniqueness: one live job per key, enforced by a partial index, not an
-- application lock. Released by reaching a terminal state — crash-proof, cannot leak.
CREATE UNIQUE INDEX headgate_job_unique ON headgate_job (unique_key)
  WHERE unique_key IS NOT NULL AND unique_expires_at_ms IS NULL
    AND state = ANY(ARRAY['scheduled','available','running','retryable']::headgate_state[]);
-- job uniqueness THROTTLE uniqueness: at most one per window, released by the clock regardless of
-- job state. Expired holders are released lazily by the conflicting enqueue.
CREATE UNIQUE INDEX headgate_job_unique_throttle ON headgate_job (unique_key)
  WHERE unique_key IS NOT NULL AND unique_expires_at_ms IS NOT NULL;

-- admission policy fleet-wide token buckets. THE differentiator: shared, not per-process.
CREATE TABLE headgate_rate_bucket (
  name             text     PRIMARY KEY,
  tokens           bigint   NOT NULL,
  burst            bigint   NOT NULL,
  limit_per_window bigint   NOT NULL,
  window_ms        bigint   NOT NULL,
  refilled_at_ms   bigint   NOT NULL
);

-- crash quarantine quarantined fingerprints
CREATE TABLE headgate_quarantine (
  fingerprint    text PRIMARY KEY,
  kind           text   NOT NULL,
  crash_count    int    NOT NULL,
  quarantined_at_ms bigint NOT NULL,
  sample_payload bytea,
  reason         text
);

-- tenant fairness deficit round-robin state
CREATE TABLE headgate_partition_deficit (
  queue         text   NOT NULL,
  partition_key text   NOT NULL,
  deficit       bigint NOT NULL DEFAULT 0,
  updated_at_ms bigint NOT NULL,
  PRIMARY KEY (queue, partition_key)
);

-- tenant fairness/adaptive admission THE ACTIVE-PARTITION SET — maintained incrementally, never derived.
-- The gate used to compute DISTINCT (queue, partition_key) over the available rows on
-- every admission. partition_key is not in the admission index and Postgres has no skip
-- scan, so that was a full scan of the backlog per call and the LIMIT could not help:
-- measured 9k claims/s at a 20k backlog against 55k for a plain SKIP LOCKED fetch.
-- This is the SQL twin of the Redis gate's `parts:{queue}` set, which never had the
-- problem because it has always been maintained.
--
-- STALENESS IS TOLERATED IN ONE DIRECTION ONLY. A listed partition with no available job
-- costs one LATERAL probe (index lookup, zero rows) and is pruned by the promote_due
-- duty. A partition holding an available job that is MISSING here is starvation, so every
-- write path that produces an available row inserts here inside the SAME transaction, and
-- inserts with ON CONFLICT DO UPDATE rather than DO NOTHING: the no-op update takes the
-- row lock that serializes the producer against the pruner. Without that lock the pruner
-- can observe "no available jobs", a producer can commit one, and the pruner can then
-- delete a partition that has work — the forbidden direction.
CREATE TABLE headgate_active_partition (
  queue         text NOT NULL,
  partition_key text NOT NULL,
  PRIMARY KEY (queue, partition_key)
);

-- admission policy/adaptive admission THE INFLIGHT COUNT — maintained incrementally, never derived. Second
-- application of the headgate_active_partition trick, to the gate's other full scan.
-- The gate's concurrency clause used to read
--   SELECT queue, partition_key, count(*) FROM headgate_job WHERE state = 'running' GROUP BY 1,2
-- which aggregates EVERY running row in the fleet on EVERY admission — paid even by a
-- deployment with no ceiling configured anywhere. Measured on the adaptive admission bench: 0.09 ms at
-- 200 running, 2.5 ms at 10k, 4.3 ms at 20k, 11.5 ms at 50k, and it was the entire
-- remaining sensitivity of admission latency to in-flight volume.
--
-- GRANULARITY: (queue, partition_key), matching the aggregate it replaces and matching
-- admission policy's wording — headgate_concurrency_limit is keyed per QUEUE, and the ceiling it
-- names is enforced per PARTITION of that queue. A per-queue counter would silently be a
-- different, stricter policy.
--
-- STALENESS IS NOT TOLERATED IN EITHER DIRECTION, so unlike headgate_active_partition
-- this is not a best-effort set. +1 happens in the SAME statement as the claim; −1 in the
-- same statement as EVERY running → * transition (all ack arms, reclaim_expired, operator
-- cancel, bulk cancel). Both directions are wrong in a way that matters: too low admits
-- past a ceiling, too high stalls a partition permanently. The safety net is
-- `reconcile_inflight`, a bounded sweep inside the promote_due duty that recomputes the
-- least-recently-checked rows from the running set — drift heals, it never accumulates.
-- Rows are never deleted (cardinality is one per partition ever claimed, the same class
-- as headgate_partition_deficit) so a missing row unambiguously means zero.
CREATE TABLE headgate_inflight (
  queue            text   NOT NULL,
  partition_key    text   NOT NULL,
  n                bigint NOT NULL DEFAULT 0,
  reconciled_at_ms bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, partition_key)
);
-- the reconciliation sweep picks the least-recently-verified rows, so it needs this
-- ordering to stay bounded rather than sorting the whole table each pass.
CREATE INDEX headgate_inflight_stale ON headgate_inflight (reconciled_at_ms);

-- admission policy global concurrency ceilings
CREATE TABLE headgate_concurrency_limit (
  name           text   PRIMARY KEY,
  queue          text   NOT NULL UNIQUE,
  max_concurrent bigint NOT NULL CHECK (max_concurrent > 0),
  on_saturated   text   NOT NULL DEFAULT 'queue'
    CHECK (on_saturated IN ('queue','discard','cancel_running','cancel_incoming'))
);

-- backlog metrics backlog derivatives, maintained incrementally so reads never scan (bounded-count contract, bounded live-control contract)
CREATE TABLE headgate_queue_counter (
  queue       text   NOT NULL,
  bucket_ms   bigint NOT NULL,
  arrived     bigint NOT NULL DEFAULT 0,
  completed   bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, bucket_ms)
);

-- Queue-wide counters cannot reconstruct rates after one fairness partition is removed.
CREATE TABLE headgate_partition_counter (
  queue         text   NOT NULL,
  partition_key text   NOT NULL,
  bucket_ms     bigint NOT NULL,
  arrived       bigint NOT NULL DEFAULT 0,
  completed     bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, partition_key, bucket_ms)
);
CREATE INDEX headgate_partition_counter_recent
  ON headgate_partition_counter (queue, bucket_ms, partition_key);

CREATE TABLE headgate_queue_state (
  queue          text PRIMARY KEY,
  paused         boolean NOT NULL DEFAULT false,
  weight         int     NOT NULL DEFAULT 1 CHECK (weight > 0),
  dispatch_count bigint  NOT NULL DEFAULT 0 CHECK (dispatch_count >= 0)
);

-- singleton duties singleton duties: a duty is a row, claiming it is the same compare-and-set as
-- claiming a job. One lock mechanism, no separate leader election. Store time, always —
-- a skewed node must not be able to steal a duty early.
CREATE TABLE headgate_duty (
  name          text   PRIMARY KEY,
  holder        text,
  expires_at_ms bigint NOT NULL DEFAULT 0
);

-- surveyed policy behavior periodic schedules. DURABLE state — never a leader's memory (River can skip a
-- tick entirely across an election). Ticks are enqueued behind a unique key per
-- (schedule, tick), so N racing nodes produce one job with no election at all (GoodJob).
CREATE TABLE headgate_schedule (
  id               text   PRIMARY KEY,
  kind             text   NOT NULL,
  payload          bytea  NOT NULL DEFAULT '\x',
  queue            text   NOT NULL DEFAULT 'default',
  partition_key    text   NOT NULL DEFAULT '',
  rate_class       text   NOT NULL DEFAULT '',
  priority         int    NOT NULL DEFAULT 0,
  max_attempts     int    NOT NULL DEFAULT 25,
  retention_ms     bigint NOT NULL DEFAULT 0,
  -- "@every:<ms>" (epoch-aligned UTC) or a cron expression, optionally prefixed
  -- `CRON_TZ=<IANA zone>` (regression revision) — the zone lives IN this string, which is why a
  -- per-schedule timezone needed no column and no migration, and why changing the zone
  -- is a changed spec that re-anchors the phase. wire schema-adjacent: both languages must
  -- derive IDENTICAL tick times, because ticks feed unique keys.
  spec             text   NOT NULL,
  next_run_ms      bigint NOT NULL,          -- the next UNFIRED tick
  last_enqueued_ms bigint,
  on_missed        text   NOT NULL DEFAULT 'skip',  -- surveyed policy behavior skip | run_once | backfill
  backfill_limit   int    NOT NULL DEFAULT 0,
  paused           boolean NOT NULL DEFAULT false,
  updated_at_ms    bigint NOT NULL DEFAULT 0
);
CREATE INDEX headgate_schedule_due ON headgate_schedule (next_run_ms) WHERE NOT paused;

-- Worker registry, upserted on the lease-renewal heartbeat that already runs.
-- surveyed policy behavior `command` is the server->worker control channel riding that heartbeat
-- (Faktory's BEAT): 'quiet' stops admitting, 'resume' resumes, 'terminate' shuts the
-- worker down — an operator drains a fleet without a deploy.
--
-- regression revision grew the beat's PAYLOAD, additively. `queues` and `concurrency` were
-- already here, so the registry knew what each worker was FOR and not what it was
-- DOING — which left two operational questions unanswerable from the store: "which
-- queues have zero live workers" (surveyed policy behavior's cluster view) and "is this fleet the right
-- size" (backlog metrics's autoscaling signal). All three new columns are LEVELS reported by the
-- worker, never derived server-side, and all three default to 0 so a worker running
-- older code simply reports nothing rather than reporting wrong.
CREATE TABLE headgate_worker (
  worker_id       text   PRIMARY KEY,
  host            text   NOT NULL DEFAULT '',
  pid             int    NOT NULL DEFAULT 0,
  queues          text[] NOT NULL DEFAULT '{}',
  concurrency     int    NOT NULL DEFAULT 0,
  started_at_ms   bigint NOT NULL DEFAULT 0,
  heartbeat_at_ms bigint NOT NULL DEFAULT 0,
  command         text,
  -- jobs running on this worker right now; the numerator of inflight/concurrency
  inflight        int    NOT NULL DEFAULT 0,
  -- backlog metrics the empty-poll window: admissions attempted, and how many came back with
  -- nothing. Two counters rather than a float because the fleet aggregate must be an
  -- exact sum, and because neither language then has to match the other's float
  -- formatting on the wire.
  polls           bigint NOT NULL DEFAULT 0,
  empty_polls     bigint NOT NULL DEFAULT 0
);

-- transactional effects the effect-key table behind Job.Once: at most one execution of a keyed effect,
-- ever, committed ATOMICALLY with the job's completion. This is the tool the surveyed
-- queues tell users to build themselves. Keys are job ids today and (job_id, step)
-- when step replay's step-scoped Once lands.
CREATE TABLE headgate_effect (
  key   text   PRIMARY KEY,
  at_ms bigint NOT NULL DEFAULT 0
);

-- control API contract bulk operations are ASYNCHRONOUS by construction: a synchronous unbounded
-- write violates bounded-count contract, so a request becomes a row the executor duty works in bounded
-- batches, and the caller polls /operations/{id}.
CREATE TABLE headgate_operation (
  id              text    PRIMARY KEY,
  action          text    NOT NULL,
  selector        jsonb   NOT NULL,
  status          text    NOT NULL DEFAULT 'pending',
  affected        bigint  NOT NULL DEFAULT 0,
  total_estimated bigint  NOT NULL DEFAULT 0,
  dry_run         boolean NOT NULL DEFAULT false,
  error           text,
  created_at_ms   bigint  NOT NULL DEFAULT 0
);
