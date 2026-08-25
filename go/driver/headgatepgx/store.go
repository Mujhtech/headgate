// Package headgatepgx is the Go Postgres adapter (Phase 6) — the same schema, the same
// admit.sql, and statement-for-statement the same SQL as crates/headgate-postgres, so
// Go and Rust workers share one database with identical behavior. The cross-language
// section of scripts/test-admission.sh runs both against one store at once.
//
// admit.sql here is a CHECKED-IN COPY of crates/headgate-postgres/queries/admit.sql
// (go:embed cannot leave the module); scripts/verify.sh fails if the two ever differ.
package headgatepgx

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	headgate "github.com/mujhtech/headgate"
)

//go:embed admit.sql
var rawAdmitSQL string

//go:embed admit_direct.sql
var admitDirectSQL string

// The unused $3 (the retired caller-clock slot — time comes from the store) defeats
// type inference at Parse. Rust types it via prepare_typed; here an unreferenced typing
// CTE is prepended AT RUNTIME — the shared file itself is untouched.
var admitSQL = strings.Replace(rawAdmitSQL,
	"WITH params AS (",
	"WITH _hg_p3 AS (SELECT $3::bigint), params AS (", 1)

// Milliseconds since the Unix epoch from the STORE's clock — the only clock every
// worker shares (trap #0 in AGENTS.md).
const nowMS = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint"

const enqueueBackpressureDepthSQL = `
	SELECT p.queue, p.max_unfinished_jobs,
	       COALESCE(ent.n, 0), COALESCE(ext.n, 0)
	FROM headgate_enqueue_policy p
	LEFT JOIN headgate_enqueue_counter ent
	  ON ent.queue = p.queue AND ent.counter_kind = 'entered'
	LEFT JOIN headgate_enqueue_counter ext
	  ON ext.queue = p.queue AND ext.counter_kind = 'exited'
	WHERE p.queue = ANY($1::text[])
	ORDER BY p.queue`

// The per-job identity clause every ack-path statement shares: the job id selects the
// row, lease_id + fence gate the write so a superseded holder is rejected (lease fencing).
const ident = "j.ulid = $1 AND j.lease_id = $2 AND j.fence = $3 AND j.state = 'running'"

// adaptive admission the −1 half of the maintained inflight count (headgate_inflight; the +1 is the
// `infl` arm of admit.sql, and the gate reads the table instead of aggregating every
// running row in the fleet). `src` names a CTE returning (queue, partition_key) for
// EXACTLY the rows that just left `running`.
//
// It is spliced into the transition's own statement deliberately. A separate statement
// can be lost to a crash in between, and the two directions of drift are not symmetric:
// a count left too HIGH never recovers on its own and stalls that partition against its
// ceiling forever. GREATEST(0, …) clamps the other direction instead of letting a
// negative count quietly raise a ceiling. reconcileInflight (PromoteDue) heals both.
//
// Rows are aggregated first — one row-update per partition, not per job.
// Mirrors inflightDecSQL in crates/headgate-postgres/src/lib.rs.
func inflightDec(src string) string {
	return `UPDATE headgate_inflight f SET n = GREATEST(0, f.n - x.c)
	        FROM (SELECT queue, partition_key, count(*)::bigint AS c FROM ` + src + ` GROUP BY 1, 2) x
	        WHERE f.queue = x.queue AND f.partition_key = x.partition_key`
}

type Options struct {
	Overfetch   int32 // admit.sql $8
	CrashLimit  int32 // crash quarantine crashes before quarantine
	RetryBaseMs int64 // default backoff: base * 2^attempt, capped
	RetryCapMs  int64
}

func DefaultOptions() Options {
	return Options{Overfetch: 8, CrashLimit: 3, RetryBaseMs: 1000, RetryCapMs: 3_600_000}
}

// PgxStore implements headgate.Store and headgate.TransactionalStore over a
// caller-supplied pgx pool (failure classification: never closed by this package). For T concurrently
// transaction-holding callbacks shared across workers, budget T+2 pooled connections;
// WithListen adds one physical connection outside that pool. See
// docs/connection-budget.md.
type PgxStore struct {
	pool                *schemaPool
	opts                Options
	listen              *listener // push wakeups push wakeup; nil = poll-only (and Caps say so)
	directProbeCooldown atomic.Uint32
}

type IndexHealth struct {
	Name       string
	Bytes      int64
	Scans      int64
	LiveTuples int64
	DeadTuples int64
}

var maintainableIndexes = []string{
	"headgate_job_admit", "headgate_job_lease", "headgate_job_avail_partition",
	"headgate_job_avail_sticky", "headgate_job_sticky_available",
	"headgate_job_oldest_available", "headgate_job_oldest_available_partition",
	"headgate_job_retention", "headgate_job_unique", "headgate_job_unique_throttle",
	"headgate_job_tag_lookup",
}

// IndexHealth reports a fixed, bounded allowlist; it never enumerates arbitrary schema
// objects and therefore remains independent of queue depth.
func (s *PgxStore) IndexHealth(ctx context.Context) ([]IndexHealth, error) {
	rows, err := s.pool.raw.Query(ctx, `SELECT indexrelname,pg_relation_size(indexrelid),idx_scan,COALESCE(n_live_tup,0),COALESCE(n_dead_tup,0) FROM pg_stat_user_indexes i LEFT JOIN pg_stat_user_tables t USING(schemaname,relname) WHERE schemaname=COALESCE(NULLIF($1,''),current_schema()) AND indexrelname=ANY($2::text[]) ORDER BY indexrelname`, s.Schema(), maintainableIndexes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IndexHealth
	for rows.Next() {
		var h IndexHealth
		if err := rows.Scan(&h.Name, &h.Bytes, &h.Scans, &h.LiveTuples, &h.DeadTuples); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// ReindexConcurrently accepts only known Headgate indexes. The fixed allowlist is the
// injection boundary; CONCURRENTLY keeps normal reads/writes available.
func (s *PgxStore) ReindexConcurrently(ctx context.Context, name string) error {
	if !isMaintainableIndex(name) {
		return headgate.Invalidf("index %s is not a maintainable Headgate index", name)
	}
	_, err := s.pool.raw.Exec(ctx, "REINDEX INDEX CONCURRENTLY "+s.pool.namespace.qualified(name))
	return err
}

func isMaintainableIndex(name string) bool { return slices.Contains(maintainableIndexes, name) }

const directProbeCooldown = uint32(128)

func (s *PgxStore) directProbeDue() bool {
	for {
		remaining := s.directProbeCooldown.Load()
		if remaining == 0 {
			return true
		}
		if s.directProbeCooldown.CompareAndSwap(remaining, remaining-1) {
			return false
		}
	}
}

var (
	_ headgate.Store              = (*PgxStore)(nil)
	_ headgate.TransactionalStore = (*PgxStore)(nil) // transactional API's compile-time capability check
	_ headgate.ResultStore        = (*PgxStore)(nil)
	_ headgate.OutputStore        = (*PgxStore)(nil)
	_ headgate.ProgressStore      = (*PgxStore)(nil)
)

func New(pool *pgxpool.Pool) *PgxStore { return WithOptions(pool, DefaultOptions()) }

func WithOptions(pool *pgxpool.Pool, opts Options) *PgxStore {
	return &PgxStore{pool: &schemaPool{raw: pool}, opts: opts}
}

// NewInSchema explicitly qualifies every durable headgate object. The underlying pool
// may be shared with another schema because no connection-level search_path is changed.
func NewInSchema(pool *pgxpool.Pool, schema string) (*PgxStore, error) {
	return WithOptionsInSchema(pool, DefaultOptions(), schema)
}

func WithOptionsInSchema(pool *pgxpool.Pool, opts Options, schema string) (*PgxStore, error) {
	namespace, err := newPostgresNamespace(schema)
	if err != nil {
		return nil, err
	}
	return &PgxStore{pool: &schemaPool{raw: pool, namespace: namespace}, opts: opts}, nil
}

// Schema returns the explicitly configured schema, or "" for the legacy/default
// connection namespace.
func (s *PgxStore) Schema() string {
	return s.pool.namespace.name()
}

func Connect(ctx context.Context, conninfo string) (*PgxStore, error) {
	pool, err := pgxpool.New(ctx, conninfo)
	if err != nil {
		return nil, err
	}
	return New(pool).WithListen(conninfo), nil
}

func ConnectInSchema(ctx context.Context, conninfo, schema string) (*PgxStore, error) {
	pool, err := pgxpool.New(ctx, conninfo)
	if err != nil {
		return nil, err
	}
	store, err := NewInSchema(pool, schema)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return store.WithListen(conninfo), nil
}

func (s *PgxStore) Caps() headgate.Caps {
	c := headgate.CapTransactional | headgate.CapInspect
	if s.listen != nil {
		c |= headgate.CapNotifying
	}
	return c
}

// ---------- admit ----------

func (s *PgxStore) Admit(ctx context.Context, req headgate.AdmitRequest) ([]headgate.AdmissionUnit, error) {
	slices.Sort(req.Queues)
	req.Queues = slices.Compact(req.Queues)
	leaseMs := req.Lease.Milliseconds()
	if leaseMs <= 0 {
		return nil, errors.New("headgate: lease must be >= 1ms (boundary validation)")
	}
	// adaptive admission compact no-policy/single-partition claim. A true sentinel means the statement
	// made no write and the complete gate below must decide; an empty result is a handled
	// empty poll. Policy detection and claim share the statement snapshot.
	if s.directProbeDue() {
		units, fallback, err := s.admitPassSQL(ctx, admitDirectSQL, req, leaseMs, 0)
		if err != nil {
			return nil, err
		}
		if !fallback {
			return units, nil
		}
		// Conservative performance hint only: skipped probes use the complete gate. Retry
		// periodically so policy removal or partition drain can restore the direct path.
		s.directProbeCooldown.Store(directProbeCooldown)
	}
	// adaptive admission ADAPTIVE WIDENING: the gate is issued NARROW ($9 = 0) and re-issued
	// WIDE ($9 = 1) only when the statement itself proves the narrow window could have
	// changed the admitted set (the proof lives in admit.sql's header). Two passes and no
	// more: with $9 = 1 the window IS quantum*4, so the verdict is false by construction.
	// A widening pass claims, spends and charges NOTHING, so there is nothing to undo.
	// Store-internal by design — headgate.AdmitRequest is unchanged.
	for _, wide := range [...]int32{0, 1} {
		units, widen, err := s.admitPass(ctx, req, leaseMs, wide)
		if err != nil {
			return nil, err
		}
		if !widen {
			return units, nil
		}
	}
	return nil, nil // unreachable: a wide pass never widens
}

func (s *PgxStore) admitPass(ctx context.Context, req headgate.AdmitRequest, leaseMs int64, wide int32) ([]headgate.AdmissionUnit, bool, error) {
	return s.admitPassSQL(ctx, admitSQL, req, leaseMs, wide)
}

func (s *PgxStore) admitPassSQL(ctx context.Context, sql string, req headgate.AdmitRequest, leaseMs int64, wide int32) ([]headgate.AdmissionUnit, bool, error) {
	rows, err := s.pool.Query(ctx, sql,
		req.Queues, int32(req.Capacity), int64(0), leaseMs,
		req.Worker, req.LeaseID, req.Quantum, s.opts.Overfetch, wide)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var units []headgate.AdmissionUnit
	for rows.Next() {
		var (
			id                                              int64
			e                                               headgate.Envelope
			schemaVersion, weight, prio, att, crash, maxAtt int32
			cpJSON, cpCursor, hdrJSON                       []byte
			fence, leaseExpires                             int64
			leaseID                                         string
			hgWiden                                         bool
		)
		// hg_widen is the LAST column, so every position above is exactly what it was.
		if err := rows.Scan(&id, &e.ID, &e.Kind, &schemaVersion, &e.Payload, &e.Queue,
			&e.RateClass, &e.PartitionKey, &weight, &e.Fingerprint, &prio, &att, &crash, &maxAtt,
			&e.ScheduledAtMs, &e.TimeoutMs, &e.DeadlineMs, &e.RetentionMs,
			&cpJSON, &cpCursor, &hdrJSON, &e.PeriodicScheduleID, &e.PeriodicTickMs,
			&e.StickyWorker,
			&fence, &leaseID, &leaseExpires,
			&hgWiden); err != nil {
			return nil, false, err
		}
		if hgWiden {
			// The escalation sentinel: dummy values, no claim, no side effects.
			return nil, true, rows.Err()
		}
		// telemetry and trace context the envelope's opaque headers ride the claim so the runtime
		// can read the RESERVED traceparent at dispatch.
		e.Headers = headgate.DecodeHeaders(hdrJSON)
		e.SchemaVersion = uint32(schemaVersion)
		e.Weight = uint32(weight)
		e.Priority = prio
		e.Attempt, e.CrashAttempt, e.MaxAttempts = uint32(att), uint32(crash), uint32(maxAtt)
		units = append(units, headgate.AdmissionUnit{Claims: []headgate.Claim{{
			Envelope:   e,
			LeaseID:    leaseID,
			Fence:      uint64(fence),
			Expires:    time.UnixMilli(leaseExpires),
			Checkpoint: decodeCheckpoint(cpJSON, cpCursor),
		}}})
	}
	return units, false, rows.Err()
}

// ---------- ack: the transition table (lifecycle state machine), same arms as the Rust adapter ----------

func (s *PgxStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return s.AckAttempt(ctx, lease, outcome, errMsg, delayMs, nil)
}

func (s *PgxStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return s.ackAttemptOn(ctx, s.pool, lease, outcome, errMsg, delayMs, logs, nil)
}

func (s *PgxStore) AckAttemptWithActualWeight(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string, actualWeight *uint32) error {
	if actualWeight == nil {
		return s.AckAttempt(ctx, lease, outcome, errMsg, delayMs, logs)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.reconcileActualWeightPgx(ctx, tx, lease, *actualWeight); err != nil {
		return err
	}
	if err := s.ackAttemptOn(ctx, tx, lease, outcome, errMsg, delayMs, logs, nil); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PgxStore) reconcileActualWeightPgx(ctx context.Context, q querier, lease headgate.LeaseRef, actual uint32) error {
	q = s.pool.scope(q)
	_, err := q.Exec(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms),
		held AS MATERIALIZED (
		  SELECT j.rate_class, j.rate_charge, b.tokens, b.burst,
		         b.limit_per_window, b.window_ms, b.refilled_at_ms
		  FROM headgate_job j
		  JOIN headgate_rate_bucket b ON b.name = j.rate_class
		  WHERE `+ident+` AND j.rate_charge > 0
		  FOR UPDATE OF b
		),
		adjusted AS (
		  UPDATE headgate_rate_bucket b SET
		    tokens = LEAST(h.burst,
		      LEAST(h.burst,
		        h.tokens + GREATEST(0, p.now_ms - h.refilled_at_ms)
		                   * h.limit_per_window / h.window_ms)
		      + h.rate_charge - $4::bigint),
		    refilled_at_ms = p.now_ms
		  FROM held h CROSS JOIN p WHERE b.name = h.rate_class
		  RETURNING 1
		)
		UPDATE headgate_job j SET rate_charge = 0 WHERE `+ident,
		lease.JobID, lease.LeaseID, int64(lease.Fence), int64(actual))
	return err
}

func (s *PgxStore) ackAttemptOn(ctx context.Context, q querier, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string, result *headgate.JobResult) error {
	q = s.pool.scope(q)
	var n int64
	var err error
	fence := int64(lease.Fence)
	var msg any
	if errMsg != "" {
		msg = errMsg
	}
	// attempt-log contract per-attempt logs, folded into the entry each arm writes. NULL = none.
	var logsJSON any
	if len(logs) > 0 {
		if b, e := json.Marshal(logs); e == nil {
			logsJSON = string(b)
		}
	}
	switch outcome {
	case headgate.OutcomeSuccess:
		// retention policy retention 0 deletes; both arms in one statement, atomic with the fence.
		var resultVersion any
		var resultBytes any
		if result != nil {
			resultVersion = int32(result.SchemaVersion)
			resultBytes = result.Bytes
		}
		err = q.QueryRow(ctx, `
			WITH p AS (SELECT `+nowMS+` AS now_ms, $4::jsonb AS logs,
			                  $5::integer AS result_schema_version, $6::bytea AS result_bytes),
			del AS (
			  DELETE FROM headgate_job j USING p
			  WHERE `+ident+` AND j.retention_ms = 0
			  RETURNING j.queue, j.partition_key
			),
			upd AS (
			  UPDATE headgate_job j SET
			    state = 'completed', lease_id = NULL, lease_expires_at_ms = NULL,
			    claimed_by = NULL, finalized_at_ms = p.now_ms,
			    result_schema_version = p.result_schema_version,
			    result_bytes = p.result_bytes,
			    errors = j.errors || CASE WHEN p.logs IS NULL THEN '[]'::jsonb
			        ELSE jsonb_build_array(jsonb_build_object(
			             'at_ms', p.now_ms, 'attempt', j.attempt,
			             'outcome', 'success', 'logs', p.logs)) END
			  FROM p WHERE `+ident+` AND j.retention_ms > 0
			  RETURNING j.queue, j.partition_key
			),
			done AS (SELECT queue, partition_key FROM del
			         UNION ALL SELECT queue, partition_key FROM upd),
			counters AS (
			  INSERT INTO headgate_queue_counter (queue, bucket_ms, completed)
			  SELECT queue, (SELECT now_ms FROM p) / 60000 * 60000, count(*) FROM done GROUP BY 1
			  ON CONFLICT (queue, bucket_ms) DO UPDATE
			    SET completed = headgate_queue_counter.completed + EXCLUDED.completed
			),
			partition_counters AS (
			  INSERT INTO headgate_partition_counter
			    (queue, partition_key, bucket_ms, completed)
			  SELECT queue, partition_key,
			         (SELECT now_ms FROM p) / 60000 * 60000, count(*)
			  FROM done GROUP BY 1, 2
			  ON CONFLICT (queue, partition_key, bucket_ms) DO UPDATE
			    SET completed = headgate_partition_counter.completed + EXCLUDED.completed
			),
			-- adaptive admission running -> completed AND running -> deleted, both arms
			infl AS (`+inflightDec("done")+`)
			SELECT count(*)::bigint FROM done`,
			lease.JobID, lease.LeaseID, fence, logsJSON, resultVersion, resultBytes).Scan(&n)
	case headgate.OutcomeRetry:
		var d any
		if delayMs > 0 {
			d = delayMs
		}
		err = q.QueryRow(ctx, `
			WITH p AS (SELECT `+nowMS+` AS now_ms, $4::bigint AS delay_ms,
			                  $5::text AS err, $6::bigint AS base, $7::bigint AS cap,
			                  $8::jsonb AS logs),
			upd AS (
			  UPDATE headgate_job j SET
			    attempt = j.attempt + 1,
			    state = CASE WHEN j.attempt + 1 < j.max_attempts
			                 THEN 'retryable' ELSE 'archived' END::headgate_state,
			    lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL,
			    scheduled_at_ms = CASE WHEN j.attempt + 1 < j.max_attempts
			        THEN p.now_ms + COALESCE(p.delay_ms,
			             LEAST(p.cap, (p.base * (2 ^ LEAST(j.attempt, 20)))::bigint)
			             + (random() * p.base)::bigint)
			        ELSE j.scheduled_at_ms END,
			    finalized_at_ms = CASE WHEN j.attempt + 1 >= j.max_attempts
			                           THEN p.now_ms ELSE NULL END,
			    errors = (CASE WHEN jsonb_array_length(j.errors) >= 50 THEN j.errors - 0 ELSE j.errors END)
			             || jsonb_build_array(jsonb_build_object(
			                'at_ms', p.now_ms, 'attempt', j.attempt + 1,
			                'outcome', 'retry', 'error', p.err)
			                || CASE WHEN p.logs IS NULL THEN '{}'::jsonb
			                        ELSE jsonb_build_object('logs', p.logs) END)
			  FROM p WHERE `+ident+`
			  RETURNING j.queue, j.partition_key
			),
			-- adaptive admission running -> retryable AND running -> archived, both arms
			infl AS (`+inflightDec("upd")+`)
			SELECT count(*)::bigint FROM upd`,
			lease.JobID, lease.LeaseID, fence, d, msg, s.opts.RetryBaseMs, s.opts.RetryCapMs, logsJSON).Scan(&n)
	case headgate.OutcomeSkip:
		err = s.ackTerminal(ctx, lease, "archived", msg, logsJSON, &n)
	case headgate.OutcomeUndecodable:
		err = s.ackTerminal(ctx, lease, "undecodable", msg, logsJSON, &n)
	case headgate.OutcomeRevoke:
		err = q.QueryRow(ctx,
			`WITH del AS (DELETE FROM headgate_job j WHERE `+ident+`
			              RETURNING j.queue, j.partition_key),
			 -- adaptive admission running -> deleted
			 infl AS (`+inflightDec("del")+`)
			 SELECT count(*)::bigint FROM del`,
			lease.JobID, lease.LeaseID, fence).Scan(&n)
	case headgate.OutcomeSnooze:
		if delayMs <= 0 {
			return errors.New("headgate: snooze requires delayMs > 0 (boundary validation)")
		}
		err = q.QueryRow(ctx, `
			WITH p AS (SELECT `+nowMS+` AS now_ms, $4::bigint AS delay_ms),
			upd AS (
			  UPDATE headgate_job j SET
			    state = 'scheduled', lease_id = NULL, lease_expires_at_ms = NULL,
			    claimed_by = NULL, scheduled_at_ms = p.now_ms + p.delay_ms
			  FROM p WHERE `+ident+`
			  RETURNING j.queue, j.partition_key
			),
			-- adaptive admission running -> scheduled
			infl AS (`+inflightDec("upd")+`)
			SELECT count(*)::bigint FROM upd`,
			lease.JobID, lease.LeaseID, fence, delayMs).Scan(&n)
	case headgate.OutcomeRateLimited:
		// surveyed policy behavior NOT a failure: back to available, neither counter moves.
		err = q.QueryRow(ctx, `
			WITH upd AS (
			  UPDATE headgate_job j SET
			    state = 'available', lease_id = NULL, lease_expires_at_ms = NULL,
			    claimed_by = NULL
			  WHERE `+ident+`
			  RETURNING j.queue, j.partition_key
			),
			-- tenant fairness/adaptive admission requeue puts the partition back in the gate's set, in the
			-- same statement that makes the row available.
			active AS (
			  INSERT INTO headgate_active_partition (queue, partition_key)
			  SELECT queue, partition_key FROM upd
			  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
			),
			-- adaptive admission running -> available (not a failure, but it does leave running)
			infl AS (`+inflightDec("upd")+`)
			SELECT count(*)::bigint FROM upd`,
			lease.JobID, lease.LeaseID, fence).Scan(&n)
	case headgate.OutcomeLeaseLost:
		return errors.New("headgate: lease_lost is applied by the reclaimer, not acked")
	default:
		return fmt.Errorf("headgate: unknown outcome %d", outcome)
	}
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

func (s *PgxStore) AckSuccessWithResult(ctx context.Context, lease headgate.LeaseRef, logs []string, actualWeight *uint32, result headgate.JobResult) error {
	if result.SchemaVersion == 0 {
		return &headgate.InvalidError{Msg: "result schema_version must be greater than zero"}
	}
	if result.SchemaVersion > headgate.MaxOpaqueSchemaVersion {
		return &headgate.InvalidError{Msg: "result schema_version exceeds the portable signed-integer limit"}
	}
	if len(result.Bytes) > 32*1024*1024 {
		return &headgate.InvalidError{Msg: "result bytes exceed the 32 MiB limit"}
	}
	resultBytes := make([]byte, len(result.Bytes))
	copy(resultBytes, result.Bytes)
	result.Bytes = resultBytes
	if actualWeight == nil {
		return s.ackAttemptOn(ctx, s.pool, lease, headgate.OutcomeSuccess, "", 0, logs, &result)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.reconcileActualWeightPgx(ctx, tx, lease, *actualWeight); err != nil {
		return err
	}
	if err := s.ackAttemptOn(ctx, tx, lease, headgate.OutcomeSuccess, "", 0, logs, &result); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PgxStore) WriteJobOutput(
	ctx context.Context,
	lease headgate.LeaseRef,
	output headgate.JobResult,
) (*headgate.JobOutput, error) {
	if output.SchemaVersion == 0 {
		return nil, &headgate.InvalidError{Msg: "output schema_version must be greater than zero"}
	}
	if output.SchemaVersion > headgate.MaxOpaqueSchemaVersion {
		return nil, &headgate.InvalidError{Msg: "output schema_version exceeds the portable signed-integer limit"}
	}
	if len(output.Bytes) > 32*1024*1024 {
		return nil, &headgate.InvalidError{Msg: "output bytes exceed the 32 MiB limit"}
	}
	storedBytes := make([]byte, len(output.Bytes))
	copy(storedBytes, output.Bytes)
	var fence, updatedAtMs int64
	err := s.pool.QueryRow(ctx, `UPDATE headgate_job
		SET output_schema_version = $4, output_bytes = $5, output_fence = fence,
		    output_updated_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint
		WHERE ulid = $1 AND lease_id = $2 AND fence = $3 AND state = 'running'
		RETURNING output_fence, output_updated_at_ms`,
		lease.JobID, lease.LeaseID, int64(lease.Fence), int32(output.SchemaVersion), storedBytes,
	).Scan(&fence, &updatedAtMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	if err != nil {
		return nil, err
	}
	return &headgate.JobOutput{
		SchemaVersion: output.SchemaVersion,
		Bytes:         storedBytes,
		Fence:         uint64(fence),
		UpdatedAtMs:   updatedAtMs,
	}, nil
}

func (s *PgxStore) WriteJobProgress(
	ctx context.Context,
	lease headgate.LeaseRef,
	update headgate.ProgressUpdate,
) (*headgate.JobProgress, error) {
	if err := headgate.ValidateProgress(update); err != nil {
		return nil, err
	}
	var message *string
	if update.Message != "" {
		message = &update.Message
	}
	var fence, updatedAtMs int64
	err := s.pool.QueryRow(ctx, `UPDATE headgate_job
		SET progress_current = $4, progress_total = $5, progress_message = $6,
		    progress_fence = fence,
		    progress_updated_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint
		WHERE ulid = $1 AND lease_id = $2 AND fence = $3 AND state = 'running'
		RETURNING progress_fence, progress_updated_at_ms`,
		lease.JobID, lease.LeaseID, int64(lease.Fence), int64(update.Current),
		int64(update.Total), message,
	).Scan(&fence, &updatedAtMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	if err != nil {
		return nil, err
	}
	return &headgate.JobProgress{
		Current: update.Current, Total: update.Total, Message: update.Message,
		Fence: uint64(fence), UpdatedAtMs: updatedAtMs,
	}, nil
}

func (s *PgxStore) ackTerminal(ctx context.Context, lease headgate.LeaseRef, state string, msg, logsJSON any, n *int64) error {
	return s.pool.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms, $4::text AS err, $5::jsonb AS logs),
		upd AS (
		  UPDATE headgate_job j SET
		    state = '`+state+`'::headgate_state,
		    lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL,
		    finalized_at_ms = p.now_ms,
		    errors = j.errors || CASE WHEN p.err IS NULL AND p.logs IS NULL THEN '[]'::jsonb
		        ELSE jsonb_build_array(jsonb_build_object(
		             'at_ms', p.now_ms, 'outcome', '`+state+`', 'error', p.err)
		             || CASE WHEN p.logs IS NULL THEN '{}'::jsonb
		                     ELSE jsonb_build_object('logs', p.logs) END) END
		  FROM p WHERE `+ident+`
		  RETURNING j.queue, j.partition_key
		),
		-- adaptive admission running -> archived / undecodable
		infl AS (`+inflightDec("upd")+`)
		SELECT count(*)::bigint FROM upd`,
		lease.JobID, lease.LeaseID, int64(lease.Fence), msg, logsJSON).Scan(n)
}

// ---------- renew ----------

func (s *PgxStore) Renew(ctx context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return nil, errors.New("headgate: lease must be >= 1ms (boundary validation)")
	}
	ids := make([]string, len(leases))
	leaseIDs := make([]string, len(leases))
	fences := make([]int64, len(leases))
	for i, l := range leases {
		ids[i], leaseIDs[i], fences[i] = l.JobID, l.LeaseID, int64(l.Fence)
	}
	rows, err := s.pool.Query(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms, $4::bigint AS lease_ms),
		req AS (
		  SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[])
		         AS t(ulid, lease_id, fence)
		),
		upd AS (
		  UPDATE headgate_job j SET lease_expires_at_ms = p.now_ms + p.lease_ms
		  FROM p, req r
		  WHERE j.ulid = r.ulid AND j.lease_id = r.lease_id AND j.fence = r.fence
		    AND j.state = 'running'
		  RETURNING j.ulid
		)
		SELECT r.ulid FROM req r WHERE r.ulid NOT IN (SELECT ulid FROM upd)`,
		ids, leaseIDs, fences, leaseMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lost []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		lost = append(lost, id)
	}
	return lost, rows.Err()
}

// ---------- enqueue ----------

func (s *PgxStore) Enqueue(ctx context.Context, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	scoped := append([]headgate.Envelope(nil), batch...)
	for i := range scoped {
		scoped[i].UniqueKey = headgate.EffectiveUniqueKey(scoped[i])
	}
	batch = scoped
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return headgate.WrapUnavailable(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit
	if err := s.enqueueOn(ctx, tx, batch); err != nil {
		var duplicate *headgate.DuplicateError
		if errors.As(err, &duplicate) && duplicate.Replaced {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return headgate.WrapUnavailable(commitErr)
			}
			return err
		}
		return headgate.WrapUnavailable(err)
	}
	return headgate.WrapUnavailable(tx.Commit(ctx))
}

// querier covers both the pool and a pgx.Tx, so transactional enqueue shares this path.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (s *PgxStore) enqueueOn(ctx context.Context, q querier, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	q = s.pool.scope(q)
	// typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	// idempotent enqueue identity the strict caller-supplied id contract. A batch is all-or-nothing, so the
	// whole classification happens BEFORE anything is written: an id whose row exists
	// with matching content drops out of the batch (idempotent success — this is what
	// makes the API's Idempotency-Key replay safe), and an id whose row exists with
	// DIFFERENT content rejects the entire batch naming the offender. A terminal row
	// still counts as existing; reuse follows retention eviction.
	allIDs := make([]string, len(batch))
	for i, e := range batch {
		allIDs[i] = e.ID
	}
	present := map[string][3]string{}
	rows, err := q.Query(ctx,
		`SELECT ulid, kind, fingerprint, queue FROM headgate_job WHERE ulid = ANY($1::text[])`,
		allIDs)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, kind, fp, queue string
		if err := rows.Scan(&id, &kind, &fp, &queue); err != nil {
			rows.Close()
			return err
		}
		present[id] = [3]string{kind, fp, queue}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	kept := make([]headgate.Envelope, 0, len(batch))
	for _, e := range batch {
		ex, exists := present[e.ID]
		switch {
		case !exists:
			kept = append(kept, e)
		case headgate.SameJobContent(e, ex[0], ex[1], ex[2]):
			// idempotent replay — do not re-write it, do not duplicate it
		default:
			return &headgate.IDConflictError{JobID: e.ID}
		}
	}
	if len(kept) == 0 {
		return nil // every row already exists, unchanged — nothing to write
	}
	batch = kept
	n := len(batch)
	ulids := make([]string, n)
	kinds := make([]string, n)
	versions := make([]int32, n)
	payloads := make([][]byte, n)
	queues := make([]string, n)
	partitions := make([]string, n)
	rateClasses := make([]string, n)
	weights := make([]int32, n)
	fingerprints := make([]string, n)
	priorities := make([]int32, n)
	maxAttempts := make([]int32, n)
	scheduled := make([]int64, n)
	timeouts := make([]int64, n)
	deadlines := make([]int64, n)
	uniqueKeys := make([][]byte, n) // nil element = NULL
	uniqueStates := make([]int32, n)
	uniqueWindows := make([]int64, n)
	retentions := make([]int64, n)
	// telemetry and trace context the envelope's opaque headers. Encoded here, never interpreted.
	headers := make([]string, n)
	periodicScheduleIDs := make([]string, n)
	periodicTicks := make([]int64, n)
	debounceWindows := make([]int64, n)
	pendingFlags := make([]bool, n)
	tagsJSON := make([]string, n)
	stickyWorkers := make([]string, n)
	for i, e := range batch {
		ulids[i], kinds[i], payloads[i] = e.ID, e.Kind, e.Payload
		versions[i] = int32(e.SchemaVersion)
		if versions[i] == 0 {
			versions[i] = 1
		}
		queues[i] = e.Queue
		if queues[i] == "" {
			queues[i] = "default"
		}
		partitions[i], rateClasses[i], fingerprints[i] = e.PartitionKey, e.RateClass, e.Fingerprint
		weights[i] = int32(headgate.EffectiveWeight(e.Weight))
		priorities[i] = e.Priority
		maxAttempts[i] = int32(e.MaxAttempts)
		if maxAttempts[i] == 0 {
			maxAttempts[i] = 25
		}
		scheduled[i], timeouts[i], deadlines[i] = e.ScheduledAtMs, e.TimeoutMs, e.DeadlineMs
		uniqueKeys[i] = e.UniqueKey
		uniqueStates[i] = int32(e.UniqueStates)
		uniqueWindows[i] = e.UniqueWindowMs
		retentions[i] = e.RetentionMs
		hdr := headgate.EncodeHeaders(e.Headers)
		if hdr == "" {
			hdr = "{}" // the column is NOT NULL DEFAULT '{}'
		}
		headers[i] = hdr
		periodicScheduleIDs[i] = e.PeriodicScheduleID
		periodicTicks[i] = e.PeriodicTickMs
		debounceWindows[i] = e.UniqueDebounceMs
		pendingFlags[i] = e.Pending
		canonicalTags := headgate.CanonicalTags(e.Tags)
		if canonicalTags == nil {
			canonicalTags = []string{}
		}
		tagBytes, _ := json.Marshal(canonicalTags)
		tagsJSON[i] = string(tagBytes)
		stickyWorkers[i] = e.StickyWorker
	}

	// crash quarantine quarantined fingerprints are rejected at enqueue, loudly.
	var quarantined *string
	err = q.QueryRow(ctx, `
		SELECT q.fingerprint FROM unnest($1::text[]) f(fp)
		JOIN headgate_quarantine q ON q.fingerprint = f.fp LIMIT 1`,
		fingerprints).Scan(&quarantined)
	if err == nil && quarantined != nil {
		return &headgate.QuarantinedError{Fingerprint: *quarantined}
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// Exact O(1) enqueue policy: producers serialize on one policy row per queue and
	// read only the two monotonic counter rows. Matching-id replays were removed above,
	// so they consume no capacity. This function must run in a transaction; every public
	// Store/Transactional entry point supplies one.
	demand := map[string]int64{}
	for _, e := range batch {
		queue := e.Queue
		if queue == "" {
			queue = "default"
		}
		demand[queue]++
	}
	demandQueues := make([]string, 0, len(demand))
	for queue := range demand {
		demandQueues = append(demandQueues, queue)
	}
	slices.Sort(demandQueues)
	if _, err := q.Exec(ctx, `
		INSERT INTO headgate_enqueue_policy (queue)
		SELECT unnest($1::text[]) ON CONFLICT (queue) DO NOTHING`, demandQueues); err != nil {
		return err
	}
	// Lock and counter read are separate statements. A joined SELECT FOR UPDATE takes
	// its snapshot before waiting; after the wait its joined counters can remain stale
	// even though the policy tuple is refreshed (the exact over-admission trap).
	lockRows, err := q.Query(ctx, `
		SELECT queue FROM headgate_enqueue_policy
		WHERE queue = ANY($1::text[]) ORDER BY queue FOR UPDATE`, demandQueues)
	if err != nil {
		return err
	}
	for lockRows.Next() {
		var locked string
		if err := lockRows.Scan(&locked); err != nil {
			lockRows.Close()
			return err
		}
	}
	lockRows.Close()
	if err := lockRows.Err(); err != nil {
		return err
	}
	policyRows, err := q.Query(ctx, enqueueBackpressureDepthSQL, demandQueues)
	if err != nil {
		return err
	}
	defer policyRows.Close()
	for policyRows.Next() {
		var queue string
		var limit *int64
		var entered, exited int64
		if err := policyRows.Scan(&queue, &limit, &entered, &exited); err != nil {
			return err
		}
		if limit == nil {
			continue
		}
		current := max(int64(0), entered-exited)
		incoming := demand[queue]
		if current+incoming > *limit {
			return &headgate.BackpressureError{Queue: queue, Limit: uint64(*limit), Current: uint64(current), Incoming: uint64(incoming)}
		}
	}
	if err := policyRows.Err(); err != nil {
		return err
	}

	insert := `
		WITH now AS (SELECT ` + nowMS + ` AS ms),
		input AS (
		  SELECT * FROM unnest(
		    $1::text[], $2::text[], $3::int[], $4::bytea[], $5::text[], $6::text[],
		    $7::text[], $8::int[], $9::text[], $10::int[], $11::int[], $12::bigint[],
		    $13::bigint[], $14::bigint[], $15::bytea[], $16::int[], $17::bigint[],
		    $18::bigint[], $19::jsonb[], $20::text[], $21::bigint[],
		    $22::bigint[], $23::boolean[], $24::jsonb[], $25::text[]
		  ) AS t(ulid, kind, schema_version, payload, queue, partition_key,
		         rate_class, weight, fingerprint, priority, max_attempts, scheduled_at_ms,
		         timeout_ms, deadline_ms, unique_key, unique_states, unique_window_ms,
		         retention_ms, headers, periodic_schedule_id, periodic_tick_ms,
		         unique_debounce_ms, pending, tags, sticky_worker)
		),
		ins AS (
		  INSERT INTO headgate_job
		    (ulid, kind, schema_version, payload, queue, state, partition_key,
		     rate_class, weight, fingerprint, priority, max_attempts, enqueued_at_ms,
		     scheduled_at_ms, timeout_ms, deadline_ms, retention_ms,
		     unique_key, unique_states, unique_expires_at_ms, headers,
		     periodic_schedule_id, periodic_tick_ms, sticky_worker)
		  SELECT i.ulid, i.kind, i.schema_version, i.payload, i.queue,
		         CASE WHEN i.pending THEN 'pending'
		              WHEN i.unique_debounce_ms > 0 THEN 'scheduled'
		              WHEN i.scheduled_at_ms > n.ms THEN 'scheduled'
		              ELSE 'available' END::headgate_state,
		         i.partition_key, i.rate_class, i.weight, i.fingerprint, i.priority,
		         i.max_attempts, n.ms,
		         CASE WHEN i.unique_debounce_ms > 0 THEN n.ms + i.unique_debounce_ms
		              WHEN i.scheduled_at_ms = 0 THEN n.ms ELSE i.scheduled_at_ms END,
		         i.timeout_ms, i.deadline_ms, i.retention_ms,
		         i.unique_key, i.unique_states,
		         CASE WHEN i.unique_window_ms > 0 THEN n.ms + i.unique_window_ms
		              ELSE NULL END,
		         i.headers, i.periodic_schedule_id, i.periodic_tick_ms, i.sticky_worker
		  FROM input i CROSS JOIN now n
		  RETURNING id, ulid, queue, partition_key, state
		),
		tag_rows AS (
		  INSERT INTO headgate_job_tag (job_id, tag)
		  SELECT ins.id, jsonb_array_elements_text(i.tags)
		  FROM ins JOIN input i USING (ulid)
		  RETURNING 1
		),
		queue_defaults AS (
		  INSERT INTO headgate_queue_state (queue)
		  SELECT DISTINCT queue FROM ins
		  ON CONFLICT (queue) DO NOTHING
		),
		-- Seed the per-partition ceiling counter before the first possible claim. This
		-- makes the gate's FOR UPDATE lock real even when inflight is still zero.
		inflight_defaults AS (
		  INSERT INTO headgate_inflight (queue, partition_key, n)
		  SELECT DISTINCT queue, partition_key, 0 FROM ins
		  ON CONFLICT (queue, partition_key) DO NOTHING
		),
		-- tenant fairness/adaptive admission the maintained active-partition set the gate reads instead of
		-- scanning. Only rows that landed 'available' count; a 'scheduled' row's
		-- partition is added by PromoteDue when it actually becomes drawable.
		-- ON CONFLICT DO UPDATE, not DO NOTHING: the no-op update takes the row lock
		-- the pruner must wait behind, which is the whole reason a producer can never
		-- lose a race to it (see the migration's comment).
		active AS (
		  INSERT INTO headgate_active_partition (queue, partition_key)
		  SELECT DISTINCT queue, partition_key FROM ins WHERE state = 'available'
		  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
		),
		counters AS (
		  INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived)
		  SELECT queue, (SELECT ms FROM now) / 60000 * 60000, count(*)
		  FROM ins GROUP BY 1
		  ON CONFLICT (queue, bucket_ms) DO UPDATE
		    SET arrived = headgate_queue_counter.arrived + EXCLUDED.arrived
		),
		partition_counters AS (
		  INSERT INTO headgate_partition_counter
		    (queue, partition_key, bucket_ms, arrived)
		  SELECT queue, partition_key, (SELECT ms FROM now) / 60000 * 60000, count(*)
		  FROM ins GROUP BY 1, 2
		  ON CONFLICT (queue, partition_key, bucket_ms) DO UPDATE
		    SET arrived = headgate_partition_counter.arrived + EXCLUDED.arrived
		),
		wakeup AS ( -- push wakeups delivered on COMMIT; spurious wakeups cost latency only
		  SELECT pg_notify('headgate_wakeup', queue)
		  FROM (SELECT DISTINCT queue FROM ins) nq
		)
		-- wakeup MUST be referenced: an unreferenced SELECT CTE is never executed
		-- (only data-modifying CTEs run unconditionally).
		SELECT (SELECT count(*) FROM ins) AS inserted,
		       (SELECT count(*) FROM wakeup) AS notified`
	args := []any{ulids, kinds, versions, payloads, queues, partitions, rateClasses,
		weights, fingerprints, priorities, maxAttempts, scheduled, timeouts, deadlines,
		uniqueKeys, uniqueStates, uniqueWindows, retentions, headers,
		periodicScheduleIDs, periodicTicks, debounceWindows, pendingFlags, tagsJSON, stickyWorkers}
	candidates := make([][]byte, 0, n)
	for _, k := range uniqueKeys {
		if k != nil {
			candidates = append(candidates, k)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		// PostgreSQL aborts the caller transaction after any statement error. A unique
		// collision is a normal typed enqueue result, so isolate the insert and restore
		// the transaction before querying the holder. Plain enqueue now needs this too:
		// its backpressure policy lock and insert deliberately share one transaction.
		if _, err := q.Exec(ctx, "SAVEPOINT headgate_enqueue_insert_attempt"); err != nil {
			return err
		}
		var inserted, notified int64
		err := q.QueryRow(ctx, insert, args...).Scan(&inserted, &notified)
		if err == nil {
			if _, err := q.Exec(ctx, "RELEASE SAVEPOINT headgate_enqueue_insert_attempt"); err != nil {
				return err
			}
			return nil
		}
		if _, rollbackErr := q.Exec(ctx, "ROLLBACK TO SAVEPOINT headgate_enqueue_insert_attempt"); rollbackErr != nil {
			return rollbackErr
		}
		if _, releaseErr := q.Exec(ctx, "RELEASE SAVEPOINT headgate_enqueue_insert_attempt"); releaseErr != nil {
			return releaseErr
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			return err
		}
		// Throttle keys release LAZILY: clear expired holders once, retry once.
		if attempt == 0 {
			tag, rerr := q.Exec(ctx, `
				UPDATE headgate_job SET unique_expires_at_ms = NULL
				WHERE unique_key = ANY($1::bytea[])
				  AND unique_expires_at_ms IS NOT NULL
				  AND unique_expires_at_ms <= `+nowMS, candidates)
			if rerr == nil && tag.RowsAffected() > 0 {
				continue
			}
		}
		// job uniqueness one semantic: the duplicate carries the winner's id.
		var existing, holderState string
		lerr := q.QueryRow(ctx, `
			SELECT ulid, state::text FROM headgate_job
			WHERE unique_key = ANY($1::bytea[])
			  AND (unique_expires_at_ms IS NOT NULL
			       OR state = ANY(ARRAY['pending','scheduled','available','running','retryable']::headgate_state[]))
			LIMIT 1 FOR UPDATE`, candidates).Scan(&existing, &holderState)
		if lerr == nil {
			incoming := batch[0]
			replaced := false
			if incoming.UniqueDebounceMs > 0 {
				schemaVersion := incoming.SchemaVersion
				if schemaVersion == 0 {
					schemaVersion = 1
				}
				var changed int64
				updateErr := q.QueryRow(ctx, `WITH upd AS (
				  UPDATE headgate_job SET schema_version=$2, payload=$3, fingerprint=$4,
				         state='scheduled', scheduled_at_ms=`+nowMS+`+$5
				  WHERE ulid=$1 AND state=ANY(ARRAY['pending','scheduled','available','retryable']::headgate_state[])
				  RETURNING id, queue, partition_key
				), del AS (DELETE FROM headgate_job_tag WHERE job_id IN (SELECT id FROM upd)),
				ins AS (INSERT INTO headgate_job_tag(job_id,tag)
				  SELECT upd.id, unnest($6::text[]) FROM upd RETURNING 1)
				SELECT count(*)::bigint FROM upd`, existing, int32(schemaVersion), incoming.Payload,
					incoming.Fingerprint, incoming.UniqueDebounceMs, headgate.CanonicalTags(incoming.Tags)).Scan(&changed)
				if updateErr != nil {
					return updateErr
				}
				replaced = changed > 0
			} else if incoming.UniqueReplace != 0 {
				schemaVersion := incoming.SchemaVersion
				if schemaVersion == 0 {
					schemaVersion = 1
				}
				maxAttempts := incoming.MaxAttempts
				if maxAttempts == 0 {
					maxAttempts = 25
				}
				tag, updateErr := q.Exec(ctx, `UPDATE headgate_job SET
				schema_version = CASE WHEN ($2::integer & $9::integer) <> 0 THEN $3::integer ELSE schema_version END,
				payload = CASE WHEN ($2::integer & $9::integer) <> 0 THEN $4::bytea ELSE payload END,
				fingerprint = CASE WHEN ($2::integer & $9::integer) <> 0 THEN $5::text ELSE fingerprint END,
				scheduled_at_ms = CASE WHEN ($2::integer & $10::integer) <> 0 AND state = 'scheduled'
				  THEN CASE WHEN $6::bigint = 0 THEN `+nowMS+` ELSE $6::bigint END ELSE scheduled_at_ms END,
				priority = CASE WHEN ($2::integer & $11::integer) <> 0 THEN $7::integer ELSE priority END,
				max_attempts = CASE WHEN ($2::integer & $12::integer) <> 0 THEN $8::integer ELSE max_attempts END
				  WHERE ulid = $1 AND state = ANY(ARRAY['scheduled','available','retryable']::headgate_state[])
				    AND (($2::integer & ($9::integer|$11::integer|$12::integer)) <> 0 OR (($2::integer & $10::integer) <> 0 AND state = 'scheduled'))`,
					existing, int32(incoming.UniqueReplace), int32(schemaVersion), incoming.Payload,
					incoming.Fingerprint, incoming.ScheduledAtMs, incoming.Priority, int32(maxAttempts),
					int32(headgate.UniqueReplacePayload), int32(headgate.UniqueReplaceScheduledAt),
					int32(headgate.UniqueReplacePriority), int32(headgate.UniqueReplaceMaxAttempts))
				if updateErr != nil {
					return updateErr
				}
				replaced = tag.RowsAffected() > 0
			}
			_ = holderState
			return &headgate.DuplicateError{ExistingID: existing, Replaced: replaced}
		}
		// Not a uniqueness index — the ulid PK collided. The pre-check above already
		// classified every id this call knew about, so reaching here means a CONCURRENT
		// producer inserted the row between the read and the write. idempotent enqueue identity's answer is
		// the same typed conflict rather than a bare constraint error; name the offender
		// instead of guessing which id raced.
		var raced string
		if rerr := q.QueryRow(ctx,
			`SELECT ulid FROM headgate_job WHERE ulid = ANY($1::text[]) LIMIT 1`,
			ulids).Scan(&raced); rerr != nil && !errors.Is(rerr, pgx.ErrNoRows) {
			return rerr
		}
		return &headgate.IDConflictError{JobID: raced}
	}
	return errors.New("headgate: enqueue retries at most once")
}

// ---------- step replay checkpoint ----------

func (s *PgxStore) Checkpoint(ctx context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	var n int64
	err := s.pool.QueryRow(ctx, `
		WITH upd AS (
		  UPDATE headgate_job j SET checkpoint = $4::jsonb, cp_cursor = $5
		  WHERE `+ident+`
		  RETURNING 1
		) SELECT count(*)::bigint FROM upd`,
		lease.JobID, lease.LeaseID, int64(lease.Fence), encodeCheckpoint(cp), cp.Cursor).Scan(&n)
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

// ---------- sweeps ----------

func (s *PgxStore) ReclaimExpired(ctx context.Context, limit int64) ([]headgate.Reclaimed, error) {
	rows, err := s.pool.Query(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms, $1::int AS crash_limit, $2::bigint AS lim,
		                  $3::bigint AS base, $4::bigint AS cap),
		expired AS (
		  SELECT j.id FROM headgate_job j, p
		  WHERE j.state = 'running' AND j.lease_expires_at_ms < p.now_ms
		  ORDER BY j.id
		  LIMIT (SELECT lim FROM p)
		  FOR UPDATE SKIP LOCKED
		),
		bumped AS (
		  UPDATE headgate_job j SET
		    crash_attempt = j.crash_attempt + 1,
		    state = CASE WHEN j.crash_attempt + 1 >= p.crash_limit
		                 THEN 'quarantined' ELSE 'retryable' END::headgate_state,
		    lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL,
		    scheduled_at_ms = CASE WHEN j.crash_attempt + 1 < p.crash_limit
		        THEN p.now_ms + LEAST(p.cap, (p.base * (2 ^ LEAST(j.crash_attempt, 20)))::bigint)
		        ELSE j.scheduled_at_ms END,
		    finalized_at_ms = CASE WHEN j.crash_attempt + 1 >= p.crash_limit
		                           THEN p.now_ms ELSE NULL END,
		    errors = j.errors || jsonb_build_array(jsonb_build_object(
		        'at_ms', p.now_ms, 'crash_attempt', j.crash_attempt + 1,
		        'outcome', 'lease_lost', 'error', 'lease expired without ack')),
		    checkpoint = CASE WHEN j.checkpoint ? 'in_progress' THEN
		        jsonb_set(
		          jsonb_set(j.checkpoint, '{crashes}',
		                    COALESCE(j.checkpoint->'crashes', '{}'::jsonb)),
		          ARRAY['crashes', j.checkpoint->>'in_progress'],
		          to_jsonb(COALESCE((j.checkpoint->'crashes'
		                              ->>(j.checkpoint->>'in_progress'))::bigint, 0) + 1))
		      ELSE j.checkpoint END
		  FROM p WHERE j.id IN (SELECT id FROM expired)
		  RETURNING j.ulid, j.kind, j.fingerprint, j.payload, j.crash_attempt, j.state,
		            j.queue, j.partition_key
		),
		-- adaptive admission running -> retryable AND running -> quarantined. The reclaimer is the one
		-- exit a crashed worker cannot take for itself, so it is also the one that MUST
		-- decrement: a lease that expires without this leaks a slot for every process
		-- that ever died mid-job.
		infl AS (`+inflightDec("bumped")+`),
		quar AS (
		  INSERT INTO headgate_quarantine
		         (fingerprint, kind, crash_count, quarantined_at_ms, sample_payload, reason)
		  SELECT DISTINCT ON (b.fingerprint)
		         b.fingerprint, b.kind, b.crash_attempt, (SELECT now_ms FROM p),
		         b.payload, 'crash limit reached'
		  FROM bumped b WHERE b.state = 'quarantined'
		  ON CONFLICT (fingerprint) DO UPDATE
		    SET crash_count = GREATEST(headgate_quarantine.crash_count, EXCLUDED.crash_count)
		)
		SELECT ulid, fingerprint, crash_attempt, (state = 'quarantined') AS quarantined
		FROM bumped`,
		s.opts.CrashLimit, limit, s.opts.RetryBaseMs, s.opts.RetryCapMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.Reclaimed
	for rows.Next() {
		var r headgate.Reclaimed
		var crash int32
		if err := rows.Scan(&r.JobID, &r.Fingerprint, &crash, &r.Quarantined); err != nil {
			return nil, err
		}
		r.CrashAttempt = uint32(crash)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PgxStore) PromoteDue(ctx context.Context, limit int64) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		WITH due AS (
		  SELECT id FROM headgate_job
		  WHERE state IN ('scheduled', 'retryable') AND scheduled_at_ms <= `+nowMS+`
		  ORDER BY scheduled_at_ms, id
		  LIMIT $1::bigint
		  FOR UPDATE SKIP LOCKED
		),
		upd AS (
		  UPDATE headgate_job j SET state = 'available'
		  WHERE j.id IN (SELECT id FROM due)
		  RETURNING j.queue, j.partition_key
		),
		-- tenant fairness/adaptive admission same statement, same transaction: a row cannot become available
		-- without its partition being listed. ON CONFLICT DO UPDATE takes the row lock
		-- (see the migration comment) so the pruner below can never delete under us.
		active AS (
		  INSERT INTO headgate_active_partition (queue, partition_key)
		  SELECT DISTINCT queue, partition_key FROM upd
		  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
		)
		SELECT count(*) FROM upd`, limit).Scan(&n)
	if err != nil {
		return 0, err
	}
	// The counterpart duty: drop partitions that have drained. Staleness is only ever
	// wasted LATERAL probes, so this is best-effort and bounded — but it must never
	// drop a partition that still has work, hence the two-statement lock protocol.
	if _, err := s.pruneActivePartitions(ctx, limit); err != nil {
		return 0, err
	}
	// adaptive admission the inflight counter's safety net, on the duty that already sweeps.
	if _, err := s.reconcileInflight(ctx, limit); err != nil {
		return 0, err
	}
	return n, nil
}

// reconcileInflight recomputes headgate_inflight against the truth, a bounded batch per
// sweep (adaptive admission). Every running → * edge decrements in the same statement as the transition,
// so this should find nothing — it exists because "should" is not a guarantee. A future
// edge added without a decrement, an operator UPDATE run by hand, a restore from a backup
// taken mid-flight all drift the counter. Drift LOW admits past a ceiling for a while;
// drift HIGH chokes a partition against its ceiling permanently with no self-healing
// path, and that asymmetry is why the net is required rather than nice to have.
//
// Bounded two ways so it can sit in a duty that runs constantly: at most `limit`
// partitions per sweep, chosen least-recently-verified (headgate_inflight_stale), each
// one's truth a single index scan of headgate_job_running_partition. FOR UPDATE SKIP
// LOCKED keeps concurrent sweepers and concurrent claims off each other.
//
// Returns how many rows were actually WRONG — the number worth alerting on.
// Mirrors PgStore::reconcile_inflight in crates/headgate-postgres/src/lib.rs.
func (s *PgxStore) reconcileInflight(ctx context.Context, limit int64) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms, $1::bigint AS lim),
		due AS (
		  SELECT queue, partition_key, n AS old_n FROM headgate_inflight
		  ORDER BY reconciled_at_ms
		  LIMIT (SELECT lim FROM p)
		  FOR UPDATE SKIP LOCKED
		),
		truth AS (
		  -- old_n is carried through the due CTE on purpose: an UPDATE's RETURNING sees
		  -- the NEW row, so comparing f.n there would always report "agreed".
		  SELECT d.queue, d.partition_key, d.old_n,
		         (SELECT count(*)::bigint FROM headgate_job j
		           WHERE j.state = 'running'
		             AND j.queue = d.queue AND j.partition_key = d.partition_key) AS n
		  FROM due d
		),
		fixed AS (
		  UPDATE headgate_inflight f
		  SET n = t.n, reconciled_at_ms = (SELECT now_ms FROM p)
		  FROM truth t
		  WHERE f.queue = t.queue AND f.partition_key = t.partition_key
		  RETURNING (t.old_n IS DISTINCT FROM t.n) AS was_wrong
		)
		SELECT count(*) FILTER (WHERE was_wrong) FROM fixed`, limit).Scan(&n)
	return n, err
}

// pruneActivePartitions drops active-partition rows whose partition has drained. Two
// statements inside one READ COMMITTED transaction, and the order is load-bearing:
//
//  1. lock a bounded batch of candidate rows (FOR UPDATE SKIP LOCKED — never block a
//     producer, never deadlock with a concurrent pruner);
//  2. in a SECOND statement, which under READ COMMITTED takes a FRESH snapshot, delete
//     only those with no available job left.
//
// One statement cannot do this. All CTEs in a statement share one snapshot, so a producer
// that committed after that snapshot is invisible and the delete would strand its job —
// the one direction of staleness that is a correctness bug. With the split, a producer
// either committed before step 2's snapshot (we see its job and keep the row) or is still
// blocked on our row lock (it re-inserts after we commit, because ON CONFLICT DO UPDATE
// retries the insert when the conflicting row has been deleted).
func (s *PgxStore) pruneActivePartitions(ctx context.Context, limit int64) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after a successful Commit
	rows, err := tx.Query(ctx, `
		SELECT queue, partition_key FROM headgate_active_partition
		ORDER BY queue, partition_key
		LIMIT $1::bigint
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var queues, parts []string
	for rows.Next() {
		var q, p string
		if err := rows.Scan(&q, &p); err != nil {
			rows.Close()
			return 0, err
		}
		queues, parts = append(queues, q), append(parts, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(queues) == 0 {
		return 0, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM headgate_active_partition ap
		USING unnest($1::text[], $2::text[]) AS l(queue, partition_key)
		WHERE ap.queue = l.queue AND ap.partition_key = l.partition_key
		  AND NOT EXISTS (
		    SELECT 1 FROM headgate_job j
		    WHERE j.state = 'available'
		      AND j.queue = ap.queue AND j.partition_key = ap.partition_key)`, queues, parts)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), tx.Commit(ctx)
}

func (s *PgxStore) EvictRetained(ctx context.Context, limit int64) (int64, error) {
	// retention and eviction contract quarantined is NOT here on purpose: it parks visibly until an operator
	// acts. retention_ms = 0 rows never reach this (deleted at ack time).
	tag, err := s.pool.Exec(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms),
		lapsed AS (
		  SELECT j.*, p.now_ms, a.archive_retention_ms
		  FROM headgate_job j
		  CROSS JOIN p
		  LEFT JOIN headgate_archive_policy a ON a.queue = j.queue
		  WHERE j.state IN ('completed', 'archived', 'cancelled', 'undecodable')
		    AND j.retention_ms > 0
		    AND j.finalized_at_ms + j.retention_ms <= p.now_ms
		  LIMIT $1::bigint
		  FOR UPDATE OF j SKIP LOCKED
		),
		archived AS (
		  INSERT INTO headgate_job_archive (
		    evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
		    fingerprint, attempt, crash_attempt, payload, errors,
		    archive_retention_ms
		  )
		  SELECT now_ms, finalized_at_ms, ulid, kind, queue, state,
		         fingerprint, attempt, crash_attempt, payload, errors,
		         archive_retention_ms
		  FROM lapsed WHERE archive_retention_ms IS NOT NULL
		  RETURNING ulid
		)
		DELETE FROM headgate_job j WHERE j.id IN (SELECT id FROM lapsed)`, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ---------- singleton duties duties ----------

func (s *PgxStore) ClaimDuty(ctx context.Context, name, holder string, lease time.Duration) (bool, error) {
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return false, errors.New("headgate: duty lease must be >= 1ms")
	}
	var n int64
	err := s.pool.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms, $3::bigint AS lease_ms),
		up AS (
		  INSERT INTO headgate_duty AS d (name, holder, expires_at_ms)
		  SELECT $1::text, $2::text, p.now_ms + p.lease_ms FROM p
		  ON CONFLICT (name) DO UPDATE
		    SET holder = EXCLUDED.holder, expires_at_ms = EXCLUDED.expires_at_ms
		    WHERE d.expires_at_ms < EXCLUDED.expires_at_ms - $3::bigint
		       OR d.holder = EXCLUDED.holder
		  RETURNING name
		)
		SELECT count(*)::bigint FROM up`, name, holder, leaseMs).Scan(&n)
	return n == 1, err
}

func (s *PgxStore) ReleaseDuty(ctx context.Context, name, holder string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE headgate_duty SET expires_at_ms = 0 WHERE name = $1 AND holder = $2`,
		name, holder)
	return err
}

// ---------- runtime capability boundary the transactional tier ----------

type pgxTx struct{ tx pgx.Tx }

func (t pgxTx) Unwrap() any { return t.tx }

// Begin opens a store transaction for EnqueueTx/CompleteTx. Callers with their own
// pgx.Tx can pass WrapTx(tx) instead — the caller's transaction is the point (caller-owned transaction contract).
func (s *PgxStore) Begin(ctx context.Context) (headgate.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return pgxTx{tx: tx}, nil
}

func WrapTx(tx pgx.Tx) headgate.Tx { return pgxTx{tx: tx} }

func unwrapTx(tx headgate.Tx) (pgx.Tx, error) {
	if t, ok := tx.Unwrap().(pgx.Tx); ok {
		return t, nil
	}
	// runtime capability boundary never a silent no-op: a foreign handle is a hard, typed error.
	return nil, errors.New("headgate: Tx is not a headgatepgx transaction")
}

func Commit(ctx context.Context, tx headgate.Tx) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	return t.Commit(ctx)
}

func Rollback(ctx context.Context, tx headgate.Tx) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	return t.Rollback(ctx)
}

func (s *PgxStore) EnqueueTx(ctx context.Context, tx headgate.Tx, batch []headgate.Envelope) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	return headgate.WrapUnavailable(s.enqueueOn(ctx, t, batch))
}

// CompleteTx is transactional completion's transactional completion: the job finishes iff the caller's
// writes commit. Success path only, same statement shape as Ack(OutcomeSuccess).
func (s *PgxStore) CompleteTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef) error {
	return s.CompleteTxWithActualWeight(ctx, tx, lease, nil)
}

func (s *PgxStore) CompleteTxWithActualWeight(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, actualWeight *uint32) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	if actualWeight != nil {
		if err := s.reconcileActualWeightPgx(ctx, t, lease, *actualWeight); err != nil {
			return err
		}
	}
	q := s.pool.scope(t)
	var n int64
	err = q.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms),
		del AS (
		  DELETE FROM headgate_job j USING p
		  WHERE `+ident+` AND j.retention_ms = 0
		  RETURNING j.queue
		),
		upd AS (
		  UPDATE headgate_job j SET
		    state = 'completed', lease_id = NULL, lease_expires_at_ms = NULL,
		    claimed_by = NULL, finalized_at_ms = p.now_ms
		  FROM p WHERE `+ident+` AND j.retention_ms > 0
		  RETURNING j.queue
		)
		SELECT (SELECT count(*) FROM del) + (SELECT count(*) FROM upd)`,
		lease.JobID, lease.LeaseID, int64(lease.Fence)).Scan(&n)
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

// ---------- checkpoint JSON, same field names as the other adapters ----------

type cpJSON struct {
	Completed  []string          `json:"completed,omitempty"`
	InProgress string            `json:"in_progress,omitempty"`
	CursorStep string            `json:"cursor_step,omitempty"`
	Version    uint32            `json:"version,omitempty"`
	Hash       string            `json:"hash,omitempty"`
	Crashes    map[string]uint32 `json:"crashes,omitempty"`
}

func encodeCheckpoint(cp headgate.Checkpoint) []byte {
	j := cpJSON{
		Completed:  cp.CompletedSteps,
		InProgress: cp.InProgressStep,
		CursorStep: cp.CursorStep,
		Version:    cp.SchemaVersion,
		Hash:       cp.StepSetHash,
		Crashes:    cp.CrashesByStep,
	}
	b, err := json.Marshal(j)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func decodeCheckpoint(raw, cursor []byte) headgate.Checkpoint {
	cp := headgate.Checkpoint{Cursor: cursor}
	if len(raw) == 0 {
		return cp
	}
	var j cpJSON
	if json.Unmarshal(raw, &j) != nil {
		return cp
	}
	cp.CompletedSteps = j.Completed
	if len(j.Completed) > 0 {
		cp.LastCompletedStep = j.Completed[len(j.Completed)-1]
	}
	cp.InProgressStep = j.InProgress
	cp.CursorStep = j.CursorStep
	cp.SchemaVersion = j.Version
	cp.StepSetHash = j.Hash
	cp.CrashesByStep = j.Crashes
	return cp
}

// ---------- the dyn transactional path (transactional API) + transactional effects effect keys ----------

func (s *PgxStore) BeginTx(ctx context.Context) (headgate.Tx, error) { return s.Begin(ctx) }

func (s *PgxStore) CommitTx(ctx context.Context, tx headgate.Tx) error { return Commit(ctx, tx) }

func (s *PgxStore) RollbackTx(ctx context.Context, tx headgate.Tx) error { return Rollback(ctx, tx) }

func (s *PgxStore) ClaimEffect(ctx context.Context, tx headgate.Tx, key string) (bool, error) {
	t, err := unwrapTx(tx)
	if err != nil {
		return false, err
	}
	q := s.pool.scope(t)
	tag, err := q.Exec(ctx,
		`INSERT INTO headgate_effect (key, at_ms) VALUES ($1, `+nowMS+`)
		 ON CONFLICT (key) DO NOTHING`, key)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PgxStore) CheckpointTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	q := s.pool.scope(t)
	var n int64
	err = q.QueryRow(ctx, `
		WITH upd AS (
		  UPDATE headgate_job j SET checkpoint = $4::jsonb, cp_cursor = $5
		  WHERE `+ident+`
		  RETURNING 1
		) SELECT count(*)::bigint FROM upd`,
		lease.JobID, lease.LeaseID, int64(lease.Fence), encodeCheckpoint(cp), cp.Cursor).Scan(&n)
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}
