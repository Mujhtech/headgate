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
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	headgate "github.com/mujhtech/headgate/go"
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

// the −1 half of the maintained inflight count (headgate_inflight; the +1 is the
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

// Options controls PostgreSQL admission and retry behavior.
type Options struct {
	Overfetch   int32 // admit.sql $8
	CrashLimit  int32 // crash quarantine crashes before quarantine
	RetryBaseMs int64 // default backoff: base * 2^attempt, capped
	RetryCapMs  int64
}

// DefaultOptions returns the recommended PostgreSQL store settings.
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
	listen              *listener // Push wakeup; nil = poll-only (and Caps say so)
	ownedPool           bool
	directProbeCooldown atomic.Uint32
}

// IndexHealth describes usage and tuple statistics for a Headgate index.
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

// New wraps pool with the default store options.
func New(pool *pgxpool.Pool) *PgxStore { return WithOptions(pool, DefaultOptions()) }

// WithOptions wraps pool with the supplied store options.
func WithOptions(pool *pgxpool.Pool, opts Options) *PgxStore {
	return &PgxStore{pool: &schemaPool{raw: pool}, opts: opts}
}

// NewInSchema explicitly qualifies every durable headgate object. The underlying pool
// may be shared with another schema because no connection-level search_path is changed.
func NewInSchema(pool *pgxpool.Pool, schema string) (*PgxStore, error) {
	return WithOptionsInSchema(pool, DefaultOptions(), schema)
}

// WithOptionsInSchema wraps pool and qualifies Headgate objects with schema.
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

// Connect opens an owned connection pool and enables PostgreSQL notifications.
func Connect(ctx context.Context, conninfo string) (*PgxStore, error) {
	pool, err := pgxpool.New(ctx, conninfo)
	if err != nil {
		return nil, err
	}
	store := New(pool).WithListen(conninfo)
	store.ownedPool = true
	return store, nil
}

// ConnectInSchema opens an owned store whose objects are qualified with schema.
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
	store.WithListen(conninfo)
	store.ownedPool = true
	return store, nil
}

// Close stops the dedicated LISTEN lifecycle. Stores created by Connect also own and
// close their command pool; stores wrapping a caller-supplied pool leave it untouched.
func (s *PgxStore) Close() {
	if s == nil {
		return
	}
	if s.listen != nil {
		s.listen.close()
	}
	if s.ownedPool {
		s.pool.raw.Close()
	}
}

// Caps reports the capabilities enabled for this store instance.
func (s *PgxStore) Caps() headgate.Caps {
	c := headgate.CapTransactional | headgate.CapInspect
	if s.listen != nil {
		c |= headgate.CapNotifying
	}
	return c
}

// ---------- admit ----------

// Admit atomically evaluates policy and leases eligible jobs.
func (s *PgxStore) Admit(ctx context.Context, req headgate.AdmitRequest) ([]headgate.AdmissionUnit, error) {
	var err error
	req, leaseMs, err := headgate.NormalizeAdmitRequest(req)
	if err != nil {
		return nil, err
	}
	// compact no-policy/single-partition claim. A true sentinel means the statement
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
	// ADAPTIVE WIDENING: the gate is issued NARROW ($9 = 0) and re-issued
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
		// Carry opaque headers with the claim so the runtime can read traceparent at dispatch.
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
