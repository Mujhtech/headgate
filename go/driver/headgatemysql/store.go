// Package headgatemysql is the Go MySQL driver (push wakeups) — the sixth adapter, completing
// the Rust/Go × PG/Redis/MySQL matrix for the Store port. SQL is ported
// statement-for-statement from crates/headgate-mysql (the same discipline as
// headgatepgx); the gate's policy step is a byte-identical COPY of that crate's
// queries/eligible.sql, drift-gated by scripts/verify.sh exactly like admit.sql.
//
// push wakeups's two loud, permanent MySQL differences apply here identically: NO push wakeup
// (poll only — no LISTEN/NOTIFY), and job uniqueness uses generated columns.
//
// REQUIRED DSN PARAMETER: clientFoundRows=true. Every fence-gated write treats
// "0 rows" as a lost lease, and MySQL's default counts only CHANGED rows — a replayed
// checkpoint or a same-millisecond renew would then read as LeaseRejected. Connect()
// appends it; a caller-supplied *sql.DB (failure classification) must carry it in its own DSN.
package headgatemysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"

	_ "embed"
)

//go:embed eligible.sql
var eligibleSQL string

// nowMS is milliseconds since the Unix epoch, read from the store's clock — the only
// clock every worker shares (boundary validation).
const nowMS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)"

func enqueueBackpressureDepthSQL(queueCount int) string {
	return `SELECT p.queue, p.max_unfinished_jobs,
	       COALESCE(ent.n, 0), COALESCE(ext.n, 0)
	FROM headgate_enqueue_policy p
	LEFT JOIN headgate_enqueue_counter ent
	  ON ent.queue = p.queue AND ent.counter_kind = 'entered'
	LEFT JOIN headgate_enqueue_counter ext
	  ON ext.queue = p.queue AND ext.counter_kind = 'exited'
	WHERE p.queue IN (` + placeholders(queueCount) + `)
	ORDER BY p.queue FOR UPDATE`
}

// ident is the identity clause every fence-gated write shares: ulid, lease_id, fence.
const ident = "ulid = ? AND lease_id = ? AND fence = ? AND state = 'running'"

// activePartByULID lists one job's partition in the gate's maintained active-partition
// set. ON DUPLICATE KEY UPDATE, never INSERT IGNORE: the no-op update takes the
// row lock that serializes this producer against the pruner. Arg: ulid.
const activePartByULID = `INSERT INTO headgate_active_partition (queue, partition_key)
	SELECT queue, partition_key FROM headgate_job WHERE ulid = ?
	ON DUPLICATE KEY UPDATE queue = VALUES(queue)`

// inflightDecByLease decrements the maintained inflight count
// (headgate_inflight; the +1 is admitTx's post-claim upsert, and eligible.sql reads the
// table instead of aggregating every running row in the fleet). Args: ulid, lease_id,
// fence.
//
// MySQL has neither data-modifying CTEs nor RETURNING, so this cannot ride the transition
// statement the way Postgres's does. It runs FIRST, inside the SAME transaction, guarded
// by the identical fence clause — the idiom activePartByULID already uses for the
// rate-limited requeue, and for the same reason: the row must still be 'running' for the
// join to find it, and after the transition statement it no longer is. The multi-table
// UPDATE writes only headgate_inflight; headgate_job is read (and row-locked) to resolve
// the partition. GREATEST(0, …) clamps downward drift instead of letting a negative count
// quietly raise a ceiling; reconcileInflight heals both directions.
const inflightDecByLease = `UPDATE headgate_inflight f
	JOIN headgate_job j ON j.queue = f.queue AND j.partition_key = f.partition_key
	 SET f.n = GREATEST(0, f.n - 1)
	WHERE j.ulid = ? AND j.lease_id = ? AND j.fence = ? AND j.state = 'running'`

// inflightDecByID is the same decrement for paths that already hold the row locked by id
// (the reclaimer). Arg: id.
const inflightDecByID = `UPDATE headgate_inflight f
	JOIN headgate_job j ON j.queue = f.queue AND j.partition_key = f.partition_key
	 SET f.n = GREATEST(0, f.n - 1)
	WHERE j.id = ? AND j.state = 'running'`

// Options controls MySQL admission and retry behavior.
type Options struct {
	// Overfetch: how many partitions beyond capacity enter the candidate set.
	Overfetch int64
	// CrashLimit is the crash quarantine threshold (default 3).
	CrashLimit int64
	// RetryBaseMs/RetryCapMs shape the default retry backoff (defaults 1000 / 1h).
	RetryBaseMs, RetryCapMs int64
}

func defaults(o Options) Options {
	if o.Overfetch == 0 {
		o.Overfetch = 8
	}
	if o.CrashLimit == 0 {
		o.CrashLimit = 3
	}
	if o.RetryBaseMs == 0 {
		o.RetryBaseMs = 1000
	}
	if o.RetryCapMs == 0 {
		o.RetryCapMs = 3_600_000
	}
	return o
}

// MysqlStore implements Headgate storage over a caller-supplied MySQL database.
type MysqlStore struct {
	db   *sql.DB
	opts Options
}

var _ headgate.Store = (*MysqlStore)(nil)
var _ headgate.ResultStore = (*MysqlStore)(nil)
var _ headgate.OutputStore = (*MysqlStore)(nil)
var _ headgate.ProgressStore = (*MysqlStore)(nil)

// New wraps a caller-owned *sql.DB (failure classification — never closed by this package). For T
// concurrently transaction-holding callbacks shared across workers, set MaxOpenConns to
// T+2; MySQL has no notifier connection outside that cap. The DSN MUST include
// clientFoundRows=true; see the package docs and docs/connection-budget.md.
func New(db *sql.DB) *MysqlStore {
	return NewWithOptions(db, Options{})
}

// NewWithOptions wraps db with the supplied store options.
func NewWithOptions(db *sql.DB, o Options) *MysqlStore {
	return &MysqlStore{db: db, opts: defaults(o)}
}

// Connect opens a DB from a go-sql-driver DSN (user:pass@tcp(host:port)/db) or a
// mysql:// URL, forcing clientFoundRows=true.
func Connect(dsn string) (*MysqlStore, error) {
	if strings.HasPrefix(dsn, "mysql://") {
		u := strings.TrimPrefix(dsn, "mysql://")
		// user:pass@host:port/db -> user:pass@tcp(host:port)/db
		at := strings.LastIndex(u, "@")
		slash := strings.Index(u[at+1:], "/")
		if at < 0 || slash < 0 {
			return nil, errors.New("headgate: bad mysql url")
		}
		dsn = u[:at+1] + "tcp(" + u[at+1:at+1+slash] + ")" + u[at+1+slash:]
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("headgate: bad mysql dsn: %w", err)
	}
	cfg.ClientFoundRows = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

func isDupKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// MySQL's generated uniqueness columns are also the concurrency bound. A lazy-release
// UPDATE over raw unique_key has no supporting index, so InnoDB next-key-locks a table
// scan and can deadlock an unrelated unique insert. Keep both conflict queries on the
// generated columns whose unique indexes enforce the same semantics.
func lazyUniqueReleaseSQL(n int) string {
	return `UPDATE headgate_job SET unique_expires_at_ms = NULL
		 WHERE unique_throttle IN (` + placeholders(n) + `)
		   AND unique_expires_at_ms <= ` + nowMS
}

func uniqueHolderSQL(n int) string {
	keys := placeholders(n)
	return `SELECT ulid FROM headgate_job
		 WHERE unique_active IN (` + keys + `) OR unique_throttle IN (` + keys + `)
		 LIMIT 1 FOR UPDATE`
}

func encodeCheckpoint(cp headgate.Checkpoint) string {
	return string(headgateshared.EncodeCheckpoint(cp))
}

func decodeCheckpoint(raw sql.NullString, cursor []byte) headgate.Checkpoint {
	if !raw.Valid || raw.String == "" {
		return headgateshared.DecodeCheckpoint(nil, cursor)
	}
	return headgateshared.DecodeCheckpoint([]byte(raw.String), cursor)
}

// ---------- Store ----------

// Admit atomically evaluates policy and leases eligible jobs.
func (s *MysqlStore) Admit(ctx context.Context, req headgate.AdmitRequest) ([]headgate.AdmissionUnit, error) {
	var err error
	req, leaseMs, err := headgate.NormalizeAdmitRequest(req)
	if err != nil {
		return nil, err
	}
	if len(req.Queues) == 0 {
		return nil, nil
	}
	// store port boundary: the READ COMMITTED transaction IS the atomic unit — MySQL's native gate.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	units, err := s.admitTx(ctx, tx, req, leaseMs)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return units, nil
}

func (s *MysqlStore) admitTx(ctx context.Context, tx *sql.Tx, req headgate.AdmitRequest, leaseMs int64) ([]headgate.AdmissionUnit, error) {
	// TIME COMES FROM THE STORE, NEVER THE CALLER — read once, used consistently.
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return nil, err
	}
	// Lock the token buckets FIRST: FOR UPDATE makes the limit fleet-wide — concurrent
	// admissions serialize here, and eligible.sql's recomputed avail cannot move.
	type bucket struct {
		name  string
		avail int64
	}
	var buckets []bucket
	rows, err := tx.QueryContext(ctx,
		`SELECT name,
		        LEAST(burst, tokens + ((? - refilled_at_ms) * limit_per_window DIV window_ms)) AS avail
		 FROM headgate_rate_bucket FOR UPDATE`, now)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var b bucket
		if err := rows.Scan(&b.name, &b.avail); err != nil {
			_ = rows.Close()
			return nil, err
		}
		buckets = append(buckets, b)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Queue weights and their virtual service counters are fleet policy. Seed defaults,
	// then lock all requested rows in stable order so concurrent admissions cannot spend
	// the same service position.
	queueRows := make([]string, len(req.Queues))
	queueArgs := make([]any, len(req.Queues))
	for i, q := range req.Queues {
		queueRows[i], queueArgs[i] = "SELECT ? AS queue", q
	}
	queueRowSQL := strings.Join(queueRows, " UNION ALL ")
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO headgate_queue_state (queue)
		 SELECT queue FROM (`+queueRowSQL+`) q
		 ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, queueArgs...); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx,
		`SELECT queue, weight, dispatch_count FROM headgate_queue_state
		 WHERE queue IN (`+placeholders(len(req.Queues))+`) ORDER BY queue FOR UPDATE`, queueArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var queue string
		var weight uint32
		var dispatch uint64
		if err := rows.Scan(&queue, &weight, &dispatch); err != nil {
			_ = rows.Close()
			return nil, err
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	type part struct{ queue, partition string }
	type concurrencyLimit struct {
		max      int64
		strategy string
	}
	limits := map[string]concurrencyLimit{}
	rows, err = tx.QueryContext(ctx,
		`SELECT queue, max_concurrent, CAST(on_saturated AS CHAR)
		 FROM headgate_concurrency_limit
		 WHERE queue IN (`+placeholders(len(req.Queues))+`)
		 ORDER BY queue FOR UPDATE`, queueArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var queue, strategy string
		var maxConcurrent uint64
		if err := rows.Scan(&queue, &maxConcurrent, &strategy); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if maxConcurrent > uint64(^uint64(0)>>1) {
			limits[queue] = concurrencyLimit{max: int64(^uint64(0) >> 1), strategy: strategy}
		} else {
			limits[queue] = concurrencyLimit{max: int64(maxConcurrent), strategy: strategy}
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// ADAPTIVE WIDENING. Read the bounded active routes themselves,
	// rather than only their count, because those exact counter rows are the locks that
	// serialize ceiling decisions for this transaction.
	wideLim := req.Quantum * 4
	activeArgs := append([]any(nil), queueArgs...)
	activeArgs = append(activeArgs, int64(req.Capacity)*s.opts.Overfetch)
	rows, err = tx.QueryContext(ctx,
		`WITH requested_queues AS (`+queueRowSQL+`)
		 SELECT t.queue, t.partition_key FROM requested_queues rq
		 JOIN LATERAL (
		   SELECT ap.queue, ap.partition_key FROM headgate_active_partition ap
		   LEFT JOIN headgate_queue_state qs ON qs.queue = ap.queue
		   WHERE ap.queue = rq.queue AND COALESCE(qs.paused, FALSE) = FALSE
		   ORDER BY ap.partition_key LIMIT ?
		 ) t ON TRUE
		 ORDER BY t.queue, t.partition_key`, activeArgs...)
	if err != nil {
		return nil, err
	}
	var activeParts []part
	for rows.Next() {
		var p part
		if err := rows.Scan(&p.queue, &p.partition); err != nil {
			_ = rows.Close()
			return nil, err
		}
		activeParts = append(activeParts, p)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Enqueue seeds these rows; the no-op upsert heals legacy/direct-SQL fixtures.
	// Locking them before policy evaluation makes the first and every later slot atomic.
	inflightBefore := map[part]int64{}
	if len(activeParts) > 0 {
		values := make([]string, len(activeParts))
		counterArgs := make([]any, 0, len(activeParts)*2)
		for i, p := range activeParts {
			values[i] = "(?, ?, 0)"
			counterArgs = append(counterArgs, p.queue, p.partition)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO headgate_inflight (queue, partition_key, n) VALUES `+
				strings.Join(values, ", ")+`
			 ON DUPLICATE KEY UPDATE n = headgate_inflight.n`, counterArgs...); err != nil {
			return nil, err
		}
		pairs := make([]string, len(activeParts))
		for i := range pairs {
			pairs[i] = "(?, ?)"
		}
		rows, err = tx.QueryContext(ctx,
			`SELECT queue, partition_key, n FROM headgate_inflight
			 WHERE (queue, partition_key) IN (`+strings.Join(pairs, ", ")+`)
			 ORDER BY queue, partition_key FOR UPDATE`, counterArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p part
			var n int64
			if err := rows.Scan(&p.queue, &p.partition, &n); err != nil {
				_ = rows.Close()
				return nil, err
			}
			inflightBefore[p] = n
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// Policy read: incoming decisions + every (queue, partition) ranking saw.
	// ReplaceAll also substitutes the marker in the file's header comment, matching Rust.
	q := strings.ReplaceAll(eligibleSQL, "/*QUEUE_ROWS*/", queueRowSQL)
	nParts := int64(len(activeParts))
	c := nParts
	if c < 1 {
		c = 1
	}
	narrowLim := (int64(req.Capacity)+c-1)/c + 1
	if narrowLim > wideLim {
		narrowLim = wideLim
	}

	type decision struct {
		id                       int64
		action, queue, partition string
	}
	var decisions []decision
	var rankedParts []part
	// Two passes at most, and the second is the window this gate has always drawn: the
	// verdict is false by construction once draw_limit IS quantum*4. A widening pass
	// locked nothing, refilled nothing and charged nothing — it is a pure SELECT — so
	// re-running it inside this same transaction is free of side effects to undo.
	for _, drawLim := range [...]int64{narrowLim, wideLim} {
		args := append([]any(nil), queueArgs...)
		// active_parts reads the maintained set now, so it no longer takes a now_ms
		// placeholder — it has no scheduled_at_ms predicate left to compare against.
		args = append(args, int64(req.Capacity)*s.opts.Overfetch, now, drawLim,
			req.Worker, now, drawLim, drawLim,
			now, req.Quantum,
			// `elig_free`'s quantum, then the weighted selector's one
			// capacity limit after both arms.
			req.Quantum, int64(req.Capacity),
			// the round-32d verdict's own five
			int64(req.Capacity), int64(req.Capacity), drawLim, drawLim, wideLim)
		rows, err = tx.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		var passDecisions []decision
		var passParts []part
		widen := false
		for rows.Next() {
			var tag, queue, partition string
			var id int64
			if err := rows.Scan(&tag, &id, &queue, &partition); err != nil {
				_ = rows.Close()
				return nil, err
			}
			switch tag {
			case "p":
				passParts = append(passParts, part{queue, partition})
			case "w":
				widen = id != 0
			default:
				passDecisions = append(passDecisions, decision{
					id: id, action: tag, queue: queue, partition: partition,
				})
			}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if widen && drawLim != wideLim {
			continue
		}
		decisions, rankedParts = passDecisions, passParts
		break
	}

	var units []headgate.AdmissionUnit
	claimedPer := map[part]int64{}
	terminalPer := map[part]int64{}
	victimPer := map[part]int64{}
	spent := map[string]int64{}
	if len(decisions) > 0 {
		// Only selected incoming decisions are locked; queue-saturated jobs never enter
		// this list and therefore remain visible and unleased (invariant 2).
		// REQUIRED state re-check: under READ COMMITTED, SKIP LOCKED only skips rows
		// locked RIGHT NOW — a row another worker claimed and COMMITTED mid-gate is
		// unlocked and would pass straight through. Re-checking state drops it.
		inList := placeholders(len(decisions))
		idArgs := make([]any, len(decisions))
		for i, d := range decisions {
			idArgs[i] = d.id
		}
		rows, err = tx.QueryContext(ctx,
			`SELECT id FROM headgate_job
			 WHERE id IN (`+inList+`) AND state = 'available'
			 ORDER BY id FOR UPDATE SKIP LOCKED`, idArgs...)
		if err != nil {
			return nil, err
		}
		var locked []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return nil, err
			}
			locked = append(locked, id)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(locked) > 0 {
			lockedSet := make(map[int64]struct{}, len(locked))
			for _, id := range locked {
				lockedSet[id] = struct{}{}
			}
			lockedDecisions := make([]decision, 0, len(locked))
			var claimIDs []int64
			replacements := map[part][]int64{}
			for _, d := range decisions {
				if _, ok := lockedSet[d.id]; !ok {
					continue
				}
				lockedDecisions = append(lockedDecisions, d)
				switch d.action {
				case "claim":
					claimIDs = append(claimIDs, d.id)
				case "cancel_running":
					p := part{d.queue, d.partition}
					replacements[p] = append(replacements[p], d.id)
				}
			}

			// Newest wins. Cancel only the oldest running siblings needed to make room.
			// If an ack currently owns one, SKIP LOCKED yields fewer victims; the allowed
			// count below then keeps this transaction at or below the ceiling.
			var victimIDs []int64
			for p, incoming := range replacements {
				limit, ok := limits[p.queue]
				if !ok {
					continue
				}
				before := inflightBefore[p]
				need := before + int64(len(incoming)) - limit.max
				if need < 0 {
					need = 0
				}
				var victims []int64
				if need > 0 {
					rows, err = tx.QueryContext(ctx,
						`SELECT id FROM headgate_job
						 WHERE state = 'running' AND queue = ? AND partition_key = ?
						 ORDER BY claimed_at_ms, id LIMIT ? FOR UPDATE SKIP LOCKED`,
						p.queue, p.partition, need)
					if err != nil {
						return nil, err
					}
					for rows.Next() {
						var id int64
						if err := rows.Scan(&id); err != nil {
							_ = rows.Close()
							return nil, err
						}
						victims = append(victims, id)
					}
					_ = rows.Close()
					if err := rows.Err(); err != nil {
						return nil, err
					}
				}
				victimPer[p] += int64(len(victims))
				victimIDs = append(victimIDs, victims...)

				allowed := limit.max - before + int64(len(victims))
				if allowed < 0 {
					allowed = 0
				}
				if allowed > int64(len(incoming)) {
					allowed = int64(len(incoming))
				}
				incomingArgs := make([]any, len(incoming))
				for i, id := range incoming {
					incomingArgs[i] = id
				}
				rows, err = tx.QueryContext(ctx,
					`SELECT id FROM headgate_job WHERE id IN (`+placeholders(len(incoming))+`)
					 ORDER BY priority DESC, scheduled_at_ms, id`, incomingArgs...)
				if err != nil {
					return nil, err
				}
				var selected int64
				for rows.Next() {
					var id int64
					if err := rows.Scan(&id); err != nil {
						_ = rows.Close()
						return nil, err
					}
					if selected < allowed {
						claimIDs = append(claimIDs, id)
					}
					selected++
				}
				_ = rows.Close()
				if err := rows.Err(); err != nil {
					return nil, err
				}
			}

			if len(victimIDs) > 0 {
				args := make([]any, 0, len(victimIDs)+1)
				args = append(args, now)
				for _, id := range victimIDs {
					args = append(args, id)
				}
				if _, err := tx.ExecContext(ctx,
					`UPDATE headgate_job SET state = 'cancelled', finalized_at_ms = ?,
					   lease_id = NULL, lease_expires_at_ms = NULL, claimed_at_ms = NULL,
					   claimed_by = NULL, rate_charge = 0, fence = fence + 1
					 WHERE id IN (`+placeholders(len(victimIDs))+`) AND state = 'running'`,
					args...); err != nil {
					return nil, err
				}
			}

			for _, terminal := range []struct{ action, state string }{
				{"discard", "archived"},
				{"cancel_incoming", "cancelled"},
			} {
				var ids []int64
				for _, d := range lockedDecisions {
					if d.action == terminal.action {
						ids = append(ids, d.id)
						terminalPer[part{d.queue, d.partition}]++
					}
				}
				if len(ids) == 0 {
					continue
				}
				args := make([]any, 0, len(ids)+1)
				args = append(args, now)
				for _, id := range ids {
					args = append(args, id)
				}
				if _, err := tx.ExecContext(ctx,
					`UPDATE headgate_job SET state = '`+terminal.state+`', finalized_at_ms = ?,
					   lease_id = NULL, lease_expires_at_ms = NULL, claimed_at_ms = NULL,
					   claimed_by = NULL, rate_charge = 0
					 WHERE id IN (`+placeholders(len(ids))+`) AND state = 'available'`,
					args...); err != nil {
					return nil, err
				}
			}

			if len(claimIDs) > 0 {
				// lease fencing the lease is written by the same transaction that claims.
				inList := placeholders(len(claimIDs))
				lockArgs := []any{req.LeaseID, now + leaseMs, now, req.Worker}
				for _, id := range claimIDs {
					lockArgs = append(lockArgs, id)
				}
				if _, err := tx.ExecContext(ctx,
					`UPDATE headgate_job SET
				   state = 'running', lease_id = ?, lease_expires_at_ms = ?,
				   claimed_at_ms = ?, fence = fence + 1, claimed_by = ?, rate_charge = 0
				 WHERE id IN (`+inList+`) AND state = 'available'`, lockArgs...); err != nil {
					return nil, err
				}
				// A fail-open job spent no configured bucket and must remain unchargeable if
				// an operator creates that class before the attempt acks.
				if len(buckets) > 0 {
					bucketArgs := make([]any, 0, len(claimIDs)+len(buckets))
					for _, id := range claimIDs {
						bucketArgs = append(bucketArgs, id)
					}
					for _, b := range buckets {
						bucketArgs = append(bucketArgs, b.name)
					}
					if _, err := tx.ExecContext(ctx,
						`UPDATE headgate_job SET rate_charge = weight
					 WHERE id IN (`+inList+`) AND rate_class IN (`+placeholders(len(buckets))+`)`,
						bucketArgs...); err != nil {
						return nil, err
					}
				}
				idArgs := make([]any, len(claimIDs))
				for i, id := range claimIDs {
					idArgs[i] = id
				}
				rows, err = tx.QueryContext(ctx,
					`SELECT ulid, kind, schema_version, payload, queue, rate_class,
				        partition_key, weight, fingerprint, priority, attempt, crash_attempt,
				        max_attempts, scheduled_at_ms, timeout_ms, deadline_ms,
				        retention_ms, CAST(checkpoint AS CHAR), cp_cursor,
				        CAST(headers AS CHAR), periodic_schedule_id, periodic_tick_ms, sticky_worker, fence,
				        lease_id, lease_expires_at_ms, unique_states, unique_window_ms
				 FROM headgate_job WHERE id IN (`+inList+`) ORDER BY id`, idArgs...)
				if err != nil {
					return nil, err
				}
				for rows.Next() {
					var e headgate.Envelope
					var cpJSON, hdrJSON sql.NullString
					var cursor []byte
					var fence uint64
					var leaseID string
					var expiresMs int64
					if err := rows.Scan(&e.ID, &e.Kind, &e.SchemaVersion, &e.Payload,
						&e.Queue, &e.RateClass, &e.PartitionKey, &e.Weight, &e.Fingerprint,
						&e.Priority, &e.Attempt, &e.CrashAttempt, &e.MaxAttempts,
						&e.ScheduledAtMs, &e.TimeoutMs, &e.DeadlineMs, &e.RetentionMs,
						&cpJSON, &cursor, &hdrJSON, &e.PeriodicScheduleID, &e.PeriodicTickMs,
						&e.StickyWorker,
						&fence, &leaseID, &expiresMs,
						&e.UniqueStates, &e.UniqueWindowMs); err != nil {
						_ = rows.Close()
						return nil, err
					}
					// Carry opaque headers with the claim so the runtime can read traceparent at dispatch.
					if hdrJSON.Valid {
						e.Headers = headgate.DecodeHeaders([]byte(hdrJSON.String))
					}
					if e.RateClass != "" {
						spent[e.RateClass] += int64(headgate.EffectiveWeight(e.Weight))
					}
					claimedPer[part{e.Queue, e.PartitionKey}]++
					units = append(units, headgate.AdmissionUnit{Claims: []headgate.Claim{{
						Envelope: e, LeaseID: leaseID, Fence: fence,
						Expires:    time.UnixMilli(expiresMs),
						Checkpoint: decodeCheckpoint(cpJSON, cursor),
					}}})
				}
				_ = rows.Close()
				if err := rows.Err(); err != nil {
					return nil, err
				}
			}
		}
	}

	// Apply the net (+claims - displaced victims) in the same transaction.
	delta := map[part]int64{}
	for p, n := range claimedPer {
		delta[p] += n
	}
	for p, n := range victimPer {
		delta[p] -= n
	}
	for p, n := range delta {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO headgate_inflight (queue, partition_key, n)
			 VALUES (?, ?, GREATEST(0, ?)) AS new
			 ON DUPLICATE KEY UPDATE n = GREATEST(0, headgate_inflight.n + ?)`,
			p.queue, p.partition, n, n); err != nil {
			return nil, err
		}
	}

	decisionPer := map[part]int64{}
	for p, n := range claimedPer {
		decisionPer[p] += n
	}
	for p, n := range terminalPer {
		decisionPer[p] += n
	}
	// Claims and incoming terminal decisions consume queue service. Displaced running
	// victims do not: they are the cost of newest-wins, not newly selected work.
	served := map[string]int64{}
	for p, n := range decisionPer {
		served[p.queue] += n
	}
	for queue, n := range served {
		if _, err := tx.ExecContext(ctx,
			`UPDATE headgate_queue_state
			 SET dispatch_count = dispatch_count + ? WHERE queue = ?`, n, queue); err != nil {
			return nil, err
		}
	}

	// Spend: refill + spend in one write per bucket (they are locked by us).
	for _, b := range buckets {
		if _, err := tx.ExecContext(ctx,
			"UPDATE headgate_rate_bucket SET tokens = ?, refilled_at_ms = ? WHERE name = ?",
			b.avail-spent[b.name], now, b.name); err != nil {
			return nil, err
		}
	}
	// tenant fairness terminal incoming decisions count as service too; otherwise a discard loop
	// would accumulate an accidental future fairness burst.
	for _, p := range rankedParts {
		credit := req.Quantum - decisionPer[p]
		if credit < 0 {
			credit = 0
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO headgate_partition_deficit (queue, partition_key, deficit, updated_at_ms)
			 VALUES (?, ?, ?, ?) AS new
			 ON DUPLICATE KEY UPDATE
			   deficit = LEAST(?, headgate_partition_deficit.deficit + new.deficit),
			   updated_at_ms = new.updated_at_ms`,
			p.queue, p.partition, credit, now, req.Quantum*4); err != nil {
			return nil, err
		}
	}
	return units, nil
}

// Ack records an attempt outcome if lease still identifies the current holder.
