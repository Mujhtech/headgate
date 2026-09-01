// Package headgatemysql is the Go MySQL driver (push wakeups) — the sixth adapter, completing
// the Rust/Go × PG/Redis/MySQL matrix for the Store port. SQL is ported
// statement-for-statement from crates/headgate-mysql (the same discipline as
// headgatepgx); the gate's policy step is a byte-identical COPY of that crate's
// queries/eligible.sql, drift-gated by scripts/verify.sh exactly like admit.sql.
//
// push wakeups's two loud, permanent MySQL differences apply here identically: NO push wakeup
// (poll only — no LISTEN/NOTIFY), and job uniqueness uniqueness rides generated columns.
//
// REQUIRED DSN PARAMETER: clientFoundRows=true. Every fence-gated write treats
// "0 rows" as a lost lease, and MySQL's default counts only CHANGED rows — a replayed
// checkpoint or a same-millisecond renew would then read as LeaseRejected. Connect()
// appends it; a caller-supplied *sql.DB (failure classification) must carry it in its own DSN.
package headgatemysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

import _ "embed"

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
// set (tenant fairness/adaptive admission). ON DUPLICATE KEY UPDATE, never INSERT IGNORE: the no-op update takes the
// row lock that serializes this producer against the pruner. Arg: ulid.
const activePartByULID = `INSERT INTO headgate_active_partition (queue, partition_key)
	SELECT queue, partition_key FROM headgate_job WHERE ulid = ?
	ON DUPLICATE KEY UPDATE queue = VALUES(queue)`

// inflightDecByLease is the adaptive admission −1 half of the maintained inflight count
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

type Options struct {
	// Overfetch: how many partitions beyond capacity enter the candidate set.
	Overfetch int64
	// CrashLimit is the crash quarantine quarantine threshold (default 3).
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
		var max uint64
		if err := rows.Scan(&queue, &max, &strategy); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if max > uint64(^uint64(0)>>1) {
			limits[queue] = concurrencyLimit{max: int64(^uint64(0) >> 1), strategy: strategy}
		} else {
			limits[queue] = concurrencyLimit{max: int64(max), strategy: strategy}
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// adaptive admission ADAPTIVE WIDENING. Read the bounded active routes themselves,
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
		// adaptive admission active_parts reads the maintained set now, so it no longer takes a now_ms
		// placeholder — it has no scheduled_at_ms predicate left to compare against.
		args = append(args, int64(req.Capacity)*s.opts.Overfetch, now, drawLim,
			req.Worker, now, drawLim, drawLim,
			now, req.Quantum,
			// adaptive admission `elig_free`'s quantum, then the weighted selector's one
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
					// telemetry and trace context the opaque headers ride the claim so the runtime can
					// read the RESERVED traceparent at dispatch.
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

func (s *MysqlStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return s.AckAttempt(ctx, lease, outcome, errMsg, delayMs, nil)
}

func (s *MysqlStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return s.AckAttemptWithActualWeight(ctx, lease, outcome, errMsg, delayMs, logs, nil)
}

func (s *MysqlStore) AckAttemptWithActualWeight(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string, actualWeight *uint32) error {
	if err := headgate.ValidateAckRequest(outcome, delayMs); err != nil {
		return err
	}
	fence := int64(lease.Fence)
	var msg any
	if errMsg != "" {
		msg = errMsg
	}
	// attempt-log contract: the logs land INSIDE the attempt's entry, exactly as everywhere else.
	var logsObj any
	if len(logs) > 0 {
		logsObj = `{"logs": ` + headgateshared.EncodeStringList(logs) + `}`
	}
	entry := func(outcomeName, attemptExpr string) string {
		return `JSON_ARRAY_APPEND(
		   CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
		        ELSE errors END,
		   '$',
		   JSON_MERGE_PATCH(
		     JSON_OBJECT('at_ms', ` + nowMS + `, 'attempt', ` + attemptExpr + `,
		                 'outcome', '` + outcomeName + `', 'error', ?),
		     COALESCE(CAST(? AS JSON), JSON_OBJECT())))`
	}
	var n int64
	switch outcome {
	case headgate.OutcomeSuccess:
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if actualWeight != nil {
			if err := reconcileActualWeightMysql(ctx, tx, lease, *actualWeight); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		n, err = ackSuccessTx(ctx, tx, lease, fence, logsObj, nil)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	case headgate.OutcomeRetry:
		var d any
		if delayMs > 0 {
			d = delayMs
		}
		// NOTE: MySQL evaluates SET left to right; `attempt` is assigned FIRST, so
		// later expressions see the incremented value and compare with `<`.
		// adaptive admission running -> retryable AND running -> archived; both leave running, so both
		// decrement. Dec first, same transaction (see inflightDecByLease).
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if actualWeight != nil {
			if err := reconcileActualWeightMysql(ctx, tx, lease, *actualWeight); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, inflightDecByLease, lease.JobID, lease.LeaseID, fence); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE headgate_job SET
			   attempt = attempt + 1,
			   state = IF(attempt < max_attempts, 'retryable', 'archived'),
			   scheduled_at_ms = IF(attempt < max_attempts,
			       `+nowMS+` + COALESCE(?,
			         LEAST(?, CAST(? * POW(2, LEAST(attempt - 1, 20)) AS SIGNED))
			         + FLOOR(RAND() * ?)),
			       scheduled_at_ms),
			   finalized_at_ms = IF(attempt >= max_attempts, `+nowMS+`, NULL),
			   errors = `+entry("retry", "attempt")+`,
			   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
			 WHERE `+ident,
			d, s.opts.RetryCapMs, s.opts.RetryBaseMs, s.opts.RetryBaseMs,
			msg, logsObj, lease.JobID, lease.LeaseID, fence)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if err := tx.Commit(); err != nil {
			return err
		}
	case headgate.OutcomeSkip, headgate.OutcomeUndecodable:
		toState := "archived"
		if outcome == headgate.OutcomeUndecodable {
			toState = "undecodable"
		}
		// adaptive admission running -> archived / undecodable
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if actualWeight != nil {
			if err := reconcileActualWeightMysql(ctx, tx, lease, *actualWeight); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, inflightDecByLease, lease.JobID, lease.LeaseID, fence); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE headgate_job SET
			   state = '`+toState+`',
			   finalized_at_ms = `+nowMS+`,
			   errors = CASE WHEN ? IS NULL AND ? IS NULL THEN errors
			                 ELSE `+entry(toState, "attempt")+` END,
			   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
			 WHERE `+ident,
			msg, logsObj, msg, logsObj, lease.JobID, lease.LeaseID, fence)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if err := tx.Commit(); err != nil {
			return err
		}
	case headgate.OutcomeRevoke:
		// adaptive admission running -> deleted. The row is GONE after this, so the decrement must
		// precede it — there is nothing left to join against afterwards.
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if actualWeight != nil {
			if err := reconcileActualWeightMysql(ctx, tx, lease, *actualWeight); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, inflightDecByLease, lease.JobID, lease.LeaseID, fence); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			"DELETE FROM headgate_job WHERE "+ident, lease.JobID, lease.LeaseID, fence)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if err := tx.Commit(); err != nil {
			return err
		}
	case headgate.OutcomeSnooze:
		if delayMs <= 0 {
			return errors.New("headgate: snooze requires delayMs > 0 (boundary validation)")
		}
		// adaptive admission running -> scheduled
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if actualWeight != nil {
			if err := reconcileActualWeightMysql(ctx, tx, lease, *actualWeight); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, inflightDecByLease, lease.JobID, lease.LeaseID, fence); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE headgate_job SET
			   state = 'scheduled', scheduled_at_ms = `+nowMS+` + ?,
			   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
			 WHERE `+ident, delayMs, lease.JobID, lease.LeaseID, fence)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if err := tx.Commit(); err != nil {
			return err
		}
	case headgate.OutcomeRateLimited:
		// surveyed policy behavior NOT a failure: back to available, neither counter moves.
		// tenant fairness/adaptive admission MySQL has no data-modifying CTEs, so the partition is listed by a
		// SEPARATE statement — which is why the pair runs in ONE transaction and the
		// INSERT goes first (it reads the row while it is still 'running').
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if actualWeight != nil {
			if err := reconcileActualWeightMysql(ctx, tx, lease, *actualWeight); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, activePartByULID, lease.JobID); err != nil {
			return err
		}
		// adaptive admission running -> available. Not a failure, but it does leave running, so the
		// slot comes back. Same ordering rule: before the transition.
		if _, err := tx.ExecContext(ctx, inflightDecByLease, lease.JobID, lease.LeaseID, fence); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE headgate_job SET
			   state = 'available',
			   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
			 WHERE `+ident, lease.JobID, lease.LeaseID, fence)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if err := tx.Commit(); err != nil {
			return err
		}
	case headgate.OutcomeLeaseLost:
		return errors.New("headgate: lease_lost is applied by the reclaimer, not acked")
	default:
		return fmt.Errorf("headgate: unknown outcome %d", outcome)
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

// reconcileActualWeightMysql corrects the estimate with MySQL's own clock inside the
// ack/Once transaction. rate_charge is zero for a fail-open admission, so creating a
// class while the handler runs cannot retroactively debit it.
func reconcileActualWeightMysql(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, lease headgate.LeaseRef, actual uint32) error {
	_, err := q.ExecContext(ctx, `UPDATE headgate_rate_bucket b
		JOIN headgate_job j ON j.rate_class = b.name
		CROSS JOIN (SELECT `+nowMS+` AS now_ms) p
		SET b.tokens = LEAST(b.burst,
		      LEAST(b.burst,
		        b.tokens + FLOOR(GREATEST(0, p.now_ms - b.refilled_at_ms)
		                         * b.limit_per_window / b.window_ms))
		      + j.rate_charge - ?),
		    b.refilled_at_ms = p.now_ms
		WHERE j.ulid = ? AND j.lease_id = ? AND j.fence = ?
		  AND j.state = 'running' AND j.rate_charge > 0`,
		actual, lease.JobID, lease.LeaseID, int64(lease.Fence))
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE headgate_job SET rate_charge = 0 WHERE `+ident,
		lease.JobID, lease.LeaseID, int64(lease.Fence))
	return err
}

func ackSuccessTx(ctx context.Context, tx *sql.Tx, lease headgate.LeaseRef, fence int64, logsObj any, result *headgate.JobResult) (int64, error) {
	// retention policy retention 0 = ephemeral: delete, not keep. Each arm is fence-guarded, so
	// the two statements cannot both fire; a mid-pair reclaim just means REJ.
	var queue, partitionKey string
	err := tx.QueryRowContext(ctx,
		"SELECT queue, partition_key FROM headgate_job WHERE "+ident,
		lease.JobID, lease.LeaseID, fence).Scan(&queue, &partitionKey)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	// adaptive admission running -> completed AND running -> deleted. One decrement covers both arms:
	// exactly one fires (they split on retention_ms), and this must run while the row is
	// still 'running' — the ephemeral arm DELETEs it outright.
	if _, err := tx.ExecContext(ctx, inflightDecByLease, lease.JobID, lease.LeaseID, fence); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx,
		"DELETE FROM headgate_job WHERE "+ident+" AND retention_ms = 0",
		lease.JobID, lease.LeaseID, fence)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var resultVersion any
		var resultBytes any
		if result != nil {
			resultVersion = result.SchemaVersion
			resultBytes = result.Bytes
		}
		res, err = tx.ExecContext(ctx,
			`UPDATE headgate_job SET
			   state = 'completed', finalized_at_ms = `+nowMS+`,
			   result_schema_version = ?, result_bytes = ?,
			   errors = CASE WHEN ? IS NULL THEN errors ELSE
			     JSON_ARRAY_APPEND(
			       CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
			            ELSE errors END,
			       '$', JSON_MERGE_PATCH(
			              JSON_OBJECT('at_ms', `+nowMS+`, 'attempt', attempt,
			                          'outcome', 'success'),
			              CAST(? AS JSON))) END,
			   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
			 WHERE `+ident+` AND retention_ms > 0`,
			resultVersion, resultBytes, logsObj, logsObj, lease.JobID, lease.LeaseID, fence)
		if err != nil {
			return 0, err
		}
		n, _ = res.RowsAffected()
	}
	if n > 0 {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO headgate_queue_counter (queue, bucket_ms, completed)
			 VALUES (?, (`+nowMS+` DIV 60000) * 60000, 1) AS new
			 ON DUPLICATE KEY UPDATE completed = headgate_queue_counter.completed + 1`,
			queue); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO headgate_partition_counter
			   (queue, partition_key, bucket_ms, completed)
			 VALUES (?, ?, (`+nowMS+` DIV 60000) * 60000, 1) AS new
			 ON DUPLICATE KEY UPDATE completed = headgate_partition_counter.completed + 1`,
			queue, partitionKey); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (s *MysqlStore) AckSuccessWithResult(ctx context.Context, lease headgate.LeaseRef, logs []string, actualWeight *uint32, result headgate.JobResult) error {
	if err := headgate.ValidateOpaqueValue("result", result); err != nil {
		return err
	}
	resultBytes := make([]byte, len(result.Bytes))
	copy(resultBytes, result.Bytes)
	result.Bytes = resultBytes
	var logsObj any
	if len(logs) > 0 {
		logsObj = `{"logs": ` + headgateshared.EncodeStringList(logs) + `}`
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	if actualWeight != nil {
		if err := reconcileActualWeightMysql(ctx, tx, lease, *actualWeight); err != nil {
			return err
		}
	}
	n, err := ackSuccessTx(ctx, tx, lease, int64(lease.Fence), logsObj, &result)
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return tx.Commit()
}

func (s *MysqlStore) WriteJobOutput(
	ctx context.Context,
	lease headgate.LeaseRef,
	output headgate.JobResult,
) (*headgate.JobOutput, error) {
	if err := headgate.ValidateOpaqueValue("output", output); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	storedBytes := make([]byte, len(output.Bytes))
	copy(storedBytes, output.Bytes)
	result, err := tx.ExecContext(ctx, `UPDATE headgate_job
		SET output_schema_version = ?, output_bytes = ?, output_fence = fence,
		    output_updated_at_ms = `+nowMS+`
		WHERE ulid = ? AND lease_id = ? AND fence = ? AND state = 'running'`,
		output.SchemaVersion, storedBytes, lease.JobID, lease.LeaseID, lease.Fence)
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	var updatedAtMs int64
	if err := tx.QueryRowContext(ctx,
		`SELECT output_updated_at_ms FROM headgate_job WHERE ulid = ?`, lease.JobID,
	).Scan(&updatedAtMs); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &headgate.JobOutput{
		SchemaVersion: output.SchemaVersion,
		Bytes:         storedBytes,
		Fence:         lease.Fence,
		UpdatedAtMs:   updatedAtMs,
	}, nil
}

func (s *MysqlStore) WriteJobProgress(
	ctx context.Context,
	lease headgate.LeaseRef,
	update headgate.ProgressUpdate,
) (*headgate.JobProgress, error) {
	if err := headgate.ValidateProgress(update); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	var message any
	if update.Message != "" {
		message = update.Message
	}
	result, err := tx.ExecContext(ctx, `UPDATE headgate_job
		SET progress_current = ?, progress_total = ?, progress_message = ?,
		    progress_fence = fence, progress_updated_at_ms = `+nowMS+`
		WHERE ulid = ? AND lease_id = ? AND fence = ? AND state = 'running'`,
		update.Current, update.Total, message, lease.JobID, lease.LeaseID, lease.Fence)
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	var updatedAtMs int64
	if err := tx.QueryRowContext(ctx,
		`SELECT progress_updated_at_ms FROM headgate_job WHERE ulid = ?`, lease.JobID,
	).Scan(&updatedAtMs); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &headgate.JobProgress{
		Current: update.Current, Total: update.Total, Message: update.Message,
		Fence: lease.Fence, UpdatedAtMs: updatedAtMs,
	}, nil
}

func (s *MysqlStore) Renew(ctx context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return nil, errors.New("headgate: lease must be >= 1ms")
	}
	var lost []string
	for _, l := range leases {
		res, err := s.db.ExecContext(ctx,
			"UPDATE headgate_job SET lease_expires_at_ms = "+nowMS+" + ? WHERE "+ident,
			leaseMs, l.JobID, l.LeaseID, int64(l.Fence))
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			lost = append(lost, l.JobID)
		}
	}
	return lost, nil
}

func (s *MysqlStore) Checkpoint(ctx context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE headgate_job SET checkpoint = CAST(? AS JSON), cp_cursor = ? WHERE "+ident,
		encodeCheckpoint(cp), cp.Cursor, lease.JobID, lease.LeaseID, int64(lease.Fence))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

func (s *MysqlStore) Enqueue(ctx context.Context, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	// Validate before acquiring a connection. An invalid envelope remains a caller error
	// even when MySQL is unreachable; transport state must not change the taxonomy.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	scoped := append([]headgate.Envelope(nil), batch...)
	for i := range scoped {
		scoped[i].UniqueKey = headgate.EffectiveUniqueKey(scoped[i])
	}
	batch = scoped
	// tenant fairness/adaptive admission the rows and their active-partition entries must land together: a crash
	// between them would leave an available job whose partition is not listed, which is
	// starvation. (EnqueueTx already supplies the caller's own transaction.)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return headgate.WrapUnavailable(err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	if err := s.enqueueOn(ctx, tx, batch); err != nil {
		var duplicate *headgate.DuplicateError
		if errors.As(err, &duplicate) && duplicate.Replaced {
			if commitErr := tx.Commit(); commitErr != nil {
				return headgate.WrapUnavailable(commitErr)
			}
			return err
		}
		return headgate.WrapUnavailable(err)
	}
	return headgate.WrapUnavailable(tx.Commit())
}

// execer is what enqueue needs from either *sql.DB or *sql.Tx.
type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *MysqlStore) enqueueOn(ctx context.Context, c execer, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	// typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	// idempotent enqueue identity the strict caller-supplied id contract, classified BEFORE anything is written
	// so the batch stays all-or-nothing (and this whole function already runs inside a
	// transaction, plain or caller-supplied). Matching content drops out — idempotent
	// success, what makes the API's Idempotency-Key replay safe; different content
	// rejects the whole batch naming the offender. A terminal row still counts as
	// existing; reuse follows retention eviction.
	{
		ids := make([]any, len(batch))
		for i, e := range batch {
			ids[i] = e.ID
		}
		present := map[string][3]string{}
		rows, err := c.QueryContext(ctx,
			`SELECT ulid, kind, fingerprint, queue FROM headgate_job WHERE ulid IN (`+
				placeholders(len(batch))+`)`, ids...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, kind, fp, queue string
			if err := rows.Scan(&id, &kind, &fp, &queue); err != nil {
				_ = rows.Close()
				return err
			}
			present[id] = [3]string{kind, fp, queue}
		}
		_ = rows.Close()
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
	}
	// crash quarantine quarantined fingerprints are rejected before anything is written.
	var fps []any
	for _, e := range batch {
		if e.Fingerprint != "" {
			fps = append(fps, e.Fingerprint)
		}
	}
	if len(fps) > 0 {
		var fp string
		err := c.QueryRowContext(ctx,
			"SELECT fingerprint FROM headgate_quarantine WHERE fingerprint IN ("+
				placeholders(len(fps))+") LIMIT 1", fps...).Scan(&fp)
		if err == nil {
			return &headgate.QuarantinedError{Fingerprint: fp}
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	// Exact producer depth lives in two PK counter rows. Lock policy rows in sorted
	// order so concurrent multi-queue batches cannot overrun or deadlock. Idempotent
	// replays were removed above and therefore consume no slot.
	demand := map[string]int64{}
	for _, e := range batch {
		demand[headgate.EnqueueQueue(e)]++
	}
	demandQueues := make([]string, 0, len(demand))
	for queue := range demand {
		demandQueues = append(demandQueues, queue)
	}
	slices.Sort(demandQueues)
	policyArgs := make([]any, len(demandQueues))
	for i, queue := range demandQueues {
		policyArgs[i] = queue
	}
	if _, err := c.ExecContext(ctx,
		`INSERT INTO headgate_enqueue_policy (queue) VALUES `+
			strings.TrimSuffix(strings.Repeat("(?), ", len(demandQueues)), ", ")+`
		 AS new ON DUPLICATE KEY UPDATE queue = new.queue`, policyArgs...); err != nil {
		return err
	}
	// Materialize both counter rows before the locking read. A LEFT JOIN FOR UPDATE
	// against absent rows takes next-key gap locks; producers for different queues can
	// then each hold the same gap and deadlock when the INSERT trigger creates `entered`.
	// Sorted PK inserts make the query below take record locks instead.
	counterArgs := make([]any, 0, len(demandQueues)*2)
	for _, queue := range demandQueues {
		counterArgs = append(counterArgs, queue, queue)
	}
	if _, err := c.ExecContext(ctx,
		`INSERT IGNORE INTO headgate_enqueue_counter (queue, counter_kind, n) VALUES `+
			strings.TrimSuffix(strings.Repeat("(?, 'entered', 0), (?, 'exited', 0), ", len(demandQueues)), ", "),
		counterArgs...); err != nil {
		return err
	}
	// The sorted no-op upsert takes policy X locks immediately. The counter SELECT is
	// still a locking/current read over real records: a caller's REPEATABLE READ
	// transaction may have established its snapshot in the earlier ID pre-check.
	rows, err := c.QueryContext(ctx, enqueueBackpressureDepthSQL(len(demandQueues)), policyArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var queue string
		var limit sql.NullInt64
		var entered, exited int64
		if err := rows.Scan(&queue, &limit, &entered, &exited); err != nil {
			_ = rows.Close()
			return err
		}
		if limit.Valid {
			current := max(int64(0), entered-exited)
			incoming := demand[queue]
			if current+incoming > limit.Int64 {
				_ = rows.Close()
				return &headgate.BackpressureError{Queue: queue, Limit: uint64(limit.Int64), Current: uint64(current), Incoming: uint64(incoming)}
			}
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var now int64
	if err := c.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return err
	}
	rowSQL := `(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + nowMS + `,
	            ?, ?, ?, ?, ?, JSON_ARRAY(), ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?)`
	values := strings.TrimSuffix(strings.Repeat(rowSQL+", ", len(batch)), ", ")
	stmt := `INSERT INTO headgate_job
	           (ulid, kind, schema_version, payload, queue, partition_key, rate_class,
	            weight, fingerprint, priority, max_attempts, enqueued_at_ms,
	            scheduled_at_ms, timeout_ms, deadline_ms, retention_ms,
	            state, errors, unique_key, unique_states, unique_window_ms,
	            unique_expires_at_ms, headers, periodic_schedule_id, periodic_tick_ms,
	            sticky_worker) VALUES ` + values
	var args []any
	var candidates []any
	for _, e := range batch {
		sv := headgate.EffectiveSchemaVersion(e.SchemaVersion)
		queue := headgate.EnqueueQueue(e)
		ma := headgate.EffectiveMaxAttempts(e.MaxAttempts)
		var expires any
		if e.UniqueKey != nil && e.UniqueWindowMs > 0 {
			expires = now + e.UniqueWindowMs
		}
		scheduledAt := e.ScheduledAtMs
		if e.UniqueDebounceMs > 0 {
			scheduledAt = now + e.UniqueDebounceMs
		} else if scheduledAt == 0 {
			scheduledAt = now
		}
		state := "available"
		if e.Pending {
			state = "pending"
		} else if scheduledAt > now {
			state = "scheduled"
		}
		args = append(args,
			e.ID, e.Kind, sv, e.Payload, queue, e.PartitionKey, e.RateClass,
			headgate.EffectiveWeight(e.Weight), e.Fingerprint, e.Priority, ma,
			scheduledAt, e.TimeoutMs, e.DeadlineMs, e.RetentionMs, state,
			e.UniqueKey, e.UniqueStates, e.UniqueWindowMs, expires,
			// telemetry and trace context opaque headers, encoded and never interpreted .
			headersJSON(e.Headers), e.PeriodicScheduleID, e.PeriodicTickMs, e.StickyWorker)
		if e.UniqueKey != nil {
			candidates = append(candidates, e.UniqueKey)
		}
	}

	// Lock maintained route rows before inserting jobs. Pruning and reconciliation take
	// route -> job; the old job -> route producer order formed an InnoDB deadlock. A
	// harmless stale route can be pruned, while an available job without one can starve.
	routes := make([]string, 0, len(batch))
	for _, e := range batch {
		queue := headgate.EnqueueQueue(e)
		routes = append(routes, queue+"\x00"+e.PartitionKey)
	}
	slices.Sort(routes)
	routes = slices.Compact(routes)
	routeArgs := make([]any, 0, len(routes)*2)
	for _, route := range routes {
		queue, partition, _ := strings.Cut(route, "\x00")
		routeArgs = append(routeArgs, queue, partition)
	}
	routeValues := strings.TrimSuffix(strings.Repeat("(?, ?), ", len(routes)), ", ")
	if _, err := c.ExecContext(ctx,
		`INSERT INTO headgate_active_partition (queue, partition_key)
		 VALUES `+routeValues+`
		 ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, routeArgs...); err != nil {
		return err
	}
	inflightValues := strings.TrimSuffix(strings.Repeat("(?, ?, 0), ", len(routes)), ", ")
	if _, err := c.ExecContext(ctx,
		`INSERT INTO headgate_inflight (queue, partition_key, n)
		 VALUES `+inflightValues+`
		 AS new ON DUPLICATE KEY UPDATE n = headgate_inflight.n`, routeArgs...); err != nil {
		return err
	}

	for attempt := 0; attempt < 2; attempt++ {
		_, err := c.ExecContext(ctx, stmt, args...)
		if err == nil {
			for _, e := range batch {
				for _, tag := range headgate.CanonicalTags(e.Tags) {
					if _, err := c.ExecContext(ctx, `INSERT INTO headgate_job_tag(job_id,tag) SELECT id,? FROM headgate_job WHERE ulid=?`, tag, e.ID); err != nil {
						return err
					}
				}
			}
			// backlog metrics arrived counters, one upsert per distinct queue.
			perQueue := map[string]int64{}
			for _, e := range batch {
				queue := e.Queue
				if queue == "" {
					queue = "default"
				}
				perQueue[queue]++
			}
			for q, n := range perQueue {
				if _, err := c.ExecContext(ctx,
					`INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived)
					 VALUES (?, (`+nowMS+` DIV 60000) * 60000, ?) AS new
					 ON DUPLICATE KEY UPDATE arrived = headgate_queue_counter.arrived + new.arrived`,
					q, n); err != nil {
					return err
				}
			}
			perPartition := map[string]int64{}
			for _, e := range batch {
				queue := e.Queue
				if queue == "" {
					queue = "default"
				}
				perPartition[queue+"\x00"+e.PartitionKey]++
			}
			for route, n := range perPartition {
				queue, partition, _ := strings.Cut(route, "\x00")
				if _, err := c.ExecContext(ctx,
					`INSERT INTO headgate_partition_counter
					   (queue, partition_key, bucket_ms, arrived)
					 VALUES (?, ?, (`+nowMS+` DIV 60000) * 60000, ?) AS new
					 ON DUPLICATE KEY UPDATE arrived = headgate_partition_counter.arrived + new.arrived`,
					queue, partition, n); err != nil {
					return err
				}
			}
			return nil
		}
		if !isDupKey(err) {
			return err
		}
		// Throttle keys release LAZILY: the conflicting enqueue clears any holder
		// whose window has passed, then retries once.
		if attempt == 0 && len(candidates) > 0 {
			res, rerr := c.ExecContext(ctx,
				lazyUniqueReleaseSQL(len(candidates)), candidates...)
			if rerr == nil {
				if released, _ := res.RowsAffected(); released > 0 {
					continue
				}
			}
		}
		// job uniqueness one semantic: the duplicate is a normal result carrying the winner's
		// id — never a silent skip, never a bare constraint error.
		if len(candidates) > 0 {
			var existing string
			holderArgs := append(append([]any{}, candidates...), candidates...)
			err := c.QueryRowContext(ctx,
				uniqueHolderSQL(len(candidates)), holderArgs...).Scan(&existing)
			if err == nil {
				incoming := batch[0]
				replaced := false
				if incoming.UniqueDebounceMs > 0 {
					schemaVersion := incoming.SchemaVersion
					if schemaVersion == 0 {
						schemaVersion = 1
					}
					res, updateErr := c.ExecContext(ctx, `UPDATE headgate_job SET schema_version=?,payload=?,fingerprint=?,state='scheduled',scheduled_at_ms=`+nowMS+`+? WHERE ulid=? AND state IN ('pending','scheduled','available','retryable')`, schemaVersion, incoming.Payload, incoming.Fingerprint, incoming.UniqueDebounceMs, existing)
					if updateErr != nil {
						return updateErr
					}
					rowsAffected, _ := res.RowsAffected()
					replaced = rowsAffected > 0
					if replaced {
						if _, err := c.ExecContext(ctx, `DELETE FROM headgate_job_tag WHERE job_id=(SELECT id FROM headgate_job WHERE ulid=?)`, existing); err != nil {
							return err
						}
						for _, tag := range headgate.CanonicalTags(incoming.Tags) {
							if _, err := c.ExecContext(ctx, `INSERT INTO headgate_job_tag(job_id,tag) SELECT id,? FROM headgate_job WHERE ulid=?`, tag, existing); err != nil {
								return err
							}
						}
					}
				} else if incoming.UniqueReplace != 0 {
					schemaVersion := incoming.SchemaVersion
					if schemaVersion == 0 {
						schemaVersion = 1
					}
					maxAttempts := incoming.MaxAttempts
					if maxAttempts == 0 {
						maxAttempts = 25
					}
					res, updateErr := c.ExecContext(ctx, `UPDATE headgate_job SET
						schema_version = IF((? & ?) <> 0, ?, schema_version),
						payload = IF((? & ?) <> 0, ?, payload), fingerprint = IF((? & ?) <> 0, ?, fingerprint),
						scheduled_at_ms = IF((? & ?) <> 0 AND state = 'scheduled', IF(? = 0, `+nowMS+`, ?), scheduled_at_ms),
						priority = IF((? & ?) <> 0, ?, priority), max_attempts = IF((? & ?) <> 0, ?, max_attempts)
					  WHERE ulid = ? AND state IN ('scheduled','available','retryable')
					    AND ((? & ?) <> 0 OR (? & ?) <> 0 OR (? & ?) <> 0 OR ((? & ?) <> 0 AND state = 'scheduled'))`,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, schemaVersion,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, incoming.Payload,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, incoming.Fingerprint,
						incoming.UniqueReplace, headgate.UniqueReplaceScheduledAt, incoming.ScheduledAtMs, incoming.ScheduledAtMs,
						incoming.UniqueReplace, headgate.UniqueReplacePriority, incoming.Priority,
						incoming.UniqueReplace, headgate.UniqueReplaceMaxAttempts, maxAttempts, existing,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, incoming.UniqueReplace, headgate.UniqueReplacePriority,
						incoming.UniqueReplace, headgate.UniqueReplaceMaxAttempts, incoming.UniqueReplace, headgate.UniqueReplaceScheduledAt)
					if updateErr != nil {
						return updateErr
					}
					rowsAffected, _ := res.RowsAffected()
					replaced = rowsAffected > 0
				}
				return &headgate.DuplicateError{ExistingID: existing, Replaced: replaced}
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		// Not a uniqueness index — the ulid key collided. The pre-check above classified
		// every id this call knew about, so reaching here means a CONCURRENT producer
		// inserted the row between the read and the write; idempotent enqueue identity's answer is the same
		// typed conflict, naming the id.
		ids := make([]any, len(batch))
		for i, e := range batch {
			ids[i] = e.ID
		}
		var raced string
		if rerr := c.QueryRowContext(ctx,
			`SELECT ulid FROM headgate_job WHERE ulid IN (`+placeholders(len(batch))+
				`) LIMIT 1`, ids...).Scan(&raced); rerr != nil && !errors.Is(rerr, sql.ErrNoRows) {
			return rerr
		}
		return &headgate.IDConflictError{JobID: raced}
	}
	panic("enqueue retries at most once")
}

func (s *MysqlStore) ReclaimExpired(ctx context.Context, limit int64) ([]headgate.Reclaimed, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	out, err := s.reclaimTx(ctx, tx, limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *MysqlStore) reclaimTx(ctx context.Context, tx *sql.Tx, limit int64) ([]headgate.Reclaimed, error) {
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return nil, err
	}
	// lease fencing an expired lease is LeaseLost, NEVER Retry: crash_attempt++, attempt stays.
	rows, err := tx.QueryContext(ctx,
		`SELECT id, ulid, fingerprint, crash_attempt, kind, CAST(checkpoint AS CHAR)
		 FROM headgate_job
		 WHERE state = 'running' AND lease_expires_at_ms <= ?
		 ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, err
	}
	type victim struct {
		id     int64
		ulid   string
		fp     string
		ca     uint32
		kind   string
		cpJSON sql.NullString
	}
	var victims []victim
	for rows.Next() {
		var v victim
		if err := rows.Scan(&v.id, &v.ulid, &v.fp, &v.ca, &v.kind, &v.cpJSON); err != nil {
			_ = rows.Close()
			return nil, err
		}
		victims = append(victims, v)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]headgate.Reclaimed, 0, len(victims))
	for _, v := range victims {
		ca := v.ca + 1
		// crash quarantine step attribution: the checkpoint was durable BEFORE the in-progress
		// step's side effects; the crash lands on that step. Rows are locked, so a
		// read-modify-write here is safe.
		var newCP any
		if v.cpJSON.Valid && v.cpJSON.String != "" {
			var m map[string]any
			if json.Unmarshal([]byte(v.cpJSON.String), &m) == nil {
				if step, ok := m["in_progress"].(string); ok && step != "" {
					crashes, _ := m["crashes"].(map[string]any)
					if crashes == nil {
						crashes = map[string]any{}
					}
					n, _ := crashes[step].(float64)
					crashes[step] = n + 1
					m["crashes"] = crashes
					if b, err := json.Marshal(m); err == nil {
						newCP = string(b)
					}
				}
			}
		}
		quarantined := int64(ca) >= s.opts.CrashLimit
		// adaptive admission running -> retryable AND running -> quarantined. The reclaimer is the one
		// exit a crashed worker cannot take for itself, so it is also the one that MUST
		// decrement: without this a slot leaks for every process that ever died mid-job.
		// Before the transition — the join needs state = 'running'.
		if _, err := tx.ExecContext(ctx, inflightDecByID, v.id); err != nil {
			return nil, err
		}
		if quarantined {
			if _, err := tx.ExecContext(ctx,
				`UPDATE headgate_job SET
				   state = 'quarantined', crash_attempt = ?, finalized_at_ms = ?,
				   checkpoint = COALESCE(CAST(? AS JSON), checkpoint),
				   errors = JSON_ARRAY_APPEND(
				     CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
				          ELSE errors END,
				     '$', JSON_OBJECT('at_ms', ?, 'crash_attempt', ?,
				                      'outcome', 'lease_lost',
				                      'error', 'lease expired without ack')),
				   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
				 WHERE id = ?`, ca, now, newCP, now, ca, v.id); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO headgate_quarantine
				   (fingerprint, kind, crash_count, quarantined_at_ms, reason)
				 VALUES (?, ?, ?, ?, 'crash limit reached') AS new
				 ON DUPLICATE KEY UPDATE
				   crash_count = GREATEST(headgate_quarantine.crash_count, new.crash_count)`,
				v.fp, v.kind, ca, now); err != nil {
				return nil, err
			}
		} else {
			shift := int64(ca) - 1
			if shift > 20 {
				shift = 20
			}
			backoff := s.opts.RetryBaseMs << shift
			if backoff > s.opts.RetryCapMs {
				backoff = s.opts.RetryCapMs
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE headgate_job SET
				   state = 'retryable', crash_attempt = ?, scheduled_at_ms = ?,
				   checkpoint = COALESCE(CAST(? AS JSON), checkpoint),
				   errors = JSON_ARRAY_APPEND(
				     CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
				          ELSE errors END,
				     '$', JSON_OBJECT('at_ms', ?, 'crash_attempt', ?,
				                      'outcome', 'lease_lost',
				                      'error', 'lease expired without ack')),
				   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
				 WHERE id = ?`, ca, now+backoff, newCP, now, ca, v.id); err != nil {
				return nil, err
			}
		}
		out = append(out, headgate.Reclaimed{
			JobID: v.ulid, Fingerprint: v.fp, CrashAttempt: ca, Quarantined: quarantined,
		})
	}
	return out, nil
}

func (s *MysqlStore) PromoteDue(ctx context.Context, limit int64) (int64, error) {
	// tenant fairness/adaptive admission the ids are captured FIRST and the UPDATE is keyed by them, because the
	// partitions must be listed before the rows become available and MySQL cannot do both
	// in one statement. Two statements picking the due set independently could pick
	// different rows under READ COMMITTED — that gap is exactly the starvation direction,
	// so the id list is the contract between them.
	n, err := func() (int64, error) {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return 0, err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		rows, err := tx.QueryContext(ctx,
			`SELECT id FROM headgate_job
			 WHERE state IN ('scheduled', 'retryable') AND scheduled_at_ms <= `+nowMS+`
			 ORDER BY scheduled_at_ms, id LIMIT ?`, limit)
		if err != nil {
			return 0, err
		}
		var ids []any
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return 0, err
			}
			ids = append(ids, id)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return 0, err
		}
		if len(ids) == 0 {
			return 0, tx.Commit()
		}
		inList := placeholders(len(ids))
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO headgate_active_partition (queue, partition_key)
			 SELECT DISTINCT queue, partition_key FROM headgate_job WHERE id IN (`+inList+`)
			 ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, ids...); err != nil {
			return 0, err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE headgate_job SET state = 'available'
			 WHERE id IN (`+inList+`) AND state IN ('scheduled', 'retryable')`, ids...)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		return n, tx.Commit()
	}()
	if err != nil {
		return 0, err
	}
	// The counterpart duty: drop partitions that have drained. See pruneActivePartitions.
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
// sweep (adaptive admission). Every running → * edge decrements in the same transaction as the
// transition, so this should find nothing — it exists because "should" is not a
// guarantee. A future edge added without a decrement, an operator UPDATE run by hand, a
// restore from a backup taken mid-flight all drift the counter. Drift LOW admits past a
// ceiling for a while; drift HIGH chokes a partition against its ceiling permanently with
// no self-healing path, and that asymmetry is why the net is required, not optional.
//
// Bounded two ways: at most `limit` partitions per sweep, chosen least-recently-verified
// (headgate_inflight_stale), each one's truth a single index probe. FOR UPDATE SKIP
// LOCKED keeps concurrent sweepers and concurrent claims off each other. Returns how many
// rows were actually WRONG. Mirrors MysqlStore::reconcile_inflight in crates/headgate-mysql.
func (s *MysqlStore) reconcileInflight(ctx context.Context, limit int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return 0, err
	}
	type row struct {
		queue, partition string
		oldN             int64
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT queue, partition_key, n FROM headgate_inflight
		 ORDER BY reconciled_at_ms LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var due []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.queue, &r.partition, &r.oldN); err != nil {
			_ = rows.Close()
			return 0, err
		}
		due = append(due, r)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var wrong int64
	for _, r := range due {
		var truth int64
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM headgate_job
			 WHERE state = 'running' AND queue = ? AND partition_key = ?`,
			r.queue, r.partition).Scan(&truth); err != nil {
			return 0, err
		}
		if truth != r.oldN {
			wrong++
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE headgate_inflight SET n = ?, reconciled_at_ms = ?
			 WHERE queue = ? AND partition_key = ?`,
			truth, now, r.queue, r.partition); err != nil {
			return 0, err
		}
	}
	return wrong, tx.Commit()
}

// pruneActivePartitions drops active-partition rows whose partition has drained — the
// MySQL twin of the Postgres pruner. Two statements inside ONE READ COMMITTED
// transaction, and the order is load-bearing:
//
//  1. lock a bounded batch of candidate rows (FOR UPDATE SKIP LOCKED — never wait behind
//     a producer, never deadlock with a concurrent pruner);
//  2. in a SECOND statement, which under READ COMMITTED takes a FRESH read view, delete
//     only those with no available job left.
//
// One statement cannot do this: it would decide emptiness from a read view taken before
// the lock, so a producer that committed in between would be invisible and its job
// stranded — the one direction of staleness that is a correctness bug. With the split, a
// producer either committed before step 2's read view (we see its job and keep the row) or
// is still blocked on our row lock before it can insert its job (it re-inserts after we
// commit, because ON DUPLICATE KEY UPDATE re-attempts once the conflicting row is gone).
// Enqueue uses this same route -> job lock order.
func (s *MysqlStore) pruneActivePartitions(ctx context.Context, limit int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	rows, err := tx.QueryContext(ctx,
		`SELECT queue, partition_key FROM headgate_active_partition
		 ORDER BY queue, partition_key LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var args []any
	var pairs []string
	for rows.Next() {
		var q, p string
		if err := rows.Scan(&q, &p); err != nil {
			_ = rows.Close()
			return 0, err
		}
		args = append(args, q, p)
		pairs = append(pairs, "(?, ?)")
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(pairs) == 0 {
		return 0, tx.Commit()
	}
	res, err := tx.ExecContext(ctx,
		`DELETE ap FROM headgate_active_partition ap
		 WHERE (ap.queue, ap.partition_key) IN (`+strings.Join(pairs, ", ")+`)
		   AND NOT EXISTS (
		     SELECT 1 FROM headgate_job j
		     WHERE j.state = 'available'
		       AND j.queue = ap.queue AND j.partition_key = ap.partition_key)`, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

func (s *MysqlStore) EvictRetained(ctx context.Context, limit int64) (int64, error) {
	// retention and eviction contract quarantined is NOT here on purpose; retention 0 was deleted at ack.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM headgate_job
		 WHERE state IN ('completed', 'archived', 'cancelled', 'undecodable')
		   AND retention_ms > 0
		   AND finalized_at_ms + retention_ms <= `+nowMS+`
		 ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var ids []any
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, tx.Commit()
	}
	inList := placeholders(len(ids))
	_, err = tx.ExecContext(ctx,
		`INSERT INTO headgate_job_archive (
		   evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
		   fingerprint, attempt, crash_attempt, payload, errors, archive_retention_ms
		 )
		 SELECT `+nowMS+`, j.finalized_at_ms, j.ulid, j.kind, j.queue, j.state,
		        j.fingerprint, j.attempt, j.crash_attempt, j.payload, j.errors,
		        a.archive_retention_ms
		 FROM headgate_job j
		 JOIN headgate_archive_policy a ON a.queue = j.queue
		 WHERE j.id IN (`+inList+`)`, ids...)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM headgate_job WHERE id IN (`+inList+`)`, ids...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

func (s *MysqlStore) ClaimDuty(ctx context.Context, name, holder string, lease time.Duration) (bool, error) {
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return false, errors.New("headgate: duty lease must be >= 1ms")
	}
	// singleton duties the same compare-and-set as claiming a job, on store time.
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO headgate_duty (name, holder, expires_at_ms)
		 VALUES (?, ?, `+nowMS+` + ?) AS new
		 ON DUPLICATE KEY UPDATE
		   holder = IF(headgate_duty.expires_at_ms <= `+nowMS+`
		               OR headgate_duty.holder = new.holder, new.holder, headgate_duty.holder),
		   expires_at_ms = IF(headgate_duty.expires_at_ms <= `+nowMS+`
		                      OR headgate_duty.holder = new.holder,
		                      new.expires_at_ms, headgate_duty.expires_at_ms)`,
		name, holder, leaseMs); err != nil {
		return false, err
	}
	var ours string
	if err := s.db.QueryRowContext(ctx,
		"SELECT holder FROM headgate_duty WHERE name = ?", name).Scan(&ours); err != nil {
		return false, err
	}
	return ours == holder, nil
}

func (s *MysqlStore) ReleaseDuty(ctx context.Context, name, holder string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM headgate_duty WHERE name = ? AND holder = ?", name, holder)
	return err
}

func (s *MysqlStore) Caps() headgate.Caps {
	// runtime capability boundary/push wakeups: TRANSACTIONAL (InnoDB) | INSPECT — inspect.go ports the full
	// 30-method surface from crates/headgate-mysql/src/inspect.rs). NO Notifying, ever:
	// MySQL has no LISTEN/NOTIFY, so this store polls and its caps say so out loud
	// (invariant 5 — a capability whose scenarios cannot pass must not be declared).
	return headgate.CapTransactional | headgate.CapInspect
}

// headersJSON is telemetry and trace context's envelope headers as MySQL stores them: NULL for the header-less
// case, so a job with no headers writes exactly what it wrote before this existed.
func headersJSON(h map[string]string) any {
	if s := headgate.EncodeHeaders(h); s != "" {
		return s
	}
	return nil
}
