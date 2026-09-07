package headgatemysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
)

// ReclaimExpired makes jobs with expired leases eligible for admission again.
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
		// running -> retryable AND running -> quarantined. The reclaimer is the one
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

// PromoteDue makes due scheduled and retryable jobs available.
func (s *MysqlStore) PromoteDue(ctx context.Context, limit int64) (int64, error) {
	// the ids are captured FIRST and the UPDATE is keyed by them, because the
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
	// the inflight counter's safety net, on the duty that already sweeps.
	if _, err := s.reconcileInflight(ctx, limit); err != nil {
		return 0, err
	}
	return n, nil
}

// reconcileInflight recomputes headgate_inflight against the source rows in bounded
// batches. Every running → * edge decrements in the same transaction as the
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

// EvictRetained removes terminal jobs whose retention deadline has elapsed.
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

// ClaimDuty acquires or renews a singleton duty lease for holder.
