package headgatepgx

import (
	"context"

	headgate "github.com/mujhtech/headgate/go"
)

// ---------- sweeps ----------

// ReclaimExpired makes jobs with expired leases eligible for admission again.
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
		-- running -> retryable AND running -> quarantined. The reclaimer is the one
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

// PromoteDue makes due scheduled and retryable jobs available.
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
		-- same statement, same transaction: a row cannot become available
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
	// the inflight counter's safety net, on the duty that already sweeps.
	if _, err := s.reconcileInflight(ctx, limit); err != nil {
		return 0, err
	}
	return n, nil
}

// reconcileInflight recomputes headgate_inflight against the source rows in bounded
// batches. Every running → * edge decrements in the same statement as the transition,
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

// EvictRetained removes terminal jobs whose retention deadline has elapsed.
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
