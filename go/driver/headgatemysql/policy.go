package headgatemysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

// SetQueuePaused changes whether admission may draw from queue.
func (s *MysqlStore) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_queue_state (queue, paused) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE paused = new.paused`, queue, paused)
	return err
}

// SetQueueWeight changes queue weight while preserving its scheduling position.
func (s *MysqlStore) SetQueueWeight(ctx context.Context, queue string, weight uint32) error {
	if weight == 0 {
		return &headgate.InvalidError{Msg: "weight must be >= 1"}
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_queue_state (queue, weight) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE
		  dispatch_count = FLOOR(headgate_queue_state.dispatch_count
		                         * new.weight / headgate_queue_state.weight),
		  weight = new.weight`, queue, weight)
	return err
}

// SetEnqueueLimit sets or clears a queue's unfinished-job limit.
func (s *MysqlStore) SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_enqueue_policy (queue, max_unfinished_jobs) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE max_unfinished_jobs = new.max_unfinished_jobs`, queue, maxUnfinishedJobs)
	return err
}

// RateClasses returns the configured rate classes and their current state.
func (s *MysqlStore) RateClasses(ctx context.Context) ([]headgate.RateClassState, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT b.name, b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((`+nowMS+` - b.refilled_at_ms) * b.limit_per_window DIV b.window_ms))
		            ELSE b.tokens END AS avail,
		       (SELECT COUNT(*) FROM (
		          SELECT 1 FROM headgate_job w
		          WHERE w.state = 'available' AND w.rate_class = b.name LIMIT ?
		       ) t) AS waiting
		FROM headgate_rate_bucket b ORDER BY b.name`, positionLimit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.RateClassState
	for rows.Next() {
		var r headgate.RateClassState
		if err := rows.Scan(&r.Name, &r.Burst, &r.LimitPerWindow, &r.WindowMs,
			&r.TokensAvailable, &r.JobsWaiting); err != nil {
			return nil, err
		}
		r.Paused = r.LimitPerWindow == 0 // the kill switch is limit 0 + empty bucket
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpsertRateClass creates or updates a rate class.
func (s *MysqlStore) UpsertRateClass(ctx context.Context, cfg headgate.RateClassConfig) error {
	if err := headgate.ValidateRateClassConfig(cfg); err != nil {
		return err
	}
	// Invariant 16 kill switch: paused = limit 0 AND tokens 0, refill adds nothing.
	limit, tokensInsert := cfg.Limit, cfg.Burst
	if cfg.Paused {
		limit, tokensInsert = 0, 0
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_rate_bucket
		       (name, tokens, burst, limit_per_window, window_ms, refilled_at_ms)
		VALUES (?, ?, ?, ?, ?, `+nowMS+`) AS new
		ON DUPLICATE KEY UPDATE
		  burst = new.burst, limit_per_window = new.limit_per_window,
		  window_ms = new.window_ms,
		  tokens = IF(?, 0, LEAST(headgate_rate_bucket.tokens, new.burst)),
		  refilled_at_ms = new.refilled_at_ms`,
		cfg.Name, tokensInsert, cfg.Burst, limit, cfg.WindowMs, cfg.Paused)
	return err
}

// ConcurrencyLimits returns all configured concurrency limits.
func (s *MysqlStore) ConcurrencyLimits(ctx context.Context) ([]headgate.ConcurrencyLimit, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, queue, max_concurrent, CAST(on_saturated AS CHAR)
		FROM headgate_concurrency_limit ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.ConcurrencyLimit
	for rows.Next() {
		var v headgate.ConcurrencyLimit
		if err := rows.Scan(&v.Name, &v.Queue, &v.MaxConcurrent, &v.OnSaturated); err != nil {
			return nil, err
		}
		if !v.OnSaturated.Valid() {
			return nil, fmt.Errorf("headgate: invalid saturation strategy `%s` in store", v.OnSaturated)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// UpsertConcurrencyLimit creates or updates a concurrency limit.
func (s *MysqlStore) UpsertConcurrencyLimit(ctx context.Context, cfg headgate.ConcurrencyLimit) error {
	if err := headgate.ValidateConcurrencyLimit(cfg); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_concurrency_limit
		       (name, queue, max_concurrent, on_saturated)
		VALUES (?, ?, ?, ?) AS new
		ON DUPLICATE KEY UPDATE
		  queue = new.queue,
		  max_concurrent = new.max_concurrent,
		  on_saturated = new.on_saturated`,
		cfg.Name, cfg.Queue, cfg.MaxConcurrent, cfg.OnSaturated)
	return err
}

// Partitions returns bounded fairness state for a queue's partitions.
func (s *MysqlStore) Partitions(ctx context.Context, queue string) ([]headgate.PartitionState, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.partition_key, COALESCE(d.deficit, 0), p.n
		FROM (SELECT partition_key, COUNT(*) AS n FROM
		        (SELECT partition_key FROM headgate_job
		         WHERE queue = ? AND state = 'available' LIMIT ?) s
		      GROUP BY 1) p
		LEFT JOIN headgate_partition_deficit d
		       ON d.queue = ? AND d.partition_key = p.partition_key
		UNION
		SELECT d.partition_key, d.deficit, 0
		FROM headgate_partition_deficit d
		WHERE d.queue = ?
		  AND d.partition_key NOT IN
		      (SELECT partition_key FROM
		         (SELECT DISTINCT partition_key FROM headgate_job
		          WHERE queue = ? AND state = 'available' LIMIT 1000) t)
		ORDER BY 1 LIMIT 10000`, queue, sampleLimit, queue, queue, queue)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.PartitionState
	for rows.Next() {
		var p headgate.PartitionState
		if err := rows.Scan(&p.PartitionKey, &p.Deficit, &p.Waiting); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// QuarantineList returns quarantined fingerprints newest first.
func (s *MysqlStore) QuarantineList(ctx context.Context) ([]headgate.QuarantineEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT fingerprint, kind, crash_count, quarantined_at_ms, reason
		FROM headgate_quarantine ORDER BY quarantined_at_ms DESC LIMIT ?`, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.QuarantineEntry
	for rows.Next() {
		var q headgate.QuarantineEntry
		var reason sql.NullString
		if err := rows.Scan(&q.Fingerprint, &q.Kind, &q.CrashCount, &q.QuarantinedAtMs,
			&reason); err != nil {
			return nil, err
		}
		q.Reason = reason.String
		out = append(out, q)
	}
	return out, rows.Err()
}

// QuarantineRelease releases a fingerprint and makes its jobs available.
func (s *MysqlStore) QuarantineRelease(ctx context.Context, fingerprint string) (uint64, error) {
	// one transaction: released rows become available, so their partitions
	// must be listed in the same commit. The INSERT reads the still-quarantined rows,
	// so it goes first (MySQL has no data-modifying CTEs to fuse the two).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO headgate_active_partition (queue, partition_key)
		SELECT DISTINCT queue, partition_key FROM headgate_job
		WHERE fingerprint = ? AND state = 'quarantined'
		ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, fingerprint); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE headgate_job SET state = 'available', scheduled_at_ms = `+nowMS+`,
		       finalized_at_ms = NULL
		WHERE fingerprint = ? AND state = 'quarantined'`, fingerprint)
	if err != nil {
		return 0, err
	}
	released, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	res, err = tx.ExecContext(ctx,
		`DELETE FROM headgate_quarantine WHERE fingerprint = ?`, fingerprint)
	if err != nil {
		return 0, err
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if released == 0 && deleted == 0 {
		// `not found: ` is load-bearing — see the note in headgatepgx/inspect.go.
		return 0, headgate.NotFoundf("fingerprint %s is not quarantined", fingerprint)
	}
	return uint64(released), nil
}

func (s *MysqlStore) jobState(ctx context.Context, id string) (string, bool, error) {
	var st string
	err := s.db.QueryRowContext(ctx,
		`SELECT CAST(state AS CHAR) FROM headgate_job WHERE ulid = ?`, id).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return st, true, nil
}

// OperatorRetry makes a retryable terminal job available immediately.
func (s *MysqlStore) OperatorRetry(ctx context.Context, id string) error {
	// same commit as the transition that makes the row available.
	retried, err := func() (int64, error) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO headgate_active_partition (queue, partition_key)
			SELECT queue, partition_key FROM headgate_job
			WHERE ulid = ? AND state IN ('archived', 'cancelled', 'undecodable')
			ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, id); err != nil {
			return 0, err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE headgate_job SET state = 'available', scheduled_at_ms = `+nowMS+`,
			       finalized_at_ms = NULL
			WHERE ulid = ? AND state IN ('archived', 'cancelled', 'undecodable')`, id)
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
		return err
	}
	if retried == 1 {
		return nil
	}
	st, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return headgate.Invalidf("operator_retry is only defined from archived, cancelled, or undecodable; job %s is %s", id, st)
}

// OperatorCancel moves a nonterminal job to the cancelled state.
func (s *MysqlStore) OperatorCancel(ctx context.Context, id string) error {
	// cancelling a RUNNING job releases its slot; cancelling a scheduled or
	// available one must not decrement a slot it never took. The decrement therefore
	// carries `state = 'running'` in its own guard and runs FIRST, while that is still
	// true — after the UPDATE the row is 'cancelled' and unjoinable. (Postgres reads
	// was_running out of a locking pick CTE; MySQL has neither, so ORDER is the fence.)
	n, err := func() (int64, error) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if _, err := tx.ExecContext(ctx, `
			UPDATE headgate_inflight f
			  JOIN headgate_job j ON j.queue = f.queue AND j.partition_key = f.partition_key
			   SET f.n = GREATEST(0, f.n - 1)
			 WHERE j.ulid = ? AND j.state = 'running'`, id); err != nil {
			return 0, err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE headgate_job SET state = 'cancelled', lease_id = NULL,
			       lease_expires_at_ms = NULL, claimed_by = NULL,
			       finalized_at_ms = `+nowMS+`
			WHERE ulid = ? AND state IN ('pending', 'scheduled', 'available', 'running', 'retryable')`, id)
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
		return err
	}
	if n == 1 {
		return nil
	}
	st, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return headgate.Invalidf("operator_cancel is not defined from %s", st)
}

// DeleteJob permanently removes a job that is not running.
func (s *MysqlStore) DeleteJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM headgate_job WHERE ulid = ? AND state <> 'running'`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	_, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return &headgate.InvalidError{Msg: "cannot delete a running job; cancel it first"}
}

// ExplainAdmission replays the gate's own clause order read-only for one job — the
// admission policy/control plane endpoint only this design needs. Same query and assembly as the Rust MySQL
// adapter's explain_admission, whose clause order is in turn the Postgres one (the two
// SQL gates share it).
func (s *MysqlStore) ExplainAdmission(ctx context.Context, id string) (*headgate.AdmissionExplain, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT CAST(j.state AS CHAR) AS state, j.queue, j.scheduled_at_ms, j.priority,
		       j.rate_class, j.partition_key, j.fingerprint, j.id,
		       CAST(j.weight AS SIGNED) AS weight,
		       `+nowMS+` AS now_ms,
		       COALESCE(qs.paused, FALSE) AS paused,
		       (q.fingerprint IS NOT NULL) AS quarantined,
		       b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.name IS NULL THEN NULL
		            WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((`+nowMS+` - b.refilled_at_ms) * b.limit_per_window DIV b.window_ms))
		            ELSE b.tokens END AS avail,
		       COALESCE(d.deficit, 0) AS deficit,
		       cl.max_concurrent, CAST(cl.on_saturated AS CHAR) AS on_saturated,
		       -- read the counter the GATE reads, not a fresh count of running
		       -- rows. Why-is-this-job-not-running must answer for the gate that is
		       -- actually deciding: if headgate_inflight ever drifts, an explain that
		       -- quietly recomputed the truth would report a ceiling as clear while
		       -- admission kept refusing -- the one failure this endpoint exists to
		       -- make visible. Also O(1) instead of O(running).
		       COALESCE((SELECT f.n FROM headgate_inflight f
		                 WHERE f.queue = j.queue
		                   AND f.partition_key = j.partition_key), 0) AS inflight,
		       (SELECT CAST(COALESCE(SUM(t.weight), 0) AS SIGNED) FROM (
		          SELECT a.weight FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.rate_class = j.rate_class
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms < j.scheduled_at_ms
		                          OR (a.scheduled_at_ms = j.scheduled_at_ms AND a.id < j.id))))
		          ORDER BY a.priority DESC, a.scheduled_at_ms, a.id
		          LIMIT ?
		       ) t) AS cost_ahead_in_class,
		       (SELECT COUNT(*) FROM (
		          SELECT 1 FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.partition_key = j.partition_key
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms < j.scheduled_at_ms
		                          OR (a.scheduled_at_ms = j.scheduled_at_ms AND a.id < j.id))))
		          LIMIT ?
		       ) t) AS ahead_in_partition
		FROM headgate_job j
		LEFT JOIN headgate_queue_state qs ON qs.queue = j.queue
		LEFT JOIN headgate_quarantine q ON q.fingerprint = j.fingerprint
		LEFT JOIN headgate_rate_bucket b ON b.name = j.rate_class AND j.rate_class <> ''
		LEFT JOIN headgate_partition_deficit d
		       ON d.queue = j.queue AND d.partition_key = j.partition_key
		LEFT JOIN headgate_concurrency_limit cl ON cl.queue = j.queue
		WHERE j.ulid = ?`, positionLimit, positionLimit, id)

	var (
		state, queue, rateClass, partitionKey, fingerprint string
		scheduledAt, nowMs, deficit, inflight, weight      int64
		aheadInClass, aheadInPartition, internalID         int64
		priority                                           int32
		paused, quarantined                                bool
		burst, limitPerWindow, windowMs, avail, maxConc    sql.NullInt64
		onSaturated                                        sql.NullString
	)
	err := row.Scan(&state, &queue, &scheduledAt, &priority, &rateClass, &partitionKey,
		&fingerprint, &internalID, &weight, &nowMs, &paused, &quarantined,
		&burst, &limitPerWindow, &windowMs, &avail, &deficit, &maxConc, &onSaturated,
		&inflight, &aheadInClass, &aheadInPartition)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	nullableInt := func(value sql.NullInt64) *int64 {
		if !value.Valid {
			return nil
		}
		integer := value.Int64
		return &integer
	}
	strategy := ""
	if onSaturated.Valid {
		strategy = onSaturated.String
	}
	return headgate.EvaluateAdmission(headgateshared.AdmissionFacts{
		State: state, NowMs: nowMs, ScheduledAtMs: scheduledAt,
		QueuePaused: paused, Quarantined: quarantined, Fingerprint: fingerprint,
		RateClass: rateClass, Weight: weight, TokensAvailable: nullableInt(avail),
		TokensAhead: aheadInClass, LimitPerWindow: limitPerWindow.Int64,
		WindowMs: windowMs.Int64, MaxConcurrent: nullableInt(maxConc), Inflight: inflight,
		Saturation: strategy, Position: aheadInPartition, Deficit: deficit,
	}), nil
}

// History returns queue traffic aggregated into fixed time buckets.
func (s *MysqlStore) History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]headgate.HistoryBucket, error) {
	if bucketMs < 60_000 {
		return nil, &headgate.InvalidError{Msg: "bucket_ms must be >= 60000 (the stored granularity)"}
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT (bucket_ms DIV ?) * ? AS at_ms,
		       CAST(SUM(arrived) AS SIGNED), CAST(SUM(completed) AS SIGNED)
		FROM headgate_queue_counter
		WHERE queue = ? AND bucket_ms >= ?
		GROUP BY 1 ORDER BY 1 LIMIT 10000`, bucketMs, bucketMs, queue, sinceMs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.HistoryBucket
	for rows.Next() {
		var b headgate.HistoryBucket
		if err := rows.Scan(&b.AtMs, &b.Arrived, &b.Completed); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// QuarantineSweep moves eligible jobs with quarantined fingerprints into quarantine.
func (s *MysqlStore) QuarantineSweep(ctx context.Context, limit int64) (int64, error) {
	// crash quarantine quarantined is TERMINAL and VISIBLE; the generated column releases any
	// lifecycle unique key these jobs held. (MySQL cannot self-join the updated table
	// in a subquery; the join form sidesteps ER_UPDATE_TABLE_USED.)
	res, err := s.db.ExecContext(ctx, `
		UPDATE headgate_job j
		JOIN (SELECT id FROM headgate_job
		      WHERE state IN ('pending', 'available', 'scheduled', 'retryable')
		        AND fingerprint IN (SELECT fingerprint FROM headgate_quarantine)
		      LIMIT ?) pick ON pick.id = j.id
		SET j.state = 'quarantined', j.finalized_at_ms = `+nowMS, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// RescheduleJob changes when a scheduled or retryable job becomes due.
func (s *MysqlStore) RescheduleJob(ctx context.Context, id string, atMs int64) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE headgate_job SET scheduled_at_ms = ?
		WHERE ulid = ? AND state IN ('scheduled', 'retryable')`, atMs, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	st, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return headgate.Invalidf("reschedule is only defined for scheduled/retryable; job %s is %s", id, st)
}

// EditPayload replaces the payload of a job that is safe to edit.
func (s *MysqlStore) EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE headgate_job SET payload = ?, schema_version = ?, fingerprint = ?
		WHERE ulid = ? AND state <> 'running'`,
		payload, int64(schemaVersion), fingerprint, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	_, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return &headgate.InvalidError{Msg: "cannot edit a running job's payload"}
}
