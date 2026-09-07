package headgatepgx

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

// SetQueuePaused changes whether admission may draw from queue.
func (s *PgxStore) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_queue_state (queue, paused) VALUES ($1, $2)
		ON CONFLICT (queue) DO UPDATE SET paused = EXCLUDED.paused`, queue, paused)
	return err
}

// SetQueueWeight changes queue weight while preserving its scheduling position.
func (s *PgxStore) SetQueueWeight(ctx context.Context, queue string, weight uint32) error {
	if weight == 0 {
		return &headgate.InvalidError{Msg: "weight must be >= 1"}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_queue_state (queue, weight) VALUES ($1, $2)
		ON CONFLICT (queue) DO UPDATE SET
		  dispatch_count = floor(headgate_queue_state.dispatch_count::numeric
		                         * EXCLUDED.weight / headgate_queue_state.weight)::bigint,
		  weight = EXCLUDED.weight`, queue, weight)
	return err
}

// SetEnqueueLimit sets or clears a queue's unfinished-job limit.
func (s *PgxStore) SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error {
	var limit any
	if maxUnfinishedJobs != nil {
		if *maxUnfinishedJobs > uint64(^uint64(0)>>1) {
			return &headgate.InvalidError{Msg: "max_unfinished_jobs is too large"}
		}
		limit = int64(*maxUnfinishedJobs)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_enqueue_policy (queue, max_unfinished_jobs) VALUES ($1, $2)
		ON CONFLICT (queue) DO UPDATE SET max_unfinished_jobs = EXCLUDED.max_unfinished_jobs`, queue, limit)
	return err
}

// RateClasses returns the configured rate classes and their current state.
func (s *PgxStore) RateClasses(ctx context.Context) ([]headgate.RateClassState, error) {
	rows, err := s.pool.Query(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms)
		SELECT b.name, b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((p.now_ms - b.refilled_at_ms) * b.limit_per_window / b.window_ms))
		            ELSE b.tokens END AS avail,
		       (SELECT count(*) FROM (
		          SELECT 1 FROM headgate_job w
		          WHERE w.state = 'available' AND w.rate_class = b.name LIMIT $1
		       ) t)::bigint AS waiting
		FROM headgate_rate_bucket b, p
		ORDER BY b.name`, positionLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
func (s *PgxStore) UpsertRateClass(ctx context.Context, cfg headgate.RateClassConfig) error {
	if err := headgate.ValidateRateClassConfig(cfg); err != nil {
		return err
	}
	limit, tokensInsert := cfg.Limit, cfg.Burst
	if cfg.Paused {
		limit, tokensInsert = 0, 0
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_rate_bucket
		       (name, tokens, burst, limit_per_window, window_ms, refilled_at_ms)
		SELECT $1, $2, $3, $4, $5, `+nowMS+`
		ON CONFLICT (name) DO UPDATE SET
		  burst = EXCLUDED.burst,
		  limit_per_window = EXCLUDED.limit_per_window,
		  window_ms = EXCLUDED.window_ms,
		  tokens = CASE WHEN $6 THEN 0
		                ELSE LEAST(headgate_rate_bucket.tokens, EXCLUDED.burst) END,
		  refilled_at_ms = EXCLUDED.refilled_at_ms`,
		cfg.Name, tokensInsert, cfg.Burst, limit, cfg.WindowMs, cfg.Paused)
	return err
}

// ConcurrencyLimits returns all configured concurrency limits.
func (s *PgxStore) ConcurrencyLimits(ctx context.Context) ([]headgate.ConcurrencyLimit, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, queue, max_concurrent, on_saturated
		FROM headgate_concurrency_limit ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
func (s *PgxStore) UpsertConcurrencyLimit(ctx context.Context, cfg headgate.ConcurrencyLimit) error {
	if err := headgate.ValidateConcurrencyLimit(cfg); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_concurrency_limit
		       (name, queue, max_concurrent, on_saturated)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE SET
		  queue = EXCLUDED.queue,
		  max_concurrent = EXCLUDED.max_concurrent,
		  on_saturated = EXCLUDED.on_saturated`,
		cfg.Name, cfg.Queue, cfg.MaxConcurrent, cfg.OnSaturated)
	return err
}

// Partitions returns bounded fairness state for a queue's partitions.
func (s *PgxStore) Partitions(ctx context.Context, queue string) ([]headgate.PartitionState, error) {
	rows, err := s.pool.Query(ctx, `
		WITH sample AS (
		  SELECT partition_key FROM headgate_job
		  WHERE queue = $1 AND state = 'available' LIMIT $2
		),
		waiting AS (
		  SELECT partition_key, count(*)::bigint AS n FROM sample GROUP BY 1
		)
		SELECT COALESCE(w.partition_key, d.partition_key) AS partition_key,
		       COALESCE(d.deficit, 0) AS deficit,
		       COALESCE(w.n, 0) AS waiting
		FROM waiting w
		FULL OUTER JOIN headgate_partition_deficit d
		  ON d.queue = $1 AND d.partition_key = w.partition_key
		WHERE d.queue IS NULL OR d.queue = $1
		ORDER BY 1 LIMIT 10000`, queue, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
func (s *PgxStore) QuarantineList(ctx context.Context) ([]headgate.QuarantineEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT fingerprint, kind, crash_count, quarantined_at_ms,
		       COALESCE(reason, '') AS reason
		FROM headgate_quarantine ORDER BY quarantined_at_ms DESC LIMIT $1`, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.QuarantineEntry
	for rows.Next() {
		var q headgate.QuarantineEntry
		var crash int32
		if err := rows.Scan(&q.Fingerprint, &q.Kind, &crash, &q.QuarantinedAtMs, &q.Reason); err != nil {
			return nil, err
		}
		q.CrashCount = int64(crash)
		out = append(out, q)
	}
	return out, rows.Err()
}

// QuarantineRelease releases a fingerprint and makes its jobs available.
func (s *PgxStore) QuarantineRelease(ctx context.Context, fingerprint string) (uint64, error) {
	var released, deleted int64
	err := s.pool.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms),
		rel AS ( -- quarantined + operator_release -> available (the table's row)
		  UPDATE headgate_job j SET state = 'available', scheduled_at_ms = p.now_ms,
		         finalized_at_ms = NULL
		  FROM p WHERE j.fingerprint = $1 AND j.state = 'quarantined'
		  RETURNING j.queue, j.partition_key
		),
		-- released jobs are available again, so their partitions rejoin the
		-- gate's set — in this statement, never a follow-up one.
		active AS (
		  INSERT INTO headgate_active_partition (queue, partition_key)
		  SELECT DISTINCT queue, partition_key FROM rel
		  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
		),
		del AS (
		  DELETE FROM headgate_quarantine WHERE fingerprint = $1 RETURNING 1
		)
		SELECT (SELECT count(*) FROM rel)::bigint, (SELECT count(*) FROM del)::bigint`,
		fingerprint).Scan(&released, &deleted)
	if err != nil {
		return 0, err
	}
	if released == 0 && deleted == 0 {
		// The `not found: ` prefix is REQUIRED, not decoration: Go has no NotFoundError
		// type, so headgateapi.storeErr classifies by exactly this prefix and everything
		// without it falls through to 400. Rust reaches the identical bytes through
		// StoreError::NotFound's Display. Omitting it here made this route a 400 in Go
		// and a 404 in Rust for four rounds, uncaught because no diff covered the path.
		return 0, headgate.NotFoundf("fingerprint %s is not quarantined", fingerprint)
	}
	return uint64(released), nil
}

func (s *PgxStore) jobState(ctx context.Context, id string) (string, bool, error) {
	var st string
	err := s.pool.QueryRow(ctx, `SELECT state::text FROM headgate_job WHERE ulid = $1`, id).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return st, err == nil, err
}

// OperatorRetry makes a retryable terminal job available immediately.
func (s *PgxStore) OperatorRetry(ctx context.Context, id string) error {
	var retried int64
	err := s.pool.QueryRow(ctx, `
		WITH upd AS (
		  UPDATE headgate_job SET state = 'available', scheduled_at_ms = `+nowMS+`,
		         finalized_at_ms = NULL
		  WHERE ulid = $1 AND state IN ('archived', 'cancelled', 'undecodable')
		  RETURNING queue, partition_key
		),
		-- retry-now makes the row available; list its partition here.
		active AS (
		  INSERT INTO headgate_active_partition (queue, partition_key)
		  SELECT queue, partition_key FROM upd
		  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
		)
		SELECT count(*)::bigint FROM upd`, id).Scan(&retried)
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
func (s *PgxStore) OperatorCancel(ctx context.Context, id string) error {
	// the decrement must key off the PRE-update state, and an UPDATE's RETURNING
	// reports the NEW row — by then state is 'cancelled' and lease_id is NULL, so "was it
	// running?" is unanswerable from there. The row is picked and locked first, in its own
	// CTE, and was_running is read off THAT. Cancelling a scheduled or available job must
	// not decrement a slot it never took.
	var n int64
	err := s.pool.QueryRow(ctx, `
		WITH pick AS (
		  SELECT j.id, j.queue, j.partition_key, (j.state = 'running') AS was_running
		  FROM headgate_job j
		  WHERE j.ulid = $1 AND j.state IN ('pending', 'scheduled', 'available', 'running', 'retryable')
		  FOR UPDATE
		),
		upd AS (
		  UPDATE headgate_job j SET state = 'cancelled', lease_id = NULL,
		         lease_expires_at_ms = NULL, claimed_by = NULL,
		         finalized_at_ms = `+nowMS+`
		  WHERE j.id IN (SELECT id FROM pick)
		  RETURNING 1
		),
		infl AS (`+inflightDec("(SELECT queue, partition_key FROM pick WHERE was_running)")+`)
		SELECT count(*)::bigint FROM upd`, id).Scan(&n)
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
func (s *PgxStore) DeleteJob(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM headgate_job WHERE ulid = $1 AND state <> 'running'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
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

// History returns queue traffic aggregated into fixed time buckets.
func (s *PgxStore) History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]headgate.HistoryBucket, error) {
	if bucketMs < 60_000 {
		return nil, &headgate.InvalidError{Msg: "bucket_ms must be >= 60000 (the stored granularity)"}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT (bucket_ms / $2) * $2 AS at_ms,
		       sum(arrived)::bigint AS arrived, sum(completed)::bigint AS completed
		FROM headgate_queue_counter
		WHERE queue = $1 AND bucket_ms >= $3
		GROUP BY 1 ORDER BY 1 LIMIT 10000`, queue, bucketMs, sinceMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
func (s *PgxStore) QuarantineSweep(ctx context.Context, limit int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH pick AS (
		  SELECT j.id FROM headgate_job j
		  WHERE j.state IN ('pending', 'available', 'scheduled', 'retryable')
		    AND j.fingerprint IN (SELECT fingerprint FROM headgate_quarantine)
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		)
		UPDATE headgate_job j
		SET state = 'quarantined', finalized_at_ms = `+nowMS+`
		WHERE j.id IN (SELECT id FROM pick)`, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// RescheduleJob changes when a scheduled or retryable job becomes due.
func (s *PgxStore) RescheduleJob(ctx context.Context, id string, atMs int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE headgate_job SET scheduled_at_ms = $2
		WHERE ulid = $1 AND state IN ('scheduled', 'retryable')`, id, atMs)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
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
func (s *PgxStore) EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE headgate_job
		SET payload = $2, schema_version = $3, fingerprint = $4
		WHERE ulid = $1 AND state <> 'running'`,
		id, payload, int32(schemaVersion), fingerprint)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
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

// ExplainAdmission replays the gate's own clause order read-only for one job — the
// admission policy/control plane endpoint only this design needs. Same query and assembly as the Rust
// adapter's explain_admission.
func (s *PgxStore) ExplainAdmission(ctx context.Context, id string) (*headgate.AdmissionExplain, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT j.state::text AS state, j.queue, j.scheduled_at_ms, j.priority,
		       j.rate_class, j.partition_key, j.fingerprint, j.id,
		       j.weight::bigint AS weight,
		       `+nowMS+` AS now_ms,
		       COALESCE(qs.paused, false) AS paused,
		       (q.fingerprint IS NOT NULL) AS quarantined,
		       b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.name IS NULL THEN NULL
		            WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((`+nowMS+` - b.refilled_at_ms) * b.limit_per_window / b.window_ms))
		            ELSE b.tokens END AS avail,
		       COALESCE(d.deficit, 0) AS deficit,
		       cl.max_concurrent, cl.on_saturated,
		       -- read the counter the GATE reads, not a fresh count of running
		       -- rows. "Why is this job not running" must answer for the gate that is
		       -- actually deciding: if headgate_inflight ever drifts, an explain that
		       -- quietly recomputed the truth would report a ceiling as clear while
		       -- admission kept refusing — the one failure this endpoint exists to make
		       -- visible. Also O(1) instead of O(running).
		       COALESCE((SELECT f.n FROM headgate_inflight f
		                 WHERE f.queue = j.queue
		                   AND f.partition_key = j.partition_key), 0) AS inflight,
		       (SELECT COALESCE(sum(t.weight), 0)::bigint FROM (
		          SELECT a.weight FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.rate_class = j.rate_class
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms, a.id) < (j.scheduled_at_ms, j.id)))
		          ORDER BY a.priority DESC, a.scheduled_at_ms, a.id
		          LIMIT $2
		       ) t) AS cost_ahead_in_class,
		       (SELECT count(*) FROM (
		          SELECT 1 FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.partition_key = j.partition_key
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms, a.id) < (j.scheduled_at_ms, j.id)))
		          LIMIT $2
		       ) t)::bigint AS ahead_in_partition
		FROM headgate_job j
		LEFT JOIN headgate_queue_state qs ON qs.queue = j.queue
		LEFT JOIN headgate_quarantine q ON q.fingerprint = j.fingerprint
		LEFT JOIN headgate_rate_bucket b ON b.name = j.rate_class AND j.rate_class <> ''
		LEFT JOIN headgate_partition_deficit d
		       ON d.queue = j.queue AND d.partition_key = j.partition_key
		LEFT JOIN headgate_concurrency_limit cl ON cl.queue = j.queue
		WHERE j.ulid = $1`, id, positionLimit)

	var (
		state, queue, rateClass, partitionKey, fingerprint string
		scheduledAt, nowMs, deficit, inflight, weight      int64
		aheadInClass, aheadInPartition, internalID         int64
		priority                                           int32
		paused, quarantined                                bool
		burst, limitPerWindow, windowMs, avail, maxConc    *int64
		onSaturated                                        *string
	)
	err := row.Scan(&state, &queue, &scheduledAt, &priority, &rateClass, &partitionKey,
		&fingerprint, &internalID, &weight, &nowMs, &paused, &quarantined,
		&burst, &limitPerWindow, &windowMs, &avail, &deficit, &maxConc, &onSaturated,
		&inflight, &aheadInClass, &aheadInPartition)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	valueOrZero := func(value *int64) int64 {
		if value == nil {
			return 0
		}
		return *value
	}
	strategy := ""
	if onSaturated != nil {
		strategy = *onSaturated
	}
	return headgate.EvaluateAdmission(headgateshared.AdmissionFacts{
		State: state, NowMs: nowMs, ScheduledAtMs: scheduledAt,
		QueuePaused: paused, Quarantined: quarantined, Fingerprint: fingerprint,
		RateClass: rateClass, Weight: weight, TokensAvailable: avail,
		TokensAhead: aheadInClass, LimitPerWindow: valueOrZero(limitPerWindow),
		WindowMs: valueOrZero(windowMs), MaxConcurrent: maxConc, Inflight: inflight,
		Saturation: strategy, Position: aheadInPartition, Deficit: deficit,
	}), nil
}
