package headgatepgx

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- ack: the transition table (lifecycle state machine), same arms as the Rust adapter ----------

// Ack records an attempt outcome if lease still identifies the current holder.
func (s *PgxStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return s.AckAttempt(ctx, lease, outcome, errMsg, delayMs, nil)
}

// AckAttempt records an attempt outcome and its buffered logs.
func (s *PgxStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return s.ackAttemptOn(ctx, s.pool, lease, outcome, errMsg, delayMs, logs, nil)
}

// AckAttemptWithActualWeight records an outcome and reconciles estimated admission cost.
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
	if err := headgate.ValidateAckRequest(outcome, delayMs); err != nil {
		return err
	}
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
		logsJSON = headgateshared.EncodeStringList(logs)
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
			-- running -> completed AND running -> deleted, both arms
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
			-- running -> retryable AND running -> archived, both arms
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
			 -- running -> deleted
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
			-- running -> scheduled
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
			-- requeue puts the partition back in the gate's set, in the
			-- same statement that makes the row available.
			active AS (
			  INSERT INTO headgate_active_partition (queue, partition_key)
			  SELECT queue, partition_key FROM upd
			  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
			),
			-- running -> available (not a failure, but it does leave running)
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

// AckSuccessWithResult completes a leased job and stores its result atomically.
func (s *PgxStore) AckSuccessWithResult(ctx context.Context, lease headgate.LeaseRef, logs []string, actualWeight *uint32, result headgate.JobResult) error {
	if err := headgate.ValidateOpaqueValue("result", result); err != nil {
		return err
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

// WriteJobOutput replaces the durable output for the currently leased job.
func (s *PgxStore) WriteJobOutput(
	ctx context.Context,
	lease headgate.LeaseRef,
	output headgate.JobResult,
) (*headgate.JobOutput, error) {
	if err := headgate.ValidateOpaqueValue("output", output); err != nil {
		return nil, err
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

// WriteJobProgress records progress for the currently leased job.
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
		-- running -> archived / undecodable
		infl AS (`+inflightDec("upd")+`)
		SELECT count(*)::bigint FROM upd`,
		lease.JobID, lease.LeaseID, int64(lease.Fence), msg, logsJSON).Scan(n)
}
