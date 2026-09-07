package headgatemysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

// Ack records an attempt outcome if lease still identifies the current holder.
func (s *MysqlStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return s.AckAttempt(ctx, lease, outcome, errMsg, delayMs, nil)
}

// AckAttempt records an attempt outcome and its buffered logs.
func (s *MysqlStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return s.AckAttemptWithActualWeight(ctx, lease, outcome, errMsg, delayMs, logs, nil)
}

// AckAttemptWithActualWeight records an outcome and reconciles estimated admission cost.
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
		// running -> retryable AND running -> archived; both leave running, so both
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
		// running -> archived / undecodable
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
		// running -> deleted. The row is GONE after this, so the decrement must
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
		// running -> scheduled
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
		// MySQL has no data-modifying CTEs, so the partition is listed by a
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
		// running -> available. Not a failure, but it does leave running, so the
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
	// running -> completed AND running -> deleted. One decrement covers both arms:
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

// AckSuccessWithResult completes a leased job and stores its result atomically.
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

// WriteJobOutput replaces the durable output for the currently leased job.
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

// WriteJobProgress records progress for the currently leased job.
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

// Renew extends current leases and returns IDs whose lease identity was lost.
