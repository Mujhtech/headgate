package headgatesqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

var (
	_ headgate.ResultStore            = (*SqliteStore)(nil)
	_ headgate.ResultInspectStore     = (*SqliteStore)(nil)
	_ headgate.OutputStore            = (*SqliteStore)(nil)
	_ headgate.OutputInspectStore     = (*SqliteStore)(nil)
	_ headgate.ProgressStore          = (*SqliteStore)(nil)
	_ headgate.ProgressInspectStore   = (*SqliteStore)(nil)
	_ headgate.CheckpointInspectStore = (*SqliteStore)(nil)
)

// AckSuccessWithResult stores result bytes in the same transaction as fenced completion.
func (s *SqliteStore) AckSuccessWithResult(ctx context.Context, lease headgate.LeaseRef, logs []string, actualWeight *uint32, result headgate.JobResult) error {
	if err := headgate.ValidateOpaqueValue("result", result); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("beginning SQLite result ack: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	job, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM headgate_job
		WHERE id=? AND state='running' AND lease_id=? AND fence=?`, lease.JobID, lease.LeaseID, int64(lease.Fence)))
	if errors.Is(err, sql.ErrNoRows) {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	if err != nil {
		return fmt.Errorf("reading SQLite leased job: %w", err)
	}
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return fmt.Errorf("reading SQLite store time: %w", err)
	}
	if actualWeight != nil && job.rateCharge > 0 {
		if err := reconcileActualWeightOn(ctx, tx, job, *actualWeight, now); err != nil {
			return err
		}
	}
	if job.retentionMs == 0 {
		res, err := tx.ExecContext(ctx, `DELETE FROM headgate_job WHERE id=? AND state='running' AND lease_id=? AND fence=?`, lease.JobID, lease.LeaseID, int64(lease.Fence))
		if err != nil {
			return fmt.Errorf("deleting SQLite completed job: %w", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
	} else {
		errorsJSON := job.errorsJSON
		if len(logs) > 0 {
			errorsJSON = appendAttempt(errorsJSON, now, job.envelope.Attempt, headgate.OutcomeSuccess.String(), "", logs, 0)
		}
		charge := job.rateCharge
		if actualWeight != nil {
			charge = 0
		}
		res, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state='completed',finalized_at_ms=?,
			result_schema_version=?,result_bytes=?,errors_json=?,rate_charge=?,lease_id=NULL,
			lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL
			WHERE id=? AND state='running' AND lease_id=? AND fence=?`, now, result.SchemaVersion,
			append([]byte(nil), result.Bytes...), errorsJSON, charge, lease.JobID, lease.LeaseID, int64(lease.Fence))
		if err != nil {
			return fmt.Errorf("storing SQLite job result: %w", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
	}
	bucket := now / 60_000 * 60_000
	if _, err := tx.ExecContext(ctx, `INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed)
		VALUES(?,?,0,1) ON CONFLICT(queue,bucket_ms) DO UPDATE SET completed=completed+1`, job.envelope.Queue, bucket); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing SQLite result ack: %w", err)
	}
	return nil
}

// WriteJobOutput replaces output only while the fenced lease is current.
func (s *SqliteStore) WriteJobOutput(ctx context.Context, lease headgate.LeaseRef, output headgate.JobResult) (*headgate.JobOutput, error) {
	if err := headgate.ValidateOpaqueValue("output", output); err != nil {
		return nil, err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET output_schema_version=?,output_bytes=?,
		output_fence=fence,output_updated_at_ms=`+nowMS+`
		WHERE id=? AND state='running' AND lease_id=? AND fence=?`, output.SchemaVersion,
		append([]byte(nil), output.Bytes...), lease.JobID, lease.LeaseID, int64(lease.Fence))
	if err != nil {
		return nil, fmt.Errorf("writing SQLite job output: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	stored, err := s.GetJobOutput(ctx, lease.JobID)
	if err != nil || stored == nil {
		return nil, fmt.Errorf("reading SQLite job output after write: %w", err)
	}
	return stored, nil
}

// WriteJobProgress replaces progress only while the fenced lease is current.
func (s *SqliteStore) WriteJobProgress(ctx context.Context, lease headgate.LeaseRef, update headgate.ProgressUpdate) (*headgate.JobProgress, error) {
	if err := headgate.ValidateProgress(update); err != nil {
		return nil, err
	}
	var message any
	if update.Message != "" {
		message = update.Message
	}
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET progress_current=?,progress_total=?,
		progress_message=?,progress_fence=fence,progress_updated_at_ms=`+nowMS+`
		WHERE id=? AND state='running' AND lease_id=? AND fence=?`, update.Current, update.Total,
		message, lease.JobID, lease.LeaseID, int64(lease.Fence))
	if err != nil {
		return nil, fmt.Errorf("writing SQLite job progress: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	stored, err := s.GetJobProgress(ctx, lease.JobID)
	if err != nil || stored == nil {
		return nil, fmt.Errorf("reading SQLite job progress after write: %w", err)
	}
	return stored, nil
}

func (s *SqliteStore) GetJobResult(ctx context.Context, id string) (*headgate.JobResult, error) {
	var version sql.NullInt64
	var bytes []byte
	err := s.db.QueryRowContext(ctx, `SELECT result_schema_version,result_bytes FROM headgate_job WHERE id=?`, id).Scan(&version, &bytes)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !version.Valid) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading SQLite job result: %w", err)
	}
	return &headgate.JobResult{SchemaVersion: uint32(version.Int64), Bytes: append([]byte(nil), bytes...)}, nil
}

func (s *SqliteStore) GetJobOutput(ctx context.Context, id string) (*headgate.JobOutput, error) {
	var version, fence, updated sql.NullInt64
	var bytes []byte
	err := s.db.QueryRowContext(ctx, `SELECT output_schema_version,output_bytes,output_fence,output_updated_at_ms FROM headgate_job WHERE id=?`, id).Scan(&version, &bytes, &fence, &updated)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !version.Valid) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading SQLite job output: %w", err)
	}
	return &headgate.JobOutput{SchemaVersion: uint32(version.Int64), Bytes: append([]byte(nil), bytes...), Fence: uint64(fence.Int64), UpdatedAtMs: updated.Int64}, nil
}

func (s *SqliteStore) GetJobProgress(ctx context.Context, id string) (*headgate.JobProgress, error) {
	var current, total, fence, updated sql.NullInt64
	var message sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT progress_current,progress_total,progress_message,progress_fence,progress_updated_at_ms FROM headgate_job WHERE id=?`, id).Scan(&current, &total, &message, &fence, &updated)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !current.Valid) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading SQLite job progress: %w", err)
	}
	return &headgate.JobProgress{Current: uint64(current.Int64), Total: uint64(total.Int64), Message: message.String, Fence: uint64(fence.Int64), UpdatedAtMs: updated.Int64}, nil
}

func (s *SqliteStore) GetJobCheckpoint(ctx context.Context, id string) (*headgate.Checkpoint, error) {
	var raw sql.NullString
	var cursor []byte
	err := s.db.QueryRowContext(ctx, `SELECT checkpoint_json,checkpoint_cursor FROM headgate_job WHERE id=?`, id).Scan(&raw, &cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading SQLite checkpoint: %w", err)
	}
	cp := headgateshared.DecodeCheckpoint([]byte(raw.String), cursor)
	return &cp, nil
}
