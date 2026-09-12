package headgatesqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

type SqliteTx struct{ tx *sql.Tx }

func (t *SqliteTx) Unwrap() any { return t.tx }

var _ headgate.TransactionalStore = (*SqliteStore)(nil)

func WrapTx(tx *sql.Tx) headgate.Tx { return &SqliteTx{tx: tx} }

func ownTx(tx headgate.Tx) (*sql.Tx, error) {
	t, ok := tx.(*SqliteTx)
	if !ok || t.tx == nil {
		return nil, &headgate.InvalidError{Msg: "foreign transaction handle (not SqliteTx)"}
	}
	return t.tx, nil
}

func (s *SqliteStore) BeginTx(ctx context.Context) (headgate.Tx, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("beginning SQLite transaction: %w", err)
	}
	return &SqliteTx{tx: tx}, nil
}

func (s *SqliteStore) CommitTx(_ context.Context, tx headgate.Tx) error {
	t, err := ownTx(tx)
	if err != nil {
		return err
	}
	return t.Commit()
}

func (s *SqliteStore) RollbackTx(_ context.Context, tx headgate.Tx) error {
	t, err := ownTx(tx)
	if err != nil {
		return err
	}
	return t.Rollback()
}

func (s *SqliteStore) EnqueueTx(ctx context.Context, tx headgate.Tx, batch []headgate.Envelope) error {
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	t, err := ownTx(tx)
	if err != nil {
		return err
	}
	return enqueueOn(ctx, t, batch)
}

func (s *SqliteStore) CompleteTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef) error {
	return s.CompleteTxWithActualWeight(ctx, tx, lease, nil)
}

func (s *SqliteStore) CompleteTxWithActualWeight(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, actualWeight *uint32) error {
	t, err := ownTx(tx)
	if err != nil {
		return err
	}
	job, err := scanJob(t.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM headgate_job
		WHERE id=? AND state='running' AND lease_id=? AND fence=?`, lease.JobID, lease.LeaseID, int64(lease.Fence)))
	if errors.Is(err, sql.ErrNoRows) {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	} else if err != nil {
		return fmt.Errorf("reading SQLite transaction job: %w", err)
	}
	var now int64
	if err := t.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return err
	}
	if actualWeight != nil && job.rateCharge > 0 {
		if err := reconcileActualWeightOn(ctx, t, job, *actualWeight, now); err != nil {
			return err
		}
	}
	if job.retentionMs == 0 {
		res, err := t.ExecContext(ctx, `DELETE FROM headgate_job WHERE id=? AND state='running' AND lease_id=? AND fence=?`, lease.JobID, lease.LeaseID, int64(lease.Fence))
		if err != nil {
			return fmt.Errorf("deleting SQLite transaction job: %w", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
		if _, err := t.ExecContext(ctx, `INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed) VALUES(?,?,0,1) ON CONFLICT(queue,bucket_ms) DO UPDATE SET completed=completed+1`, job.envelope.Queue, now/60_000*60_000); err != nil {
			return err
		}
		return nil
	}
	charge := any(job.rateCharge)
	if actualWeight != nil {
		charge = 0
	}
	res, err := t.ExecContext(ctx, `UPDATE headgate_job SET state='completed',finalized_at_ms=`+nowMS+`,
		lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL,
		rate_charge=COALESCE(?,rate_charge) WHERE id=? AND state='running' AND lease_id=? AND fence=?`,
		charge, lease.JobID, lease.LeaseID, int64(lease.Fence))
	if err != nil {
		return fmt.Errorf("completing SQLite transaction job: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	if _, err := t.ExecContext(ctx, `INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed) VALUES(?,?,0,1) ON CONFLICT(queue,bucket_ms) DO UPDATE SET completed=completed+1`, job.envelope.Queue, now/60_000*60_000); err != nil {
		return err
	}
	return nil
}

func (s *SqliteStore) ClaimEffect(ctx context.Context, tx headgate.Tx, key string) (bool, error) {
	if key == "" {
		return false, &headgate.InvalidError{Msg: "effect key must not be empty"}
	}
	t, err := ownTx(tx)
	if err != nil {
		return false, err
	}
	res, err := t.ExecContext(ctx, `INSERT INTO headgate_effect(effect_key,claimed_at_ms)
		VALUES(?,`+nowMS+`) ON CONFLICT(effect_key) DO NOTHING`, key)
	if err != nil {
		return false, fmt.Errorf("claiming SQLite effect: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *SqliteStore) CheckpointTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	t, err := ownTx(tx)
	if err != nil {
		return err
	}
	res, err := t.ExecContext(ctx, `UPDATE headgate_job SET checkpoint_json=?,checkpoint_cursor=?
		WHERE id=? AND state='running' AND lease_id=? AND fence=?`,
		string(headgateshared.EncodeCheckpoint(cp)), cp.Cursor, lease.JobID, lease.LeaseID, int64(lease.Fence))
	if err != nil {
		return fmt.Errorf("checkpointing SQLite transaction job: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}
