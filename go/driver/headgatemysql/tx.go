package headgatemysql

// runtime capability boundary the Transactional port — the reason MySQL is in the same tier as Postgres:
// InnoDB makes transactional enqueue/completion work identically (push wakeups). MysqlTx wraps
// *sql.Tx; Unwrap hands the concrete handle back to callers doing their own writes
// inside the same transaction (caller-owned transaction contract), and a foreign handle is a hard error.

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
	headgate "github.com/mujhtech/headgate"
)

type MysqlTx struct{ tx *sql.Tx }

func (t *MysqlTx) Unwrap() any { return t.tx }

var _ headgate.TransactionalStore = (*MysqlStore)(nil)

func errForeignTx() error { return errors.New("headgate: foreign transaction handle (not MysqlTx)") }

func own(tx headgate.Tx) (*sql.Tx, error) {
	m, ok := tx.(*MysqlTx)
	if !ok {
		return nil, errForeignTx()
	}
	return m.tx, nil
}

func (s *MysqlStore) BeginTx(ctx context.Context) (headgate.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &MysqlTx{tx: tx}, nil
}

// WrapTx adapts a transaction the APPLICATION already opened, which is the whole of
// caller-owned transaction contract: a service on GORM or Bun has its *sql.Tx first and reaches for the queue
// second. The pgx driver has had this since it landed (headgatepgx.WrapTx); without the
// MySQL twin, EnqueueTx could only ever join a transaction headgate itself opened, and
// the interop matrix had no way to express the case that matters. Ownership does not
// transfer: the caller still commits or rolls back its own transaction.
func WrapTx(tx *sql.Tx) headgate.Tx { return &MysqlTx{tx: tx} }

func (s *MysqlStore) CommitTx(_ context.Context, tx headgate.Tx) error {
	t, err := own(tx)
	if err != nil {
		return err
	}
	return t.Commit()
}

func (s *MysqlStore) RollbackTx(_ context.Context, tx headgate.Tx) error {
	t, err := own(tx)
	if err != nil {
		return err
	}
	return t.Rollback()
}

func (s *MysqlStore) EnqueueTx(ctx context.Context, tx headgate.Tx, batch []headgate.Envelope) error {
	t, err := own(tx)
	if err != nil {
		return err
	}
	return headgate.WrapUnavailable(s.enqueueOn(ctx, t, batch))
}

func (s *MysqlStore) CompleteTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef) error {
	return s.CompleteTxWithActualWeight(ctx, tx, lease, nil)
}

func (s *MysqlStore) CompleteTxWithActualWeight(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, actualWeight *uint32) error {
	t, err := own(tx)
	if err != nil {
		return err
	}
	if actualWeight != nil {
		if err := reconcileActualWeightMysql(ctx, t, lease, *actualWeight); err != nil {
			return err
		}
	}
	// Runs INSIDE the caller's transaction: success ack + their writes commit as one.
	n, err := ackSuccessTx(ctx, t, lease, int64(lease.Fence), nil, nil)
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

func (s *MysqlStore) ClaimEffect(ctx context.Context, tx headgate.Tx, key string) (bool, error) {
	t, err := own(tx)
	if err != nil {
		return false, err
	}
	// INSERT IGNORE would swallow OTHER errors too; catch the dup key precisely.
	_, err = t.ExecContext(ctx,
		`INSERT INTO headgate_effect (effect_key, job_ulid, claimed_at_ms)
		 VALUES (?, SUBSTRING_INDEX(?, '/', 1), `+nowMS+`)`, key, key)
	if err == nil {
		return true, nil
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1062 {
		return false, nil
	}
	return false, err
}

func (s *MysqlStore) CheckpointTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	t, err := own(tx)
	if err != nil {
		return err
	}
	res, err := t.ExecContext(ctx,
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
