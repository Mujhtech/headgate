package headgatepgx

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- transactional storage ----------

type pgxTx struct{ tx pgx.Tx }

func (t pgxTx) Unwrap() any { return t.tx }

// Begin opens a store transaction for EnqueueTx/CompleteTx. Callers with their own
// pgx.Tx can pass WrapTx(tx) instead — the caller's transaction is the point (caller-owned transaction contract).
func (s *PgxStore) Begin(ctx context.Context) (headgate.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return pgxTx{tx: tx}, nil
}

// WrapTx adapts a pgx transaction to Headgate's transaction interface.
func WrapTx(tx pgx.Tx) headgate.Tx { return pgxTx{tx: tx} }

func unwrapTx(tx headgate.Tx) (pgx.Tx, error) {
	if t, ok := tx.Unwrap().(pgx.Tx); ok {
		return t, nil
	}
	// runtime capability boundary never a silent no-op: a foreign handle is a hard, typed error.
	return nil, errors.New("headgate: Tx is not a headgatepgx transaction")
}

// Commit commits a transaction previously returned by this package.
func Commit(ctx context.Context, tx headgate.Tx) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	return t.Commit(ctx)
}

// Rollback rolls back a transaction previously returned by this package.
func Rollback(ctx context.Context, tx headgate.Tx) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	return t.Rollback(ctx)
}

// EnqueueTx inserts jobs using an existing Headgate transaction.
func (s *PgxStore) EnqueueTx(ctx context.Context, tx headgate.Tx, batch []headgate.Envelope) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	return headgate.WrapUnavailable(s.enqueueOn(ctx, t, batch))
}

// CompleteTx provides transactional completion: the job finishes iff the caller's
// writes commit. Success path only, same statement shape as Ack(OutcomeSuccess).
func (s *PgxStore) CompleteTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef) error {
	return s.CompleteTxWithActualWeight(ctx, tx, lease, nil)
}

// CompleteTxWithActualWeight completes a job and reconciles admission cost in tx.
func (s *PgxStore) CompleteTxWithActualWeight(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, actualWeight *uint32) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	if actualWeight != nil {
		if err := s.reconcileActualWeightPgx(ctx, t, lease, *actualWeight); err != nil {
			return err
		}
	}
	q := s.pool.scope(t)
	var n int64
	err = q.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms),
		del AS (
		  DELETE FROM headgate_job j USING p
		  WHERE `+ident+` AND j.retention_ms = 0
		  RETURNING j.queue
		),
		upd AS (
		  UPDATE headgate_job j SET
		    state = 'completed', lease_id = NULL, lease_expires_at_ms = NULL,
		    claimed_by = NULL, finalized_at_ms = p.now_ms
		  FROM p WHERE `+ident+` AND j.retention_ms > 0
		  RETURNING j.queue
		)
		SELECT (SELECT count(*) FROM del) + (SELECT count(*) FROM upd)`,
		lease.JobID, lease.LeaseID, int64(lease.Fence)).Scan(&n)
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

func encodeCheckpoint(cp headgate.Checkpoint) []byte {
	return headgateshared.EncodeCheckpoint(cp)
}

func decodeCheckpoint(raw, cursor []byte) headgate.Checkpoint {
	return headgateshared.DecodeCheckpoint(raw, cursor)
}

// ---------- the dyn transactional path (transactional API) + transactional effects effect keys ----------

// BeginTx begins a transaction for the generic transactional store interface.
func (s *PgxStore) BeginTx(ctx context.Context) (headgate.Tx, error) { return s.Begin(ctx) }

// CommitTx commits tx.
func (s *PgxStore) CommitTx(ctx context.Context, tx headgate.Tx) error { return Commit(ctx, tx) }

// RollbackTx rolls back tx.
func (s *PgxStore) RollbackTx(ctx context.Context, tx headgate.Tx) error { return Rollback(ctx, tx) }

// ClaimEffect atomically claims an idempotent side-effect key in tx.
func (s *PgxStore) ClaimEffect(ctx context.Context, tx headgate.Tx, key string) (bool, error) {
	t, err := unwrapTx(tx)
	if err != nil {
		return false, err
	}
	q := s.pool.scope(t)
	tag, err := q.Exec(ctx,
		`INSERT INTO headgate_effect (key, at_ms) VALUES ($1, `+nowMS+`)
		 ON CONFLICT (key) DO NOTHING`, key)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// CheckpointTx records a fenced checkpoint in tx.
func (s *PgxStore) CheckpointTx(ctx context.Context, tx headgate.Tx, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	t, err := unwrapTx(tx)
	if err != nil {
		return err
	}
	q := s.pool.scope(t)
	var n int64
	err = q.QueryRow(ctx, `
		WITH upd AS (
		  UPDATE headgate_job j SET checkpoint = $4::jsonb, cp_cursor = $5
		  WHERE `+ident+`
		  RETURNING 1
		) SELECT count(*)::bigint FROM upd`,
		lease.JobID, lease.LeaseID, int64(lease.Fence), encodeCheckpoint(cp), cp.Cursor).Scan(&n)
	if err != nil {
		return err
	}
	if n == 0 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}
