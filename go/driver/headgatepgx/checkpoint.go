package headgatepgx

import (
	"context"

	headgate "github.com/mujhtech/headgate/go"
)

// ---------- step replay checkpoint ----------

// Checkpoint durably records resumable state after verifying the lease fence.
func (s *PgxStore) Checkpoint(ctx context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	var n int64
	err := s.pool.QueryRow(ctx, `
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
