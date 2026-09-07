package headgatemysql

import (
	"context"
	"errors"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// Renew extends current leases and returns IDs whose lease identity was lost.
func (s *MysqlStore) Renew(ctx context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return nil, errors.New("headgate: lease must be >= 1ms")
	}
	var lost []string
	for _, l := range leases {
		res, err := s.db.ExecContext(ctx,
			"UPDATE headgate_job SET lease_expires_at_ms = "+nowMS+" + ? WHERE "+ident,
			leaseMs, l.JobID, l.LeaseID, int64(l.Fence))
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			lost = append(lost, l.JobID)
		}
	}
	return lost, nil
}

// Checkpoint durably records resumable state after verifying the lease fence.
func (s *MysqlStore) Checkpoint(ctx context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	res, err := s.db.ExecContext(ctx,
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

// Enqueue inserts a batch of jobs atomically.
