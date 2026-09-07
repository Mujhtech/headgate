package headgatepgx

import (
	"context"
	"errors"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// ---------- renew ----------

// Renew extends current leases and returns IDs whose lease identity was lost.
func (s *PgxStore) Renew(ctx context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return nil, errors.New("headgate: lease must be >= 1ms (boundary validation)")
	}
	ids := make([]string, len(leases))
	leaseIDs := make([]string, len(leases))
	fences := make([]int64, len(leases))
	for i, l := range leases {
		ids[i], leaseIDs[i], fences[i] = l.JobID, l.LeaseID, int64(l.Fence)
	}
	rows, err := s.pool.Query(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms, $4::bigint AS lease_ms),
		req AS (
		  SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[])
		         AS t(ulid, lease_id, fence)
		),
		upd AS (
		  UPDATE headgate_job j SET lease_expires_at_ms = p.now_ms + p.lease_ms
		  FROM p, req r
		  WHERE j.ulid = r.ulid AND j.lease_id = r.lease_id AND j.fence = r.fence
		    AND j.state = 'running'
		  RETURNING j.ulid
		)
		SELECT r.ulid FROM req r WHERE r.ulid NOT IN (SELECT ulid FROM upd)`,
		ids, leaseIDs, fences, leaseMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lost []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		lost = append(lost, id)
	}
	return lost, rows.Err()
}
