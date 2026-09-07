package headgatepgx

import (
	"context"
	"errors"
	"time"
)

// ---------- singleton duties ----------

// ClaimDuty acquires or renews a singleton duty lease for holder.
func (s *PgxStore) ClaimDuty(ctx context.Context, name, holder string, lease time.Duration) (bool, error) {
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return false, errors.New("headgate: duty lease must be >= 1ms")
	}
	var n int64
	err := s.pool.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms, $3::bigint AS lease_ms),
		up AS (
		  INSERT INTO headgate_duty AS d (name, holder, expires_at_ms)
		  SELECT $1::text, $2::text, p.now_ms + p.lease_ms FROM p
		  ON CONFLICT (name) DO UPDATE
		    SET holder = EXCLUDED.holder, expires_at_ms = EXCLUDED.expires_at_ms
		    WHERE d.expires_at_ms < EXCLUDED.expires_at_ms - $3::bigint
		       OR d.holder = EXCLUDED.holder
		  RETURNING name
		)
		SELECT count(*)::bigint FROM up`, name, holder, leaseMs).Scan(&n)
	return n == 1, err
}

// ReleaseDuty releases a singleton duty lease held by holder.
func (s *PgxStore) ReleaseDuty(ctx context.Context, name, holder string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE headgate_duty SET expires_at_ms = 0 WHERE name = $1 AND holder = $2`,
		name, holder)
	return err
}
