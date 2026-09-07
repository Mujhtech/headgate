package headgatemysql

import (
	"context"
	"errors"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// ClaimDuty acquires or renews a singleton duty lease for holder.
func (s *MysqlStore) ClaimDuty(ctx context.Context, name, holder string, lease time.Duration) (bool, error) {
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return false, errors.New("headgate: duty lease must be >= 1ms")
	}
	// singleton duties the same compare-and-set as claiming a job, on store time.
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO headgate_duty (name, holder, expires_at_ms)
		 VALUES (?, ?, `+nowMS+` + ?) AS new
		 ON DUPLICATE KEY UPDATE
		   holder = IF(headgate_duty.expires_at_ms <= `+nowMS+`
		               OR headgate_duty.holder = new.holder, new.holder, headgate_duty.holder),
		   expires_at_ms = IF(headgate_duty.expires_at_ms <= `+nowMS+`
		                      OR headgate_duty.holder = new.holder,
		                      new.expires_at_ms, headgate_duty.expires_at_ms)`,
		name, holder, leaseMs); err != nil {
		return false, err
	}
	var ours string
	if err := s.db.QueryRowContext(ctx,
		"SELECT holder FROM headgate_duty WHERE name = ?", name).Scan(&ours); err != nil {
		return false, err
	}
	return ours == holder, nil
}

// ReleaseDuty releases a singleton duty lease held by holder.
func (s *MysqlStore) ReleaseDuty(ctx context.Context, name, holder string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM headgate_duty WHERE name = ? AND holder = ?", name, holder)
	return err
}

// Caps reports the capabilities enabled for this store.
func (s *MysqlStore) Caps() headgate.Caps {
	// runtime capability boundary/push wakeups: TRANSACTIONAL (InnoDB) | INSPECT — inspect.go ports the full
	// 30-method surface from crates/headgate-mysql/src/inspect.rs). NO Notifying, ever:
	// MySQL has no LISTEN/NOTIFY, so this store polls and its caps say so out loud
	// (invariant 5 — a capability whose scenarios cannot pass must not be declared).
	return headgate.CapTransactional | headgate.CapInspect
}

// headersJSON encodes envelope headers as MySQL stores them: NULL for the header-less
// case, so a job with no headers writes exactly what it wrote before this existed.
func headersJSON(h map[string]string) any {
	if s := headgate.EncodeHeaders(h); s != "" {
		return s
	}
	return nil
}
