package headgateredis

import (
	"context"

	headgate "github.com/mujhtech/headgate/go"
)

// QuarantineSweep moves eligible jobs with quarantined fingerprints into quarantine.
func (s *RedisStore) QuarantineSweep(ctx context.Context, limit int64) (int64, error) {
	return adminLua.Run(ctx, s.rdb, []string{s.prefix}, "q_sweep", limit).Int64()
}

// RescheduleJob changes when a scheduled or retryable job becomes due.
func (s *RedisStore) RescheduleJob(ctx context.Context, id string, atMs int64) error {
	res, err := s.adminJobOp(ctx, "reschedule", id, atMs)
	if err != nil {
		return err
	}
	switch first(res) {
	case "OK":
		return nil
	case "NF":
		return headgate.NotFoundf("job %s", id)
	default:
		return headgate.Invalidf("reschedule is only defined for scheduled/retryable; job %s is %s",
			id, second(res))
	}
}

// EditPayload replaces the payload of a job that is safe to edit.
func (s *RedisStore) EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error {
	res, err := s.adminJobOp(ctx, "edit", id, string(payload), schemaVersion, fingerprint)
	if err != nil {
		return err
	}
	switch first(res) {
	case "OK":
		return nil
	case "NF":
		return headgate.NotFoundf("job %s", id)
	default:
		return &headgate.InvalidError{Msg: "cannot edit a running job's payload"}
	}
}

// UpsertSchedule creates or updates a periodic schedule.
