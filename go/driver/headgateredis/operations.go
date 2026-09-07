package headgateredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
	"github.com/redis/go-redis/v9"
)

func opStates(action string) ([]string, bool) {
	return headgate.BulkActionStates(action)
}

func selectorStates(req headgate.BulkOp, allowed []string) []string {
	if req.State == "" {
		return allowed
	}
	if slicesContains(allowed, req.State) {
		return []string{req.State}
	}
	return nil
}

// CreateOperation queues an asynchronous bulk operation.
func (s *RedisStore) CreateOperation(ctx context.Context, req headgate.BulkOp) error {
	if !req.HasSelector() {
		// control API contract no accidental delete-everything.
		return &headgate.InvalidError{Msg: "empty selector is rejected"}
	}
	allowed, ok := opStates(req.Action)
	if !ok {
		return headgate.Invalidf("unknown action `%s`", req.Action)
	}
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return err
	}
	states := selectorStates(req, allowed)
	queues := []string{req.Queue}
	if req.Queue == "" {
		if queues, err = s.queueNames(ctx); err != nil {
			return err
		}
	}
	// Bounded estimate of the affected set — for dry runs it IS the answer. With no
	// per-job filters the ZCARDs are exact; with filters, a bounded sampled count.
	var estimated int64
	if req.Kind == "" && req.PartitionKey == "" && req.OlderThanMs == nil {
		pipe := s.rdb.Pipeline()
		var cmds []*redis.IntCmd
		for _, q := range queues {
			for _, st := range states {
				cmds = append(cmds, pipe.ZCard(ctx, s.idx(q, st)))
			}
		}
		if len(cmds) > 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
		for _, c := range cmds {
			estimated += c.Val()
		}
	} else {
		// BulkOp still carries plain strings (see the register's "Bulk operations" row:
		// its selector is PERSISTED, so the empty-value port change needs a storage
		// format that can say "absent" — deferred). Translating "" to nil here keeps
		// this call site's behavior exactly as it was.
		f := headgate.JobFilter{}
		if req.Queue != "" {
			f.Queue = &req.Queue
		}
		if req.Kind != "" {
			f.Kind = &req.Kind
		}
		if req.PartitionKey != "" {
			f.PartitionKey = &req.PartitionKey
		}
		for _, st := range states {
			st := st
			f.State = &st
			page, err := s.ListJobs(ctx, f, "", maxPage)
			if err != nil {
				return err
			}
			for _, j := range page.Jobs {
				if req.OlderThanMs == nil || j.EnqueuedAtMs < now-*req.OlderThanMs {
					estimated++
				}
			}
		}
	}
	status := "pending"
	if req.DryRun {
		status = "completed"
	}
	older := ""
	if req.OlderThanMs != nil {
		older = strconv.FormatInt(*req.OlderThanMs, 10)
	}
	dry := "0"
	if req.DryRun {
		dry = "1"
	}
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, s.key("op", req.ID),
		"action", req.Action, "queue", req.Queue, "state", req.State, "kind", req.Kind,
		"partition_key", req.PartitionKey, "older_than_ms", older,
		"status", status, "affected", 0, "total_estimated", estimated,
		"dry_run", dry, "created_at_ms", now, "qi", 1, "si", 1, "off", 0)
	if !req.DryRun {
		pipe.ZAdd(ctx, s.key("ops"), redis.Z{Score: float64(now), Member: req.ID})
	}
	_, err = pipe.Exec(ctx)
	return err
}

// GetOperation returns the status of a bulk operation.
func (s *RedisStore) GetOperation(ctx context.Context, id string) (*headgate.OperationStatus, error) {
	h, err := s.rdb.HGetAll(ctx, s.key("op", id)).Result()
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, nil
	}
	return &headgate.OperationStatus{
		ID: id, Status: h["status"], Affected: hnum(h, "affected"),
		TotalEstimated: hnum(h, "total_estimated"), DryRun: h["dry_run"] == "1",
		Error: h["error"],
	}, nil
}

// RunPendingOperations executes a bounded batch of pending bulk operations.
func (s *RedisStore) RunPendingOperations(ctx context.Context, batch int64) (uint64, error) {
	ids, err := s.rdb.ZRange(ctx, s.key("ops"), 0, 4).Result()
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, id := range ids {
		ok := s.key("op", id)
		h, err := s.rdb.HGetAll(ctx, ok).Result()
		if err != nil {
			return total, err
		}
		action := h["action"]
		allowed, valid := opStates(action)
		if !valid {
			pipe := s.rdb.Pipeline()
			pipe.HSet(ctx, ok, "status", "failed", "error", "unknown action `"+action+"`")
			pipe.ZRem(ctx, s.key("ops"), id)
			if _, err := pipe.Exec(ctx); err != nil {
				return total, err
			}
			continue
		}
		req := headgate.BulkOp{
			ID: id, Action: action, Queue: h["queue"], State: h["state"],
			Kind: h["kind"], PartitionKey: h["partition_key"],
		}
		states := selectorStates(req, allowed)
		if len(states) == 0 {
			pipe := s.rdb.Pipeline()
			pipe.HSet(ctx, ok, "status", "completed")
			pipe.ZRem(ctx, s.key("ops"), id)
			if _, err := pipe.Exec(ctx); err != nil {
				return total, err
			}
			continue
		}
		qi, si, off := hnum(h, "qi"), hnum(h, "si"), hnum(h, "off")
		if qi < 1 {
			qi = 1
		}
		if si < 1 {
			si = 1
		}
		res, err := adminLua.Run(ctx, s.rdb, []string{s.prefix},
			"bulk", action, h["queue"], strings.Join(states, ","), h["kind"],
			h["partition_key"], h["older_than_ms"], batch, qi, si, off).Int64Slice()
		if err != nil {
			return total, err
		}
		applied, nqi, nsi, noff, done := res[0], res[1], res[2], res[3], res[4] == 1
		total += uint64(applied)
		status := "running"
		if done {
			status = "completed"
		}
		pipe := s.rdb.Pipeline()
		pipe.HSet(ctx, ok, "status", status, "affected", hnum(h, "affected")+applied,
			"qi", nqi, "si", nsi, "off", noff)
		if done {
			pipe.ZRem(ctx, s.key("ops"), id)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return total, err
		}
	}
	return total, nil
}

// PromoteJob moves a pending job into the admission queue.
func (s *RedisStore) PromoteJob(ctx context.Context, id string) error {
	res, err := s.adminJobOp(ctx, "promote", id)
	if err != nil {
		return err
	}
	if len(res) == 0 {
		return errors.New("invalid promote response")
	}
	switch res[0] {
	case "OK":
		return nil
	case "NF":
		return headgate.NotFoundf("job %s", id)
	case "ERR":
		return headgate.Invalidf("operator_promote is defined only from pending")
	default:
		return errors.New("invalid promote response")
	}
}

// SchedulePendingJob moves a pending job to a future run time.
func (s *RedisStore) SchedulePendingJob(ctx context.Context, id string, atMs int64) error {
	if atMs <= 0 {
		return headgate.Invalidf("pending schedule timestamp must be positive")
	}
	res, err := s.adminJobOp(ctx, "schedule_pending", id, atMs)
	if err != nil {
		return err
	}
	if len(res) == 0 {
		return errors.New("invalid schedule_pending response")
	}
	switch res[0] {
	case "OK":
		return nil
	case "NF":
		return headgate.NotFoundf("job %s", id)
	case "ERR":
		return headgate.Invalidf("workflow_schedule is defined only from pending")
	default:
		return errors.New("invalid schedule_pending response")
	}
}

// DeleteQueue removes queue configuration and, when forced, its jobs.
func (s *RedisStore) DeleteQueue(ctx context.Context, queue string, force bool) (string, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return "", err
	}
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, queue)
	id := fmt.Sprintf("qdel-%d-%s", now, clean)
	f := 0
	if force {
		f = 1
	}
	res, err := s.adminJobOp(ctx, "queue_delete", queue, f, id)
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", errors.New("invalid queue delete response")
	}
	switch res[0] {
	case "EMPTY":
		return "", nil
	case "QUEUED":
		return id, nil
	case "NONEMPTY":
		return "", headgate.Invalidf("queue is not empty; retry with force=true")
	default:
		return "", errors.New("invalid queue delete response")
	}
}

// SampleQueueMemory refreshes bounded per-queue storage estimates.
func (s *RedisStore) SampleQueueMemory(ctx context.Context, limit uint32) (uint32, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > headgateshared.InspectionMemorySampleLimit {
		limit = headgateshared.InspectionMemorySampleLimit
	}
	qs, err := s.queueNames(ctx)
	if err != nil {
		return 0, err
	}
	states := []string{"pending", "scheduled", "available", "retryable", "running", "completed", "archived", "cancelled", "quarantined", "undecodable"}
	for _, q := range qs {
		ids := map[string]struct{}{}
		for _, st := range states {
			if len(ids) >= int(limit) {
				break
			}
			found, err := s.rdb.ZRange(ctx, s.key("idx", q, st), 0, int64(limit)-int64(len(ids))-1).Result()
			if err != nil {
				return 0, err
			}
			for _, id := range found {
				ids[id] = struct{}{}
			}
		}
		pipe := s.rdb.Pipeline()
		for id := range ids {
			pipe.MemoryUsage(ctx, s.key("job", id))
		}
		pipe.MemoryUsage(ctx, s.key("enqueue", q))
		pipe.MemoryUsage(ctx, s.key("parts", q))
		cmds, err := pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			return 0, err
		}
		var bytes int64
		for _, cmd := range cmds {
			if c, ok := cmd.(*redis.IntCmd); ok {
				n, e := c.Result()
				if e == nil {
					bytes += n
				}
			}
		}
		now, err := s.storeNowMs(ctx)
		if err != nil {
			return 0, err
		}
		if err = s.rdb.HSet(ctx, s.key("mem", q), "bytes", bytes, "sampled_jobs", len(ids), "sampled_at_ms", now).Err(); err != nil {
			return 0, err
		}
	}
	return uint32(len(qs)), nil
}
