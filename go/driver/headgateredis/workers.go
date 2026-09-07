package headgateredis

import (
	"context"
	"errors"
	"sort"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/redis/go-redis/v9"
)

// HeartbeatWorker records worker liveness and returns its pending command.
func (s *RedisStore) HeartbeatWorker(ctx context.Context, w headgate.WorkerMeta) (string, error) {
	status := w.Status
	if status == "" {
		status = "running"
	}
	cmd, err := workerLua.Run(ctx, s.rdb, []string{s.prefix},
		"beat", w.WorkerID, w.Host, w.PID, strings.Join(w.Queues, ","),
		w.Concurrency, w.StartedAtMs,
		// ADDITIVE trailing args on worker.lua's beat.
		w.Inflight, w.Polls, w.EmptyPolls, status, w.DutiesActive).Text()
	if err != nil && errors.Is(err, redis.Nil) {
		return "", nil
	}
	return cmd, err
}

// ListWorkers returns workers whose heartbeat is newer than staleAfterMs.
func (s *RedisStore) ListWorkers(ctx context.Context, staleAfterMs int64) ([]headgate.WorkerMeta, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	ids, err := s.scanSet(ctx, s.key("workers"))
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.key("worker", id)
	}
	hashes, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, err
	}
	var out []headgate.WorkerMeta
	for i, id := range ids {
		h := hashes[i]
		if len(h) == 0 {
			// The hash TTL'd out (dead > 24h); tidy the registry as we pass.
			_ = s.rdb.SRem(ctx, s.key("workers"), id).Err()
			continue
		}
		if hnum(h, "heartbeat_at_ms") < now-staleAfterMs {
			continue
		}
		var queues []string
		if q := h["queues"]; q != "" {
			queues = strings.Split(q, ",")
		}
		out = append(out, headgate.WorkerMeta{
			WorkerID: id, Host: h["host"], PID: int32(hnum(h, "pid")),
			Queues: queues, Concurrency: uint32(hnum(h, "concurrency")),
			StartedAtMs: hnum(h, "started_at_ms"), HeartbeatAtMs: hnum(h, "heartbeat_at_ms"),
			Inflight:   uint32(hnum(h, "inflight")),
			Polls:      uint64(hnum(h, "polls")),
			EmptyPolls: uint64(hnum(h, "empty_polls")),
			Status:     h["status"],
			// Older worker hashes predate this level and did run duties.
			DutiesActive:   h["duties_active"] == "1" || h["duties_active"] == "",
			PendingCommand: h["command"],
		})
	}
	return out, nil
}

// SignalWorker records a control command for workerID.
func (s *RedisStore) SignalWorker(ctx context.Context, workerID, command string) error {
	if !headgate.ValidWorkerCommand(command) {
		return &headgate.InvalidError{Msg: "command must be quiet, resume, restart, terminate, or resign"}
	}
	n, err := workerLua.Run(ctx, s.rdb, []string{s.prefix},
		"signal", workerID, command).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		return headgate.NotFoundf("worker %s", workerID)
	}
	return nil
}

// DistinctKinds returns a bounded list of known job kinds.
func (s *RedisStore) DistinctKinds(ctx context.Context, limit int64) ([]string, error) {
	boundedLimit := limit
	if boundedLimit < 1 {
		boundedLimit = 1
	}
	if boundedLimit > positionLimit {
		boundedLimit = positionLimit
	}
	queues, err := s.queueNames(ctx)
	if err != nil || len(queues) == 0 {
		return nil, err
	}
	pipe := s.rdb.Pipeline()
	var cmds []*redis.StringSliceCmd
	for _, q := range queues {
		for _, st := range []string{"available", "scheduled", "retryable"} {
			cmds = append(cmds, pipe.ZRange(ctx, s.idx(q, st), 0, boundedLimit-1))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	var sample []string
	for _, c := range cmds {
		for _, id := range c.Val() {
			if int64(len(sample)) >= boundedLimit {
				break
			}
			sample = append(sample, id)
		}
	}
	if len(sample) == 0 {
		return nil, nil
	}
	pipe = s.rdb.Pipeline()
	kindCmds := make([]*redis.StringCmd, len(sample))
	for i, id := range sample {
		kindCmds[i] = pipe.HGet(ctx, s.key("job", id), "kind")
	}
	_, _ = pipe.Exec(ctx)
	seen := map[string]bool{}
	var out []string
	for _, c := range kindCmds {
		if k := c.Val(); k != "" && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

// opStates is the same action -> allowed-states table every backend uses.
