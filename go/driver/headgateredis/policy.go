package headgateredis

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
	"github.com/redis/go-redis/v9"
)

// SetQueuePaused changes whether admission may draw from queue.
func (s *RedisStore) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	pipe := s.rdb.Pipeline()
	if paused {
		pipe.SAdd(ctx, s.key("paused"), queue)
	} else {
		pipe.SRem(ctx, s.key("paused"), queue)
	}
	pipe.SAdd(ctx, s.key("queues"), queue)
	_, err := pipe.Exec(ctx)
	return err
}

// SetQueueWeight changes queue weight while preserving its scheduling position.
func (s *RedisStore) SetQueueWeight(ctx context.Context, queue string, weight uint32) error {
	if weight == 0 {
		return &headgate.InvalidError{Msg: "weight must be >= 1"}
	}
	n, err := adminLua.Run(ctx, s.rdb, []string{s.prefix}, "qweight", queue,
		strconv.FormatUint(uint64(weight), 10)).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("headgate: unexpected qweight reply %d", n)
	}
	return nil
}

// SetEnqueueLimit sets or clears a queue's unfinished-job limit.
func (s *RedisStore) SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error {
	raw := ""
	if maxUnfinishedJobs != nil {
		raw = strconv.FormatUint(*maxUnfinishedJobs, 10)
	}
	n, err := adminLua.Run(ctx, s.rdb, []string{s.prefix}, "qlimit", queue, raw).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("headgate: unexpected qlimit reply %d", n)
	}
	return nil
}

// RateClasses returns the configured rate classes and their current state.
func (s *RedisStore) RateClasses(ctx context.Context) ([]headgate.RateClassState, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	names, err := s.scanSet(ctx, s.key("rate_classes"))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, nil
	}
	// One shared bounded sample of available jobs feeds every class's waiting count.
	queues, err := s.queueNames(ctx)
	if err != nil {
		return nil, err
	}
	var sample []string
	if len(queues) > 0 {
		pipe := s.rdb.Pipeline()
		var cmds []*redis.StringSliceCmd
		for _, q := range queues {
			cmds = append(cmds, pipe.ZRange(ctx, s.idx(q, "available"), 0, positionLimit-1))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, err
		}
		for _, c := range cmds {
			for _, id := range c.Val() {
				if len(sample) >= positionLimit {
					break
				}
				sample = append(sample, id)
			}
		}
	}
	waiting := map[string]int64{}
	if len(sample) > 0 {
		pipe := s.rdb.Pipeline()
		cmds := make([]*redis.StringCmd, len(sample))
		for i, id := range sample {
			cmds[i] = pipe.HGet(ctx, s.key("job", id), "rate_class")
		}
		_, _ = pipe.Exec(ctx) // missing fields surface as redis.Nil per-cmd; fine
		for _, c := range cmds {
			if rc := c.Val(); rc != "" {
				waiting[rc]++
			}
		}
	}
	keys := make([]string, len(names))
	for i, n := range names {
		keys[i] = s.key("rate", n)
	}
	buckets, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make([]headgate.RateClassState, 0, len(names))
	for i, name := range names {
		b := buckets[i]
		tokens, burst := hnum(b, "tokens"), hnum(b, "burst")
		limit, window, refilled := hnum(b, "limit"), hnum(b, "window"), hnum(b, "refilled")
		// The same lazy-refill math as admit.lua's bucket_avail, read-only.
		avail := tokens
		if limit > 0 && window > 0 {
			if gained := (now - refilled) * limit / window; gained > 0 {
				avail = min64(burst, tokens+gained)
			}
		}
		out = append(out, headgate.RateClassState{
			Name: name, TokensAvailable: avail, Burst: burst,
			LimitPerWindow: limit, WindowMs: window,
			JobsWaiting: waiting[name],
			// The kill switch is limit 0 + empty bucket, same as every backend.
			Paused: limit == 0,
		})
	}
	return out, nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// UpsertRateClass creates or updates a rate class.
func (s *RedisStore) UpsertRateClass(ctx context.Context, cfg headgate.RateClassConfig) error {
	if err := headgate.ValidateRateClassConfig(cfg); err != nil {
		return err
	}
	paused := "0"
	if cfg.Paused {
		paused = "1"
	}
	return adminLua.Run(ctx, s.rdb, []string{s.prefix},
		"rc_upsert", cfg.Name, cfg.Limit, cfg.WindowMs, cfg.Burst, paused).Err()
}

type redisConcurrencyLimit struct {
	Queue         string                      `json:"queue"`
	MaxConcurrent uint64                      `json:"max_concurrent"`
	OnSaturated   headgate.SaturationStrategy `json:"on_saturated"`
}

// ConcurrencyLimits returns all configured concurrency limits.
func (s *RedisStore) ConcurrencyLimits(ctx context.Context) ([]headgate.ConcurrencyLimit, error) {
	raw, err := s.rdb.HGetAll(ctx, s.key("climits")).Result()
	if err != nil {
		return nil, err
	}
	out := make([]headgate.ConcurrencyLimit, 0, len(raw))
	for name, encoded := range raw {
		var v redisConcurrencyLimit
		if err := json.Unmarshal([]byte(encoded), &v); err != nil {
			return nil, fmt.Errorf("headgate: invalid concurrency policy: %w", err)
		}
		if !v.OnSaturated.Valid() {
			return nil, fmt.Errorf("headgate: invalid saturation strategy `%s` in store", v.OnSaturated)
		}
		out = append(out, headgate.ConcurrencyLimit{
			Name: name, Queue: v.Queue, MaxConcurrent: v.MaxConcurrent,
			OnSaturated: v.OnSaturated,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// UpsertConcurrencyLimit creates or updates a concurrency limit.
func (s *RedisStore) UpsertConcurrencyLimit(ctx context.Context, cfg headgate.ConcurrencyLimit) error {
	if err := headgate.ValidateConcurrencyLimit(cfg); err != nil {
		return err
	}
	encoded, err := json.Marshal(redisConcurrencyLimit{
		Queue: cfg.Queue, MaxConcurrent: cfg.MaxConcurrent, OnSaturated: cfg.OnSaturated,
	})
	if err != nil {
		return err
	}
	return adminLua.Run(ctx, s.rdb, []string{s.prefix},
		"cl_upsert", cfg.Name, cfg.Queue, string(encoded)).Err()
}

// Partitions returns bounded fairness state for a queue's partitions.
func (s *RedisStore) Partitions(ctx context.Context, queue string) ([]headgate.PartitionState, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	active, err := s.scanSet(ctx, s.key("parts", queue))
	if err != nil {
		return nil, err
	}
	deficits, err := s.rdb.HGetAll(ctx, s.key("deficit", queue)).Result()
	if err != nil {
		return nil, err
	}
	parts := active
	for p := range deficits {
		if !slicesContains(parts, p) {
			parts = append(parts, p)
		}
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(parts))
	for i, p := range parts {
		cmds[i] = pipe.ZCount(ctx, s.key("pending", queue, p), "-inf", strconv.FormatInt(now, 10))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	out := make([]headgate.PartitionState, len(parts))
	for i, p := range parts {
		out[i] = headgate.PartitionState{
			PartitionKey: p,
			Deficit:      hnum(deficits, p),
			Waiting:      cmds[i].Val(),
		}
	}
	return out, nil
}

// QuarantineList returns quarantined fingerprints newest first.
func (s *RedisStore) QuarantineList(ctx context.Context) ([]headgate.QuarantineEntry, error) {
	fps, err := s.scanSet(ctx, s.key("quarantine"))
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(fps))
	for i, fp := range fps {
		keys[i] = s.key("qmeta", fp)
	}
	metas, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make([]headgate.QuarantineEntry, len(fps))
	for i, fp := range fps {
		m := metas[i]
		out[i] = headgate.QuarantineEntry{
			Fingerprint:     fp,
			Kind:            m["kind"],
			CrashCount:      hnum(m, "crash_count"),
			QuarantinedAtMs: hnum(m, "at_ms"),
			Reason:          m["reason"],
		}
	}
	sort.Slice(out, func(a, b int) bool {
		return out[a].QuarantinedAtMs > out[b].QuarantinedAtMs
	})
	return out, nil
}

// QuarantineRelease releases a fingerprint and makes its jobs available.
func (s *RedisStore) QuarantineRelease(ctx context.Context, fingerprint string) (uint64, error) {
	res, err := s.adminJobOp(ctx, "q_release", fingerprint)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 || res[0] != "OK" {
		// `not found: ` is load-bearing — see the note in headgatepgx/inspect.go.
		return 0, headgate.NotFoundf("fingerprint %s is not quarantined", fingerprint)
	}
	var n uint64
	if len(res) > 1 {
		n, _ = strconv.ParseUint(res[1], 10, 64)
	}
	return n, nil
}

// OperatorRetry makes a retryable terminal job available immediately.
func (s *RedisStore) OperatorRetry(ctx context.Context, id string) error {
	res, err := s.adminJobOp(ctx, "retry", id)
	if err != nil {
		return err
	}
	switch first(res) {
	case "OK":
		return nil
	case "NF":
		return headgate.NotFoundf("job %s", id)
	case "DUP":
		return &headgate.DuplicateError{ExistingID: second(res)}
	default:
		return headgate.Invalidf("operator_retry is only defined from archived, cancelled, or undecodable; job %s is %s",
			id, second(res))
	}
}

// OperatorCancel moves a nonterminal job to the cancelled state.
func (s *RedisStore) OperatorCancel(ctx context.Context, id string) error {
	res, err := s.adminJobOp(ctx, "cancel", id)
	if err != nil {
		return err
	}
	switch first(res) {
	case "OK":
		return nil
	case "NF":
		return headgate.NotFoundf("job %s", id)
	default:
		return headgate.Invalidf("operator_cancel is not defined from %s", second(res))
	}
}

// DeleteJob permanently removes a job that is not running.
func (s *RedisStore) DeleteJob(ctx context.Context, id string) error {
	res, err := s.adminJobOp(ctx, "delete", id)
	if err != nil {
		return err
	}
	switch first(res) {
	case "OK":
		return nil
	case "NF":
		return headgate.NotFoundf("job %s", id)
	default:
		return &headgate.InvalidError{Msg: "cannot delete a running job; cancel it first"}
	}
}

func first(res []string) string {
	if len(res) > 0 {
		return res[0]
	}
	return ""
}

func second(res []string) string {
	if len(res) > 1 {
		return res[1]
	}
	return "?"
}

// ExplainAdmission explains why a job is or is not currently admissible.
func (s *RedisStore) ExplainAdmission(ctx context.Context, id string) (*headgate.AdmissionExplain, error) {
	flat, err := explainLua.Run(ctx, s.rdb, []string{s.prefix}, id).StringSlice()
	if err != nil {
		return nil, err
	}
	if len(flat) == 0 {
		return nil, nil
	}
	kv := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		kv[flat[i]] = flat[i+1]
	}
	return assembleExplain(kv), nil
}

// assembleExplain replays THIS gate's evaluation order (admit.lua), read-only. An
// unconfigured rate class is unlimited and therefore never blocking.
func assembleExplain(kv map[string]string) *headgate.AdmissionExplain {
	num := func(key string) int64 {
		value, _ := strconv.ParseInt(kv[key], 10, 64)
		return value
	}
	optional := func(configured string, value int64) *int64 {
		if kv[configured] != "1" {
			return nil
		}
		return &value
	}
	return headgate.EvaluateAdmission(headgateshared.AdmissionFacts{
		State: kv["state"], NowMs: num("now"), ScheduledAtMs: num("scheduled_at_ms"),
		QueuePaused: kv["paused"] == "1", Quarantined: kv["quarantined"] == "1",
		Fingerprint: kv["fingerprint"], RateClass: kv["rate_class"], Weight: num("weight"),
		TokensAvailable: optional("rate_configured", num("tokens_available")),
		LimitPerWindow:  num("rate_limit"), WindowMs: num("rate_window"),
		MaxConcurrent: optional("concurrency_configured", num("max_concurrent")),
		Inflight:      num("inflight"), Saturation: kv["on_saturated"],
		Position: num("position_in_partition"), Deficit: num("partition_deficit"),
	})
}

// History returns queue traffic aggregated into fixed time buckets.
func (s *RedisStore) History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]headgate.HistoryBucket, error) {
	if bucketMs < 60_000 {
		return nil, &headgate.InvalidError{Msg: "bucket_ms must be >= 60000 (the stored granularity)"}
	}
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	// Counters carry a ~25h TTL; anything older is gone regardless of `since`.
	start := sinceMs
	if floor := now - histTTLMs; start < floor {
		start = floor
	}
	var keys []string
	var minutes []int64
	for m := start - start%60_000; m <= now; m += 60_000 {
		keys = append(keys, s.key("hist", queue, strconv.FormatInt(m, 10)))
		minutes = append(minutes, m)
	}
	hists, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, err
	}
	agg := map[int64]*headgate.HistoryBucket{}
	var order []int64
	for i, m := range minutes {
		a, c := hnum(hists[i], "arrived"), hnum(hists[i], "completed")
		if a == 0 && c == 0 {
			continue
		}
		b := m / bucketMs * bucketMs
		if agg[b] == nil {
			agg[b] = &headgate.HistoryBucket{AtMs: b}
			order = append(order, b)
		}
		agg[b].Arrived += a
		agg[b].Completed += c
	}
	sort.Slice(order, func(a, b int) bool { return order[a] < order[b] })
	out := make([]headgate.HistoryBucket, len(order))
	for i, b := range order {
		out[i] = *agg[b]
	}
	return out, nil
}

// QuarantineSweep moves eligible jobs with quarantined fingerprints into quarantine.
