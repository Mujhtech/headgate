package headgateredis

// control plane the inspection/control surface on Redis, Go side — the line-for-line port of the
// Rust adapter's src/inspect.rs. Reads lean on the indexes every Lua writer maintains
// (idx/fpi/qjobs/hist — see lua/admin.lua's header for the contract), so counts are
// exact ZCARDs and every read is bounded (invariant 6). Atomic writes go through the
// SHARED admin/sched/worker/explain scripts, so single-job ops, bulk batches, and the
// CAS paths have exactly one implementation across both languages. Error messages
// match the other backends word-for-word (the mutation-diff discipline).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/redis/go-redis/v9"
)

var (
	schedLua   = script("sched")
	workerLua  = script("worker")
	explainLua = script("explain")
)

// Queue-position/sampled lookups cap here; "position >= 1000" is answer enough.
const positionLimit = 1_000
const quietPartitionLimit = 1_000
const maxPage = 200

// Offset pagination walks zsets; past this depth the cursor is refused (bounded).
const listDeepLimit = 10_000

// Post-filtered listings hydrate at most this many candidates per call.
const filterScan = 2_000

// History buckets live ~25h (the TTL enqueue/ack set); reads clamp to that window.
const histTTLMs = 90_000_000

var inspectStates = []string{
	"pending", "available", "scheduled", "retryable", "running", "completed", "archived",
	"cancelled", "undecodable", "quarantined",
}

var _ headgate.InspectStore = (*RedisStore)(nil)
var _ headgate.ResultInspectStore = (*RedisStore)(nil)
var _ headgate.OutputInspectStore = (*RedisStore)(nil)
var _ headgate.ProgressInspectStore = (*RedisStore)(nil)

func (s *RedisStore) idx(queue, state string) string {
	return s.key("idx", queue, state)
}

func (s *RedisStore) storeNowMs(ctx context.Context) (int64, error) {
	t, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

func (s *RedisStore) queueNames(ctx context.Context) ([]string, error) {
	qs, err := s.rdb.SMembers(ctx, s.key("queues")).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(qs)
	return qs, nil
}

func (s *RedisStore) jobHashes(ctx context.Context, keys []string) ([]map[string]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.HGetAll(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	out := make([]map[string]string, len(keys))
	for i, c := range cmds {
		out[i] = c.Val()
	}
	return out, nil
}

// adminJobOp runs one of admin.lua's {'OK',...}|{'NF'}|{'ERR', state} single-job ops.
func (s *RedisStore) adminJobOp(ctx context.Context, args ...any) ([]string, error) {
	res, err := adminLua.Run(ctx, s.rdb, []string{s.prefix}, args...).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("headgate: unexpected admin reply %v", res)
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		sv, _ := v.(string)
		out = append(out, sv)
	}
	return out, nil
}

func jobFromHash(id string, h map[string]string, includePayload bool) *headgate.JobSummary {
	j := &headgate.JobSummary{
		ID:                 id,
		Kind:               h["kind"],
		Queue:              h["queue"],
		State:              h["state"],
		SchemaVersion:      uint32(hnum(h, "schema_version")),
		Priority:           int32(hnum(h, "priority")),
		Attempt:            uint32(hnum(h, "attempt")),
		CrashAttempt:       uint32(hnum(h, "crash_attempt")),
		MaxAttempts:        uint32(hnum(h, "max_attempts")),
		PartitionKey:       h["partition_key"],
		RateClass:          h["rate_class"],
		StickyWorker:       h["sticky_worker"],
		Weight:             headgate.EffectiveWeight(uint32(hnum(h, "weight"))),
		Fingerprint:        h["fingerprint"],
		EnqueuedAtMs:       hnum(h, "enqueued_at_ms"),
		ScheduledAtMs:      hnum(h, "scheduled_at_ms"),
		PeriodicScheduleID: h["periodic_schedule_id"],
		PeriodicTickMs:     hnum(h, "periodic_tick_ms"),
		ErrorsJSON:         h["errors"],
	}
	if _, ok := h["claimed_at_ms"]; ok {
		v := hnum(h, "claimed_at_ms")
		j.ClaimedAtMs = &v
	}
	if j.ErrorsJSON == "" {
		j.ErrorsJSON = "[]"
	}
	_ = json.Unmarshal([]byte(h["tags"]), &j.Tags)
	if _, ok := h["finalized_at_ms"]; ok {
		v := hnum(h, "finalized_at_ms")
		j.FinalizedAtMs = &v
	}
	if includePayload {
		j.Payload = []byte(h["payload"])
		j.Headers = headgate.DecodeHeaders([]byte(h["headers"]))
	}
	return j
}

// every test is `!= nil`, never `!= ""`. An explicitly empty value is a
// filter FOR the empty value — and on this backend that matters twice over, because a
// job hash simply OMITS an empty field, so `h["partition_key"]` reads "" for both "the
// default partition" and "no such field". `f.PartitionKey != nil && h[...] != ""` is
// therefore the correct comparison in both directions. See the JobFilter doc comment.
func matchesFilter(h map[string]string, f headgate.JobFilter) bool {
	var tags []string
	_ = json.Unmarshal([]byte(h["tags"]), &tags)
	set := map[string]bool{}
	for _, tag := range tags {
		set[tag] = true
	}
	for _, tag := range f.TagsAll {
		if !set[tag] {
			return false
		}
	}
	if len(f.TagsAny) > 0 {
		hit := false
		for _, tag := range f.TagsAny {
			if set[tag] {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if f.Kind != nil && h["kind"] != *f.Kind {
		return false
	}
	if f.KindPrefix != nil && !strings.HasPrefix(h["kind"], *f.KindPrefix) {
		return false
	}
	if f.PartitionKey != nil && h["partition_key"] != *f.PartitionKey {
		return false
	}
	if f.Fingerprint != nil && h["fingerprint"] != *f.Fingerprint {
		return false
	}
	if f.RateClass != nil && h["rate_class"] != *f.RateClass {
		return false
	}
	if f.Priority != nil && int32(hnum(h, "priority")) != *f.Priority {
		return false
	}
	return true
}

func (s *RedisStore) GetJob(ctx context.Context, id string, includePayload bool) (*headgate.JobSummary, error) {
	h, err := s.rdb.HGetAll(ctx, s.key("job", id)).Result()
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, nil
	}
	return jobFromHash(id, h, includePayload), nil
}

func (s *RedisStore) GetJobResult(ctx context.Context, id string) (*headgate.JobResult, error) {
	pipe := s.rdb.Pipeline()
	versionCmd := pipe.HGet(ctx, s.key("job", id), "result_schema_version")
	bytesCmd := pipe.HGet(ctx, s.key("job", id), "result_bytes")
	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	versionText, err := versionCmd.Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	version, err := strconv.ParseUint(versionText, 10, 32)
	if err != nil {
		return nil, err
	}
	bytes, err := bytesCmd.Bytes()
	if err != nil {
		return nil, err
	}
	return &headgate.JobResult{SchemaVersion: uint32(version), Bytes: bytes}, nil
}

func (s *RedisStore) GetJobOutput(ctx context.Context, id string) (*headgate.JobOutput, error) {
	pipe := s.rdb.Pipeline()
	versionCmd := pipe.HGet(ctx, s.key("job", id), "output_schema_version")
	bytesCmd := pipe.HGet(ctx, s.key("job", id), "output_bytes")
	fenceCmd := pipe.HGet(ctx, s.key("job", id), "output_fence")
	updatedCmd := pipe.HGet(ctx, s.key("job", id), "output_updated_at_ms")
	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	versionText, err := versionCmd.Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	version, err := strconv.ParseUint(versionText, 10, 32)
	if err != nil {
		return nil, err
	}
	bytes, err := bytesCmd.Bytes()
	if err != nil {
		return nil, err
	}
	fence, err := fenceCmd.Uint64()
	if err != nil {
		return nil, err
	}
	updatedAtMs, err := updatedCmd.Int64()
	if err != nil {
		return nil, err
	}
	return &headgate.JobOutput{
		SchemaVersion: uint32(version), Bytes: bytes, Fence: fence, UpdatedAtMs: updatedAtMs,
	}, nil
}

func (s *RedisStore) GetJobProgress(ctx context.Context, id string) (*headgate.JobProgress, error) {
	values, err := s.rdb.HMGet(ctx, s.key("job", id),
		"progress_current", "progress_total", "progress_message", "progress_fence", "progress_updated_at_ms").Result()
	if err != nil {
		return nil, err
	}
	if len(values) != 5 || values[0] == nil {
		return nil, nil
	}
	parse := func(index int, name string) (uint64, error) {
		text := fmt.Sprint(values[index])
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("headgate: invalid %s %q: %w", name, text, err)
		}
		return value, nil
	}
	current, err := parse(0, "progress current")
	if err != nil {
		return nil, err
	}
	total, err := parse(1, "progress total")
	if err != nil {
		return nil, err
	}
	fence, err := parse(3, "progress fence")
	if err != nil {
		return nil, err
	}
	updated, err := parse(4, "progress timestamp")
	if err != nil {
		return nil, err
	}
	return &headgate.JobProgress{
		Current: current, Total: total, Message: fmt.Sprint(values[2]),
		Fence: fence, UpdatedAtMs: int64(updated),
	}, nil
}

func (s *RedisStore) ListJobs(ctx context.Context, f headgate.JobFilter, cursor string, limit uint32) (headgate.JobPage, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > maxPage {
		limit = maxPage
	}
	// An id filter is a point lookup, not a scan. (`id:` is the one field where an
	// explicit "" can never match anything, because no job has an empty id — but the
	// lookup is still performed rather than skipped, so the answer is an empty page and
	// not the whole queue.)
	if f.ID != nil {
		h, err := s.rdb.HGetAll(ctx, s.key("job", *f.ID)).Result()
		if err != nil {
			return headgate.JobPage{}, err
		}
		var jobs []headgate.JobSummary
		if len(h) > 0 &&
			(f.Queue == nil || *f.Queue == h["queue"]) &&
			(f.State == nil || *f.State == h["state"]) &&
			matchesFilter(h, f) {
			jobs = append(jobs, *jobFromHash(*f.ID, h, false))
		}
		return headgate.JobPage{Jobs: jobs}, nil
	}
	offset := 0
	if cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil || n < 0 {
			return headgate.JobPage{}, &headgate.InvalidError{Msg: "bad cursor"}
		}
		offset = n
	}
	if offset+int(limit) > listDeepLimit {
		return headgate.JobPage{}, fmt.Errorf(
			"headgate: cursor too deep: offset pagination is bounded at %d", listDeepLimit)
	}
	var queues []string
	if f.Queue != nil {
		queues = []string{*f.Queue}
	} else {
		var err error
		if queues, err = s.queueNames(ctx); err != nil {
			return headgate.JobPage{}, err
		}
	}
	states := inspectStates
	if f.State != nil {
		states = []string{*f.State}
	}
	filtered := f.Kind != nil || f.KindPrefix != nil || f.PartitionKey != nil ||
		f.Fingerprint != nil || f.RateClass != nil || f.Priority != nil || len(f.TagsAll) > 0 || len(f.TagsAny) > 0
	scanCap := int(limit)
	if filtered {
		scanCap = filterScan
	}
	// Merge the newest offset+scanCap of every (queue, state) zset, newest first
	// (score desc, id desc breaks ties — deterministic across calls).
	need := int64(offset + scanCap)
	pipe := s.rdb.Pipeline()
	var cmds []*redis.ZSliceCmd
	for _, q := range queues {
		for _, st := range states {
			cmds = append(cmds, pipe.ZRevRangeWithScores(ctx, s.idx(q, st), 0, need-1))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return headgate.JobPage{}, err
	}
	type entry struct {
		score int64
		id    string
	}
	var merged []entry
	for _, c := range cmds {
		for _, z := range c.Val() {
			id, _ := z.Member.(string)
			merged = append(merged, entry{int64(z.Score), id})
		}
	}
	sort.Slice(merged, func(a, b int) bool {
		if merged[a].score != merged[b].score {
			return merged[a].score > merged[b].score
		}
		return merged[a].id > merged[b].id
	})
	total := len(merged)
	end := offset + scanCap
	if end > total {
		end = total
	}
	var candidates []string
	if offset < total {
		for _, e := range merged[offset:end] {
			candidates = append(candidates, e.id)
		}
	}
	keys := make([]string, len(candidates))
	for i, id := range candidates {
		keys[i] = s.key("job", id)
	}
	hashes, err := s.jobHashes(ctx, keys)
	if err != nil {
		return headgate.JobPage{}, err
	}
	var jobs []headgate.JobSummary
	consumed := 0
	for i, id := range candidates {
		consumed++
		h := hashes[i]
		if len(h) == 0 || !matchesFilter(h, f) {
			continue
		}
		jobs = append(jobs, *jobFromHash(id, h, false))
		if len(jobs) == int(limit) {
			break
		}
	}
	nextOffset := offset + consumed
	page := headgate.JobPage{Jobs: jobs}
	bound := total
	if bound > listDeepLimit {
		bound = listDeepLimit
	}
	if nextOffset < bound {
		page.NextCursor = strconv.Itoa(nextOffset)
	}
	return page, nil
}

func (s *RedisStore) Counts(ctx context.Context, queue *string) (headgate.StateCounts, error) {
	// nil = every queue; a pointer to "" = the queue literally named "". See headgatepgx.
	var queues []string
	if queue != nil {
		queues = []string{*queue}
	} else {
		var err error
		if queues, err = s.queueNames(ctx); err != nil {
			return headgate.StateCounts{}, err
		}
	}
	pipe := s.rdb.Pipeline()
	var cmds []*redis.IntCmd
	for _, q := range queues {
		for _, st := range inspectStates {
			cmds = append(cmds, pipe.ZCard(ctx, s.idx(q, st)))
		}
	}
	if len(cmds) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return headgate.StateCounts{}, err
		}
	}
	// The index zsets make these exact ZCARDs, never a scan.
	counts := map[string]int64{}
	for i, c := range cmds {
		if n := c.Val(); n > 0 {
			counts[inspectStates[i%len(inspectStates)]] += n
		}
	}
	return headgate.StateCounts{Counts: counts, Approximate: false}, nil
}

func (s *RedisStore) QueueStats(ctx context.Context) ([]headgate.QueueStatsView, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	curBucket := now - now%60_000
	queues, err := s.queueNames(ctx)
	if err != nil {
		return nil, err
	}
	pausedSet, err := s.rdb.SMembers(ctx, s.key("paused")).Result()
	if err != nil {
		return nil, err
	}
	paused := map[string]bool{}
	for _, q := range pausedSet {
		paused[q] = true
		if !slicesContains(queues, q) {
			queues = append(queues, q)
		}
	}
	sort.Strings(queues)
	out := make([]headgate.QueueStatsView, 0, len(queues))
	for _, q := range queues {
		weight := uint32(1)
		if raw, err := s.rdb.HGet(ctx, s.key("qweights"), q).Uint64(); err == nil {
			weight = uint32(raw)
		} else if err != redis.Nil {
			return nil, err
		}
		var maxUnfinishedJobs *uint64
		if raw, err := s.rdb.HGet(ctx, s.key("enqueue", q), "limit").Uint64(); err == nil {
			maxUnfinishedJobs = &raw
		} else if err != redis.Nil {
			return nil, err
		}
		pipe := s.rdb.Pipeline()
		var zcards []*redis.IntCmd
		for _, st := range inspectStates {
			zcards = append(zcards, pipe.ZCard(ctx, s.idx(q, st)))
		}
		prev := pipe.HGetAll(ctx, s.key("hist", q, strconv.FormatInt(curBucket-60_000, 10)))
		cur := pipe.HGetAll(ctx, s.key("hist", q, strconv.FormatInt(curBucket, 10)))
		oldest := pipe.ZRangeWithScores(ctx, s.idx(q, "available"), 0, 0)
		memory := pipe.HGet(ctx, s.key("mem", q), "bytes")
		// The optional cached memory sample is an HGET in this pipeline. A queue
		// without a sample returns redis.Nil for that command; that means
		// memory_bytes is unknown, not that the whole queue-stats request failed.
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, err
		}
		byState := map[string]int64{}
		var backlog int64
		for i, c := range zcards {
			st := inspectStates[i]
			if n := c.Val(); n > 0 {
				byState[st] = n
				switch st {
				case "available", "scheduled", "retryable", "running":
					backlog += n
				}
			}
		}
		// backlog metrics rates over the last minute, from the same counters History reads.
		arrived := hnum(prev.Val(), "arrived") + hnum(cur.Val(), "arrived")
		completed := hnum(prev.Val(), "completed") + hnum(cur.Val(), "completed")
		arrival, drain := float64(arrived)/60.0, float64(completed)/60.0
		v := headgate.QueueStatsView{
			Queue: q, Weight: weight, ByState: byState, CountsApproximate: false,
			ArrivalRate: arrival, DrainRate: drain, Paused: paused[q],
			UnfinishedJobs: uint64(max(backlog, 0)), MaxUnfinishedJobs: maxUnfinishedJobs,
		}
		if raw, err := memory.Uint64(); err == nil {
			v.MemoryBytes = &raw
		} else if err != redis.Nil {
			return nil, err
		}
		if zs := oldest.Val(); len(zs) > 0 {
			age := max(now-int64(zs[0].Score), 0)
			v.OldestAvailableMs = &age
		}
		metricPartsKey := s.key("metricparts", q)
		partCount, err := s.rdb.ZCard(ctx, metricPartsKey).Result()
		if err != nil {
			return nil, err
		}
		parts, err := s.rdb.ZRevRange(ctx, metricPartsKey, 0, quietPartitionLimit).Result()
		if err != nil {
			return nil, err
		}
		qp := s.rdb.Pipeline()
		inflightCmds := make([]*redis.SliceCmd, 0, len(parts))
		waitingCmds := make([]*redis.IntCmd, 0, len(parts))
		oldestCmds := make([]*redis.ZSliceCmd, 0, len(parts))
		prevPartHists := make([]*redis.MapStringStringCmd, 0, len(parts))
		curPartHists := make([]*redis.MapStringStringCmd, 0, len(parts))
		for _, part := range parts {
			inflightCmds = append(inflightCmds, qp.HMGet(ctx, s.key("inflight", q), part))
			waitingCmds = append(waitingCmds, qp.ZCard(ctx, s.key("pending", q, part)))
			oldestCmds = append(oldestCmds, qp.ZRangeWithScores(ctx, s.key("avail", q, part), 0, 0))
			prevPartHists = append(prevPartHists, qp.HGetAll(ctx,
				s.key("histp", q, part, strconv.FormatInt(curBucket-60_000, 10))))
			curPartHists = append(curPartHists, qp.HGetAll(ctx,
				s.key("histp", q, part, strconv.FormatInt(curBucket, 10))))
		}
		if len(parts) > 0 {
			if _, err := qp.Exec(ctx); err != nil {
				return nil, err
			}
		}
		loads := make(map[string]int64, len(parts))
		inflight := make([]int64, len(parts))
		for i, part := range parts {
			vals := inflightCmds[i].Val()
			if len(vals) > 0 && vals[0] != nil {
				inflight[i], _ = strconv.ParseInt(fmt.Sprint(vals[0]), 10, 64)
			}
			loads[part] = inflight[i]
		}
		noisy := headgate.NoisyPartitionKeys(loads)
		var quietArrived, quietCompleted, quietBacklog int64
		var quietOldestAt *int64
		for i, part := range parts {
			if noisy[part] {
				continue
			}
			quietArrived += hnum(prevPartHists[i].Val(), "arrived") + hnum(curPartHists[i].Val(), "arrived")
			quietCompleted += hnum(prevPartHists[i].Val(), "completed") + hnum(curPartHists[i].Val(), "completed")
			quietBacklog += waitingCmds[i].Val() + max(inflight[i], 0)
			if zs := oldestCmds[i].Val(); len(zs) > 0 {
				at := int64(zs[0].Score)
				if quietOldestAt == nil || at < *quietOldestAt {
					quietOldestAt = &at
				}
			}
		}
		v.QuietGroups = headgate.QuietGroupMetrics{
			ArrivalRate: float64(quietArrived) / 60.0, DrainRate: float64(quietCompleted) / 60.0,
			NoisyPartitions: uint32(len(noisy)), Approximate: partCount > quietPartitionLimit,
		}
		if v.QuietGroups.DrainRate > v.QuietGroups.ArrivalRate && v.QuietGroups.DrainRate > 0 {
			ttd := int64(float64(quietBacklog) / (v.QuietGroups.DrainRate - v.QuietGroups.ArrivalRate) * 1000.0)
			v.QuietGroups.TimeToDrainMs = &ttd
		}
		if quietOldestAt != nil {
			age := max(now-*quietOldestAt, 0)
			v.QuietGroups.OldestAvailableMs = &age
		}
		// backlog metrics time-to-drain: nil when arrival >= drain — the alert condition.
		if drain > arrival && drain > 0 {
			ttd := int64(float64(backlog) / (drain - arrival) * 1000.0)
			v.TimeToDrainMs = &ttd
		}
		out = append(out, v)
	}
	return out, nil
}

func slicesContains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

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

func (s *RedisStore) RateClasses(ctx context.Context) ([]headgate.RateClassState, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	names, err := s.rdb.SMembers(ctx, s.key("rate_classes")).Result()
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

func (s *RedisStore) UpsertRateClass(ctx context.Context, cfg headgate.RateClassConfig) error {
	if cfg.WindowMs < 1 {
		return &headgate.InvalidError{Msg: "window_ms must be >= 1"}
	}
	if cfg.Limit < 0 || cfg.Burst < 1 {
		return &headgate.InvalidError{Msg: "limit must be >= 0 and burst >= 1"}
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

func (s *RedisStore) UpsertConcurrencyLimit(ctx context.Context, cfg headgate.ConcurrencyLimit) error {
	if cfg.Name == "" || cfg.Queue == "" {
		return &headgate.InvalidError{Msg: "name and queue must not be empty"}
	}
	if cfg.MaxConcurrent == 0 {
		return &headgate.InvalidError{Msg: "max_concurrent must be >= 1"}
	}
	if !cfg.OnSaturated.Valid() {
		return &headgate.InvalidError{Msg: fmt.Sprintf("unknown saturation strategy `%s`", cfg.OnSaturated)}
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

func (s *RedisStore) Partitions(ctx context.Context, queue string) ([]headgate.PartitionState, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	active, err := s.rdb.SMembers(ctx, s.key("parts", queue)).Result()
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

func (s *RedisStore) QuarantineList(ctx context.Context) ([]headgate.QuarantineEntry, error) {
	fps, err := s.rdb.SMembers(ctx, s.key("quarantine")).Result()
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
	default:
		return headgate.Invalidf("operator_retry is only defined from archived; job %s is %s",
			id, second(res))
	}
}

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
	num := func(k string) int64 { n, _ := strconv.ParseInt(kv[k], 10, 64); return n }
	state := kv["state"]
	now := num("now")
	ex := &headgate.AdmissionExplain{State: state, Detail: map[string]string{"state": state}}
	zero := int64(0)
	switch state {
	case "running":
		ex.Admissible = true
		ex.EstimatedAdmissionMs = &zero
		return ex
	case "scheduled", "retryable":
		at := num("scheduled_at_ms")
		ex.Detail["scheduled_at_ms"] = kv["scheduled_at_ms"]
		ex.BlockedBy = "schedule"
		eta := at - now
		if eta < 0 {
			eta = 0
		}
		ex.EstimatedAdmissionMs = &eta
		return ex
	case "quarantined":
		ex.BlockedBy = "quarantine"
		return ex // will not clear on its own
	case "available":
	default:
		return ex // terminal: not admissible, nothing blocks
	}
	if kv["paused"] == "1" {
		ex.BlockedBy = "queue_paused"
		return ex
	}
	if at := num("scheduled_at_ms"); at > now {
		ex.Detail["scheduled_at_ms"] = kv["scheduled_at_ms"]
		ex.BlockedBy = "schedule"
		eta := at - now
		ex.EstimatedAdmissionMs = &eta
		return ex
	}
	if kv["quarantined"] == "1" {
		ex.Detail["fingerprint"] = kv["fingerprint"]
		ex.BlockedBy = "quarantine"
		return ex
	}
	if rc := kv["rate_class"]; rc != "" && kv["rate_configured"] == "1" {
		avail := num("tokens_available")
		cost := num("weight")
		if cost < 1 {
			cost = 1
		}
		ex.Detail["rate_class"] = rc
		ex.Detail["tokens_available"] = kv["tokens_available"]
		ex.Detail["weight"] = strconv.FormatInt(cost, 10)
		if avail < cost {
			ex.BlockedBy = "rate_class"
			if limit := num("rate_limit"); limit > 0 {
				need := cost - avail
				if need < 1 {
					need = 1
				}
				eta := need * num("rate_window") / limit
				ex.EstimatedAdmissionMs = &eta
			} // paused class: nil — will not clear on its own
			return ex
		}
	}
	// Fairness never blocks outright (invariant 11); position says when.
	ex.Detail["position_in_partition"] = kv["position_in_partition"]
	ex.Detail["partition_deficit"] = kv["partition_deficit"]
	if kv["concurrency_configured"] == "1" {
		maxConcurrent, inflight := num("max_concurrent"), num("inflight")
		ex.Detail["max_concurrent"] = kv["max_concurrent"]
		ex.Detail["inflight"] = kv["inflight"]
		ex.Detail["on_saturated"] = kv["on_saturated"]
		if inflight >= maxConcurrent && kv["on_saturated"] != "cancel_running" {
			ex.BlockedBy = "concurrency_limit"
			return ex
		}
	}
	ex.Admissible = true
	ex.EstimatedAdmissionMs = &zero
	return ex
}

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

func (s *RedisStore) QuarantineSweep(ctx context.Context, limit int64) (int64, error) {
	return adminLua.Run(ctx, s.rdb, []string{s.prefix}, "q_sweep", limit).Int64()
}

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

func (s *RedisStore) UpsertSchedule(ctx context.Context, e headgate.ScheduleEntry) error {
	paused := "0"
	if e.Paused {
		paused = "1"
	}
	return schedLua.Run(ctx, s.rdb, []string{s.prefix},
		"upsert", e.ID, e.Kind, string(e.Payload), e.Queue, e.PartitionKey, e.RateClass,
		e.Priority, e.MaxAttempts, e.RetentionMs, e.Spec, e.NextRunMs,
		missedName(e.OnMissed), e.BackfillLimit, paused).Err()
}

func missedName(p headgate.MissedPolicy) string {
	switch p {
	case headgate.MissedRunOnce:
		return "run_once"
	case headgate.MissedBackfill:
		return "backfill"
	default:
		return "skip"
	}
}

func (s *RedisStore) DeleteSchedule(ctx context.Context, id string) error {
	n, err := schedLua.Run(ctx, s.rdb, []string{s.prefix}, "delete", id).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		return headgate.NotFoundf("schedule %s", id)
	}
	return nil
}

func scheduleFromHash(id string, h map[string]string) headgate.ScheduleEntry {
	e := headgate.ScheduleEntry{
		ID: id, Kind: h["kind"], Payload: []byte(h["payload"]),
		Queue: h["queue"], PartitionKey: h["partition_key"], RateClass: h["rate_class"],
		Priority: int32(hnum(h, "priority")), MaxAttempts: uint32(hnum(h, "max_attempts")),
		RetentionMs: hnum(h, "retention_ms"), Spec: h["spec"],
		NextRunMs: hnum(h, "next_run_ms"), BackfillLimit: uint32(hnum(h, "backfill_limit")),
		Paused: h["paused"] == "1",
	}
	if _, ok := h["last_enqueued_ms"]; ok {
		v := hnum(h, "last_enqueued_ms")
		e.LastEnqueued = &v
	}
	switch h["on_missed"] {
	case "run_once":
		e.OnMissed = headgate.MissedRunOnce
	case "backfill":
		e.OnMissed = headgate.MissedBackfill
	default:
		e.OnMissed = headgate.MissedSkip
	}
	return e
}

func (s *RedisStore) ListSchedules(ctx context.Context) ([]headgate.ScheduleEntry, error) {
	ids, err := s.rdb.ZRange(ctx, s.key("schedules"), 0, 9_999).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.key("schedule", id)
	}
	hashes, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, err
	}
	var out []headgate.ScheduleEntry
	for i, id := range ids {
		if len(hashes[i]) > 0 {
			out = append(out, scheduleFromHash(id, hashes[i]))
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (s *RedisStore) DueSchedules(ctx context.Context, limit int64) ([]headgate.ScheduleEntry, int64, error) {
	flat, err := schedLua.Run(ctx, s.rdb, []string{s.prefix}, "due", limit).StringSlice()
	if err != nil {
		return nil, 0, err
	}
	if len(flat) == 0 {
		return nil, 0, nil
	}
	now, _ := strconv.ParseInt(flat[0], 10, 64)
	ids := flat[1:]
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.key("schedule", id)
	}
	hashes, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, 0, err
	}
	var due []headgate.ScheduleEntry
	for i, id := range ids {
		if len(hashes[i]) > 0 {
			due = append(due, scheduleFromHash(id, hashes[i]))
		}
	}
	return due, now, nil
}

func (s *RedisStore) AdvanceSchedule(ctx context.Context, id string, fromNextRunMs, toNextRunMs int64) (bool, error) {
	n, err := schedLua.Run(ctx, s.rdb, []string{s.prefix},
		"advance", id, fromNextRunMs, toNextRunMs).Int64()
	return n == 1, err
}

func (s *RedisStore) RecordScheduleEvent(ctx context.Context, event headgate.ScheduleEvent) error {
	if !event.Outcome.Valid() {
		return headgate.Invalidf("invalid schedule event outcome")
	}
	if len(event.Reason) > 64 {
		return headgate.Invalidf("schedule event reason exceeds 64 bytes")
	}
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return fmt.Errorf("reading store time for schedule event: %w", err)
	}
	event.RecordedAtMs = now
	eventID, err := s.rdb.Incr(ctx, s.key("schedule-event-seq")).Uint64()
	if err != nil {
		return fmt.Errorf("allocating schedule event id: %w", err)
	}
	event.EventID = eventID
	encoded, err := json.Marshal(map[string]any{
		"event_id":       event.EventID,
		"schedule_id":    event.ScheduleID,
		"tick_ms":        event.TickMs,
		"job_id":         event.JobID,
		"outcome":        event.Outcome,
		"reason":         event.Reason,
		"recorded_at_ms": event.RecordedAtMs,
	})
	if err != nil {
		return fmt.Errorf("encoding schedule event: %w", err)
	}
	key := s.key("schedule-events", event.ScheduleID)
	// Negative ranks outside a short set clamp to zero, so pruning with a fixed
	// `0, -101` range would delete early events before the set reaches its limit.
	const appendAndPrune = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
local excess = redis.call('ZCARD', KEYS[1]) - tonumber(ARGV[3])
if excess > 0 then redis.call('ZREMRANGEBYRANK', KEYS[1], 0, excess - 1) end
return excess`
	if err := s.rdb.Eval(ctx, appendAndPrune, []string{key}, event.EventID, encoded, headgate.ScheduleEventLimit).Err(); err != nil {
		return fmt.Errorf("recording schedule event: %w", err)
	}
	return nil
}

func (s *RedisStore) ListScheduleEvents(ctx context.Context, scheduleID string, beforeEventID uint64, limit uint32) ([]headgate.ScheduleEvent, error) {
	if limit == 0 || limit > headgate.ScheduleEventLimit {
		return nil, headgate.Invalidf("schedule event limit must be between 1 and 100")
	}
	max := "+inf"
	if beforeEventID != 0 {
		max = "(" + strconv.FormatUint(beforeEventID, 10)
	}
	values, err := s.rdb.ZRevRangeByScore(ctx, s.key("schedule-events", scheduleID), &redis.ZRangeBy{
		Max: max, Min: "-inf", Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]headgate.ScheduleEvent, 0, len(values))
	for _, value := range values {
		var raw struct {
			EventID      uint64                        `json:"event_id"`
			ScheduleID   string                        `json:"schedule_id"`
			TickMs       int64                         `json:"tick_ms"`
			JobID        string                        `json:"job_id"`
			Outcome      headgate.ScheduleEventOutcome `json:"outcome"`
			Reason       string                        `json:"reason"`
			RecordedAtMs int64                         `json:"recorded_at_ms"`
		}
		if err := json.Unmarshal([]byte(value), &raw); err != nil {
			return nil, fmt.Errorf("decoding stored schedule event: %w", err)
		}
		out = append(out, headgate.ScheduleEvent{
			EventID: raw.EventID, ScheduleID: raw.ScheduleID, TickMs: raw.TickMs, JobID: raw.JobID,
			Outcome: raw.Outcome, Reason: raw.Reason, RecordedAtMs: raw.RecordedAtMs,
		})
	}
	return out, nil
}

func (s *RedisStore) HeartbeatWorker(ctx context.Context, w headgate.WorkerMeta) (string, error) {
	cmd, err := workerLua.Run(ctx, s.rdb, []string{s.prefix},
		"beat", w.WorkerID, w.Host, w.PID, strings.Join(w.Queues, ","),
		w.Concurrency, w.StartedAtMs,
		// ADDITIVE trailing args on worker.lua's beat.
		w.Inflight, w.Polls, w.EmptyPolls).Text()
	if err != nil && errors.Is(err, redis.Nil) {
		return "", nil
	}
	return cmd, err
}

func (s *RedisStore) ListWorkers(ctx context.Context, staleAfterMs int64) ([]headgate.WorkerMeta, error) {
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return nil, err
	}
	ids, err := s.rdb.SMembers(ctx, s.key("workers")).Result()
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
		})
	}
	return out, nil
}

func (s *RedisStore) SignalWorker(ctx context.Context, workerID, command string) error {
	if command != "" && command != "quiet" && command != "resume" && command != "restart" && command != "terminate" && command != "resign" {
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

func (s *RedisStore) DistinctKinds(ctx context.Context, limit int64) ([]string, error) {
	cap := limit
	if cap < 1 {
		cap = 1
	}
	if cap > positionLimit {
		cap = positionLimit
	}
	queues, err := s.queueNames(ctx)
	if err != nil || len(queues) == 0 {
		return nil, err
	}
	pipe := s.rdb.Pipeline()
	var cmds []*redis.StringSliceCmd
	for _, q := range queues {
		for _, st := range []string{"available", "scheduled", "retryable"} {
			cmds = append(cmds, pipe.ZRange(ctx, s.idx(q, st), 0, cap-1))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	var sample []string
	for _, c := range cmds {
		for _, id := range c.Val() {
			if int64(len(sample)) >= cap {
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
func opStates(action string) ([]string, bool) {
	switch action {
	case "retry":
		return []string{"archived"}, true
	case "cancel":
		return []string{"scheduled", "available", "running"}, true
	case "delete":
		return []string{"scheduled", "available", "retryable", "completed", "archived",
			"cancelled", "quarantined", "undecodable"}, true
	default:
		return nil, false
	}
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

func (s *RedisStore) CreateOperation(ctx context.Context, req headgate.BulkOp) error {
	if req.Queue == "" && req.State == "" && req.Kind == "" && req.PartitionKey == "" &&
		req.OlderThanMs == nil {
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

func (s *RedisStore) SampleQueueMemory(ctx context.Context, limit uint32) (uint32, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 1000 {
		limit = 1000
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
		if err != nil && err != redis.Nil {
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
