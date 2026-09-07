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
	"github.com/mujhtech/headgate/go/headgateshared"
	"github.com/redis/go-redis/v9"
)

var (
	schedLua   = script("sched")
	workerLua  = script("worker")
	explainLua = script("explain")
)

// Queue-position/sampled lookups cap here; "position >= 1000" is answer enough.
const positionLimit = headgateshared.InspectionPositionLimit
const quietPartitionLimit = headgateshared.InspectionQuietPartitionLimit
const maxPage = headgateshared.InspectionMaxPage

// Offset pagination walks zsets; past this depth the cursor is refused (bounded).
const listDeepLimit = 10_000
const controlScanLimit = 10_000

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
var _ headgate.CheckpointInspectStore = (*RedisStore)(nil)
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

func (s *RedisStore) scanSet(ctx context.Context, key string) ([]string, error) {
	members := make([]string, 0, min(256, controlScanLimit))
	var cursor uint64
	for {
		batch, next, err := s.rdb.SScan(ctx, key, cursor, "", 256).Result()
		if err != nil {
			return nil, err
		}
		remaining := controlScanLimit - len(members)
		if len(batch) > remaining {
			batch = batch[:remaining]
		}
		members = append(members, batch...)
		if next == 0 || len(members) == controlScanLimit {
			return members, nil
		}
		cursor = next
	}
}

func (s *RedisStore) queueNames(ctx context.Context) ([]string, error) {
	qs, err := s.scanSet(ctx, s.key("queues"))
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

// GetJob returns a job summary, optionally including its payload and headers.
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

// GetJobResult returns a completed job's durable result, if present.
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

// GetJobOutput returns a job's latest durable output, if present.
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

// GetJobProgress returns a job's latest progress update, if present.
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

// GetJobCheckpoint returns a job's latest resumable checkpoint.
func (s *RedisStore) GetJobCheckpoint(ctx context.Context, id string) (*headgate.Checkpoint, error) {
	key := s.key("job", id)
	pipe := s.rdb.Pipeline()
	exists := pipe.Exists(ctx, key)
	raw := pipe.HGet(ctx, key, "checkpoint")
	cursor := pipe.HGet(ctx, key, "cp_cursor")
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if exists.Val() == 0 {
		return nil, nil
	}
	var cursorBytes []byte
	if err := cursor.Err(); err == nil {
		cursorBytes = []byte(cursor.Val())
	} else if !errors.Is(err, redis.Nil) {
		return nil, err
	}
	checkpoint := decodeCheckpoint(raw.Val(), cursorBytes)
	return &checkpoint, nil
}

// ListJobs returns a newest-first page matching f.
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

// Counts returns bounded per-state counts for one queue or all queues.
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

// QueueStats returns bounded operational statistics for known queues.
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
	pausedSet, err := s.scanSet(ctx, s.key("paused"))
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
		} else if !errors.Is(err, redis.Nil) {
			return nil, err
		}
		var maxUnfinishedJobs *uint64
		if raw, err := s.rdb.HGet(ctx, s.key("enqueue", q), "limit").Uint64(); err == nil {
			maxUnfinishedJobs = &raw
		} else if !errors.Is(err, redis.Nil) {
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
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
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
		} else if !errors.Is(err, redis.Nil) {
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
		v.QuietGroups.TimeToDrainMs = headgate.TimeToDrainMillis(
			quietBacklog, v.QuietGroups.ArrivalRate, v.QuietGroups.DrainRate,
		)
		if quietOldestAt != nil {
			age := headgate.AgeMillis(now, *quietOldestAt)
			v.QuietGroups.OldestAvailableMs = &age
		}
		// backlog metrics time-to-drain: nil when arrival >= drain — the alert condition.
		v.TimeToDrainMs = headgate.TimeToDrainMillis(backlog, arrival, drain)
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

// SetQueuePaused changes whether admission may draw from queue.
