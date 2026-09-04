// Package headgateredis is the Go Redis driver — the fourth corner of the conformance
// square (Rust/Go × Postgres/Redis). Every operation is one Lua script and the scripts
// are the SHARED TESTED ARTIFACTS: byte-for-byte copies of crates/headgate-redis/lua/*
// (drift-gated by scripts/verify.sh, exactly like admit.sql), so this file is thin
// invocation and parsing — the semantics live server-side where both languages read
// the identical text. Time comes from redis.call('TIME') inside every script; a caller
// clock is never trusted (boundary validation).
//
// Capability honesty (runtime capability boundary): Store + NotifyingStore (pub/sub, Connect-created stores).
// No TransactionalStore (structurally impossible on Redis) and no InspectStore yet in
// Go (the Rust adapter has it; the port is pending here).
package headgateredis

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/*.lua
var luaFS embed.FS

func script(name string) *redis.Script {
	src, err := luaFS.ReadFile("lua/" + name + ".lua")
	if err != nil {
		panic("headgateredis: missing embedded script " + name)
	}
	return redis.NewScript(string(src))
}

var (
	admitLua      = script("admit")
	enqueueLua    = script("enqueue")
	ackLua        = script("ack")
	renewLua      = script("renew")
	checkpointLua = script("checkpoint")
	reclaimLua    = script("reclaim")
	promoteLua    = script("promote")
	dutyLua       = script("duty")
	adminLua      = script("admin")
	outputLua     = script("output")
	progressLua   = script("progress")
)

type Options struct {
	// CrashLimit is the crash quarantine quarantine threshold (default 3).
	CrashLimit int64
	// RetryBaseMs/RetryCapMs shape the default retry backoff (defaults 1000 / 1h).
	RetryBaseMs, RetryCapMs int64
}

func defaults(o Options) Options {
	if o.CrashLimit == 0 {
		o.CrashLimit = 3
	}
	if o.RetryBaseMs == 0 {
		o.RetryBaseMs = 1000
	}
	if o.RetryCapMs == 0 {
		o.RetryCapMs = 3_600_000
	}
	return o
}

type RedisStore struct {
	rdb       redis.UniversalClient
	prefix    string
	opts      Options
	wake      *waker // nil on client-supplied stores: no URL, no pub/sub, honest Caps
	owned     bool
	ownedWake bool
}

var _ headgate.Store = (*RedisStore)(nil)
var _ headgate.ResultStore = (*RedisStore)(nil)
var _ headgate.OutputStore = (*RedisStore)(nil)
var _ headgate.ProgressStore = (*RedisStore)(nil)

// New wraps a caller-owned client (failure classification — never closed by this package). Push wakeup
// needs its own pub/sub connection; enable it with WithWake or use Connect.
func New(rdb redis.UniversalClient, prefix string) *RedisStore {
	return NewWithOptions(rdb, prefix, Options{})
}

func NewWithOptions(rdb redis.UniversalClient, prefix string, o Options) *RedisStore {
	return &RedisStore{rdb: rdb, prefix: prefix, opts: defaults(o)}
}

// Connect opens a client from a URL — push wakeup enabled.
func Connect(url, prefix string) (*RedisStore, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("headgate: bad redis url: %w", err)
	}
	s := New(redis.NewClient(opt), prefix)
	s.owned = true
	s.WithWake(redis.NewClient(opt))
	s.ownedWake = true
	return s, nil
}

// ConnectSentinel uses go-redis's failover client, which re-resolves the named master
// after a Sentinel promotion. The same client supplies pub/sub wakeups.
func ConnectSentinel(masterName string, sentinelAddrs []string, password string, db int, prefix string) (*RedisStore, error) {
	if masterName == "" || len(sentinelAddrs) == 0 {
		return nil, errors.New("headgate: sentinel master name and at least one address are required")
	}
	client := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: masterName, SentinelAddrs: sentinelAddrs, Password: password, DB: db})
	store := New(client, prefix).WithWake(client)
	store.owned = true
	return store, nil
}

// Close stops pub/sub wakeups and closes clients created by Connect/ConnectSentinel.
// New and WithWake continue to treat caller-supplied clients as caller-owned.
func (s *RedisStore) Close() error {
	if s == nil {
		return nil
	}
	if s.wake != nil {
		s.wake.close()
		if s.ownedWake {
			_ = s.wake.rdb.Close()
		}
	}
	if s.owned {
		return s.rdb.Close()
	}
	return nil
}

// ConnectCluster requires one explicit Redis hash tag in the installation prefix.
// Every admission script touches fleet-global and queue-local keys atomically; the tag
// deliberately puts the entire Headgate installation in one slot.
func ConnectCluster(addrs []string, prefix string) (*RedisStore, error) {
	if len(addrs) == 0 {
		return nil, errors.New("headgate: at least one Redis Cluster address is required")
	}
	if err := validateClusterPrefix(prefix); err != nil {
		return nil, err
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	return New(client, prefix).WithWake(client), nil
}

func validateClusterPrefix(prefix string) error {
	open := strings.IndexByte(prefix, '{')
	close := strings.IndexByte(prefix, '}')
	if open < 0 || close <= open+1 || strings.Contains(prefix[close+1:], "{") {
		return errors.New("headgate: Redis Cluster prefix must contain exactly one non-empty hash tag, for example headgate:{fleet}")
	}
	return nil
}

func (s *RedisStore) key(parts ...string) string {
	return s.prefix + ":" + strings.Join(parts, ":")
}

func encodeCheckpoint(cp headgate.Checkpoint) string {
	return string(headgateshared.EncodeCheckpoint(cp))
}

func decodeCheckpoint(raw string, cursor []byte) headgate.Checkpoint {
	return headgateshared.DecodeCheckpoint([]byte(raw), cursor)
}

// parseTagged maps the scripts' {'OK',...}|{'DUP',id}|{'QUAR',fp}|{'REJ'}|{'ERR',msg}
// replies onto the same typed errors every other adapter returns.
func parseTagged(res any) error {
	arr, ok := res.([]any)
	if !ok || len(arr) == 0 {
		return fmt.Errorf("headgate: unexpected script reply %v", res)
	}
	tag, _ := arr[0].(string)
	arg := ""
	if len(arr) > 1 {
		arg, _ = arr[1].(string)
	}
	switch tag {
	case "OK":
		return nil
	case "DUP":
		return &headgate.DuplicateError{ExistingID: arg}
	case "DUPR":
		return &headgate.DuplicateError{ExistingID: arg, Replaced: true}
	case "IDC":
		// idempotent enqueue identity enqueue.lua's id pass rejected the batch: the id names a row whose
		// content differs.
		return &headgate.IDConflictError{JobID: arg}
	case "QUAR":
		return &headgate.QuarantinedError{Fingerprint: arg}
	case "BACK":
		field := func(i int) uint64 {
			if len(arr) <= i {
				return 0
			}
			raw, _ := arr[i].(string)
			n, _ := strconv.ParseUint(raw, 10, 64)
			return n
		}
		return &headgate.BackpressureError{Queue: arg, Limit: field(2), Current: field(3), Incoming: field(4)}
	case "REJ":
		return &headgate.LeaseRejectedError{JobID: arg}
	case "ERR":
		// Rust maps this tag to StoreError::Invalid; typed the same way here so a Lua
		// rejection is a 400 at the API rather than falling through to the 500 that
		// made the default for an unclassifiable store error.
		return &headgate.InvalidError{Msg: arg}
	default:
		return fmt.Errorf("headgate: unexpected script reply `%s`", tag)
	}
}

func hnum(h map[string]string, k string) int64 {
	n, _ := strconv.ParseInt(h[k], 10, 64)
	return n
}

// ---------- Store ----------

func (s *RedisStore) Admit(ctx context.Context, req headgate.AdmitRequest) ([]headgate.AdmissionUnit, error) {
	var err error
	req, leaseMs, err := headgate.NormalizeAdmitRequest(req)
	if err != nil {
		return nil, err
	}
	ids, err := admitLua.Run(ctx, s.rdb, []string{s.prefix},
		strings.Join(req.Queues, ","), req.Capacity, 0 /* UNUSED (was now_ms) */, leaseMs,
		req.Worker, req.LeaseID, req.Quantum).StringSlice()
	if err != nil {
		return nil, err
	}
	// Post-claim reads are safe: we hold the lease, and the fields read here do not
	// change while it is held.
	units := make([]headgate.AdmissionUnit, 0, len(ids))
	for _, id := range ids {
		h, err := s.rdb.HGetAll(ctx, s.key("job", id)).Result()
		if err != nil {
			return nil, err
		}
		units = append(units, headgate.AdmissionUnit{Claims: []headgate.Claim{{
			Envelope: headgate.Envelope{
				ID:                 id,
				Kind:               h["kind"],
				SchemaVersion:      uint32(hnum(h, "schema_version")),
				Payload:            []byte(h["payload"]),
				Queue:              h["queue"],
				PartitionKey:       h["partition_key"],
				RateClass:          h["rate_class"],
				StickyWorker:       h["sticky_worker"],
				Weight:             headgate.EffectiveWeight(uint32(hnum(h, "weight"))),
				Fingerprint:        h["fingerprint"],
				Priority:           int32(hnum(h, "priority")),
				Attempt:            uint32(hnum(h, "attempt")),
				CrashAttempt:       uint32(hnum(h, "crash_attempt")),
				MaxAttempts:        uint32(hnum(h, "max_attempts")),
				EnqueuedAtMs:       hnum(h, "enqueued_at_ms"),
				ScheduledAtMs:      hnum(h, "scheduled_at_ms"),
				TimeoutMs:          hnum(h, "timeout_ms"),
				DeadlineMs:         hnum(h, "deadline_ms"),
				RetentionMs:        hnum(h, "retention_ms"),
				PeriodicScheduleID: h["periodic_schedule_id"],
				PeriodicTickMs:     hnum(h, "periodic_tick_ms"),
				UniqueStates:       uint32(hnum(h, "unique_states")),
				UniqueWindowMs:     hnum(h, "unique_window_ms"),
				// telemetry and trace context the opaque headers ride the claim (admit.lua needed no
				// change — the store reads the job hash after the atomic claim).
				Headers: headgate.DecodeHeaders([]byte(h["headers"])),
			},
			LeaseID:    h["lease_id"],
			Fence:      uint64(hnum(h, "fence")),
			Expires:    time.UnixMilli(hnum(h, "lease_expires_at_ms")),
			Checkpoint: decodeCheckpoint(h["checkpoint"], cursorBytes(h)),
		}}})
	}
	return units, nil
}

func cursorBytes(h map[string]string) []byte {
	if c, ok := h["cp_cursor"]; ok {
		return []byte(c)
	}
	return nil
}

func (s *RedisStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return s.AckAttempt(ctx, lease, outcome, errMsg, delayMs, nil)
}

func (s *RedisStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return s.AckAttemptWithActualWeight(ctx, lease, outcome, errMsg, delayMs, logs, nil)
}

func (s *RedisStore) AckAttemptWithActualWeight(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string, actualWeight *uint32) error {
	if err := headgate.ValidateAckRequest(outcome, delayMs); err != nil {
		return err
	}
	name := outcome.String()
	if name == "unknown" {
		return fmt.Errorf("headgate: unknown outcome %v", outcome)
	}
	if delayMs <= 0 {
		delayMs = -1 // the scripts' "use the default backoff" sentinel
	}
	logsJSON := "" // attempt-log contract per-attempt logs; '' = none (ARGV[9])
	if len(logs) > 0 {
		logsJSON = headgateshared.EncodeStringList(logs)
	}
	actual := ""
	if actualWeight != nil {
		actual = strconv.FormatUint(uint64(*actualWeight), 10)
	}
	res, err := ackLua.Run(ctx, s.rdb, []string{s.prefix},
		lease.JobID, lease.LeaseID, lease.Fence, name, errMsg, delayMs,
		s.opts.RetryBaseMs, s.opts.RetryCapMs, logsJSON, actual).Result()
	if err != nil {
		return err
	}
	if e := parseTagged(res); e != nil {
		var lr *headgate.LeaseRejectedError
		if errors.As(e, &lr) {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
		return e
	}
	return nil
}

func (s *RedisStore) AckSuccessWithResult(ctx context.Context, lease headgate.LeaseRef, logs []string, actualWeight *uint32, result headgate.JobResult) error {
	if err := headgate.ValidateOpaqueValue("result", result); err != nil {
		return err
	}
	logsJSON := ""
	if len(logs) > 0 {
		logsJSON = headgateshared.EncodeStringList(logs)
	}
	actual := ""
	if actualWeight != nil {
		actual = strconv.FormatUint(uint64(*actualWeight), 10)
	}
	res, err := ackLua.Run(ctx, s.rdb, []string{s.prefix},
		lease.JobID, lease.LeaseID, lease.Fence, "success", "", -1,
		s.opts.RetryBaseMs, s.opts.RetryCapMs, logsJSON, actual,
		result.SchemaVersion, result.Bytes).Result()
	if err != nil {
		return err
	}
	if err := parseTagged(res); err != nil {
		var rejected *headgate.LeaseRejectedError
		if errors.As(err, &rejected) {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
		return err
	}
	return nil
}

func (s *RedisStore) WriteJobOutput(
	ctx context.Context,
	lease headgate.LeaseRef,
	output headgate.JobResult,
) (*headgate.JobOutput, error) {
	if err := headgate.ValidateOpaqueValue("output", output); err != nil {
		return nil, err
	}
	res, err := outputLua.Run(ctx, s.rdb, []string{s.prefix},
		lease.JobID, lease.LeaseID, lease.Fence, output.SchemaVersion, output.Bytes).Result()
	if err != nil {
		return nil, err
	}
	if err := parseTagged(res); err != nil {
		var rejected *headgate.LeaseRejectedError
		if errors.As(err, &rejected) {
			return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
		return nil, err
	}
	arr, _ := res.([]any)
	updatedText := ""
	if len(arr) > 1 {
		updatedText, _ = arr[1].(string)
	}
	updatedAtMs, err := strconv.ParseInt(updatedText, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("headgate: invalid output timestamp %q: %w", updatedText, err)
	}
	return &headgate.JobOutput{
		SchemaVersion: output.SchemaVersion,
		Bytes:         append([]byte(nil), output.Bytes...),
		Fence:         lease.Fence,
		UpdatedAtMs:   updatedAtMs,
	}, nil
}

func (s *RedisStore) WriteJobProgress(
	ctx context.Context,
	lease headgate.LeaseRef,
	update headgate.ProgressUpdate,
) (*headgate.JobProgress, error) {
	if err := headgate.ValidateProgress(update); err != nil {
		return nil, err
	}
	res, err := progressLua.Run(ctx, s.rdb, []string{s.prefix},
		lease.JobID, lease.LeaseID, lease.Fence, update.Current, update.Total, update.Message).Result()
	if err != nil {
		return nil, err
	}
	if err := parseTagged(res); err != nil {
		var rejected *headgate.LeaseRejectedError
		if errors.As(err, &rejected) {
			return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
		return nil, err
	}
	arr, _ := res.([]any)
	updatedText := ""
	if len(arr) > 1 {
		updatedText, _ = arr[1].(string)
	}
	updatedAtMs, err := strconv.ParseInt(updatedText, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("headgate: invalid progress timestamp %q: %w", updatedText, err)
	}
	return &headgate.JobProgress{
		Current: update.Current, Total: update.Total, Message: update.Message,
		Fence: lease.Fence, UpdatedAtMs: updatedAtMs,
	}, nil
}

func (s *RedisStore) Renew(ctx context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return nil, errors.New("headgate: lease must be >= 1ms")
	}
	args := make([]any, 0, 1+3*len(leases))
	args = append(args, leaseMs)
	for _, l := range leases {
		args = append(args, l.JobID, l.LeaseID, l.Fence)
	}
	return renewLua.Run(ctx, s.rdb, []string{s.prefix}, args...).StringSlice()
}

func (s *RedisStore) Enqueue(ctx context.Context, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	// typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend. The idempotent enqueue identity id
	// classification itself happens inside enqueue.lua, where the script IS the
	// transaction — no pre-check race window exists on this backend at all.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	scoped := append([]headgate.Envelope(nil), batch...)
	for i := range scoped {
		scoped[i].UniqueKey = headgate.EffectiveUniqueKey(scoped[i])
	}
	batch = scoped
	args := make([]any, 0, 1+19*len(batch))
	args = append(args, len(batch))
	for _, e := range batch {
		sv := headgate.EffectiveSchemaVersion(e.SchemaVersion)
		queue := headgate.EnqueueQueue(e)
		ma := headgate.EffectiveMaxAttempts(e.MaxAttempts)
		args = append(args,
			e.ID, e.Kind, sv, string(e.Payload), queue, e.PartitionKey, e.RateClass,
			e.Fingerprint, e.Priority, ma, e.ScheduledAtMs, e.TimeoutMs, e.DeadlineMs,
			e.RetentionMs, string(e.UniqueKey), e.UniqueWindowMs, e.UniqueStates)
	}
	// telemetry and trace context the headers ride in a TRAILING block, after every per-job field,
	// so enqueue.lua's `2 + i * F + k` index math is untouched.
	for _, e := range batch {
		args = append(args, headgate.EncodeHeaders(e.Headers))
	}
	// surveyed policy behavior a second trailing block keeps enqueue.lua's long-lived 17-field stride
	// untouched. Old producers omit it and the script normalizes that to one.
	for _, e := range batch {
		args = append(args, headgate.EffectiveWeight(e.Weight))
	}
	for _, e := range batch {
		args = append(args, e.PeriodicScheduleID)
	}
	for _, e := range batch {
		args = append(args, e.PeriodicTickMs)
	}
	for _, e := range batch {
		args = append(args, e.UniqueReplace)
	}
	for _, e := range batch {
		args = append(args, e.UniqueDebounceMs)
	}
	for _, e := range batch {
		if e.Pending {
			args = append(args, 1)
		} else {
			args = append(args, 0)
		}
	}
	for _, e := range batch {
		canonicalTags := headgate.CanonicalTags(e.Tags)
		if canonicalTags == nil {
			canonicalTags = []string{}
		}
		args = append(args, headgateshared.EncodeStringList(canonicalTags))
	}
	for _, e := range batch {
		args = append(args, e.StickyWorker)
	}
	res, err := enqueueLua.Run(ctx, s.rdb, []string{s.prefix}, args...).Result()
	if err != nil {
		return headgate.WrapUnavailable(err)
	}
	return parseTagged(res)
}

func (s *RedisStore) Checkpoint(ctx context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	hasCursor := 0
	if cp.Cursor != nil {
		hasCursor = 1
	}
	res, err := checkpointLua.Run(ctx, s.rdb, []string{s.prefix},
		lease.JobID, lease.LeaseID, lease.Fence, encodeCheckpoint(cp),
		hasCursor, string(cp.Cursor)).Result()
	if err != nil {
		return err
	}
	if e := parseTagged(res); e != nil {
		var lr *headgate.LeaseRejectedError
		if errors.As(e, &lr) {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
		return e
	}
	return nil
}

func (s *RedisStore) ReclaimExpired(ctx context.Context, limit int64) ([]headgate.Reclaimed, error) {
	flat, err := reclaimLua.Run(ctx, s.rdb, []string{s.prefix},
		limit, s.opts.CrashLimit, s.opts.RetryBaseMs, s.opts.RetryCapMs).StringSlice()
	if err != nil {
		return nil, err
	}
	out := make([]headgate.Reclaimed, 0, len(flat)/4)
	for i := 0; i+3 < len(flat); i += 4 {
		ca, _ := strconv.ParseUint(flat[i+2], 10, 32)
		out = append(out, headgate.Reclaimed{
			JobID:        flat[i],
			Fingerprint:  flat[i+1],
			CrashAttempt: uint32(ca),
			Quarantined:  flat[i+3] == "1",
		})
	}
	return out, nil
}

func (s *RedisStore) PromoteDue(ctx context.Context, limit int64) (int64, error) {
	return promoteLua.Run(ctx, s.rdb, []string{s.prefix}, limit).Int64()
}

func (s *RedisStore) EvictRetained(ctx context.Context, limit int64) (int64, error) {
	return adminLua.Run(ctx, s.rdb, []string{s.prefix}, "evict", limit).Int64()
}

func (s *RedisStore) ClaimDuty(ctx context.Context, name, holder string, lease time.Duration) (bool, error) {
	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		return false, errors.New("headgate: duty lease must be >= 1ms")
	}
	n, err := dutyLua.Run(ctx, s.rdb, []string{s.prefix}, "claim", name, holder, leaseMs).Int64()
	return n == 1, err
}

func (s *RedisStore) ReleaseDuty(ctx context.Context, name, holder string) error {
	return dutyLua.Run(ctx, s.rdb, []string{s.prefix}, "release", name, holder, 0).Err()
}

func (s *RedisStore) Caps() headgate.Caps {
	// runtime capability boundary/push wakeups: no Transactional (structurally impossible on Redis). Inspect is
	// inspect.go; Notifying only when this store can open a pub/sub connection.
	c := headgate.CapInspect
	if s.wake != nil {
		c |= headgate.CapNotifying
	}
	return c
}
