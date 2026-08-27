// Package headgatetest is the in-process test double for headgate: a complete
// in-memory implementation of the Store port so handler code, retry behavior, steps,
// and runner wiring can be tested with no database at all (River's rivertest, asynq's
// asynqtest — the helper every serious queue ships).
//
// What it keeps FAITHFUL to the real backends: the transition table (every Ack
// outcome, fence-gated identity, LeaseRejected on a superseded holder), attempts vs
// crash_attempts, quarantine at the crash limit, job uniqueness uniqueness in both modes
// (lifecycle + throttle), retention policy ephemeral retention-0 delete, retention and eviction contract retention eviction,
// per-partition round-robin admission, priority ordering, and duty leases.
//
// What it simplifies, capability-honestly (runtime capability boundary): Caps() is 0 — no Transactional (so
// Job.Once errors, as it must without a real transaction), no Inspect (scheduler /
// operations / quarantine duties idle), no Notifying (runners poll). Like the SQL
// backends it admits state=available only — pair Admit with PromoteDue (the Runner and
// its Drain already do). An unconfigured rate class is UNLIMITED here; configure one
// with SetRateLimit to test throttling. Time comes from NowFunc, so tests can freeze
// or step the store clock deterministically.
package headgatetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

type memJob struct {
	env          headgate.Envelope
	state        string
	fence        uint64
	leaseID      string
	leaseExpires int64
	rateCharge   int64
	finalizedAt  int64
	checkpoint   headgate.Checkpoint
	errs         []string
	result       *headgate.JobResult
	output       *headgate.JobOutput
	progress     *headgate.JobProgress
}

type bucket struct {
	tokens, burst, limit, window int64
	refilled                     int64
}

// MemStore implements headgate.Store entirely in memory behind one mutex.
type MemStore struct {
	// NowFunc is the store clock (boundary validation: store-supplied time, even here). Replace it to
	// freeze or step time in tests. Defaults to time.Now.
	NowFunc func() time.Time
	// CrashLimit is the crash quarantine quarantine threshold (default 3).
	CrashLimit uint32
	// RetryBaseMs/RetryCapMs shape the default retry backoff (defaults 1000 / 1h).
	RetryBaseMs, RetryCapMs int64

	mu       sync.Mutex
	jobs     map[string]*memJob
	unique   map[string]string // lifecycle key -> holder id
	throttle map[string]struct {
		id     string
		expiry int64
	}
	quarantine map[string]bool
	paused     map[string]bool
	rate       map[string]*bucket
	duties     map[string]struct {
		holder  string
		expires int64
	}
	rr map[string]int // queue -> round-robin start offset
}

var _ headgate.Store = (*MemStore)(nil)

func New() *MemStore {
	return &MemStore{
		NowFunc:     time.Now,
		CrashLimit:  3,
		RetryBaseMs: 1000,
		RetryCapMs:  3_600_000,
		jobs:        map[string]*memJob{},
		unique:      map[string]string{},
		throttle: map[string]struct {
			id     string
			expiry int64
		}{},
		quarantine: map[string]bool{},
		paused:     map[string]bool{},
		rate:       map[string]*bucket{},
		duties: map[string]struct {
			holder  string
			expires int64
		}{},
		rr: map[string]int{},
	}
}

func (m *MemStore) now() int64 { return m.NowFunc().UnixMilli() }

// ---------- test-facing helpers ----------

// JobState returns (envelope snapshot, state, exists). State "": no such job.
func (m *MemStore) JobState(id string) (headgate.Envelope, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return headgate.Envelope{}, "", false
	}
	return j.env, j.state, true
}

// Errors returns the per-attempt error history recorded for a job.
func (m *MemStore) Errors(id string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[id]; ok {
		return append([]string(nil), j.errs...)
	}
	return nil
}

// Counts returns state -> count for one queue ("" = all).
func (m *MemStore) Counts(queue string) map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for _, j := range m.jobs {
		if queue == "" || j.env.Queue == queue {
			out[j.state]++
		}
	}
	return out
}

// SetQueuePaused mirrors the gate predicate the real backends read.
func (m *MemStore) SetQueuePaused(queue string, paused bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paused[queue] = paused
}

// SetRateLimit configures a fleet token bucket. Unconfigured classes are unlimited.
func (m *MemStore) SetRateLimit(name string, limit, windowMs, burst int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rate[name] = &bucket{tokens: burst, burst: burst, limit: limit, window: windowMs, refilled: m.now()}
}

// ---------- Store ----------

func (m *MemStore) Enqueue(_ context.Context, batch []headgate.Envelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	// typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	// idempotent enqueue identity the id pass, over the WHOLE batch before any other check so all four backends
	// classify a mixed batch identically. Matching content is skipped — idempotent
	// success, no re-write, and no unique-key check that would find the job conflicting
	// with ITSELF. A terminal job's row still exists, so id reuse follows retention
	// eviction.
	skip := make([]bool, len(batch))
	for i, e := range batch {
		if j, exists := m.jobs[e.ID]; exists {
			if headgate.SameJobContent(e, j.env.Kind, j.env.Fingerprint, j.env.Queue) {
				skip[i] = true
			} else {
				return &headgate.IDConflictError{JobID: e.ID}
			}
		}
	}
	// Validate pass — all-or-nothing, like the batch enqueues in both real backends.
	for i, e := range batch {
		if skip[i] {
			continue
		}
		if e.Fingerprint != "" && m.quarantine[e.Fingerprint] {
			return &headgate.QuarantinedError{Fingerprint: e.Fingerprint}
		}
		if effective := headgate.EffectiveUniqueKey(e); len(effective) > 0 {
			k := string(effective)
			holder := ""
			if e.UniqueWindowMs > 0 {
				if h, ok := m.throttle[k]; ok && h.expiry > now {
					holder = h.id
				}
			} else if h, ok := m.unique[k]; ok {
				holder = h
			}
			if holder != "" {
				replaced := false
				if e.UniqueReplace != 0 || e.UniqueDebounceMs > 0 {
					if j := m.jobs[holder]; j != nil && (j.state == "scheduled" || j.state == "available" || j.state == "retryable") {
						if e.UniqueDebounceMs > 0 {
							j.env.SchemaVersion = e.SchemaVersion
							if j.env.SchemaVersion == 0 {
								j.env.SchemaVersion = 1
							}
							j.env.Payload = append(j.env.Payload[:0], e.Payload...)
							j.env.Fingerprint = e.Fingerprint
							j.env.Tags = headgate.CanonicalTags(e.Tags)
							j.env.ScheduledAtMs = now + e.UniqueDebounceMs
							j.state = "scheduled"
							replaced = true
						}
						if e.UniqueReplace&headgate.UniqueReplacePayload != 0 {
							j.env.SchemaVersion = e.SchemaVersion
							if j.env.SchemaVersion == 0 {
								j.env.SchemaVersion = 1
							}
							j.env.Payload = append(j.env.Payload[:0], e.Payload...)
							j.env.Fingerprint = e.Fingerprint
							replaced = true
						}
						if e.UniqueReplace&headgate.UniqueReplaceScheduledAt != 0 && j.state == "scheduled" {
							j.env.ScheduledAtMs = e.ScheduledAtMs
							if j.env.ScheduledAtMs == 0 {
								j.env.ScheduledAtMs = now
							}
							replaced = true
						}
						if e.UniqueReplace&headgate.UniqueReplacePriority != 0 {
							j.env.Priority = e.Priority
							replaced = true
						}
						if e.UniqueReplace&headgate.UniqueReplaceMaxAttempts != 0 {
							j.env.MaxAttempts = e.MaxAttempts
							if j.env.MaxAttempts == 0 {
								j.env.MaxAttempts = 25
							}
							replaced = true
						}
					}
				}
				return &headgate.DuplicateError{ExistingID: holder, Replaced: replaced}
			}
		}
	}
	for i, e := range batch {
		if skip[i] {
			continue
		}
		if e.Queue == "" {
			e.Queue = "default"
		}
		if e.MaxAttempts == 0 {
			e.MaxAttempts = 25
		}
		if e.SchemaVersion == 0 {
			e.SchemaVersion = 1
		}
		e.Weight = headgate.EffectiveWeight(e.Weight)
		e.Tags = headgate.CanonicalTags(e.Tags)
		e.EnqueuedAtMs = now
		if e.UniqueDebounceMs > 0 {
			e.ScheduledAtMs = now + e.UniqueDebounceMs
		} else if e.ScheduledAtMs == 0 {
			e.ScheduledAtMs = now
		}
		state := "available"
		if e.Pending {
			state = "pending"
		} else if e.ScheduledAtMs > now {
			state = "scheduled"
		}
		m.jobs[e.ID] = &memJob{env: e, state: state}
		if effective := headgate.EffectiveUniqueKey(e); len(effective) > 0 {
			k := string(effective)
			if e.UniqueWindowMs > 0 {
				m.throttle[k] = struct {
					id     string
					expiry int64
				}{e.ID, now + e.UniqueWindowMs}
			} else {
				m.unique[k] = e.ID
			}
		}
	}
	return nil
}

// EnqueueWithoutUniqueness is a test-only, call-scoped bypass. Caller IDs remain strict;
// no mutable disable flag can leak between parallel tests.
func (m *MemStore) EnqueueWithoutUniqueness(ctx context.Context, batch []headgate.Envelope) error {
	cloned := make([]headgate.Envelope, len(batch))
	for i := range batch {
		cloned[i] = batch[i]
		cloned[i].UniqueKey = nil
		cloned[i].UniqueWindowMs = 0
		cloned[i].UniqueReplace = 0
		cloned[i].UniqueDebounceMs = 0
	}
	return m.Enqueue(ctx, cloned)
}

func (m *MemStore) Admit(_ context.Context, req headgate.AdmitRequest) ([]headgate.AdmissionUnit, error) {
	if req.Lease <= 0 {
		return nil, errors.New("headgatetest: lease must be >= 1ms")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var units []headgate.AdmissionUnit
	taken := map[string]int64{} // rate class -> spent this call
	for _, queue := range req.Queues {
		if len(units) >= req.Capacity || m.paused[queue] {
			continue
		}
		// tenant fairness draw per partition, never one flat window: candidates grouped, then a
		// rotating round-robin across the groups so a flooding tenant cannot starve
		// quiet ones. Within a partition: priority DESC, then scheduled_at, then id.
		byPart := map[string][]*memJob{}
		var parts []string
		for _, j := range m.jobs {
			if j.env.Queue != queue || j.state != "available" || j.env.ScheduledAtMs > now ||
				(j.env.StickyWorker != "" && j.env.StickyWorker != req.Worker) {
				continue
			}
			if _, ok := byPart[j.env.PartitionKey]; !ok {
				parts = append(parts, j.env.PartitionKey)
			}
			byPart[j.env.PartitionKey] = append(byPart[j.env.PartitionKey], j)
		}
		sort.Strings(parts)
		for _, p := range parts {
			sort.Slice(byPart[p], func(a, b int) bool {
				x, y := byPart[p][a], byPart[p][b]
				if x.env.Priority != y.env.Priority {
					return x.env.Priority > y.env.Priority
				}
				if x.env.ScheduledAtMs != y.env.ScheduledAtMs {
					return x.env.ScheduledAtMs < y.env.ScheduledAtMs
				}
				return x.env.ID < y.env.ID
			})
		}
		if len(parts) == 0 {
			continue
		}
		start := m.rr[queue] % len(parts)
		m.rr[queue]++
		for round := 0; len(units) < req.Capacity; round++ {
			progressed := false
			for i := 0; i < len(parts) && len(units) < req.Capacity; i++ {
				p := parts[(start+i)%len(parts)]
				var j *memJob
				for len(byPart[p]) > 0 {
					cand := byPart[p][0]
					byPart[p] = byPart[p][1:]
					if m.admissible(cand, taken, now) {
						j = cand
						break
					}
				}
				if j == nil {
					continue
				}
				progressed = true
				j.fence++
				j.state = "running"
				j.leaseID = req.LeaseID
				j.leaseExpires = now + req.Lease.Milliseconds()
				env := j.env
				units = append(units, headgate.AdmissionUnit{Claims: []headgate.Claim{{
					Envelope:   env,
					LeaseID:    req.LeaseID,
					Fence:      j.fence,
					Expires:    time.UnixMilli(j.leaseExpires),
					Checkpoint: j.checkpoint,
				}}})
				if j.env.RateClass != "" && m.rate[j.env.RateClass] != nil {
					cost := int64(headgate.EffectiveWeight(j.env.Weight))
					j.rateCharge = cost
					taken[j.env.RateClass] += cost
				} else {
					j.rateCharge = 0
				}
			}
			if !progressed {
				break
			}
		}
	}
	// Spend the tokens actually consumed.
	for rc, n := range taken {
		if b := m.rate[rc]; b != nil {
			b.tokens -= n
		}
	}
	return units, nil
}

// admissible mirrors the gate's clause order: quarantine, then the fleet rate limit.
func (m *MemStore) admissible(j *memJob, taken map[string]int64, now int64) bool {
	if j.env.Fingerprint != "" && m.quarantine[j.env.Fingerprint] {
		return false
	}
	rc := j.env.RateClass
	if rc == "" {
		return true
	}
	b := m.rate[rc]
	if b == nil {
		return true // unconfigured class is unlimited HERE (see package docs)
	}
	if b.limit > 0 && b.window > 0 {
		gained := (now - b.refilled) * b.limit / b.window
		if gained > 0 {
			b.tokens = min64(b.burst, b.tokens+gained)
			b.refilled = now
		}
	}
	cost := int64(headgate.EffectiveWeight(j.env.Weight))
	return taken[rc]+cost <= b.tokens
}

func (m *MemStore) identity(lease headgate.LeaseRef) (*memJob, error) {
	j, ok := m.jobs[lease.JobID]
	if !ok || j.state != "running" || j.leaseID != lease.LeaseID || j.fence != lease.Fence {
		return nil, &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return j, nil
}

func (m *MemStore) releaseUnique(j *memJob) {
	if effective := headgate.EffectiveUniqueKey(j.env); len(effective) > 0 && j.env.UniqueWindowMs == 0 {
		k := string(effective)
		if m.unique[k] == j.env.ID {
			delete(m.unique, k)
		}
	}
}

func (m *MemStore) dropLease(j *memJob) {
	j.leaseID = ""
	j.leaseExpires = 0
}

func (m *MemStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return m.AckAttempt(ctx, lease, outcome, errMsg, delayMs, nil)
}

func (m *MemStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return m.AckAttemptWithActualWeight(ctx, lease, outcome, errMsg, delayMs, logs, nil)
}

func (m *MemStore) AckAttemptWithActualWeight(_ context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string, actualWeight *uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	j, err := m.identity(lease)
	if err != nil {
		return err
	}
	if actualWeight != nil {
		if j.rateCharge > 0 {
			if b := m.rate[j.env.RateClass]; b != nil {
				elapsed := now - b.refilled
				if elapsed < 0 {
					elapsed = 0
				}
				gained := int64(0)
				if b.limit > 0 && b.window > 0 {
					gained = elapsed * b.limit / b.window
				}
				available := min64(b.burst, b.tokens+gained)
				b.tokens = min64(b.burst, available+j.rateCharge-int64(*actualWeight))
				b.refilled = now
			}
		}
		j.rateCharge = 0
	}
	// attempt-log contract per-attempt logs, rendered into the same history Errors() returns.
	logline := ""
	if len(logs) > 0 {
		logline = "logs: " + strings.Join(logs, " | ")
	}
	pushErr := func(tag string) {
		if errMsg != "" {
			j.errs = append(j.errs, tag+": "+errMsg)
		}
		if logline != "" {
			j.errs = append(j.errs, logline)
		}
	}
	switch outcome {
	case headgate.OutcomeSuccess:
		m.dropLease(j)
		m.releaseUnique(j)
		if j.env.RetentionMs == 0 {
			delete(m.jobs, j.env.ID) // retention policy ephemeral: delete, not keep
		} else {
			j.state = "completed"
			j.finalizedAt = now
			if logline != "" {
				j.errs = append(j.errs, "success "+logline)
			}
		}
	case headgate.OutcomeRetry:
		j.env.Attempt++
		m.dropLease(j)
		j.errs = append(j.errs, fmt.Sprintf("retry (attempt %d): %s", j.env.Attempt, errMsg))
		if logline != "" {
			j.errs = append(j.errs, logline)
		}
		if j.env.Attempt < j.env.MaxAttempts {
			backoff := delayMs
			if backoff <= 0 {
				backoff = defaultBackoff(int64(j.env.Attempt), m.RetryBaseMs, m.RetryCapMs)
			}
			j.state = "retryable"
			j.env.ScheduledAtMs = now + backoff
		} else {
			j.state = "archived"
			j.finalizedAt = now
			m.releaseUnique(j)
		}
	case headgate.OutcomeSkip:
		m.dropLease(j)
		j.state = "archived"
		j.finalizedAt = now
		m.releaseUnique(j)
		pushErr("archived")
	case headgate.OutcomeUndecodable:
		m.dropLease(j)
		j.state = "undecodable"
		j.finalizedAt = now
		m.releaseUnique(j)
		pushErr("undecodable")
	case headgate.OutcomeRevoke:
		m.dropLease(j)
		m.releaseUnique(j)
		delete(m.jobs, j.env.ID) // transition table: revoke -> deleted
	case headgate.OutcomeSnooze:
		if delayMs <= 0 {
			return errors.New("headgatetest: snooze requires delayMs > 0")
		}
		m.dropLease(j)
		j.state = "scheduled" // surveyed policy behavior no attempt consumed
		j.env.ScheduledAtMs = now + delayMs
	case headgate.OutcomeRateLimited:
		m.dropLease(j)
		// surveyed policy behavior NOT a failure: back to available, neither counter moves.
		j.state = "available"
		if j.env.ScheduledAtMs > now {
			j.env.ScheduledAtMs = now
		}
	default:
		return fmt.Errorf("headgatetest: outcome %v is not acked (lease_lost is the reclaimer's)", outcome)
	}
	return nil
}

func (m *MemStore) AckSuccessWithResult(
	_ context.Context,
	lease headgate.LeaseRef,
	logs []string,
	actualWeight *uint32,
	result headgate.JobResult,
) error {
	if result.SchemaVersion == 0 {
		return &headgate.InvalidError{Msg: "result schema_version must be greater than zero"}
	}
	if result.SchemaVersion > headgate.MaxOpaqueSchemaVersion {
		return &headgate.InvalidError{Msg: "result schema_version exceeds the portable signed-integer limit"}
	}
	if len(result.Bytes) > 32*1024*1024 {
		return &headgate.InvalidError{Msg: "result bytes exceed the 32 MiB limit"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	job, err := m.identity(lease)
	if err != nil {
		return err
	}
	if actualWeight != nil {
		if job.rateCharge > 0 {
			if bucket := m.rate[job.env.RateClass]; bucket != nil {
				elapsed := now - bucket.refilled
				if elapsed < 0 {
					elapsed = 0
				}
				gained := int64(0)
				if bucket.limit > 0 && bucket.window > 0 {
					gained = elapsed * bucket.limit / bucket.window
				}
				available := min64(bucket.burst, bucket.tokens+gained)
				bucket.tokens = min64(
					bucket.burst, available+job.rateCharge-int64(*actualWeight),
				)
				bucket.refilled = now
			}
		}
		job.rateCharge = 0
	}
	m.dropLease(job)
	m.releaseUnique(job)
	if job.env.RetentionMs == 0 {
		delete(m.jobs, job.env.ID)
		return nil
	}
	job.state = "completed"
	job.finalizedAt = now
	job.result = &headgate.JobResult{
		SchemaVersion: result.SchemaVersion,
		Bytes:         append([]byte(nil), result.Bytes...),
	}
	if len(logs) > 0 {
		job.errs = append(job.errs, "success logs: "+strings.Join(logs, " | "))
	}
	return nil
}

func (m *MemStore) GetJobResult(_ context.Context, id string) (*headgate.JobResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || job.result == nil {
		return nil, nil
	}
	return &headgate.JobResult{
		SchemaVersion: job.result.SchemaVersion,
		Bytes:         append([]byte(nil), job.result.Bytes...),
	}, nil
}

func (m *MemStore) WriteJobOutput(
	_ context.Context,
	lease headgate.LeaseRef,
	output headgate.JobResult,
) (*headgate.JobOutput, error) {
	if output.SchemaVersion == 0 {
		return nil, &headgate.InvalidError{Msg: "output schema_version must be greater than zero"}
	}
	if output.SchemaVersion > headgate.MaxOpaqueSchemaVersion {
		return nil, &headgate.InvalidError{Msg: "output schema_version exceeds the portable signed-integer limit"}
	}
	if len(output.Bytes) > 32*1024*1024 {
		return nil, &headgate.InvalidError{Msg: "output bytes exceed the 32 MiB limit"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, err := m.identity(lease)
	if err != nil {
		return nil, err
	}
	persisted := &headgate.JobOutput{
		SchemaVersion: output.SchemaVersion,
		Bytes:         append([]byte(nil), output.Bytes...),
		Fence:         lease.Fence,
		UpdatedAtMs:   m.now(),
	}
	job.output = persisted
	copy := *persisted
	copy.Bytes = append([]byte(nil), persisted.Bytes...)
	return &copy, nil
}

func (m *MemStore) GetJobOutput(_ context.Context, id string) (*headgate.JobOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || job.output == nil {
		return nil, nil
	}
	copy := *job.output
	copy.Bytes = append([]byte(nil), job.output.Bytes...)
	return &copy, nil
}

func (m *MemStore) WriteJobProgress(
	_ context.Context,
	lease headgate.LeaseRef,
	update headgate.ProgressUpdate,
) (*headgate.JobProgress, error) {
	if err := headgate.ValidateProgress(update); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, err := m.identity(lease)
	if err != nil {
		return nil, err
	}
	persisted := &headgate.JobProgress{
		Current: update.Current, Total: update.Total, Message: update.Message,
		Fence: lease.Fence, UpdatedAtMs: m.now(),
	}
	job.progress = persisted
	copy := *persisted
	return &copy, nil
}

func (m *MemStore) GetJobProgress(_ context.Context, id string) (*headgate.JobProgress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || job.progress == nil {
		return nil, nil
	}
	copy := *job.progress
	return &copy, nil
}

func (m *MemStore) Renew(_ context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if lease <= 0 {
		return nil, errors.New("headgatetest: lease must be >= 1ms")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var lost []string
	for _, l := range leases {
		j, ok := m.jobs[l.JobID]
		if !ok || j.state != "running" || j.leaseID != l.LeaseID || j.fence != l.Fence {
			lost = append(lost, l.JobID)
			continue
		}
		j.leaseExpires = now + lease.Milliseconds()
	}
	return lost, nil
}

func (m *MemStore) Checkpoint(_ context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.identity(lease)
	if err != nil {
		return err
	}
	j.checkpoint = cp
	return nil
}

func (m *MemStore) ReclaimExpired(_ context.Context, limit int64) ([]headgate.Reclaimed, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var out []headgate.Reclaimed
	for _, id := range m.sortedIDs() {
		if int64(len(out)) >= limit {
			break
		}
		j := m.jobs[id]
		if j.state != "running" || j.leaseExpires > now {
			continue
		}
		j.env.CrashAttempt++
		m.dropLease(j)
		j.errs = append(j.errs, fmt.Sprintf("lease_lost (crash %d)", j.env.CrashAttempt))
		// crash quarantine step attribution: the checkpoint was durable BEFORE the in-progress
		// step's side effects, so the crash lands on exactly that step.
		if s := j.checkpoint.InProgressStep; s != "" {
			if j.checkpoint.CrashesByStep == nil {
				j.checkpoint.CrashesByStep = map[string]uint32{}
			}
			j.checkpoint.CrashesByStep[s]++
		}
		q := false
		if j.env.CrashAttempt >= m.CrashLimit {
			j.state = "quarantined"
			j.finalizedAt = now
			m.releaseUnique(j)
			if j.env.Fingerprint != "" {
				m.quarantine[j.env.Fingerprint] = true
			}
			q = true
		} else {
			j.state = "retryable"
			j.env.ScheduledAtMs = now + defaultBackoff(int64(j.env.CrashAttempt), m.RetryBaseMs, m.RetryCapMs)
		}
		out = append(out, headgate.Reclaimed{
			JobID: id, Fingerprint: j.env.Fingerprint,
			CrashAttempt: j.env.CrashAttempt, Quarantined: q,
		})
	}
	return out, nil
}

func (m *MemStore) PromoteDue(_ context.Context, limit int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var n int64
	for _, id := range m.sortedIDs() {
		if n >= limit {
			break
		}
		j := m.jobs[id]
		if (j.state == "scheduled" || j.state == "retryable") && j.env.ScheduledAtMs <= now {
			j.state = "available"
			n++
		}
	}
	return n, nil
}

func (m *MemStore) EvictRetained(_ context.Context, limit int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var n int64
	for _, id := range m.sortedIDs() {
		if n >= limit {
			break
		}
		j := m.jobs[id]
		switch j.state {
		case "completed", "archived", "cancelled", "undecodable":
			if j.env.RetentionMs > 0 && j.finalizedAt+j.env.RetentionMs <= now {
				delete(m.jobs, id) // quarantined exempt by design (retention and eviction contract)
				n++
			}
		}
	}
	return n, nil
}

func (m *MemStore) ClaimDuty(_ context.Context, name, holder string, lease time.Duration) (bool, error) {
	if lease <= 0 {
		return false, errors.New("headgatetest: duty lease must be >= 1ms")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	d, held := m.duties[name]
	if held && d.expires > now && d.holder != holder {
		return false, nil
	}
	m.duties[name] = struct {
		holder  string
		expires int64
	}{holder, now + lease.Milliseconds()}
	return true, nil
}

func (m *MemStore) ReleaseDuty(_ context.Context, name, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d, ok := m.duties[name]; ok && d.holder == holder {
		delete(m.duties, name)
	}
	return nil
}

func (m *MemStore) Caps() headgate.Caps {
	// runtime capability boundary capability honesty: no Transactional, no Inspect, no Notifying. The duties
	// that need Inspect idle; Job.Once errors; runners poll. See the package docs.
	return 0
}

// sortedIDs keeps sweep order deterministic (map iteration is not).
func (m *MemStore) sortedIDs() []string {
	ids := make([]string, 0, len(m.jobs))
	for id := range m.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func defaultBackoff(attempt, base, cap int64) int64 {
	shift := attempt - 1
	if shift > 20 {
		shift = 20
	}
	b := base * (1 << shift)
	if b > cap {
		return cap
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
