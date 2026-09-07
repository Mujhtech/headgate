// Package headgatetest is the in-process test double for headgate: a complete
// in-memory implementation of the Store port so handler code, retry behavior, steps,
// and runner wiring can be tested with no database at all (River's rivertest, asynq's
// asynqtest — the helper every serious queue ships).
//
// It preserves the real backends' transition table (every Ack
// outcome, fence-gated identity, LeaseRejected on a superseded holder), attempts vs
// crash attempts, quarantine at the crash limit, both uniqueness modes, retention,
// per-partition round-robin admission, priority ordering, and duty leases.
//
// Caps returns zero: the store is not transactional, inspectable, or notifying. Job.Once
// therefore returns an error, inspection duties remain idle, and runners poll. Like the SQL
// backends it admits state=available only — pair Admit with PromoteDue (the Runner and
// its Drain already do). An unconfigured rate class is UNLIMITED here; configure one
// with SetRateLimit to test throttling. Time comes from NowFunc, so tests can freeze
// or step the store clock deterministically.
package headgatetest

import (
	"context"
	"errors"
	"sort"
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
	// CrashLimit is the crash quarantine threshold (default 3).
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

// New creates an empty MemStore with production-equivalent defaults.
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

// Enqueue validates and atomically inserts a batch into memory.
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
			holder := ""
			if e.UniqueWindowMs > 0 {
				if h, ok := m.throttle[string(effective)]; ok && h.expiry > now {
					holder = h.id
				}
			} else if h, ok := m.unique[string(effective)]; ok {
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

// Admit atomically claims eligible jobs using the in-memory policy model.
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

// Ack applies a fence-verified lifecycle outcome.
