package headgate

import (
	"context"
	"math/bits"

	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- control plane the inspection/control port (mirrors Rust's Inspect trait) ----------

// JobSummary is the bounded operator-facing representation of a job.
type JobSummary struct {
	ID, Kind, Queue, State                string
	SchemaVersion                         uint32
	Priority                              int32
	Attempt, CrashAttempt, MaxAttempts    uint32
	PartitionKey, RateClass, StickyWorker string
	Weight                                uint32
	Fingerprint                           string
	EnqueuedAtMs, ScheduledAtMs           int64
	ClaimedAtMs                           *int64
	PeriodicScheduleID                    string
	PeriodicTickMs                        int64
	FinalizedAtMs                         *int64
	// Payload is nil unless explicitly requested (invariant 9).
	Payload []byte
	// Headers are opaque producer metadata and accompany only an explicit detail read.
	Headers    map[string]string
	ErrorsJSON string
	Tags       []string
}

// IsOrphaned reports durable provenance: the store has reclaimed this job from an
// expired worker lease at least once. It is derived from CrashAttempt, not a state.
func (j JobSummary) IsOrphaned() bool { return j.CrashAttempt > 0 }

// JobFilter is control API contract's list/search predicate. Every field is a POINTER, and that is
// load-bearing rather than stylistic ; PORT CHANGE, reason recorded in the
// register's "Job search / filter" row).
//
// An EMPTY value is a real, filterable value here. `partition_key` is the case that
// forces it: the empty string is the DEFAULT partition, so it is the single most common
// partition in any store that never set one — and `?partition_key=` is the only way to
// ask for it. Rust models these as `Option<String>`, so `?partition_key=` arrives as
// `Some("")` and filters FOR the empty value; Go's plain `string` collapsed that into
// "no filter" and answered with the WHOLE queue. Same divergence on `?queue=`,
// `?state=`, `?kind=` and every `field:` term of the `q=` grammar (`q=queue:` asks for
// the empty queue name). Rust is right and Go's port could not express the question at
// all, which is why the port moved rather than the semantic.
//
// Nil means "no filter". A non-nil pointer to "" means "match the empty value".
type JobFilter struct {
	Queue, State, Kind, KindPrefix *string
	PartitionKey, ID               *string
	Fingerprint, RateClass         *string
	Priority                       *int32
	TagsAll, TagsAny               []string
}

// Ptr is the constructor for the pointer-valued filter fields above (and for `Counts`'
// queue argument). `headgate.Ptr("")` is how a caller asks for the empty value, which is
// the whole reason those fields are pointers.
func Ptr[T any](v T) *T { return &v }

// Deref reads a filter field with nil meaning "absent". Drivers use it where the SQL or
// the Lua wants a plain string and the nil case has already been handled.
func Deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// JobPage contains one cursor-paginated page of job summaries.
type JobPage struct {
	Jobs       []JobSummary
	NextCursor string
}

// StateCounts contains incrementally maintained counts by lifecycle state.
type StateCounts struct {
	Counts      map[string]int64
	Approximate bool
}

// QueueStatsView contains bounded operational metrics for one queue.
type QueueStatsView struct {
	Queue string
	// Weight selects BETWEEN queues inside the atomic gate. It is unrelated to the
	// envelope Weight that spends one selected job's rate budget.
	Weight uint32
	// UnfinishedJobs is exact O(1) producer depth, unlike bounded/approximate ByState.
	UnfinishedJobs uint64
	// nil disables producer backpressure; zero rejects every new unfinished job.
	MaxUnfinishedJobs *uint64
	ByState           map[string]int64
	CountsApproximate bool
	ArrivalRate       float64
	DrainRate         float64
	// TimeToDrainMs is nil when arrival >= drain — the alert condition (backlog metrics).
	TimeToDrainMs *int64
	// OldestAvailableMs is the store-clock age of the oldest currently available job.
	// Nil means there is no available job; it is an age so it is directly SLO-shaped.
	OldestAvailableMs *int64
	QuietGroups       QuietGroupMetrics
	Paused            bool
	// nil until an explicit bounded sampler has stored an estimate.
	MemoryBytes *uint64
}

// QuietGroupMetrics summarizes fairness for partitions with little recent traffic.
type QuietGroupMetrics struct {
	ArrivalRate       float64
	DrainRate         float64
	TimeToDrainMs     *int64
	OldestAvailableMs *int64
	NoisyPartitions   uint32
	Approximate       bool
}

// AgeMillis returns the non-negative age of a timestamp.
func AgeMillis(nowMs, atMs int64) int64 {
	return headgateshared.AgeMillis(nowMs, atMs)
}

// TimeToDrainMillis estimates drain time, or nil when the queue is not draining.
func TimeToDrainMillis(backlog int64, arrivalRate, drainRate float64) *int64 {
	return headgateshared.TimeToDrainMillis(backlog, arrivalRate, drainRate)
}

// NoisyPartitionKeys classifies noisy neighbours from observed in-flight skew (tenant fairness/backlog metrics).
// A partition needs at least two in-flight jobs and more than twice the mean load of all
// peers. A lone partition is never noisy. Integer 128-bit products keep the threshold
// equivalent to Rust without float rounding at the boundary.
func NoisyPartitionKeys(loads map[string]int64) map[string]bool {
	out := map[string]bool{}
	if len(loads) < 2 {
		return out
	}
	for key, rawN := range loads {
		n := uint64(0)
		if rawN > 0 {
			n = uint64(rawN)
		}
		if n < 2 {
			continue
		}
		var others uint64
		for peer, raw := range loads {
			if peer == key || raw <= 0 {
				continue
			}
			v := uint64(raw)
			if ^uint64(0)-others < v {
				others = ^uint64(0)
			} else {
				others += v
			}
		}
		leftHi, leftLo := bits.Mul64(n, uint64(len(loads)-1))
		rightHi, rightLo := bits.Mul64(others, 2)
		if leftHi > rightHi || leftHi == rightHi && leftLo > rightLo {
			out[key] = true
		}
	}
	return out
}

// RateClassConfig defines a store-coordinated rate budget.
type RateClassConfig struct {
	Name     string
	Limit    int64
	WindowMs int64
	Burst    int64
	// Paused is the invariant-16 kill switch: admit nothing until unpaused.
	Paused bool
}

// ValidateRateClassConfig checks a rate-class definition at the API boundary.
func ValidateRateClassConfig(cfg RateClassConfig) error {
	if cfg.WindowMs < 1 {
		return &InvalidError{Msg: "window_ms must be >= 1"}
	}
	if cfg.Limit < 0 || cfg.Burst < 1 {
		return &InvalidError{Msg: "limit must be >= 0 and burst >= 1"}
	}
	return nil
}

// RateClassState is the operator-facing state of a configured rate budget.
type RateClassState struct {
	Name            string
	TokensAvailable int64
	Burst           int64
	LimitPerWindow  int64
	WindowMs        int64
	JobsWaiting     int64
	Paused          bool
}

// PartitionState is the operator-facing fairness state for one queue partition.
type PartitionState struct {
	PartitionKey string
	Deficit      int64
	Waiting      int64
}

// QuarantineEntry describes a fingerprint held after repeated crashes.
type QuarantineEntry struct {
	Fingerprint, Kind, Reason string
	CrashCount                int64
	QuarantinedAtMs           int64
}

// AdmissionExplain describes whether a job can run and, when blocked, why.
type AdmissionExplain struct {
	State      string
	Admissible bool
	// BlockedBy: rate_class | concurrency_limit | fairness | quarantine | schedule |
	// queue_paused; empty when nothing blocks.
	BlockedBy string
	Detail    map[string]string
	// EstimatedAdmissionMs is nil when the block will not clear on its own.
	EstimatedAdmissionMs *int64
}

// EvaluateAdmission converts normalized facts into an operator-facing explanation.
func EvaluateAdmission(facts headgateshared.AdmissionFacts) *AdmissionExplain {
	evaluation := headgateshared.EvaluateAdmission(facts)
	return &AdmissionExplain{
		State: facts.State, Admissible: evaluation.Admissible,
		BlockedBy: evaluation.BlockedBy, Detail: evaluation.Detail,
		EstimatedAdmissionMs: evaluation.ETA,
	}
}

// HistoryBucket contains queue counters for one time bucket.
type HistoryBucket struct {
	AtMs, Arrived, Completed int64
}

// MissedPolicy is declared in the config section below (MissedSkip et al.).

// ScheduleEntry is a surveyed policy behavior periodic entry — durable in the store, never in a leader's
// memory. Spec is "@every:<ms>" (epoch-aligned) or a UTC cron expression; both
// languages must derive identical tick times, because ticks feed unique keys.
type ScheduleEntry struct {
	ID, Kind      string
	Payload       []byte
	Queue         string
	PartitionKey  string
	RateClass     string
	Priority      int32
	MaxAttempts   uint32
	RetentionMs   int64
	Spec          string
	NextRunMs     int64
	LastEnqueued  *int64
	OnMissed      MissedPolicy
	BackfillLimit uint32
	Paused        bool
}

// WorkerMeta describes a registered worker and its current control state.
type WorkerMeta struct {
	WorkerID, Host string
	PID            int32
	Queues         []string
	// Concurrency is the worker's configured capacity — the denominator of
	// Inflight / Concurrency.
	Concurrency   uint32
	StartedAtMs   int64
	HeartbeatAtMs int64

	// ----- the cluster view and the backlog metrics autoscaling signal -----
	// ADDITIVE on the heartbeat that already runs. The registry knew each worker's
	// queues and capacity but not what it was DOING, so "which queues have zero live
	// workers" and "is this fleet the right size" were both unanswerable from the store.
	// All three are levels reported by the worker, never derived by the server.

	// Inflight is how many jobs this worker is running right now.
	Inflight uint32
	// Polls is admissions attempted in the runner's rolling window.
	Polls uint64
	// EmptyPolls is how many of those returned zero jobs. The RATIO is the scale-down
	// signal; the two counters ride the wire instead of a float so the aggregate is
	// exact and so neither language has to agree with the other about float formatting.
	EmptyPolls uint64

	// Status is the worker-acknowledged control state: running, quiet, restarting,
	// or terminating. PendingCommand is the store mailbox and is populated only by
	// inspection reads; workers never write it in a heartbeat.
	Status         string
	DutiesActive   bool
	PendingCommand string
}

// Utilization is backlog metrics's Inflight / Concurrency. 0 when capacity is 0 — never a division
// by zero, and never 1.0 for a worker that cannot run anything.
func (w WorkerMeta) Utilization() float64 {
	if w.Concurrency == 0 {
		return 0
	}
	return float64(w.Inflight) / float64(w.Concurrency)
}

// EmptyPollRatio is backlog metrics's empty admissions / total admissions over the reported window.
// 0 when the window is empty — an idle-since-startup worker has no evidence either way,
// and reporting 1.0 there would signal "scale down" from no data at all.
func (w WorkerMeta) EmptyPollRatio() float64 {
	if w.Polls == 0 {
		return 0
	}
	return float64(w.EmptyPolls) / float64(w.Polls)
}

// BulkOp is control API contract's asynchronous bulk mutation as data. An empty selector is rejected.
type BulkOp struct {
	ID, Action         string
	Queue, State, Kind string
	PartitionKey       string
	OlderThanMs        *int64
	DryRun             bool
}

// HasSelector reports whether the bulk operation identifies any jobs.
func (op BulkOp) HasSelector() bool {
	return op.Queue != "" || op.State != "" || op.Kind != "" || op.PartitionKey != "" || op.OlderThanMs != nil
}

// BulkActionStates returns the lifecycle states eligible for action.
func BulkActionStates(action string) ([]string, bool) {
	return headgateshared.BulkActionStates(action)
}

// ValidWorkerCommand reports whether command is supported by worker control.
func ValidWorkerCommand(command string) bool {
	return headgateshared.ValidWorkerCommand(command)
}

// FormatGeneratedID returns a stable-width identifier from store time and process state.
func FormatGeneratedID(nowMs int64, processID int, sequence uint64) string {
	return headgateshared.FormatGeneratedID(nowMs, processID, sequence)
}

// OperationStatus describes one asynchronous control operation.
type OperationStatus struct {
	ID, Status     string
	Affected       int64
	TotalEstimated int64
	DryRun         bool
	Error          string
}

// ScheduleEventOutcome identifies the durable result of a periodic tick.
type ScheduleEventOutcome string

// Durable periodic schedule outcomes.
const (
	ScheduleEventEnqueued     ScheduleEventOutcome = "enqueued"
	ScheduleEventDeduplicated ScheduleEventOutcome = "deduplicated"
	ScheduleEventFailed       ScheduleEventOutcome = "failed"
	ScheduleEventSkipped      ScheduleEventOutcome = "skipped"
	ScheduleEventLimit                             = uint32(100)
)

// ScheduleEvent is one durable scheduler enqueue attempt. Reason is a stable,
// low-cardinality classification, never a raw backend error or payload.
type ScheduleEvent struct {
	EventID      uint64
	ScheduleID   string
	TickMs       int64
	JobID        string
	Outcome      ScheduleEventOutcome
	Reason       string
	RecordedAtMs int64
}

// InspectStore is the control API's store surface, separate from Store the way
// TransactionalStore is (runtime capability boundary): a backend that cannot answer these does not have them.
// Every read is bounded — no method may be O(queue depth) (invariant 6).
type InspectStore interface {
	Store
	GetJob(ctx context.Context, id string, includePayload bool) (*JobSummary, error)
	ListJobs(ctx context.Context, f JobFilter, cursor string, limit uint32) (JobPage, error)
	// Counts: nil queue = every queue; a non-nil pointer to "" = the queue literally
	// named "" — the same Option<&str> contract Rust's `counts` has. See JobFilter.
	Counts(ctx context.Context, queue *string) (StateCounts, error)
	QueueStats(ctx context.Context) ([]QueueStatsView, error)
	SetQueuePaused(ctx context.Context, queue string, paused bool) error
	SetQueueWeight(ctx context.Context, queue string, weight uint32) error
	SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error
	RateClasses(ctx context.Context) ([]RateClassState, error)
	UpsertRateClass(ctx context.Context, cfg RateClassConfig) error
	ConcurrencyLimits(ctx context.Context) ([]ConcurrencyLimit, error)
	UpsertConcurrencyLimit(ctx context.Context, cfg ConcurrencyLimit) error
	Partitions(ctx context.Context, queue string) ([]PartitionState, error)
	QuarantineList(ctx context.Context) ([]QuarantineEntry, error)
	QuarantineRelease(ctx context.Context, fingerprint string) (released uint64, err error)
	OperatorRetry(ctx context.Context, id string) error
	OperatorCancel(ctx context.Context, id string) error
	PromoteJob(ctx context.Context, id string) error
	DeleteJob(ctx context.Context, id string) error
	ExplainAdmission(ctx context.Context, id string) (*AdmissionExplain, error)
	History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]HistoryBucket, error)
	// QuarantineSweep (crash quarantine): waiting jobs whose fingerprint is quarantined move to
	// the terminal quarantined state, VISIBLY — never an invisible gate-skip forever.
	QuarantineSweep(ctx context.Context, limit int64) (int64, error)
	RescheduleJob(ctx context.Context, id string, atMs int64) error
	EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error
	UpsertSchedule(ctx context.Context, s ScheduleEntry) error
	DeleteSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context) ([]ScheduleEntry, error)
	DueSchedules(ctx context.Context, limit int64) (due []ScheduleEntry, storeNowMs int64, err error)
	AdvanceSchedule(ctx context.Context, id string, fromNextRunMs, toNextRunMs int64) (bool, error)
	RecordScheduleEvent(ctx context.Context, event ScheduleEvent) error
	ListScheduleEvents(ctx context.Context, scheduleID string, beforeEventID uint64, limit uint32) ([]ScheduleEvent, error)
	// HeartbeatWorker upserts the worker row and returns any pending operator COMMAND —
	// the surveyed policy behavior control channel riding the heartbeat (Faktory's BEAT): "quiet" stops
	// admitting, "resume" resumes, "restart" drains without a timeout, "terminate"
	// performs a bounded shutdown, and "resign" releases the worker's singleton duties.
	// "" = none.
	HeartbeatWorker(ctx context.Context, w WorkerMeta) (command string, err error)
	ListWorkers(ctx context.Context, staleAfterMs int64) ([]WorkerMeta, error)
	// SignalWorker sets (or clears, with "") a worker's pending command. Runtimes
	// clear every command after applying it, then publish the acknowledged state.
	SignalWorker(ctx context.Context, workerID, command string) error
	// DistinctKinds: kinds present among waiting jobs (bounded sample), for typed dispatch's
	// startup warning about kinds no registered handler answers.
	DistinctKinds(ctx context.Context, limit int64) ([]string, error)
	CreateOperation(ctx context.Context, req BulkOp) error
	GetOperation(ctx context.Context, id string) (*OperationStatus, error)
	RunPendingOperations(ctx context.Context, batch int64) (uint64, error)
	DeleteQueue(ctx context.Context, queue string, force bool) (operationID string, err error)
	SampleQueueMemory(ctx context.Context, limit uint32) (sampled uint32, err error)
}

// Caps is the bit set of optional capabilities implemented by a Store.
type Caps uint32

// Optional store capabilities.
const (
	CapTransactional Caps = 1 << iota
	CapNotifying
	CapInspect
)

// Has reports whether all bits in x are present.
func (c Caps) Has(x Caps) bool { return c&x != 0 }
