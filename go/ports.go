package headgate

import (
	"time"
)

// ---------- other ports (payload codecs) ----------

// Codec encodes and decodes registered task arguments.
type Codec interface {
	Encode(Args) ([]byte, error)
	Decode(kind string, version uint32, b []byte) (Args, error)
}

// Telemetry receives runtime events without coupling core to an exporter.
type Telemetry interface{ OnEvent(Event) }

// MemorySampler reports the worker process's memory footprint in bytes. The default
// sampler uses the process resident-set high-water mark where the standard library
// exposes it. Tests and unusual platforms can inject an equivalent process sampler.
type MemorySampler interface {
	MemoryBytes() (uint64, error)
}

// MemorySamplerFunc adapts a function to MemorySampler.
type MemorySamplerFunc func() (uint64, error)

// MemoryBytes calls f.
func (f MemorySamplerFunc) MemoryBytes() (uint64, error) { return f() }

// Event is the facade's payload. It is a struct rather than a sum type so new signals
// remain additive: bridges switch on Type and ignore fields they do not understand.
// Adding a field is compatible; renaming or repurposing one is not.
type Event struct {
	// Type is one of: admitted | rejected | completed | quarantined | evicted
	// | job_span | worker_saturation | worker_memory.
	Type        string
	Queue, Kind string
	Fingerprint string
	Count       int
	Duration    time.Duration

	// Policy names the clause that refused the job when Type == "rejected", from the
	// admission-explain vocabulary (rate_class | concurrency_limit | fairness |
	// quarantine | schedule | queue_paused) — so a dashboard counting rejections by
	// policy and GET /jobs/{id}/admission use one word for one thing. This field matches
	// the Rust `Event::Rejected { queue, policy, count }` variant already carried.
	Policy string

	// Job-span fields (Type == "job_span").
	// Emitted exactly once per attempt, after the handler returns, carrying everything
	// an OTel-bridged deployment needs to build one span: identity, outcome, and — the
	// point of the addition — the traceparent the PRODUCER put on the envelope, already
	// parsed.
	//
	// It fires at the END and carries StartedAtMs + Duration rather than firing at the
	// start, because a facade has no span object to hand back: a start-only callback
	// would force every bridge to keep its own job-id -> span map and to leak one
	// whenever a worker is killed mid-attempt. An OTel span builder takes explicit start
	// and end timestamps, so one event is enough and nothing has to be remembered.
	JobID   string
	Attempt uint32
	// Outcome is success | retry | skip | revoke | snooze | undecodable | rate_limited.
	Outcome     string
	StartedAtMs int64
	// Trace is the parsed trace context from the envelope's reserved headers. The
	// ZERO VALUE (Trace.Valid() == false) means the envelope carried no traceparent OR
	// carried an invalid one — see ParseTraceparent. A bridge then starts a root span.
	Trace TraceContext

	// Worker-saturation fields (Type == "worker_saturation").
	// Emitted by the runner on every heartbeat, alongside the registry upsert that
	// already happens — so the same numbers reach a metrics exporter and GET /cluster
	// from one place and cannot disagree. This is a SIGNAL, not an autoscaler: headgate
	// never sizes a fleet, it only publishes the two numbers that decide the direction.
	//
	//   Utilization    = Inflight / Capacity — scale UP when high AND the backlog's
	//                    time-to-drain is growing (backlog metrics).
	//   EmptyPollRatio = admits returning zero / total admits over the rolling window —
	//                    scale DOWN when high: the fleet is asking for work that is not
	//                    there.
	Worker         string
	Inflight       uint32
	Capacity       uint32
	Utilization    float64
	EmptyPollRatio float64
	// Polls / EmptyPolls are the window totals behind the ratio, so an exporter can
	// publish counters too.
	Polls, EmptyPolls uint64

	// ----- rolling restart / memory guard (Type == "worker_memory") -----
	// Emitted on every configured sample. RestartRequested is true exactly once: the
	// sample that crossed the limit and sent the runner through graceful shutdown.
	MemoryBytes      uint64
	MemoryLimitBytes uint64
	RestartRequested bool
}

// Clock is injectable so scheduling and lease expiry are testable without sleeping.
type Clock interface{ NowMs() int64 }

// IDGen creates unique job identifiers.
type IDGen interface{ New() string }

// RetryPolicy calculates the delay after a returned handler error.
type RetryPolicy interface {
	NextRetry(attempt uint32, err error) time.Duration
}
