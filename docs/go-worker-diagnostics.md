# Go worker diagnostics

Go runners attach standard `runtime/pprof` labels to work:

| Label | Scope |
| --- | --- |
| `headgate.worker` | Runner identity, inherited by its goroutines |
| `headgate.role` | `worker`, `duty`, or `job` |
| `headgate.duty` | Individual maintenance duty |
| `headgate.queue` | Queue of the dispatched job |
| `headgate.kind` | Durable task kind |

Job dispatch adds these labels even through `Drain` and `PerformOne`. Tracked child
goroutines inherit the job's labels. Caller labels remain available, and dispatch
restores them when it returns. Payloads, headers, partition keys and job IDs are
not added as labels. Applications should choose non-sensitive operational names.

Go 1.27 includes labels in tracebacks by default. Standard goroutine and CPU
profiles can also use them to identify blocked work. A profile is process-wide;
it may include unrelated application goroutines and labels.

## Capture on demand

The application owns the output destination and when collection runs. Core does
not install an HTTP handler or start a diagnostic listener.

```go
import (
    "io"
    "runtime/pprof"
)

func writeBlockedWork(w io.Writer) error {
    return pprof.Lookup("goroutine").WriteTo(w, 1)
}

func writeLeaks(w io.Writer) error {
    return pprof.Lookup("goroutineleak").WriteTo(w, 1)
}
```

Use the goroutine profile first when a worker will not drain. The Go 1.27
`goroutineleak` profile specifically detects goroutines blocked on unreachable
synchronization primitives. A handler waiting on a globally reachable channel,
external I/O, or a live application lock can remain stuck without appearing in
that profile. An empty leak profile does not prove the worker is healthy.

For an application that already exposes `net/http/pprof` on its own diagnostic
server, `/debug/pprof/goroutineleak` is available there. Profile collection is an
on-demand diagnostic, not a queue-depth query or a periodic admission operation.

See [Go 1.27 runtime changes](https://go.dev/doc/go1.27#runtime) and the existing
[stuck-handler contract](stuck-job-handlers.md). Profiling does not change lease
fencing, cancellation, or the cooperative nature of Go handler shutdown.

## Capture the lead-up to an incident

Applications can opt into Go's `runtime/trace.FlightRecorder` (introduced in 1.25)
to keep a rolling execution trace. Start it at application startup, then snapshot
it when a drain stalls or latency crosses an application-defined threshold:

```go
recorder := trace.NewFlightRecorder(trace.FlightRecorderConfig{
    MinAge: 10 * time.Second,
    MaxBytes: 8 << 20,
})
if err := recorder.Start(); err != nil {
    return err
}
defer recorder.Stop()

// After the application's incident trigger, with an application-owned io.Writer:
_, err := recorder.WriteTo(destination)
```

Import `runtime/trace` and `time`. Keep the recorder alive for the application's
lifetime and serialize snapshots; `WriteTo` must not overlap another `WriteTo`.
`MaxBytes` takes precedence over `MinAge`, but is a hint rather than a strict cap
on memory use or snapshot size. Open a saved trace with `go tool trace incident.trace`.
This is an application integration recipe: headgate does not automatically start
a recorder, select incident thresholds, or persist trace files. Trace collection
is process-wide and has runtime overhead.

## Container CPU quotas

The Go runtime now accounts for Linux container CPU quotas when choosing
`GOMAXPROCS` and periodically updates it. Headgate does not override it. An
application-supplied `GOMAXPROCS` environment variable or runtime call disables
automatic updates; review deployment overrides before expecting this benefit.
This controls CPU execution parallelism, independently of headgate's configured
job concurrency and store-enforced fleet ceilings.
