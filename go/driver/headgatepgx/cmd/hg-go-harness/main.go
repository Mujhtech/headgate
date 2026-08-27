// hg-go-harness drives the Go pgx store for scripts/test-admission.sh — the same
// command grammar as hg-pg-harness, so the cross-language section can interleave the
// two against ONE database. Connection from $HG_PG.
//
// Errors print "ERR <message>" and exit 1, so the script can assert on them.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	headgatepgx "github.com/mujhtech/headgate/go/driver/headgatepgx"
)

func kv() (string, map[string]string) {
	args := os.Args[1:]
	if len(args) == 0 {
		return "", nil
	}
	m := map[string]string{}
	for _, a := range args[1:] {
		if k, v, ok := strings.Cut(a, "="); ok {
			m[k] = v
		}
	}
	return args[0], m
}

// wireMsg accepts any JSON payload of kind "w" for the cross-language drain.
type wireMsg struct{}

func (wireMsg) Kind() string { return "w" }

// cursorState is the step replay cursor shape both languages write. Go's SetCursor is generic and
// JSON-encodes; Rust's set_cursor takes RAW BYTES. The two are interoperable only if the
// raw side writes the JSON the generic side would — so this struct's wire form, `{"page":N}`,
// IS the cross-language contract, and the keyspace diff is what pins it.
type cursorState struct {
	Page int64 `json:"page"`
}

func geti(m map[string]string, k string, def int64) int64 {
	if v, ok := m[k]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func main() {
	if err := run(); err != nil {
		fmt.Printf("ERR %s\n", strings.TrimPrefix(err.Error(), "headgate: "))
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	conninfo := os.Getenv("HG_PG")
	if conninfo == "" {
		conninfo = "host=/tmp port=5432 user=postgres dbname=hg"
	}
	store, err := headgatepgx.Connect(ctx, conninfo)
	if err != nil {
		return err
	}
	cmd, m := kv()
	switch cmd {
	case "enqueue":
		count := geti(m, "count", 1)
		batch := make([]headgate.Envelope, 0, count)
		for g := int64(1); g <= count; g++ {
			kind := m["kind"]
			if kind == "" {
				kind = "w"
			}
			payload := []byte{0}
			if p := m["payload"]; p != "" {
				payload = []byte(p)
			}
			fp := m["fp"]
			if fp == "auto" {
				// content fingerprinting client-side derivation — the cross-language parity check.
				fp = headgate.Fingerprint(kind, payload)
			} else if fp == "" {
				fp = "fp"
			}
			e := headgate.Envelope{
				ID:           m["prefix"] + strconv.FormatInt(g, 10),
				Kind:         kind,
				Payload:      payload,
				Queue:        m["queue"],
				PartitionKey: m["partition"],
				RateClass:    m["rate"],
				Weight:       uint32(geti(m, "weight", 1)),
				Fingerprint:  fp,
				// `priority=` exists so the corpus can set a NON-DEFAULT
				// priority. Until this round every envelope in the suite carried 0.
				Priority:           int32(geti(m, "priority", 0)),
				ScheduledAtMs:      geti(m, "sched", 1000),
				RetentionMs:        geti(m, "retention", 0),
				PeriodicScheduleID: m["schedule_id"],
				PeriodicTickMs:     geti(m, "tick", 0),
				MaxAttempts:        uint32(geti(m, "max_attempts", 25)),
				UniqueWindowMs:     geti(m, "window", 0),
				UniqueReplace:      uint32(geti(m, "replace", 0)),
				// `timeout=` / `deadline=` exist so the corpus can reach worker safety's
				// per-attempt timeout and absolute deadline AT ALL. Both are implemented
				// in both worker loops and no test in either language had ever
				// constructed an envelope with a non-zero value, because no harness
				// accepted one — the identical blind spot `priority=` closed.
				TimeoutMs:  geti(m, "timeout", 0),
				DeadlineMs: geti(m, "deadline", 0),
				Headers:    traceHeaders(m),
			}
			if u, ok := m["unique"]; ok {
				e.UniqueKey = []byte(u)
			}
			batch = append(batch, e)
		}
		if err := store.Enqueue(ctx, batch); err != nil {
			return err
		}
		fmt.Println(count)
	case "admit":
		units, err := store.Admit(ctx, headgate.AdmitRequest{
			Worker:   m["worker"],
			LeaseID:  m["lease"],
			Queues:   strings.Split(m["queues"], ","),
			Capacity: int(geti(m, "capacity", 1)),
			Lease:    time.Duration(geti(m, "lease_ms", 30000)) * time.Millisecond,
			Quantum:  geti(m, "quantum", 1000),
		})
		if err != nil {
			return err
		}
		for _, u := range units {
			for _, c := range u.Claims {
				fmt.Printf("%s|%s|%d|%s|%s\n", c.Envelope.ID, c.LeaseID, c.Fence,
					c.Envelope.PartitionKey, c.Envelope.RateClass)
			}
		}
	// telemetry and trace context . Prints the RAW header exactly as the store returned it and the
	// PARSED context re-rendered, so one line proves both halves of the contract: the
	// store round-trips opaque bytes, and the runtime's lenient parse turns an invalid
	// value into an absent one (empty fields) without failing dispatch.
	case "admit_trace":
		units, err := store.Admit(ctx, headgate.AdmitRequest{
			Worker:   m["worker"],
			LeaseID:  m["lease"],
			Queues:   strings.Split(m["queues"], ","),
			Capacity: int(geti(m, "capacity", 1)),
			Lease:    time.Duration(geti(m, "lease_ms", 30000)) * time.Millisecond,
			Quantum:  geti(m, "quantum", 1000),
		})
		if err != nil {
			return err
		}
		for _, u := range units {
			for _, c := range u.Claims {
				raw := c.Envelope.Headers[headgate.TraceparentHeader]
				rendered, state := "", ""
				if tc, ok := headgate.TraceContextOf(c.Envelope.Headers); ok {
					rendered, state = tc.Traceparent(), tc.TraceState
				}
				fmt.Printf("%s|%s|%s|%s\n", c.Envelope.ID, raw, rendered, state)
			}
		}
	case "ack":
		outcome, ok := map[string]headgate.Outcome{
			"success": headgate.OutcomeSuccess, "retry": headgate.OutcomeRetry,
			"skip": headgate.OutcomeSkip, "revoke": headgate.OutcomeRevoke,
			"snooze": headgate.OutcomeSnooze, "undecodable": headgate.OutcomeUndecodable,
			"rate_limited": headgate.OutcomeRateLimited,
		}[m["outcome"]]
		if !ok {
			return fmt.Errorf("unknown outcome %q", m["outcome"])
		}
		lease := headgate.LeaseRef{
			JobID: m["job"], LeaseID: m["lease"], Fence: uint64(geti(m, "fence", 0)),
		}
		var actual *uint32
		if _, ok := m["actual"]; ok {
			v := uint32(geti(m, "actual", 0))
			actual = &v
		}
		if err := store.AckAttemptWithActualWeight(ctx, lease, outcome, m["err"],
			geti(m, "delay", 0), nil, actual); err != nil {
			return err
		}
		fmt.Println("ok")
	case "ack_result":
		lease := headgate.LeaseRef{
			JobID: m["job"], LeaseID: m["lease"], Fence: uint64(geti(m, "fence", 0)),
		}
		result := headgate.JobResult{
			SchemaVersion: uint32(geti(m, "version", 1)), Bytes: []byte(m["bytes"]),
		}
		if err := store.AckSuccessWithResult(ctx, lease, nil, nil, result); err != nil {
			return err
		}
		fmt.Println("ok")
	case "orphaned":
		job, err := store.GetJob(ctx, m["job"], false)
		if err != nil {
			return err
		}
		if job == nil {
			fmt.Println("none")
		} else {
			fmt.Println(job.IsOrphaned())
		}
	case "origin":
		job, err := store.GetJob(ctx, m["job"], false)
		if err != nil {
			return err
		}
		if job == nil || job.PeriodicScheduleID == "" {
			fmt.Println("none")
		} else {
			fmt.Printf("%s|%d\n", job.PeriodicScheduleID, job.PeriodicTickMs)
		}
	case "get_result":
		result, err := store.GetJobResult(ctx, m["job"])
		if err != nil {
			return err
		}
		if result == nil {
			fmt.Println("none")
		} else {
			fmt.Printf("%d|%s\n", result.SchemaVersion, result.Bytes)
		}
	case "write_output":
		lease := headgate.LeaseRef{
			JobID: m["job"], LeaseID: m["lease"], Fence: uint64(geti(m, "fence", 0)),
		}
		output, err := store.WriteJobOutput(ctx, lease, headgate.JobResult{
			SchemaVersion: uint32(geti(m, "version", 1)), Bytes: []byte(m["bytes"]),
		})
		if err != nil {
			return err
		}
		fmt.Printf("%d|%s|%d|%d\n", output.SchemaVersion, output.Bytes, output.Fence, output.UpdatedAtMs)
	case "get_output":
		output, err := store.GetJobOutput(ctx, m["job"])
		if err != nil {
			return err
		}
		if output == nil {
			fmt.Println("none")
		} else {
			fmt.Printf("%d|%s|%d|%d\n", output.SchemaVersion, output.Bytes, output.Fence, output.UpdatedAtMs)
		}
	case "write_progress":
		lease := headgate.LeaseRef{
			JobID: m["job"], LeaseID: m["lease"], Fence: uint64(geti(m, "fence", 0)),
		}
		progress, err := store.WriteJobProgress(ctx, lease, headgate.ProgressUpdate{
			Current: uint64(geti(m, "current", 0)), Total: uint64(geti(m, "total", 100)),
			Message: m["message"],
		})
		if err != nil {
			return err
		}
		fmt.Printf("%d|%d|%s|%d|%d\n", progress.Current, progress.Total,
			progress.Message, progress.Fence, progress.UpdatedAtMs)
	case "get_progress":
		progress, err := store.GetJobProgress(ctx, m["job"])
		if err != nil {
			return err
		}
		if progress == nil {
			fmt.Println("none")
		} else {
			fmt.Printf("%d|%d|%s|%d|%d\n", progress.Current, progress.Total,
				progress.Message, progress.Fence, progress.UpdatedAtMs)
		}
	case "renew":
		var refs []headgate.LeaseRef
		for _, s := range strings.Split(m["refs"], ",") {
			if s == "" {
				continue
			}
			p := strings.Split(s, ":")
			fence, _ := strconv.ParseUint(p[2], 10, 64)
			refs = append(refs, headgate.LeaseRef{JobID: p[0], LeaseID: p[1], Fence: fence})
		}
		lost, err := store.Renew(ctx, refs, time.Duration(geti(m, "lease_ms", 30000))*time.Millisecond)
		if err != nil {
			return err
		}
		for _, id := range lost {
			fmt.Println(id)
		}
	case "reclaim":
		rec, err := store.ReclaimExpired(ctx, geti(m, "limit", 1000))
		if err != nil {
			return err
		}
		for _, r := range rec {
			fmt.Printf("%s|%s|%v\n", r.JobID, r.Fingerprint, r.Quarantined)
		}
	case "promote":
		n, err := store.PromoteDue(ctx, geti(m, "limit", 10000))
		if err != nil {
			return err
		}
		fmt.Println(n)
	case "evict":
		n, err := store.EvictRetained(ctx, geti(m, "limit", 1000))
		if err != nil {
			return err
		}
		fmt.Println(n)
	case "duty":
		got, err := store.ClaimDuty(ctx, m["name"], m["holder"],
			time.Duration(geti(m, "lease_ms", 30000))*time.Millisecond)
		if err != nil {
			return err
		}
		fmt.Println(got)
	case "duty-release":
		if err := store.ReleaseDuty(ctx, m["name"], m["holder"]); err != nil {
			return err
		}
		fmt.Println("ok")
	case "drain":
		// Cross-language execution conformance: run kind-`w` jobs through the REAL Go
		// runtime path — dispatch, handler, ack.
		sleepMs := geti(m, "sleep", 0)
		reg := headgate.NewRegistry()
		if err := headgate.RegisterFunc[wireMsg](reg, func(hctx context.Context, _ *headgate.Job[wireMsg]) error {
			// `sleep=` is what makes worker safety's per-attempt timeout reachable — a
			// handler that returns instantly can never outrun one. Cancellation is
			// COOPERATIVE in Go, so the wait has to watch the context or the timeout
			// would be a lie the harness told itself.
			if sleepMs > 0 {
				select {
				case <-time.After(time.Duration(sleepMs) * time.Millisecond):
				case <-hctx.Done():
					return hctx.Err()
				}
			}
			return nil
		}); err != nil {
			return err
		}
		queues := map[string]headgate.QueueConfig{}
		for _, q := range strings.Split(m["queues"], ",") {
			queues[q] = headgate.QueueConfig{MaxWorkers: 10}
		}
		r := headgate.NewRunner(store, reg, headgate.Config{Queues: queues, DisableDuties: true})
		done, err := r.Drain(ctx, int(geti(m, "count", 10)))
		if err != nil {
			return err
		}
		fmt.Println(len(done))
	// step replay CURSOR ITERATION, . StepCursor / SetCursor had ZERO call sites
	// repo-wide outside their own definitions and doc comments, so the resumable-loop half
	// of step replay was implemented, documented, claimed — and never once executed. This
	// runs one job through the REAL runtime (PerformOne, the round-32k execute-one-job
	// helper) with a cursor step that persists its position, is interrupted, and — on the
	// next call — resumes from the cursor. Grammar and output match hg-pg-harness exactly.
	//
	//   cursor queues=Q pages=N [stop=K] [steal=K]
	//     stop=K  : after page K, RELEASE the job (ErrRateLimited — requeued available, no
	//               attempt consumed) so the resume needs no backoff wait.
	//     steal=K : after page K, an operator CANCELS the job, clearing the lease. The
	//               NEXT SetCursor must be refused by the fence and the handler must stop
	//               THERE — step replay's boundary rule, on the cursor path.
	//
	// The cursor is a JSON `{"page":N}`, which is what Go's generic SetCursor writes for
	// this shape and what the Rust harness writes by hand — see the keyspace diff.
	case "cursor":
		pages, stop, steal := geti(m, "pages", 3), geti(m, "stop", 0), geti(m, "steal", 0)
		var from int64
		var processed []int64
		reg := headgate.NewRegistry()
		if err := headgate.RegisterFunc[wireMsg](reg, func(hctx context.Context, job *headgate.Job[wireMsg]) error {
			return headgate.StepCursor(hctx, "scan", func(sctx context.Context, cur cursorState) error {
				from = cur.Page
				for page := from + 1; page <= pages; page++ {
					if stop > 0 && page > stop {
						return headgate.ErrRateLimited
					}
					if err := headgate.SetCursor(sctx, cursorState{Page: page}); err != nil {
						return err
					}
					processed = append(processed, page)
					if steal > 0 && page == steal {
						insp, ok := any(store).(headgate.InspectStore)
						if !ok {
							return fmt.Errorf("store has no Inspect surface")
						}
						if err := insp.OperatorCancel(sctx, job.ID); err != nil {
							return err
						}
					}
				}
				return nil
			})
		}); err != nil {
			return err
		}
		queues := map[string]headgate.QueueConfig{}
		for _, q := range strings.Split(m["queues"], ",") {
			queues[q] = headgate.QueueConfig{MaxWorkers: 10}
		}
		r := headgate.NewRunner(store, reg, headgate.Config{Queues: queues, DisableDuties: true})
		got, ok, err := r.PerformOne(ctx)
		if err != nil {
			return err
		}
		outcome := "nothing-admitted"
		if ok {
			outcome = got.Outcome
		}
		parts := make([]string, 0, len(processed))
		for _, p := range processed {
			parts = append(parts, strconv.FormatInt(p, 10))
		}
		fmt.Printf("resumed_from=%d|processed=%s|outcome=%s\n", from, strings.Join(parts, ","), outcome)
	// backlog metrics the BACKLOG DERIVATIVES. Computed in all four adapters, served on GET /queues,
	// and asserted by nothing previously — the one diff that transported them emptied
	// the counters first, so the rates were time-stable at 0. Fixed decimals so the two
	// languages print byte-identically.
	case "qstats":
		insp, ok := any(store).(headgate.InspectStore)
		if !ok {
			return fmt.Errorf("store has no Inspect surface")
		}
		qs, err := insp.QueueStats(ctx)
		if err != nil {
			return err
		}
		for _, q := range qs {
			if m["queue"] != "" && q.Queue != m["queue"] {
				continue
			}
			ttd := "-"
			if q.TimeToDrainMs != nil {
				ttd = strconv.FormatInt(*q.TimeToDrainMs, 10)
			}
			if m["quiet"] == "1" {
				age, qttd, qage := "-", "-", "-"
				if q.OldestAvailableMs != nil {
					age = strconv.FormatInt(*q.OldestAvailableMs, 10)
				}
				if q.QuietGroups.TimeToDrainMs != nil {
					qttd = strconv.FormatInt(*q.QuietGroups.TimeToDrainMs, 10)
				}
				if q.QuietGroups.OldestAvailableMs != nil {
					qage = strconv.FormatInt(*q.QuietGroups.OldestAvailableMs, 10)
				}
				fmt.Printf("%s|%.3f|%.3f|%s|%s|%.3f|%.3f|%s|%s|%d|%t\n",
					q.Queue, q.ArrivalRate, q.DrainRate, ttd, age,
					q.QuietGroups.ArrivalRate, q.QuietGroups.DrainRate, qttd, qage,
					q.QuietGroups.NoisyPartitions, q.QuietGroups.Approximate)
			} else if m["age"] == "1" {
				age := "-"
				if q.OldestAvailableMs != nil {
					age = strconv.FormatInt(*q.OldestAvailableMs, 10)
				}
				fmt.Printf("%s|%.3f|%.3f|%s|%s\n", q.Queue, q.ArrivalRate, q.DrainRate, ttd, age)
			} else {
				fmt.Printf("%s|%.3f|%.3f|%s\n", q.Queue, q.ArrivalRate, q.DrainRate, ttd)
			}
		}
	case "explain":
		insp, ok := any(store).(headgate.InspectStore)
		if !ok {
			return fmt.Errorf("store has no Inspect surface")
		}
		ex, err := insp.ExplainAdmission(ctx, m["job"])
		if err != nil {
			return err
		}
		if ex == nil {
			fmt.Println("not_found")
			break
		}
		blocked := ex.BlockedBy
		if blocked == "" {
			blocked = "none"
		}
		fmt.Printf("admissible=%t blocked_by=%s\n", ex.Admissible, blocked)
	case "queue-weight":
		insp, ok := any(store).(headgate.InspectStore)
		if !ok {
			return fmt.Errorf("store has no Inspect surface")
		}
		if err := insp.SetQueueWeight(ctx, m["queue"], uint32(geti(m, "weight", 1))); err != nil {
			return err
		}
		fmt.Println("ok")
	case "concurrency":
		insp, ok := any(store).(headgate.InspectStore)
		if !ok {
			return fmt.Errorf("store has no Inspect surface")
		}
		strategy := headgate.SaturationStrategy(m["strategy"])
		if strategy == "" {
			strategy = headgate.SaturateQueue
		}
		if err := insp.UpsertConcurrencyLimit(ctx, headgate.ConcurrencyLimit{
			Name: m["name"], Queue: m["queue"], MaxConcurrent: uint64(geti(m, "max", 1)),
			OnSaturated: strategy,
		}); err != nil {
			return err
		}
		fmt.Println("ok")
	case "tx":
		// runtime capability boundary's Go form: the capability is a typed interface, asserted at runtime.
		txs, ok := any(store).(headgate.TransactionalStore)
		if !ok {
			return fmt.Errorf("store is not transactional")
		}
		tx, err := store.Begin(ctx)
		if err != nil {
			return err
		}
		e := headgate.Envelope{
			ID: m["id"], Kind: "w", Payload: []byte{0}, Queue: "default",
			Fingerprint: "fp", ScheduledAtMs: 1000,
		}
		if err := txs.EnqueueTx(ctx, tx, []headgate.Envelope{e}); err != nil {
			_ = headgatepgx.Rollback(ctx, tx)
			return err
		}
		switch m["mode"] {
		case "commit":
			err = headgatepgx.Commit(ctx, tx)
		case "rollback":
			err = headgatepgx.Rollback(ctx, tx)
		default:
			return fmt.Errorf("unknown tx mode %q", m["mode"])
		}
		if err != nil {
			return err
		}
		fmt.Println("ok")
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

// traceHeaders sets the two RESERVED telemetry and trace context envelope headers verbatim from tp= / ts=.
// Verbatim is the point: the store must round-trip an INVALID traceparent unchanged
// (opaque bytes down there), and only the runtime's parse treats it as absent.
func traceHeaders(m map[string]string) map[string]string {
	h := map[string]string{}
	if tp, ok := m["tp"]; ok {
		h[headgate.TraceparentHeader] = tp
	}
	if ts, ok := m["ts"]; ok {
		h[headgate.TracestateHeader] = ts
	}
	if len(h) == 0 {
		return nil
	}
	return h
}
