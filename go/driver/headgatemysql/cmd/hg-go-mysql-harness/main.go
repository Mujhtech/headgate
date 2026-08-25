// hg-go-mysql-harness drives the Go MySQL store for scripts/test-admission.sh — the
// same command grammar as hg-mysql-harness, so cross-language MySQL sections can
// interleave the two against one database. Connection comes from $HG_MYSQL.
//
// Errors print "ERR <message>" and exit 1, so the script can assert on them.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	headgate "github.com/mujhtech/headgate"
	headgatemysql "github.com/mujhtech/headgate/driver/headgatemysql"
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
	url := os.Getenv("HG_MYSQL")
	if url == "" {
		url = "mysql://root:hg@127.0.0.1:3307/hg"
	}
	store, err := headgatemysql.Connect(url)
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
				Priority: int32(geti(m, "priority", 0)),
				// worker safety's per-attempt timeout and absolute deadline, so the
				// grammar matches every other harness. WRITTEN, NOT RUN — no MySQL server
				// has been reachable to preserve compatibility (MYSQL_VERIFICATION.md).
				TimeoutMs:          geti(m, "timeout", 0),
				DeadlineMs:         geti(m, "deadline", 0),
				ScheduledAtMs:      geti(m, "sched", 1000),
				RetentionMs:        geti(m, "retention", 0),
				PeriodicScheduleID: m["schedule_id"],
				PeriodicTickMs:     geti(m, "tick", 0),
				MaxAttempts:        uint32(geti(m, "max_attempts", 25)),
				UniqueWindowMs:     geti(m, "window", 0),
				UniqueReplace:      uint32(geti(m, "replace", 0)),
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
	// ----- control plane Inspect the same commands and the same OUTPUT SHAPE as
	// hg-mysql-harness, so the conformance script can drive either binary through one
	// assertion. Available at all because the Go MySQL driver now answers InspectStore.
	case "qstats":
		qs, err := store.QueueStats(ctx)
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
	case "counts":
		// the harness names a queue explicitly, so it is always Some(queue).
		c, err := store.Counts(ctx, headgate.Ptr(m["queue"]))
		if err != nil {
			return err
		}
		states := make([]string, 0, len(c.Counts))
		for st := range c.Counts {
			states = append(states, st)
		}
		sort.Strings(states) // a map has no order; the Rust twin prints GROUP BY order
		for _, st := range states {
			fmt.Printf("%s=%d\n", st, c.Counts[st])
		}
	case "explain":
		ex, err := store.ExplainAdmission(ctx, m["job"])
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
		fmt.Printf("admissible=%v blocked_by=%s\n", ex.Admissible, blocked)
	case "pause":
		if err := store.SetQueuePaused(ctx, m["queue"], m["paused"] != "false"); err != nil {
			return err
		}
		fmt.Println("ok")
	case "queue-weight":
		if err := store.SetQueueWeight(ctx, m["queue"], uint32(geti(m, "weight", 1))); err != nil {
			return err
		}
		fmt.Println("ok")
	case "concurrency":
		strategy := headgate.SaturationStrategy(m["strategy"])
		if strategy == "" {
			strategy = headgate.SaturateQueue
		}
		if err := store.UpsertConcurrencyLimit(ctx, headgate.ConcurrencyLimit{
			Name: m["name"], Queue: m["queue"], MaxConcurrent: uint64(geti(m, "max", 1)),
			OnSaturated: strategy,
		}); err != nil {
			return err
		}
		fmt.Println("ok")
	case "sweep-quarantine":
		n, err := store.QuarantineSweep(ctx, geti(m, "limit", 1000))
		if err != nil {
			return err
		}
		fmt.Println(n)
	case "quarantine-release":
		n, err := store.QuarantineRelease(ctx, m["fp"])
		if err != nil {
			return err
		}
		fmt.Println(n)
	case "state":
		// The Rust twin reads this with raw SQL; the Go store exposes no raw handle, so
		// it goes through the Inspect surface instead — same answer, one more method
		// exercised. A missing job prints the empty line the Rust twin prints.
		j, err := store.GetJob(ctx, m["job"], false)
		if err != nil {
			return err
		}
		if j == nil {
			fmt.Println("")
			break
		}
		fmt.Println(j.State)
	case "schedules":
		// surveyed policy behavior the scheduler duty's read side, for the script's MySQL assertions:
		// one line per schedule, `id|spec|next_run_ms|paused`.
		ss, err := store.ListSchedules(ctx)
		if err != nil {
			return err
		}
		for _, e := range ss {
			fmt.Printf("%s|%s|%d|%v\n", e.ID, e.Spec, e.NextRunMs, e.Paused)
		}
	case "drain":
		// Cross-language execution conformance: kind-`w` jobs through the REAL Go
		// runtime path — dispatch, handler, ack — over the Redis store.
		reg := headgate.NewRegistry()
		if err := headgate.RegisterFunc[wireMsg](reg, func(context.Context, *headgate.Job[wireMsg]) error {
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
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}
