package headgatesqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

func admitQueues(t *testing.T, store *SqliteStore, lease string, queues ...string) []headgate.AdmissionUnit {
	t.Helper()
	units, err := store.Admit(context.Background(), headgate.AdmitRequest{Worker: "policy-worker", LeaseID: lease, Queues: queues, Capacity: 1, Lease: time.Minute, Quantum: 1})
	if err != nil {
		t.Fatal(err)
	}
	return units
}

func TestSqliteStore_AdmissionPolicyGate(t *testing.T) {
	t.Parallel()
	store := openLifecycleStore(t, DefaultOptions())
	ctx := context.Background()

	paused := testEnvelope("paused", []byte("paused"))
	paused.Queue = "paused"
	if err := store.Enqueue(ctx, []headgate.Envelope{paused}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetQueuePaused(ctx, "paused", true); err != nil {
		t.Fatal(err)
	}
	if got := admitQueues(t, store, "paused-lease", "paused"); len(got) != 0 {
		t.Fatalf("paused admitted %#v", got)
	}
	var state string
	if err := store.db.QueryRowContext(ctx, "SELECT state FROM headgate_job WHERE id=?", paused.ID).Scan(&state); err != nil || state != "available" {
		t.Fatalf("paused state=%q err=%v", state, err)
	}

	if err := store.UpsertRateClass(ctx, headgate.RateClassConfig{Name: "api", Limit: 0, WindowMs: 1000, Burst: 1}); err != nil {
		t.Fatal(err)
	}
	first, second := testEnvelope("rate-1", []byte("one")), testEnvelope("rate-2", []byte("two"))
	first.Queue, second.Queue, first.RateClass, second.RateClass = "rate", "rate", "api", "api"
	if err := store.Enqueue(ctx, []headgate.Envelope{first, second}); err != nil {
		t.Fatal(err)
	}
	claim := admitQueues(t, store, "rate-first", "rate")[0].Claims[0]
	if got := admitQueues(t, store, "rate-blocked", "rate"); len(got) != 0 {
		t.Fatalf("empty bucket admitted %#v", got)
	}
	zero := uint32(0)
	if err := store.AckAttemptWithActualWeight(ctx, headgate.LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}, headgate.OutcomeSuccess, "", 0, nil, &zero); err != nil {
		t.Fatal(err)
	}
	if got := admitQueues(t, store, "rate-refund", "rate"); len(got) != 1 {
		t.Fatalf("refunded bucket admitted %#v", got)
	}

	if err := store.UpsertConcurrencyLimit(ctx, headgate.ConcurrencyLimit{Name: "serial", Queue: "serial", MaxConcurrent: 1, OnSaturated: headgate.SaturateQueue}); err != nil {
		t.Fatal(err)
	}
	a, b := testEnvelope("serial-1", []byte("a")), testEnvelope("serial-2", []byte("b"))
	a.Queue, b.Queue, a.PartitionKey, b.PartitionKey = "serial", "serial", "tenant", "tenant"
	if err := store.Enqueue(ctx, []headgate.Envelope{a, b}); err != nil {
		t.Fatal(err)
	}
	if got := admitQueues(t, store, "serial-first", "serial"); len(got) != 1 {
		t.Fatalf("first serial=%#v", got)
	}
	if got := admitQueues(t, store, "serial-block", "serial"); len(got) != 0 {
		t.Fatalf("saturated serial=%#v", got)
	}
}

func TestSqliteStore_WeightedQueuesDoNotMixWithPriority(t *testing.T) {
	t.Parallel()
	store := openLifecycleStore(t, DefaultOptions())
	ctx := context.Background()
	if err := store.SetQueueWeight(ctx, "heavy", 2); err != nil {
		t.Fatal(err)
	}
	if err := store.SetQueueWeight(ctx, "light", 1); err != nil {
		t.Fatal(err)
	}
	var jobs []headgate.Envelope
	for i := 0; i < 6; i++ {
		h := testEnvelope(fmt.Sprintf("heavy-%d", i), []byte(fmt.Sprintf("h%d", i)))
		h.Queue = "heavy"
		h.Priority = -100
		l := testEnvelope(fmt.Sprintf("light-%d", i), []byte(fmt.Sprintf("l%d", i)))
		l.Queue = "light"
		l.Priority = 100
		jobs = append(jobs, h, l)
	}
	if err := store.Enqueue(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for i := 0; i < 6; i++ {
		units := admitQueues(t, store, fmt.Sprintf("weighted-%d", i), "heavy", "light")
		if len(units) != 1 {
			t.Fatalf("round %d=%#v", i, units)
		}
		counts[units[0].Claims[0].Envelope.Queue]++
	}
	if counts["heavy"] != 4 || counts["light"] != 2 {
		t.Fatalf("weighted counts=%v", counts)
	}
}
