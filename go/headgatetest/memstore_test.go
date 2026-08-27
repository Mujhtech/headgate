package headgatetest

// The proof that matters (adaptive admission's port lesson again): the REAL Runner drains the memory
// store unchanged — typed dispatch, retries, uniqueness, quarantine, retention — with
// no database anywhere.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

type tMsg struct {
	Mode string `json:"mode"`
}

func (tMsg) Kind() string { return "memtest:msg" }

func env(id, mode string) headgate.Envelope {
	payload := []byte(`{"mode":"` + mode + `"}`)
	return headgate.Envelope{
		ID: id, Kind: "memtest:msg", Payload: payload,
		Queue: "mem", Fingerprint: headgate.Fingerprint("memtest:msg", payload),
		ScheduledAtMs: 1, RetentionMs: 86_400_000,
	}
}

func runner(s *MemStore) (*headgate.Runner, *headgate.Registry) {
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[tMsg](reg, func(ctx context.Context, job *headgate.Job[tMsg]) error {
		switch job.Args.Mode {
		case "ok":
			return nil
		case "fail":
			// attempt-log contract: what the handler logs rides the ack into the attempt entry.
			headgate.Log(ctx, "opened upstream connection")
			headgate.Logf(ctx, "upstream returned %d", 500)
			return errors.New("boom")
		case "skip":
			return headgate.ErrSkipJob
		default:
			return errors.New("unexpected mode " + job.Args.Mode)
		}
	})
	return headgate.NewRunner(s, reg, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"mem": {MaxWorkers: 4}},
		LeaseDuration: 30 * time.Second,
	}), reg
}

func TestTheRealRunnerDrainsTheMemoryStore(t *testing.T) {
	s := New()
	ctx := context.Background()
	r, _ := runner(s)
	if err := s.Enqueue(ctx, []headgate.Envelope{
		env("m-ok", "ok"), env("m-fail", "fail"), env("m-skip", "skip"),
	}); err != nil {
		t.Fatal(err)
	}
	// Round 32k: the ASSERT-ENQUEUED helper, used rather than merely shipped. It asks the
	// question a producer test actually has ("did three memtest:msg jobs land on queue
	// `mem`?") instead of the question the id-lookup form forces ("is `m-ok` present?",
	// which presumes the answer).
	RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg", Queue: Ptr("mem"), Count: Ptr(3)})
	RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg", Payload: []byte(`{"mode":"skip"}`), Count: Ptr(1)})
	done, err := r.Drain(ctx, 10)
	if err != nil || len(done) != 3 {
		t.Fatalf("drain: %v %v", done, err)
	}
	if _, st, _ := s.JobState("m-ok"); st != "completed" {
		t.Fatalf("ok: %s", st)
	}
	if _, st, _ := s.JobState("m-skip"); st != "archived" {
		t.Fatalf("skip: %s", st)
	}
	e, st, _ := s.JobState("m-fail")
	if st != "retryable" || e.Attempt != 1 || e.CrashAttempt != 0 {
		t.Fatalf("fail: %s attempt=%d crash=%d", st, e.Attempt, e.CrashAttempt)
	}
	hist := strings.Join(s.Errors("m-fail"), "\n")
	if !strings.Contains(hist, "upstream returned 500") {
		t.Fatalf("per-attempt logs must land in the history: %s", hist)
	}
	// The retry drains again once its backoff passes — step the STORE clock, no sleep.
	s.NowFunc = func() time.Time { return time.Now().Add(2 * time.Hour) }
	done, err = r.Drain(ctx, 10)
	if err != nil || len(done) != 1 {
		t.Fatalf("retry drain: %v %v", done, err)
	}
	// The handler is mode-driven, so the second attempt fails too: attempt counts up.
	if e, st, _ := s.JobState("m-fail"); st != "retryable" || e.Attempt != 2 {
		t.Fatalf("after retry: %s attempt=%d", st, e.Attempt)
	}
}

func TestLifecycleFidelity(t *testing.T) {
	s := New()
	s.CrashLimit = 1
	ctx := context.Background()

	// Uniqueness, both modes.
	uq := env("u-1", "ok")
	uq.UniqueKey = []byte("K1")
	if err := s.Enqueue(ctx, []headgate.Envelope{uq}); err != nil {
		t.Fatal(err)
	}
	dup := env("u-2", "ok")
	dup.UniqueKey = []byte("K1")
	var de *headgate.DuplicateError
	if err := s.Enqueue(ctx, []headgate.Envelope{dup}); !errors.As(err, &de) || de.ExistingID != "u-1" {
		t.Fatalf("lifecycle dup: %v", err)
	}
	th := env("t-1", "ok")
	th.UniqueKey, th.UniqueWindowMs = []byte("K2"), 60_000
	if err := s.Enqueue(ctx, []headgate.Envelope{th}); err != nil {
		t.Fatal(err)
	}
	th2 := env("t-2", "ok")
	th2.UniqueKey, th2.UniqueWindowMs = []byte("K2"), 60_000
	if err := s.Enqueue(ctx, []headgate.Envelope{th2}); !errors.As(err, &de) {
		t.Fatalf("throttle dup: %v", err)
	}

	// Crash -> quarantine at the limit; sibling enqueues rejected; fence-gated ack.
	units, err := s.Admit(ctx, headgate.AdmitRequest{
		Worker: "w", LeaseID: "L1", Queues: []string{"mem"}, Capacity: 10,
		Lease: time.Millisecond, Quantum: 100,
	})
	if err != nil || len(units) != 2 {
		t.Fatalf("admit: %d %v", len(units), err)
	}
	s.NowFunc = func() time.Time { return time.Now().Add(time.Minute) }
	// Round 32h: this counted two records and inspected only rec[0] — the quarantine
	// arm could have regressed for exactly one of the two and still passed, and the
	// NAMES of the reclaimed jobs were never checked at all.
	rec, err := s.ReclaimExpired(ctx, 10)
	if err != nil || len(rec) != 2 {
		t.Fatalf("reclaim: %+v %v", rec, err)
	}
	got := map[string]bool{}
	for _, r := range rec {
		got[r.JobID] = r.Quarantined
	}
	// u-1 and t-1 are the two rows that actually landed (u-2 and t-2 were refused as
	// duplicates), and they share the kind — so the same fingerprint, so both cross the
	// crash limit together. Names AND the quarantine flag, on both.
	if len(got) != 2 || !got["u-1"] || !got["t-1"] {
		t.Fatalf("reclaim must NAME both jobs and quarantine both: %+v", rec)
	}
	sib := env("u-3", "ok")
	var qe *headgate.QuarantinedError
	if err := s.Enqueue(ctx, []headgate.Envelope{sib}); !errors.As(err, &qe) {
		t.Fatalf("quarantined enqueue: %v", err)
	}
	lease := headgate.LeaseRef{JobID: "u-1", LeaseID: "L1", Fence: 1}
	var lr *headgate.LeaseRejectedError
	if err := s.Ack(ctx, lease, headgate.OutcomeSuccess, "", 0); !errors.As(err, &lr) {
		t.Fatalf("stale ack must be rejected, got %v", err)
	}

	// Retention: ephemeral deletes at ack; retained evicts only after the lapse.
	s.NowFunc = time.Now
	eph := env("r-eph", "ok")
	eph.Fingerprint, eph.RetentionMs = "fp-r1", 0
	keep := env("r-keep", "ok")
	keep.Fingerprint, keep.RetentionMs = "fp-r2", 60_000
	if err := s.Enqueue(ctx, []headgate.Envelope{eph, keep}); err != nil {
		t.Fatal(err)
	}
	units, _ = s.Admit(ctx, headgate.AdmitRequest{
		Worker: "w", LeaseID: "L2", Queues: []string{"mem"}, Capacity: 10,
		Lease: 30 * time.Second, Quantum: 100,
	})
	for _, u := range units {
		c := u.Claims[0]
		_ = s.Ack(ctx, headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence},
			headgate.OutcomeSuccess, "", 0)
	}
	if _, _, ok := s.JobState("r-eph"); ok {
		t.Fatal("retention 0 must delete at ack")
	}
	if n, _ := s.EvictRetained(ctx, 100); n != 0 {
		t.Fatalf("nothing lapsed yet, evicted %d", n)
	}
	s.NowFunc = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if n, _ := s.EvictRetained(ctx, 100); n != 1 {
		t.Fatalf("lapsed eviction: %d", n)
	}
	if _, _, ok := s.JobState("r-keep"); ok {
		t.Fatal("lapsed retained job must be evicted")
	}
}

func TestUniqueConflictReplacementAllowlistAndRunningGuard(t *testing.T) {
	s := New()
	ctx := context.Background()
	original := env("replace-original", "old")
	original.UniqueKey = []byte("replace-key")
	original.Priority = 1
	if err := s.Enqueue(ctx, []headgate.Envelope{original}); err != nil {
		t.Fatal(err)
	}
	incoming := env("replace-new", "new")
	incoming.UniqueKey = []byte("replace-key")
	incoming.Priority = 9
	incoming.Queue = "immutable-route"
	incoming.UniqueReplace = headgate.UniqueReplacePayload | headgate.UniqueReplacePriority
	var dup *headgate.DuplicateError
	if err := s.Enqueue(ctx, []headgate.Envelope{incoming}); !errors.As(err, &dup) || !dup.Replaced || dup.ExistingID != original.ID {
		t.Fatalf("replace: %#v %v", dup, err)
	}
	updated, state, _ := s.JobState(original.ID)
	if string(updated.Payload) != `{"mode":"new"}` || updated.Priority != 9 || updated.Queue != "mem" || state != "available" {
		t.Fatalf("updated: %+v state=%s", updated, state)
	}
	if _, err := s.Admit(ctx, headgate.AdmitRequest{Worker: "w", LeaseID: "L", Queues: []string{"mem"}, Capacity: 1, Lease: time.Minute, Quantum: 100}); err != nil {
		t.Fatal(err)
	}
	blocked := env("replace-running", "blocked")
	blocked.UniqueKey = []byte("replace-key")
	blocked.Priority = 20
	blocked.UniqueReplace = headgate.UniqueReplacePriority
	if err := s.Enqueue(ctx, []headgate.Envelope{blocked}); !errors.As(err, &dup) || dup.Replaced {
		t.Fatalf("running replace: %#v %v", dup, err)
	}
	updated, _, _ = s.JobState(original.ID)
	if updated.Priority != 9 {
		t.Fatalf("running holder mutated: %d", updated.Priority)
	}
}

func TestDebounceScopeTagsPendingAndTestBypassAreExplicit(t *testing.T) {
	s := New()
	s.NowFunc = func() time.Time { return time.UnixMilli(1000) }
	ctx := context.Background()
	first := env("debounce-first", "old")
	first.UniqueKey = []byte("event")
	first.UniqueDebounceMs = 500
	first.Tags = []string{"blue", "billing"}
	if err := s.Enqueue(ctx, []headgate.Envelope{first}); err != nil {
		t.Fatal(err)
	}
	later := env("debounce-later", "new")
	later.UniqueKey = []byte("event")
	later.UniqueDebounceMs = 500
	later.Tags = []string{"urgent"}
	var dup *headgate.DuplicateError
	if err := s.Enqueue(ctx, []headgate.Envelope{later}); !errors.As(err, &dup) || !dup.Replaced {
		t.Fatalf("debounce: %#v %v", dup, err)
	}
	held, state, _ := s.JobState(first.ID)
	if string(held.Payload) != "{\"mode\":\"new\"}" || state != "scheduled" || held.ScheduledAtMs != 1500 || len(held.Tags) != 1 || held.Tags[0] != "urgent" {
		t.Fatalf("held=%+v state=%s", held, state)
	}
	other := env("other-kind", "ok")
	other.Kind = "tk:other"
	other.UniqueKey = []byte("event")
	if err := s.Enqueue(ctx, []headgate.Envelope{other}); err != nil {
		t.Fatal(err)
	}
	global := env("global", "ok")
	global.UniqueKey = []byte("global-key")
	global.UniqueExcludeKind = true
	if err := s.Enqueue(ctx, []headgate.Envelope{global}); err != nil {
		t.Fatal(err)
	}
	globalOther := env("global-other", "ok")
	globalOther.Kind = "tk:other"
	globalOther.UniqueKey = []byte("global-key")
	globalOther.UniqueExcludeKind = true
	if err := s.Enqueue(ctx, []headgate.Envelope{globalOther}); !errors.As(err, &dup) {
		t.Fatalf("global scope: %v", err)
	}
	bypass := env("bypass", "ok")
	bypass.UniqueKey = []byte("global-key")
	bypass.UniqueExcludeKind = true
	if err := s.EnqueueWithoutUniqueness(ctx, []headgate.Envelope{bypass}); err != nil {
		t.Fatal(err)
	}
	pending := env("pending", "ok")
	pending.Pending = true
	pending.ScheduledAtMs = 0
	if err := s.Enqueue(ctx, []headgate.Envelope{pending}); err != nil {
		t.Fatal(err)
	}
	units, err := s.Admit(ctx, headgate.AdmitRequest{Worker: "w", LeaseID: "P", Queues: []string{"mem"}, Capacity: 100, Lease: time.Second, Quantum: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range units {
		if u.Claims[0].Envelope.ID == "pending" {
			t.Fatal("pending job was admitted")
		}
	}
}

func TestFairnessSpansPartitionsAndRateLimitCaps(t *testing.T) {
	s := New()
	ctx := context.Background()
	batch := []headgate.Envelope{}
	for i := 0; i < 50; i++ {
		e := env(fmt.Sprintf("noisy-%d", i), "ok")
		e.PartitionKey = "noisy"
		batch = append(batch, e)
	}
	a, b := env("a-1", "ok"), env("b-1", "ok")
	a.PartitionKey, b.PartitionKey = "A", "B"
	batch = append(batch, a, b)
	if err := s.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}
	units, err := s.Admit(ctx, headgate.AdmitRequest{
		Worker: "w", LeaseID: "LF", Queues: []string{"mem"}, Capacity: 3,
		Lease: 30 * time.Second, Quantum: 1,
	})
	if err != nil || len(units) != 3 {
		t.Fatalf("admit: %d %v", len(units), err)
	}
	parts := map[string]bool{}
	for _, u := range units {
		parts[u.Claims[0].Envelope.PartitionKey] = true
	}
	if len(parts) != 3 {
		t.Fatalf("fairness must span partitions under a flood: %v", parts)
	}

	// Fleet rate limit caps at the bucket.
	s2 := New()
	s2.SetRateLimit("stripe", 5, 1000, 5)
	batch = nil
	for i := 0; i < 20; i++ {
		e := env(fmt.Sprintf("rc-%d", i), "ok")
		e.RateClass = "stripe"
		batch = append(batch, e)
	}
	if err := s2.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}
	units, err = s2.Admit(ctx, headgate.AdmitRequest{
		Worker: "w", LeaseID: "LR", Queues: []string{"mem"}, Capacity: 100,
		Lease: 30 * time.Second, Quantum: 100,
	})
	if err != nil || len(units) != 5 {
		t.Fatalf("rate cap: %d %v", len(units), err)
	}
}

func TestWeightedRateCostsAreChargedAndReconciledUnderTheFence(t *testing.T) {
	ctx := context.Background()
	weighted := func(id string, weight uint32) headgate.Envelope {
		e := env(id, "ok")
		e.RateClass, e.Weight = "points", weight
		return e
	}
	req := func(leaseID string) headgate.AdmitRequest {
		return headgate.AdmitRequest{
			Worker: "w", LeaseID: leaseID, Queues: []string{"mem"}, Capacity: 100,
			Lease: 30 * time.Second, Quantum: 100,
		}
	}
	actual := func(n uint32) *uint32 { return &n }

	// 3 + 2 exhausts five points; the trailing one-point job remains visible.
	s := New()
	s.NowFunc = func() time.Time { return time.UnixMilli(10_000) }
	s.SetRateLimit("points", 0, 60_000, 5)
	if err := s.Enqueue(ctx, []headgate.Envelope{
		weighted("cost-a", 3), weighted("cost-b", 2), weighted("cost-c", 1),
	}); err != nil {
		t.Fatal(err)
	}
	units, err := s.Admit(ctx, req("L1"))
	if err != nil || len(units) != 2 || units[0].Claims[0].Envelope.ID != "cost-a" || units[1].Claims[0].Envelope.ID != "cost-b" {
		t.Fatalf("admission must spend envelope weights, not rows: %+v %v", units, err)
	}

	// Actual one against estimate three refunds two, admitting cost-c. Actual four
	// against estimate two then debits two; a later one-point job must remain blocked.
	a := units[0].Claims[0]
	if err := s.AckAttemptWithActualWeight(ctx,
		headgate.LeaseRef{JobID: a.Envelope.ID, LeaseID: a.LeaseID, Fence: a.Fence},
		headgate.OutcomeSuccess, "", 0, nil, actual(1)); err != nil {
		t.Fatal(err)
	}
	next, err := s.Admit(ctx, req("L2"))
	if err != nil || len(next) != 1 || next[0].Claims[0].Envelope.ID != "cost-c" {
		t.Fatalf("refund must admit cost-c: %+v %v", next, err)
	}
	b := units[1].Claims[0]
	if err := s.AckAttemptWithActualWeight(ctx,
		headgate.LeaseRef{JobID: b.Envelope.ID, LeaseID: b.LeaseID, Fence: b.Fence},
		headgate.OutcomeSuccess, "", 0, nil, actual(4)); err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue(ctx, []headgate.Envelope{weighted("cost-d", 1)}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Admit(ctx, req("L3")); err != nil || len(got) != 0 {
		t.Fatalf("an underestimated actual cost must drive the bucket negative: %+v %v", got, err)
	}

	// Zero is a real actual cost (a full refund), not an omitted-value sentinel.
	refunded := New()
	refunded.NowFunc = func() time.Time { return time.UnixMilli(20_000) }
	refunded.SetRateLimit("points", 0, 60_000, 3)
	if err := refunded.Enqueue(ctx, []headgate.Envelope{weighted("refund-a", 3)}); err != nil {
		t.Fatal(err)
	}
	held, _ := refunded.Admit(ctx, req("R1"))
	c := held[0].Claims[0]
	if err := refunded.AckAttemptWithActualWeight(ctx,
		headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence},
		headgate.OutcomeSuccess, "", 0, nil, actual(0)); err != nil {
		t.Fatal(err)
	}
	if err := refunded.Enqueue(ctx, []headgate.Envelope{weighted("refund-b", 3)}); err != nil {
		t.Fatal(err)
	}
	if got, err := refunded.Admit(ctx, req("R2")); err != nil || len(got) != 1 {
		t.Fatalf("actual zero must fully refund the estimate: %+v %v", got, err)
	}

	// A class created after admission cannot retroactively charge fail-open work.
	failOpen := New()
	failOpen.NowFunc = func() time.Time { return time.UnixMilli(30_000) }
	if err := failOpen.Enqueue(ctx, []headgate.Envelope{weighted("open-a", 3)}); err != nil {
		t.Fatal(err)
	}
	held, _ = failOpen.Admit(ctx, req("O1"))
	c = held[0].Claims[0]
	failOpen.SetRateLimit("points", 0, 60_000, 5)
	if err := failOpen.AckAttemptWithActualWeight(ctx,
		headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence},
		headgate.OutcomeSuccess, "", 0, nil, actual(10)); err != nil {
		t.Fatal(err)
	}
	if err := failOpen.Enqueue(ctx, []headgate.Envelope{weighted("open-b", 5)}); err != nil {
		t.Fatal(err)
	}
	if got, err := failOpen.Admit(ctx, req("O2")); err != nil || len(got) != 1 {
		t.Fatalf("late class creation must leave fail-open work uncharged: %+v %v", got, err)
	}

	// A stale fence changes neither the job nor bucket. This catches a correction that
	// accidentally happens in a separate transaction before identity is checked.
	fenced := New()
	fenced.NowFunc = func() time.Time { return time.UnixMilli(40_000) }
	fenced.SetRateLimit("points", 0, 60_000, 3)
	if err := fenced.Enqueue(ctx, []headgate.Envelope{weighted("fence-a", 3)}); err != nil {
		t.Fatal(err)
	}
	held, _ = fenced.Admit(ctx, req("F1"))
	c = held[0].Claims[0]
	stale := headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence + 1}
	var rejected *headgate.LeaseRejectedError
	if err := fenced.AckAttemptWithActualWeight(ctx, stale, headgate.OutcomeSuccess, "", 0, nil, actual(0)); !errors.As(err, &rejected) {
		t.Fatalf("stale correction must be rejected, got %v", err)
	}
	if err := fenced.Enqueue(ctx, []headgate.Envelope{weighted("fence-b", 3)}); err != nil {
		t.Fatal(err)
	}
	if got, err := fenced.Admit(ctx, req("F2")); err != nil || len(got) != 0 {
		t.Fatalf("stale correction must not refund the bucket: %+v %v", got, err)
	}
}

// idempotent enqueue identity the strict caller-supplied id contract, at the store port.
//
// Before round 32 every backend answered a repeated id the same wrong way: a bare
// "duplicate job id" that the API served as a 400, whether or not the caller was simply
// retrying the identical enqueue. The contract is now split by CONTENT.
func TestCallerSuppliedIDIsIdempotentOnMatchAndConflictsOnChange(t *testing.T) {
	ctx := context.Background()
	m := New()
	if err := m.Enqueue(ctx, []headgate.Envelope{env("idc-1", "ok")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Same id, same (kind, fingerprint, queue) -> idempotent success, NOT duplicated.
	if err := m.Enqueue(ctx, []headgate.Envelope{env("idc-1", "ok")}); err != nil {
		t.Fatalf("idempotent re-enqueue must succeed, got %v", err)
	}
	total := 0
	for _, n := range m.Counts("") {
		total += n
	}
	if total != 1 {
		t.Fatalf("an idempotent re-enqueue must not create a second job; got %d", total)
	}

	// Same id, different payload (hence a different content fingerprinting fingerprint) -> typed conflict.
	var idc *headgate.IDConflictError
	err := m.Enqueue(ctx, []headgate.Envelope{env("idc-1", "fail")})
	if !errors.As(err, &idc) || idc.JobID != "idc-1" {
		t.Fatalf("want IDConflictError for idc-1, got %v", err)
	}
	if err.Error() != "headgate: id conflict: job idc-1" {
		t.Fatalf("uniform message drifted: %q", err.Error())
	}
	// Same id, different QUEUE -> conflict too: routing is part of the identity.
	moved := env("idc-1", "ok")
	moved.Queue = "elsewhere"
	if err := m.Enqueue(ctx, []headgate.Envelope{moved}); !errors.As(err, &idc) {
		t.Fatalf("a re-enqueue into another queue must conflict, got %v", err)
	}

	// The batch is all-or-nothing: one conflict rejects it naming the offender, and the
	// clean sibling in the same batch is NOT written.
	err = m.Enqueue(ctx, []headgate.Envelope{env("idc-2", "ok"), env("idc-1", "fail")})
	if !errors.As(err, &idc) || idc.JobID != "idc-1" {
		t.Fatalf("want IDConflictError naming idc-1, got %v", err)
	}
	if _, _, ok := m.JobState("idc-2"); ok {
		t.Fatal("a rejected batch must write nothing")
	}
	// A repeated id WITHIN one batch is the same conflict, not a constraint error.
	err = m.Enqueue(ctx, []headgate.Envelope{env("idc-3", "ok"), env("idc-3", "ok")})
	if !errors.As(err, &idc) || idc.JobID != "idc-3" {
		t.Fatalf("want IDConflictError naming idc-3, got %v", err)
	}
}

// typed dispatch the kind-format rule is enforced at the STORE boundary, because the control API
// and the conformance harnesses call Store.Enqueue directly and never come through the
// runtime. The message is what the API serves verbatim in a 400.
func TestStoreEnqueueEnforcesTheKindFormatRule(t *testing.T) {
	ctx := context.Background()
	m := New()
	for _, bad := range []string{"", "bad kind", "-leading", "a!", strings.Repeat("x", 129)} {
		e := env("kf-1", "ok")
		e.Kind = bad
		err := m.Enqueue(ctx, []headgate.Envelope{e})
		if err == nil {
			t.Fatalf("kind %q must be rejected", bad)
		}
		want := "headgate: invalid kind `" + bad + "`:"
		if !strings.HasPrefix(err.Error(), want) {
			t.Fatalf("kind %q: got %q, want prefix %q", bad, err.Error(), want)
		}
	}
	// The corpus's single-character kind stays legal (River would refuse it).
	ok := env("kf-ok", "ok")
	ok.Kind = "w"
	ok.Fingerprint = headgate.Fingerprint("w", ok.Payload)
	if err := m.Enqueue(ctx, []headgate.Envelope{ok}); err != nil {
		t.Fatalf("single-character kind must be legal: %v", err)
	}
}

// ===========================================================================
// Round 32k. Four capabilities the register claimed and round 32j's evidence linter
// could not resolve to anything: the assert-enqueued helper, the execute-one-job helper,
// alias DISPATCH (as opposed to alias declaration), and the IsFailure port. All four are
// provable with no database, which is why they belong here. Rust twins live in
// crates/headgate-testkit/tests/memstore.rs.
// ===========================================================================

// The helper itself, in both directions. A matcher that always says yes is not a matcher,
// so the negative cases are the assertion and the message content is part of the contract.
func TestRequireEnqueuedMatchesADescriptionAndNamesWhatItFound(t *testing.T) {
	s := New()
	ctx := context.Background()
	a := env("ae-1", "ok")
	a.Queue, a.ScheduledAtMs = "mail", 4242
	b := env("ae-2", "ok")
	b.Queue, b.PartitionKey = "mail", "tenant-b"
	c := env("ae-3", "fail") // queue `mem`
	if err := s.Enqueue(ctx, []headgate.Envelope{a, b, c}); err != nil {
		t.Fatal(err)
	}

	if got := RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg"}); len(got) != 3 {
		t.Fatalf("kind alone must match all three, got %d", len(got))
	}
	if got := RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg", Queue: Ptr("mail")}); len(got) != 2 {
		t.Fatalf("queue matcher: got %d want 2", len(got))
	}
	if got := RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg", ScheduledAtMs: Ptr(int64(4242))}); got[0].ID != "ae-1" {
		t.Fatalf("scheduled-at matcher: got %q", got[0].ID)
	}
	if got := RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg", PartitionKey: Ptr("tenant-b")}); got[0].ID != "ae-2" {
		t.Fatalf("partition matcher: got %q", got[0].ID)
	}
	if got := RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg", Payload: []byte(`{"mode":"fail"}`)}); got[0].ID != "ae-3" {
		t.Fatalf("payload matcher: got %q", got[0].ID)
	}
	RequireEnqueued(t, s, Enqueued{Kind: "memtest:msg", Queue: Ptr("mail"), Count: Ptr(2)})

	// Negative: EVERY matcher must be able to say no, or it is decoration.
	for _, want := range []Enqueued{
		{Kind: "nope:nothing"},
		{Kind: "memtest:msg", Queue: Ptr("priority")},
		{Kind: "memtest:msg", Payload: []byte(`{"mode":"never"}`)},
		{Kind: "memtest:msg", ScheduledAtMs: Ptr(int64(999999))},
		{Kind: "memtest:msg", PartitionKey: Ptr("tenant-z")},
		{Kind: "memtest:msg", Count: Ptr(99)},
	} {
		if _, err := FindEnqueued(s, want); err == nil {
			t.Fatalf("matcher must reject: %+v", want)
		}
	}

	// The failure message is the deliverable: it names what WAS there, not just that the
	// lookup failed. Without this the helper is an `ok` bool with more ceremony.
	_, err := FindEnqueued(s, Enqueued{Kind: "memtest:msg", Queue: Ptr("priority")})
	msg := err.Error()
	for _, want := range []string{
		`queue "priority"`, "0 match(es) found among 3 enqueued job(s)",
		`id="ae-1"`, `queue="mail"`,
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("failure message must contain %q:\n%s", want, msg)
		}
	}
	// And on an empty store it says so, rather than printing an empty list nobody reads.
	if _, err := FindEnqueued(New(), Enqueued{Kind: "memtest:msg"}); !strings.Contains(err.Error(), "the store is EMPTY") {
		t.Fatalf("empty store message: %v", err)
	}
}

// typed dispatch the capability the Kind-aliases row actually names: a job enqueued under the OLD
// kind reaches the RENAMED handler. Every citation that row had proved only that aliases
// are declared, format-checked and collision-checked — nothing dispatched one.
type renamedArgs struct {
	Body string `json:"body"`
}

func (renamedArgs) Kind() string { return "memtest:renamed" }

// KindAliases is the pre-rename dispatch key. payload versioning versioned the payload and left the KEY
// unrenameable; this is the door that closes.
func (renamedArgs) KindAliases() []string { return []string{"memtest:old-name"} }

func TestAJobEnqueuedUnderTheOldKindDispatchesToTheRenamedHandler(t *testing.T) {
	s := New()
	ctx := context.Background()
	var seen []string
	reg := headgate.NewRegistry()
	if err := headgate.RegisterFunc[renamedArgs](reg, func(_ context.Context, job *headgate.Job[renamedArgs]) error {
		seen = append(seen, job.ID+":"+job.Args.Body)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := headgate.NewRunner(s, reg, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"rn": {MaxWorkers: 4}},
		LeaseDuration: 30 * time.Second, DisableDuties: true,
	})
	mk := func(id, kind string) headgate.Envelope {
		payload := []byte(`{"body":"x"}`)
		return headgate.Envelope{
			ID: id, Kind: kind, Payload: payload, Queue: "rn",
			Fingerprint:   headgate.Fingerprint(kind, payload),
			ScheduledAtMs: 1, RetentionMs: 86_400_000,
		}
	}
	// The OLD key first (ids order the draw), so a registry that only answered to Kind()
	// fails here rather than being carried by its sibling.
	if err := s.Enqueue(ctx, []headgate.Envelope{
		mk("rn-a-old", "memtest:old-name"), mk("rn-b-new", "memtest:renamed"),
	}); err != nil {
		t.Fatal(err)
	}

	// Round 32k: PERFORM_ONE, used rather than merely shipped — one job, real dispatch
	// path, and the runtime's own verdict rather than a re-read of the store's.
	first, ok, err := r.PerformOne(ctx)
	if err != nil || !ok {
		t.Fatalf("PerformOne: %v %v", ok, err)
	}
	if first.JobID != "rn-a-old" || first.Kind != "memtest:old-name" {
		t.Fatalf("the older job is drawn first and carries the pre-rename key: %+v", first)
	}
	if first.Outcome != "success" {
		t.Fatalf("a job enqueued under the OLD kind must DISPATCH to the renamed handler, "+
			"not snooze forever as an unregistered kind: outcome %q", first.Outcome)
	}
	second, ok, _ := r.PerformOne(ctx)
	if !ok || second.JobID != "rn-b-new" || second.Outcome != "success" {
		t.Fatalf("and then the other: %+v ok=%v", second, ok)
	}
	if _, ok, _ := r.PerformOne(ctx); ok {
		t.Fatal("the queue is empty now — PerformOne must report that instead of inventing a job")
	}
	if fmt.Sprint(seen) != fmt.Sprint([]string{"rn-a-old:x", "rn-b-new:x"}) {
		t.Fatalf("ONE handler must answer both keys and decode both payloads: %v", seen)
	}
	if _, st, _ := s.JobState("rn-a-old"); st != "completed" {
		t.Fatalf("rn-a-old: %s", st)
	}
	if _, st, _ := s.JobState("rn-b-new"); st != "completed" {
		t.Fatalf("rn-b-new: %s", st)
	}
}

// failure classification the IsFailure port — the generalization of OutcomeRateLimited that round 32j found
// had ZERO coverage in either language: the word appeared in no test file at all.
// Returning false must requeue the job with NO attempt consumed, NO crash attributed and
// NO failure recorded.
func TestAnErrorIsFailureDeclinesConsumesNoAttemptAndRecordsNoFailure(t *testing.T) {
	s := New()
	ctx := context.Background()
	reg := headgate.NewRegistry()
	if err := headgate.RegisterFunc[tMsg](reg, func(_ context.Context, job *headgate.Job[tMsg]) error {
		if job.Args.Mode == "maintenance" {
			return errors.New("upstream is in a maintenance window")
		}
		return errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	r := headgate.NewRunner(s, reg, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"mem": {MaxWorkers: 4}},
		LeaseDuration: 30 * time.Second, DisableDuties: true,
		IsFailure: func(err error) bool {
			return !strings.Contains(err.Error(), "maintenance window")
		},
	})
	// Ids are ordered so the HARD job is drawn first: the soft one is requeued `available`
	// with no delay, so drawing it first would simply draw it again.
	if err := s.Enqueue(ctx, []headgate.Envelope{
		env("if-a-hard", "boom"), env("if-b-soft", "maintenance"),
	}); err != nil {
		t.Fatal(err)
	}

	// The control FIRST, and the witness that the probe can see a failure at all: the SAME
	// handler, the SAME config, an error the predicate does NOT decline.
	hard, ok, err := r.PerformOne(ctx)
	if err != nil || !ok || hard.JobID != "if-a-hard" || hard.Outcome != "retry" {
		t.Fatalf("control: %+v ok=%v err=%v", hard, ok, err)
	}
	if e, st, _ := s.JobState("if-a-hard"); st != "retryable" || e.Attempt != 1 || e.CrashAttempt != 0 {
		t.Fatalf("hard: %s attempt=%d crash=%d", st, e.Attempt, e.CrashAttempt)
	}
	if len(s.Errors("if-a-hard")) == 0 {
		t.Fatal("a real failure IS recorded")
	}

	soft, ok, _ := r.PerformOne(ctx)
	if !ok || soft.JobID != "if-b-soft" {
		t.Fatalf("soft: %+v ok=%v", soft, ok)
	}
	if soft.Outcome != "rate_limited" {
		t.Fatalf("an error IsFailure declines takes the rate_limited transition, not retry: %q", soft.Outcome)
	}
	e, st, _ := s.JobState("if-b-soft")
	if st != "available" {
		t.Fatalf("and the job goes straight back to the queue, got %s", st)
	}
	if e.Attempt != 0 || e.CrashAttempt != 0 {
		t.Fatalf("invariant 10: no attempt consumed, no crash attributed: attempt=%d crash=%d",
			e.Attempt, e.CrashAttempt)
	}
	if len(s.Errors("if-b-soft")) != 0 {
		t.Fatalf("and NO failure is recorded: %v", s.Errors("if-b-soft"))
	}
}
