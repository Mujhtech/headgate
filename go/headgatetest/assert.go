package headgatetest

// failure classification ASSERT-ENQUEUED . River ships rivertest.RequireInserted; this register
// row claimed the same affordance and 's evidence linter found NO helper of any
// name in either language, so every test that wanted "did the producer enqueue what I
// think it did" hand-rolled a JobState(id) lookup — which needs the id, i.e. needs the test
// to already know the answer to the question. This is the version that takes a DESCRIPTION
// instead, and whose failure names what it found instead.
//
// The Rust twin is headgate_testkit::{Enqueued, find_enqueued, assert_enqueued}.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// RequireStickyRouting is the six-cell live-driver proof for strict worker affinity.
// The 5,000-job other-worker prefix catches implementations that filter only after a
// bounded draw; rate-limited requeue proves the route is durable job state.
func RequireStickyRouting(t testing.TB, store headgate.Store, backend string) string {
	t.Helper()
	ctx := context.Background()
	queue := fmt.Sprintf("sticky-%s-%d", backend, time.Now().UnixNano())
	env := func(id, sticky string, priority int32) headgate.Envelope {
		return headgate.Envelope{
			ID: id, Kind: "test:sticky", Payload: []byte("{}"), Queue: queue,
			PartitionKey: "tenant", Fingerprint: "fp-sticky-" + backend,
			Priority: priority, StickyWorker: sticky, ScheduledAtMs: 1, RetentionMs: 86_400_000,
		}
	}
	aID, generalID := queue+"-a", queue+"-general"
	batch := make([]headgate.Envelope, 0, 5002)
	for i := range 5000 {
		batch = append(batch, env(fmt.Sprintf("%s-b-%04d", queue, i), "worker-b", 10_000))
	}
	batch = append(batch, env(aID, "worker-a", 50), env(generalID, "", 1))
	for start := 0; start < len(batch); start += 500 {
		end := min(start+500, len(batch))
		if err := store.Enqueue(ctx, batch[start:end]); err != nil {
			t.Fatalf("sticky enqueue: %v", err)
		}
	}
	req := func(worker, lease string, capacity int) headgate.AdmitRequest {
		return headgate.AdmitRequest{Worker: worker, LeaseID: lease, Queues: []string{queue}, Capacity: capacity, Lease: time.Minute, Quantum: 10_000}
	}
	units, err := store.Admit(ctx, req("worker-a", "sticky-la", 2))
	if err != nil {
		t.Fatalf("worker-a admit: %v", err)
	}
	claims := make([]headgate.Claim, 0, 2)
	for _, unit := range units {
		claims = append(claims, unit.Claims...)
	}
	sort.Slice(claims, func(i, j int) bool { return claims[i].Envelope.ID < claims[j].Envelope.ID })
	if len(claims) != 2 || claims[0].Envelope.ID != aID || claims[1].Envelope.ID != generalID || claims[0].Envelope.StickyWorker != "worker-a" {
		t.Fatalf("worker-a claims = %#v, want pinned-a + general", claims)
	}
	ref := func(c headgate.Claim) headgate.LeaseRef {
		return headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence}
	}
	if err := store.Ack(ctx, ref(claims[0]), headgate.OutcomeRateLimited, "", 0); err != nil {
		t.Fatalf("route-preserving requeue: %v", err)
	}
	if err := store.Ack(ctx, ref(claims[1]), headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatalf("general completion: %v", err)
	}
	if got, err := store.Admit(ctx, req("worker-c", "sticky-lc", 2)); err != nil || len(got) != 0 {
		t.Fatalf("worker-c admit = %d, %v; want none", len(got), err)
	}
	if got, err := store.Admit(ctx, req("worker-a", "sticky-la2", 1)); err != nil || len(got) != 1 || got[0].Claims[0].Envelope.ID != aID {
		t.Fatalf("worker-a re-admit = %#v, %v", got, err)
	}
	if got, err := store.Admit(ctx, req("worker-b", "sticky-lb", 1)); err != nil || len(got) != 1 || !strings.HasPrefix(got[0].Claims[0].Envelope.ID, queue+"-b-") {
		t.Fatalf("worker-b admit = %#v, %v", got, err)
	}
	return queue
}

// RequireEnqueueBackpressure is the shared live-driver proof: 64 producers race for
// 25 slots, then idempotent replay, all-or-nothing batch rejection, terminal capacity
// release, lowering below current depth, and disabling are checked through public ports.
func RequireEnqueueBackpressure(t testing.TB, store headgate.InspectStore, queue string) {
	t.Helper()
	ctx := context.Background()
	envelope := func(id string) headgate.Envelope {
		return headgate.Envelope{
			ID: id, Kind: "test:backpressure", Payload: []byte("{}"), Queue: queue,
			Fingerprint: "fp-backpressure-" + queue, ScheduledAtMs: 1, RetentionMs: 86_400_000,
		}
	}
	limit := uint64(25)
	if err := store.SetEnqueueLimit(ctx, queue, &limit); err != nil {
		t.Fatalf("configure enqueue limit: %v", err)
	}
	type result struct {
		id  string
		err error
	}
	results := make(chan result, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("%s-bp-%d", queue, i)
			results <- result{id: id, err: store.Enqueue(ctx, []headgate.Envelope{envelope(id)})}
		}()
	}
	wg.Wait()
	close(results)
	var accepted []string
	rejected := 0
	for got := range results {
		if got.err == nil {
			accepted = append(accepted, got.id)
			continue
		}
		var back *headgate.BackpressureError
		if !errors.As(got.err, &back) || back.Queue != queue || back.Limit != 25 || back.Incoming != 1 || back.Current > 25 {
			t.Fatalf("unexpected concurrent enqueue result: %T %v", got.err, got.err)
		}
		rejected++
	}
	if len(accepted) != 25 || rejected != 39 {
		t.Fatalf("accepted=%d rejected=%d, want 25/39", len(accepted), rejected)
	}

	stats, err := store.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	find := func() headgate.QueueStatsView {
		for _, stat := range stats {
			if stat.Queue == queue {
				return stat
			}
		}
		t.Fatalf("queue %s absent from stats", queue)
		return headgate.QueueStatsView{}
	}
	stat := find()
	if stat.UnfinishedJobs != 25 || stat.MaxUnfinishedJobs == nil || *stat.MaxUnfinishedJobs != 25 {
		t.Fatalf("initial stats = %+v", stat)
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{envelope(accepted[0])}); err != nil {
		t.Fatalf("idempotent replay at limit: %v", err)
	}
	batch := []headgate.Envelope{envelope(queue + "-batch-a"), envelope(queue + "-batch-b")}
	err = store.Enqueue(ctx, batch)
	var back *headgate.BackpressureError
	if !errors.As(err, &back) || back.Limit != 25 || back.Current != 25 || back.Incoming != 2 {
		t.Fatalf("batch rejection = %T %v", err, err)
	}
	for _, job := range batch {
		got, err := store.GetJob(ctx, job.ID, false)
		if err != nil || got != nil {
			t.Fatalf("rejected job %s persisted: %#v, %v", job.ID, got, err)
		}
	}
	if err := store.OperatorCancel(ctx, accepted[0]); err != nil {
		t.Fatalf("terminalize: %v", err)
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{envelope(queue + "-replacement")}); err != nil {
		t.Fatalf("replacement after drain: %v", err)
	}
	lower := uint64(10)
	if err := store.SetEnqueueLimit(ctx, queue, &lower); err != nil {
		t.Fatal(err)
	}
	err = store.Enqueue(ctx, []headgate.Envelope{envelope(queue + "-still-full")})
	if !errors.As(err, &back) || back.Limit != 10 || back.Current != 25 || back.Incoming != 1 {
		t.Fatalf("lowered-limit rejection = %T %v", err, err)
	}
	if err := store.SetEnqueueLimit(ctx, queue, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{envelope(queue + "-unbounded")}); err != nil {
		t.Fatalf("disabled policy: %v", err)
	}
	stats, err = store.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stat = find()
	if stat.UnfinishedJobs != 26 || stat.MaxUnfinishedJobs != nil {
		t.Fatalf("final stats = %+v", stat)
	}
}

// Enqueued describes an enqueue. Kind is required; every other field is an optional
// matcher, and the nil/zero value means "do not care".
type Enqueued struct {
	Kind string
	// Queue, PartitionKey, ScheduledAtMs and Count are POINTERS so that an explicitly
	// empty queue or a scheduled_at of 0 is a real matcher rather than "unset" — the same
	// distinction 's JobFilter port change was about.
	Queue         *string
	PartitionKey  *string
	Payload       []byte
	ScheduledAtMs *int64
	// Count requires exactly this many matches; nil means "at least one".
	Count *int
}

// Ptr is the one-liner that makes the optional matchers usable inline.
func Ptr[T any](v T) *T { return &v }

func (w Enqueued) matches(e headgate.Envelope) bool {
	switch {
	case e.Kind != w.Kind:
		return false
	case w.Queue != nil && *w.Queue != e.Queue:
		return false
	case w.PartitionKey != nil && *w.PartitionKey != e.PartitionKey:
		return false
	case w.ScheduledAtMs != nil && *w.ScheduledAtMs != e.ScheduledAtMs:
		return false
	case w.Payload != nil && string(w.Payload) != string(e.Payload):
		return false
	}
	return true
}

func (w Enqueued) describe() string {
	var b strings.Builder
	fmt.Fprintf(&b, "kind %q", w.Kind)
	if w.Queue != nil {
		fmt.Fprintf(&b, ", queue %q", *w.Queue)
	}
	if w.Payload != nil {
		fmt.Fprintf(&b, ", payload %q", string(w.Payload))
	}
	if w.ScheduledAtMs != nil {
		fmt.Fprintf(&b, ", scheduled_at_ms %d", *w.ScheduledAtMs)
	}
	if w.PartitionKey != nil {
		fmt.Fprintf(&b, ", partition_key %q", *w.PartitionKey)
	}
	if w.Count != nil {
		fmt.Fprintf(&b, ", exactly %d time(s)", *w.Count)
	}
	return b.String()
}

// EnqueuedJobs is whatever a test double can list back. Implemented by MemStore; a live
// backend implements it over InspectStore.ListJobs in the test that needs it.
type EnqueuedJobs interface {
	// AllEnqueued returns every job the store currently holds, id-ordered. A job DELETED
	// (retention policy ephemeral retention-0, retention and eviction contract eviction, revoke) is gone from here, which is the
	// honest answer: "was enqueued" is only observable while the row exists.
	AllEnqueued() []headgate.Envelope
}

// AllEnqueued implements EnqueuedJobs.
func (m *MemStore) AllEnqueued() []headgate.Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]headgate.Envelope, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, j.env)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	return out
}

// FindEnqueued returns the enqueued jobs matching want, or an error saying what was there
// instead.
//
// The error is the deliverable. A bare `if _, _, ok := store.JobState("x"); !ok` tells you
// a lookup failed; this tells you the store held two "mail:welcome" jobs on queue "default"
// when you expected one on "priority", which is the difference between a failing test and a
// debugged one.
func FindEnqueued(store EnqueuedJobs, want Enqueued) ([]headgate.Envelope, error) {
	all := store.AllEnqueued()
	var hits []headgate.Envelope
	for _, e := range all {
		if want.matches(e) {
			hits = append(hits, e)
		}
	}
	ok := len(hits) > 0
	if want.Count != nil {
		ok = len(hits) == *want.Count
	}
	if ok {
		return hits, nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "assert_enqueued: no job matches %s — %d match(es) found among %d enqueued job(s)",
		want.describe(), len(hits), len(all))
	if len(all) == 0 {
		b.WriteString("\n  the store is EMPTY: nothing was enqueued at all")
	} else {
		b.WriteString("\n  what IS enqueued:")
		for i, e := range all {
			if i == 20 {
				fmt.Fprintf(&b, "\n    ... and %d more", len(all)-20)
				break
			}
			fmt.Fprintf(&b, "\n    id=%q kind=%q queue=%q partition=%q scheduled_at_ms=%d payload=%q",
				e.ID, e.Kind, e.Queue, e.PartitionKey, e.ScheduledAtMs, string(e.Payload))
		}
	}
	return nil, fmt.Errorf("%s", b.String())
}

// RequireEnqueued is FindEnqueued, failing the test with that message. The assertion form.
func RequireEnqueued(t testing.TB, store EnqueuedJobs, want Enqueued) []headgate.Envelope {
	t.Helper()
	hits, err := FindEnqueued(store, want)
	if err != nil {
		t.Fatal(err)
	}
	return hits
}
