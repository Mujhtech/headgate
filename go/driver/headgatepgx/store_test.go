package headgatepgx

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

func TestStructuredAttemptLogsSurviveAck(t *testing.T) {
	store, _ := testStore(t)
	headgatetest.RequireStructuredAttemptLogs(t, store, "go-pg-logs-"+strconv.FormatInt(time.Now().UnixNano(), 10))
}

func TestEnqueueBackpressureHotPathUsesConstantSizeCounters(t *testing.T) {
	sql := strings.ToLower(enqueueBackpressureDepthSQL)
	if !strings.Contains(sql, "headgate_enqueue_policy") || strings.Count(sql, "headgate_enqueue_counter") != 2 {
		t.Fatalf("backpressure query lost its policy/counter index shape: %s", sql)
	}
	if strings.Contains(sql, "headgate_job") || strings.Contains(sql, "count(") {
		t.Fatalf("backpressure query scans queue depth: %s", sql)
	}
}

func TestReindexAllowlistRejectsIdentifiersAndUnknownIndexes(t *testing.T) {
	if !isMaintainableIndex("headgate_job_admit") {
		t.Fatal("known index missing")
	}
	for _, name := range []string{"headgate_job;DROP TABLE headgate_job", "users_email_idx", ""} {
		if isMaintainableIndex(name) {
			t.Fatalf("accepted %q", name)
		}
	}
}

// Opt-in via HG_TEST_PG (conninfo with the migration applied); skips cleanly without.
// scripts/test-admission.sh's cross-language section remains the release gate.
func testStore(t *testing.T) (*PgxStore, context.Context) {
	t.Helper()
	conninfo := os.Getenv("HG_TEST_PG")
	if conninfo == "" {
		t.Skip("HG_TEST_PG not set")
	}
	ctx := context.Background()
	store, err := Connect(ctx, conninfo)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gotest'`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	return store, ctx
}

func env(id string, retentionMs int64) headgate.Envelope {
	return headgate.Envelope{
		ID: id, Kind: "go:t", Payload: []byte{1}, Queue: "gotest",
		Fingerprint: "fp-go", ScheduledAtMs: 1000, RetentionMs: retentionMs,
	}
}

func admitReq(worker, lease string) headgate.AdmitRequest {
	return headgate.AdmitRequest{
		Worker: worker, LeaseID: lease, Queues: []string{"gotest"},
		Capacity: 10, Lease: 30 * time.Second, Quantum: 1000,
	}
}

func TestEnqueueClassifiesAnUnreachablePostgresWithoutMaskingInputErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store, err := Connect(ctx,
		"postgres://headgate@127.0.0.1:1/headgate?connect_timeout=1")
	if err != nil {
		t.Fatalf("construct lazy pool: %v", err)
	}
	defer store.pool.raw.Close()

	valid := headgate.Envelope{ID: "pg-outage", Kind: "outage"}
	err = store.Enqueue(ctx, []headgate.Envelope{valid})
	var unavailable *headgate.UnavailableError
	if !errors.Is(err, headgate.ErrUnavailable) || !errors.As(err, &unavailable) {
		t.Fatalf("refused enqueue = %T %v, want typed unavailable", err, err)
	}

	invalid := valid
	invalid.ID = ""
	err = store.Enqueue(context.Background(), []headgate.Envelope{invalid})
	var invalidErr *headgate.InvalidError
	if !errors.As(err, &invalidErr) || errors.Is(err, headgate.ErrUnavailable) {
		t.Fatalf("invalid envelope while down = %T %v, want invalid", err, err)
	}
	err = store.Enqueue(context.Background(), []headgate.Envelope{valid, valid})
	var conflict *headgate.IDConflictError
	if !errors.As(err, &conflict) || errors.Is(err, headgate.ErrUnavailable) {
		t.Fatalf("duplicate id while down = %T %v, want id conflict", err, err)
	}
}

func TestEnqueueBackpressureIsAtomicExactAndWorkConservingUnderContention(t *testing.T) {
	s, _ := testStore(t)
	headgatetest.RequireEnqueueBackpressure(t, s, "gpg-backpressure-"+strconv.Itoa(os.Getpid()))
}

func TestStickyRoutingIsStrictBoundedAndSurvivesRequeue(t *testing.T) {
	s, _ := testStore(t)
	queue := headgatetest.RequireStickyRouting(t, s, "go-postgres")
	if _, err := s.pool.Exec(context.Background(), "DELETE FROM headgate_job WHERE queue = $1", queue); err != nil {
		t.Fatalf("sticky cleanup: %v", err)
	}
}

func TestTransactionalClientInsertHooksSurroundTheRealPostgresStoreCall(t *testing.T) {
	s, ctx := testStore(t)
	id := "8900000000" + strconv.Itoa(os.Getpid())
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE id = $1`, id); err != nil {
		t.Fatalf("clean old fixture: %v", err)
	}
	var events []string
	hook := headgate.InsertHookFunc(func(_ context.Context, event headgate.InsertHookEvent) {
		attempt := event.Attempt()
		if attempt.Operation != headgate.EnqueueOperationTransactional {
			t.Fatalf("operation = %q, want transactional", attempt.Operation)
		}
		label := string(event.Phase())
		if outcome, ok := event.Outcome(); ok && outcome.Kind != headgate.InsertOutcomeSucceeded {
			t.Fatalf("outcome = %#v, want succeeded", outcome)
		}
		events = append(events, label)
	})
	client := headgate.NewClient(s, headgate.WithInsertHooks(hook))
	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := client.EnqueueTx(ctx, tx, []headgate.Envelope{
		hookEnvelopeForDriver(id),
	}); err != nil {
		_ = s.RollbackTx(ctx, tx)
		t.Fatalf("transactional enqueue: %v", err)
	}
	if err := s.CommitTx(ctx, tx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !reflect.DeepEqual(events, []string{"begin", "end"}) {
		t.Fatalf("events = %#v, want one lifecycle", events)
	}
	job, err := s.GetJob(ctx, id, false)
	if err != nil || job == nil {
		t.Fatalf("committed job = %#v, err = %v", job, err)
	}
	if err := s.DeleteJob(ctx, id); err != nil {
		t.Fatalf("fixture hygiene: %v", err)
	}
}

func hookEnvelopeForDriver(id string) headgate.Envelope {
	payload := []byte(`{"hook":"transactional"}`)
	return headgate.Envelope{
		ID: id, Kind: "hook.transactional", Queue: "gotest", Payload: payload,
		Fingerprint: headgate.Fingerprint("hook.transactional", payload),
		RetentionMs: 86_400_000,
	}
}

func TestStoreLifecycleEndToEnd(t *testing.T) {
	s, ctx := testStore(t)

	// enqueue (batch/unnest) -> admit: lease + fence written by the claim.
	if err := s.Enqueue(ctx, []headgate.Envelope{env("go-a", 0), env("go-b", 86_400_000)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	units, err := s.Admit(ctx, admitReq("gw", "GL1"))
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if len(units) != 2 {
		t.Fatalf("want 2 units, got %d", len(units))
	}
	byID := map[string]headgate.LeaseRef{}
	for _, u := range units {
		for _, c := range u.Claims {
			if c.Fence != 1 || c.LeaseID != "GL1" {
				t.Fatalf("claim %s: fence=%d lease=%s", c.Envelope.ID, c.Fence, c.LeaseID)
			}
			byID[c.Envelope.ID] = headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence}
		}
	}

	// retention policy retention 0 deletes on success; a second ack is LeaseRejected, not a no-op.
	if err := s.Ack(ctx, byID["go-a"], headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatalf("success ack: %v", err)
	}
	err = s.Ack(ctx, byID["go-a"], headgate.OutcomeSuccess, "", 0)
	var rej *headgate.LeaseRejectedError
	if !errors.As(err, &rej) || !errors.Is(err, headgate.ErrLeaseLost) {
		t.Fatalf("want LeaseRejectedError wrapping ErrLeaseLost, got %v", err)
	}

	// Retry consumes an attempt; renew names both lost leases.
	if err := s.Ack(ctx, byID["go-b"], headgate.OutcomeRetry, "boom", 1); err != nil {
		t.Fatalf("retry ack: %v", err)
	}
	lost, err := s.Renew(ctx, []headgate.LeaseRef{byID["go-a"], byID["go-b"]}, 30*time.Second)
	// Round 32h: the comment says renew NAMES both lost leases; the assertion only
	// counted them, so two empty strings or two copies of one id passed. Named now.
	sort.Strings(lost)
	if err != nil || !reflect.DeepEqual(lost, []string{"go-a", "go-b"}) {
		t.Fatalf("renew must NAME both lost leases: lost=%v err=%v", lost, err)
	}

	// Reclaim is LeaseLost, never Retry: crash=1, attempt stays.
	time.Sleep(30 * time.Millisecond)
	if _, err := s.PromoteDue(ctx, 1000); err != nil {
		t.Fatalf("promote: %v", err)
	}
	units, err = s.Admit(ctx, admitReq("gw", "GL2"))
	if err != nil || len(units) != 1 {
		t.Fatalf("re-admit: units=%d err=%v", len(units), err)
	}
	claim := units[0].Claims[0]
	if claim.Fence != 2 {
		t.Fatalf("fence must increment by exactly 1 per claim; got %d", claim.Fence)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE headgate_job SET lease_expires_at_ms = 0 WHERE ulid = 'go-b'`); err != nil {
		t.Fatal(err)
	}
	swept, err := s.ReclaimExpired(ctx, 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	found := false
	for _, r := range swept {
		if r.JobID == "go-b" {
			found = true
			if r.CrashAttempt != 1 || r.Quarantined {
				t.Fatalf("reclaim counters: %+v", r)
			}
		}
	}
	if !found {
		t.Fatal("go-b not swept")
	}

	// job uniqueness duplicate unique key carries the winner.
	uq := env("go-u1", 0)
	uq.UniqueKey = []byte("gok1")
	if err := s.Enqueue(ctx, []headgate.Envelope{uq}); err != nil {
		t.Fatalf("unique enqueue: %v", err)
	}
	dup := env("go-u2", 0)
	dup.UniqueKey = []byte("gok1")
	err = s.Enqueue(ctx, []headgate.Envelope{dup})
	var d *headgate.DuplicateError
	if !errors.As(err, &d) || d.ExistingID != "go-u1" {
		t.Fatalf("want DuplicateError{go-u1}, got %v", err)
	}

	// runtime capability boundary transactional: rollback leaves no row.
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueTx(ctx, tx, []headgate.Envelope{env("go-tx", 0)}); err != nil {
		t.Fatalf("enqueue_tx: %v", err)
	}
	if err := Rollback(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM headgate_job WHERE ulid = 'go-tx'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("rolled-back enqueue must not persist")
	}
	// Round 32h: count==0 is ALSO what an EnqueueTx that silently wrote nothing leaves
	// behind — which is the exact failure this test exists to rule out, so "rolled back"
	// and "never inserted" were indistinguishable. The commit arm is the positive
	// control: the same call path has to persist a row when told to commit.
	tx2, err := s.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueTx(ctx, tx2, []headgate.Envelope{env("go-tx2", 0)}); err != nil {
		t.Fatalf("enqueue_tx commit arm: %v", err)
	}
	if err := Commit(ctx, tx2); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM headgate_job WHERE ulid = 'go-tx2'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("control: a COMMITTED transactional enqueue must persist")
	}
}

// Mirrors the Rust notify test: a NOTIFY from an enqueue wakes a waiting subscriber
// with the queue's name; the repeated-enqueue loop is the poll-fallback contract (a
// missed notification costs latency, never correctness).
func TestNotifyWakesAWaitingSubscriber(t *testing.T) {
	s, ctx := testStore(t)
	if !s.Caps().Has(headgate.CapNotifying) {
		t.Fatal("Connect() must enable LISTEN")
	}
	// Prime the lazy listener; the first window may elapse before LISTEN is up.
	_, _, _ = s.WaitWakeup(ctx, []string{"gonfy-q"}, 300*time.Millisecond)

	type res struct {
		q  string
		ok bool
	}
	got := make(chan res, 1)
	go func() {
		q, ok, _ := s.WaitWakeup(ctx, []string{"gonfy-q"}, 10*time.Second)
		got <- res{q, ok}
	}()
	deadline := time.Now().Add(9 * time.Second)
	for i := 0; ; i++ {
		e := env("gonfy-"+strconv.Itoa(os.Getpid())+"-"+strconv.Itoa(i), 0)
		e.Queue = "gonfy-q"
		if err := s.Enqueue(ctx, []headgate.Envelope{e}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		select {
		case r := <-got:
			if !r.ok || r.q != "gonfy-q" {
				t.Fatalf("want wakeup for gonfy-q, got %+v", r)
			}
			return
		case <-time.After(150 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("no wakeup after repeated notifies")
		}
	}
}

func TestEvictRetainedSweepsLapsedTerminalJobsOnly(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'goret'`); err != nil {
		t.Fatal(err)
	}
	mk := func(id string, retentionMs int64) headgate.Envelope {
		e := env(id, retentionMs)
		e.Queue = "goret"
		return e
	}
	if err := s.Enqueue(ctx, []headgate.Envelope{mk("goret-gone", 1), mk("goret-keep", 86_400_000)}); err != nil {
		t.Fatal(err)
	}
	req := admitReq("grw", "GRL")
	req.Queues = []string{"goret"}
	units, err := s.Admit(ctx, req)
	if err != nil || len(units) != 2 {
		t.Fatalf("admit: %d %v", len(units), err)
	}
	for _, u := range units {
		c := u.Claims[0]
		l := headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence}
		if err := s.Ack(ctx, l, headgate.OutcomeSuccess, "", 0); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(30 * time.Millisecond)
	// The 1ms retention lapsed; the 24h one did not (retention and eviction contract).
	if _, err := s.EvictRetained(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	if j, err := s.GetJob(ctx, "goret-gone", false); err != nil || j != nil {
		t.Fatalf("lapsed retention must be deleted: %+v %v", j, err)
	}
	j, err := s.GetJob(ctx, "goret-keep", false)
	if err != nil || j == nil || j.State != "completed" {
		t.Fatalf("retained job must survive: %+v %v", j, err)
	}
	if err := s.DeleteJob(ctx, "goret-keep"); err != nil { // hygiene
		t.Fatal(err)
	}
}

func TestPartitionedArchiveMovesTerminalJobAndGuardsPruning(t *testing.T) {
	s, ctx := testStore(t)
	run := strconv.FormatInt(time.Now().UnixNano(), 10)
	queue, id := "goarchive-"+run, "goarchive-job-"+run
	if err := s.SetArchivePolicy(ctx, queue, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	job := env(id, 1)
	job.Queue, job.Payload, job.Fingerprint = queue, []byte("audit-body"), "archive-fp-"+run
	if err := s.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatal(err)
	}
	req := admitReq("archive-worker", "archive-lease")
	req.Queues, req.Capacity = []string{queue}, 1
	units, err := s.Admit(ctx, req)
	if err != nil || len(units) != 1 || len(units[0].Claims) != 1 {
		t.Fatalf("admit archive job: %+v %v", units, err)
	}
	claim := units[0].Claims[0]
	lease := headgate.LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	if err := s.Ack(ctx, lease, headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if n, err := s.EvictRetained(ctx, 10); err != nil || n != 1 {
		t.Fatalf("archive eviction = %d, %v", n, err)
	}
	var state string
	var payload []byte
	var retention int64
	if err := s.pool.QueryRow(ctx,
		`SELECT state, payload, archive_retention_ms
		 FROM headgate_job_archive WHERE ulid = $1`, id).
		Scan(&state, &payload, &retention); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || string(payload) != "audit-body" || retention != int64((24*time.Hour)/time.Millisecond) {
		t.Fatalf("bad archive body: %q %q %d", state, payload, retention)
	}
	if _, err := s.PruneArchiveMonth(ctx, "203112"); err == nil {
		t.Fatal("open archive month was pruned")
	}
	if _, err := s.PruneArchiveMonth(ctx, "2031;DROP"); err == nil {
		t.Fatal("unsafe partition identifier accepted")
	}
	if os.Getenv("HG_TEST_ARCHIVE_PRUNE") != "" {
		oldID := "goarchive-old-" + run
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO headgate_job_archive (
			  evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
			  fingerprint, attempt, crash_attempt, payload, errors,
			  archive_retention_ms
			) VALUES (
			  1740787201000, 1740787200000, $1, 'archive:test', $2, 'completed',
			  'old-fp', 1, 0, $3, '[]'::jsonb, 1
			)`, oldID, queue, []byte("old-audit")); err != nil {
			t.Fatal(err)
		}
		if n, err := s.PruneArchiveMonth(ctx, "202503"); err != nil || n < 1 {
			t.Fatalf("prune old partition = %d, %v", n, err)
		}
		var n int64
		if err := s.pool.QueryRow(ctx,
			"SELECT count(*)::bigint FROM headgate_job_archive WHERE ulid = $1", oldID).
			Scan(&n); err != nil || n != 0 {
			t.Fatalf("old archive row survived truncate: %d, %v", n, err)
		}
	}
	job.Payload, job.Fingerprint, job.RetentionMs = []byte("new-run"), "archive-new-"+run, 0
	if err := s.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatalf("reuse evicted identity: %v", err)
	}
	if _, err := s.pool.Exec(ctx, "DELETE FROM headgate_job WHERE queue = $1", queue); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, "DELETE FROM headgate_job_archive WHERE queue = $1", queue); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearArchivePolicy(ctx, queue); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// ROUND 32L, TASK 3.1 — EnqueuedJobs over a LIVE backend, Go side.
//
// headgatetest.EnqueuedJobs was implemented only by MemStore, so the seam was a claim
// about a map. Its doc said the live case is "a test implements the same one-method
// interface over ListJobs" and no test did. This is that test, and the adapter has to get
// the same two things right as its Rust twin:
//
//   - AllEnqueued is synchronous and ListJobs is not, so the adapter is a SNAPSHOT — the
//     honest semantics for a live store, where "what is enqueued" is a moment in time.
//   - ListJobs NEVER returns a payload (invariant 9: withheld by default, and the list
//     surface has no opt-in at all), so a Payload matcher has to ask per job via
//     GetJob(id, true). An adapter that skipped that would silently fail every payload
//     matcher while looking like it worked.
//
// ---------------------------------------------------------------------------
type liveJobs struct{ jobs []headgate.Envelope }

func snapshotLive(t *testing.T, s *PgxStore, ctx context.Context, queue string) *liveJobs {
	t.Helper()
	out := &liveJobs{}
	filter := headgate.JobFilter{Queue: headgate.Ptr(queue)}
	cursor := ""
	for {
		page, err := s.ListJobs(ctx, filter, cursor, 100)
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		for _, sum := range page.Jobs {
			full, err := s.GetJob(ctx, sum.ID, true) // invariant 9: payload is a second ask
			if err != nil || full == nil {
				t.Fatalf("GetJob(%s): %v", sum.ID, err)
			}
			out.jobs = append(out.jobs, headgate.Envelope{
				ID: full.ID, Kind: full.Kind, Payload: full.Payload, Queue: full.Queue,
				PartitionKey: full.PartitionKey, RateClass: full.RateClass,
				Fingerprint: full.Fingerprint, Priority: full.Priority,
				ScheduledAtMs: full.ScheduledAtMs,
			})
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	sort.Slice(out.jobs, func(i, j int) bool { return out.jobs[i].ID < out.jobs[j].ID })
	return out
}

func (l *liveJobs) AllEnqueued() []headgate.Envelope { return l.jobs }

func TestRequireEnqueuedReadsALiveStoreThroughTheSameOneMethodInterface(t *testing.T) {
	s, ctx := testStore(t)
	const q = "goae"
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = $1`, q); err != nil {
		t.Fatal(err)
	}
	// Three jobs differing in exactly the fields the matchers select on, so no matcher
	// can pass by accident.
	mk := func(id, payload string) headgate.Envelope {
		return headgate.Envelope{
			ID: id, Kind: "goae:t", Payload: []byte(payload), Queue: q,
			Fingerprint:   headgate.Fingerprint("goae:t", []byte(payload)),
			ScheduledAtMs: 1000, RetentionMs: 86_400_000,
		}
	}
	a, b, c := mk("goae-a", "alpha"), mk("goae-b", "beta"), mk("goae-c", "gamma")
	b.PartitionKey = "tenant-b"
	c.ScheduledAtMs = 4_102_444_800_000 // year 2100: stays scheduled, never drawn
	if err := s.Enqueue(ctx, []headgate.Envelope{a, b, c}); err != nil {
		t.Fatal(err)
	}

	live := snapshotLive(t, s, ctx, q)

	if got := headgatetest.RequireEnqueued(t, live, headgatetest.Enqueued{
		Kind: "goae:t", Queue: headgatetest.Ptr(q), Count: headgatetest.Ptr(3)}); len(got) != 3 {
		t.Fatalf("want 3 matches, got %d", len(got))
	}
	if got := headgatetest.RequireEnqueued(t, live, headgatetest.Enqueued{
		Kind: "goae:t", Payload: []byte("beta")}); got[0].ID != "goae-b" {
		t.Fatalf("payload matching needs GetJob(id, true); ListJobs withholds it; got %s", got[0].ID)
	}
	if got := headgatetest.RequireEnqueued(t, live, headgatetest.Enqueued{
		Kind: "goae:t", PartitionKey: headgatetest.Ptr("tenant-b")}); got[0].ID != "goae-b" {
		t.Fatalf("partition matcher: got %s", got[0].ID)
	}
	if got := headgatetest.RequireEnqueued(t, live, headgatetest.Enqueued{
		Kind: "goae:t", ScheduledAtMs: headgatetest.Ptr(int64(4_102_444_800_000))}); got[0].ID != "goae-c" {
		t.Fatalf("scheduled-at matcher: got %s", got[0].ID)
	}

	// A matcher that cannot say NO is decoration, and the FAILURE MESSAGE is part of the
	// contract: it restates the expectation and lists what IS enqueued, which is the whole
	// difference from an id lookup that already presumes the answer.
	_, err := headgatetest.FindEnqueued(live, headgatetest.Enqueued{
		Kind: "goae:t", Queue: headgatetest.Ptr("goae-nope")})
	if err == nil {
		t.Fatal("a queue nothing is in must NOT match")
	}
	for _, want := range []string{"goae-nope", "3 enqueued job(s)", "goae-b"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("failure message must contain %q: %s", want, err.Error())
		}
	}
	if _, err := headgatetest.FindEnqueued(live, headgatetest.Enqueued{
		Kind: "goae:t", Queue: headgatetest.Ptr(q), Count: headgatetest.Ptr(2)}); err == nil {
		t.Fatal("exactly-2 must not be satisfied by 3")
	}
}
