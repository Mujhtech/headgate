package headgatemysql

// The sixth corner: the SAME Go worker runtime, unchanged, over the Go MySQL driver —
// adaptive admission's port test on the third backend, second language. The gate's policy step is the
// byte-identical eligible.sql the Rust adapter runs; this proves the Go transaction
// around it drives it identically. Opt-in via HG_TEST_MYSQL; skips cleanly without it.
//
// NOTE for reruns: run this crate's tests with -test.parallel=1 / one binary at a
// time — a default-config container has been wedged by full-parallel suites before.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

func TestEnqueueBackpressureHotPathUsesConstantSizeCounters(t *testing.T) {
	sql := strings.ToLower(enqueueBackpressureDepthSQL(2))
	if !strings.Contains(sql, "headgate_enqueue_policy") || strings.Count(sql, "headgate_enqueue_counter") != 2 {
		t.Fatalf("backpressure query lost its policy/counter index shape: %s", sql)
	}
	if strings.Contains(sql, "headgate_job") || strings.Contains(sql, "count(") {
		t.Fatalf("backpressure query scans queue depth: %s", sql)
	}
}

func TestEnqueueBackpressureIsAtomicExactAndWorkConservingUnderContention(t *testing.T) {
	s, _ := testStore(t)
	headgatetest.RequireEnqueueBackpressure(t, s, "gmy-backpressure-"+strconv.Itoa(os.Getpid()))
}

func TestStickyRoutingIsStrictBoundedAndSurvivesRequeue(t *testing.T) {
	s, _ := testStore(t)
	queue := headgatetest.RequireStickyRouting(t, s, "go-mysql")
	if _, err := s.db.ExecContext(context.Background(), "DELETE FROM headgate_job WHERE queue = ?", queue); err != nil {
		t.Fatalf("sticky cleanup: %v", err)
	}
}

func TestPartitionedArchiveMovesTerminalJobAndGuardsPruning(t *testing.T) {
	s, ctx := testStore(t)
	run := strconv.FormatInt(time.Now().UnixNano(), 10)
	queue, id := "gmy-archive-"+run, "gmy-archive-job-"+run
	if err := s.SetArchivePolicy(ctx, queue, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	job := gmEnv(queue, id, "audit-body")
	job.RetentionMs = 1
	if err := s.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatal(err)
	}
	units, err := s.Admit(ctx, headgate.AdmitRequest{
		Worker: "archive-worker", LeaseID: "archive-lease", Queues: []string{queue},
		Capacity: 1, Lease: 30 * time.Second, Quantum: 1000,
	})
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
	if err := s.db.QueryRowContext(ctx,
		`SELECT state, payload, archive_retention_ms
		 FROM headgate_job_archive WHERE ulid = ?`, id).
		Scan(&state, &payload, &retention); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || string(payload) != `{"mode":"audit-body"}` ||
		retention != int64((24*time.Hour)/time.Millisecond) {
		t.Fatalf("bad archive body: %q %q %d", state, payload, retention)
	}
	if _, err := s.PruneArchiveMonth(ctx, "203112"); err == nil {
		t.Fatal("open archive month was pruned")
	}
	if _, err := s.PruneArchiveMonth(ctx, "2031;DROP"); err == nil {
		t.Fatal("unsafe partition identifier accepted")
	}
	if os.Getenv("HG_TEST_ARCHIVE_PRUNE") != "" {
		oldID := "gmy-archive-old-" + run
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO headgate_job_archive (
			  evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
			  fingerprint, attempt, crash_attempt, payload, errors,
			  archive_retention_ms
			) VALUES (
			  1743465601000, 1743465600000, ?, 'archive:test', ?, 'completed',
			  'old-fp', 1, 0, ?, JSON_ARRAY(), 1
			)`, oldID, queue, []byte("old-audit")); err != nil {
			t.Fatal(err)
		}
		if n, err := s.PruneArchiveMonth(ctx, "202504"); err != nil || n < 1 {
			t.Fatalf("prune old partition = %d, %v", n, err)
		}
		var n int64
		if err := s.db.QueryRowContext(ctx,
			"SELECT count(*) FROM headgate_job_archive WHERE ulid = ?", oldID).
			Scan(&n); err != nil || n != 0 {
			t.Fatalf("old archive row survived truncate: %d, %v", n, err)
		}
	}
	job.Payload, job.Fingerprint, job.RetentionMs = []byte(`{"mode":"new-run"}`), "archive-new-"+run, 0
	if err := s.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatalf("reuse evicted identity: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_job WHERE queue = ?", queue); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_job_archive WHERE queue = ?", queue); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearArchivePolicy(ctx, queue); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentFirstEnqueuesToDistinctQueuesDoNotGapDeadlock(t *testing.T) {
	s, ctx := testStore(t)
	run := strconv.FormatInt(time.Now().UnixNano(), 10)
	start := make(chan struct{})
	errs := make(chan error, 16)
	for i := range 16 {
		go func() {
			queue := "gmy-gap-" + run + "-" + strconv.Itoa(i)
			<-start
			errs <- s.Enqueue(ctx, []headgate.Envelope{gmEnv(queue, queue+"-j", "ok")})
		}()
	}
	close(start)
	for range 16 {
		if err := <-errs; err != nil {
			t.Fatalf("distinct first enqueues deadlocked on absent counter gaps: %v", err)
		}
	}
}

func testStore(t *testing.T) (*MysqlStore, context.Context) {
	t.Helper()
	url := os.Getenv("HG_TEST_MYSQL")
	if url == "" {
		t.Skip("HG_TEST_MYSQL not set")
	}
	s, err := Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	s.opts.RetryBaseMs = 1
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_job WHERE queue LIKE 'gmy%'"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_quarantine WHERE kind = 'gmy:msg'"); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}

type gmMsg struct {
	Mode string `json:"mode"`
}

func (gmMsg) Kind() string { return "gmy:msg" }

func gmEnv(queue, id, mode string) headgate.Envelope {
	payload := []byte(`{"mode":"` + mode + `"}`)
	return headgate.Envelope{
		ID: id, Kind: "gmy:msg", Payload: payload, Queue: queue,
		Fingerprint:   headgate.Fingerprint("gmy:msg", payload),
		ScheduledAtMs: 1, RetentionMs: 86_400_000,
	}
}

func TestEnqueueClassifiesAnUnreachableMysqlWithoutMaskingInputErrors(t *testing.T) {
	store, err := Connect("headgate@tcp(127.0.0.1:1)/headgate?timeout=200ms")
	if err != nil {
		t.Fatalf("construct lazy database handle: %v", err)
	}
	defer func() { _ = store.db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	valid := headgate.Envelope{ID: "mysql-outage", Kind: "outage"}
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

func jobField(t *testing.T, s *MysqlStore, id, field string) string {
	t.Helper()
	var v sql.NullString
	err := s.db.QueryRowContext(context.Background(),
		fmt.Sprintf("SELECT CAST(%s AS CHAR) FROM headgate_job WHERE ulid = ?", field), id).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return v.String
}

func jobInt(t *testing.T, s *MysqlStore, id, field string) int64 {
	t.Helper()
	v := jobField(t, s, id, field)
	if v == "" {
		t.Fatalf("job %s has no %s", id, field)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("job %s %s = %q: %v", id, field, v, err)
	}
	return n
}

func TestMySQLUniqueConflictQueriesUseGeneratedIndexes(t *testing.T) {
	release := lazyUniqueReleaseSQL(2)
	if !strings.Contains(release, "WHERE unique_throttle IN (?, ?)") ||
		strings.Contains(release, "WHERE unique_key IN") {
		t.Fatalf("lazy release is not index-bounded: %s", release)
	}
	holder := uniqueHolderSQL(2)
	if !strings.Contains(holder, "unique_active IN (?, ?)") ||
		!strings.Contains(holder, "unique_throttle IN (?, ?)") ||
		strings.Contains(holder, "WHERE unique_key IN") {
		t.Fatalf("winner lookup is not index-bounded: %s", holder)
	}
}

// lease fencing/crash quarantine ReclaimExpired over the Go MySQL driver, direct. This path was previously
// covered only by scripts/test-admission.sh's MySQL section, which drives the RUST
// harness — so the Go reclaimer's crash attribution, its suspect re-stamp and its
// quarantine arm had no unit-level assertion at all on this backend.
//
// SHARED-DATABASE DISCIPLINE, learned the hard way in round 31: ReclaimExpired is
// GLOBAL. It sweeps every expired lease in the container, including strays from other
// suites and from aborted runs. So every assertion here is on THIS test's own
// $-scoped ids and never on a sweep count or a table total.
func TestGoMysqlReclaimExpiredAttributesCrashesAndQuarantines(t *testing.T) {
	s, ctx := testStore(t)
	q := scope("rc")
	bombQueue := q + "-b"
	liveFp, bombFp := q+"-fp-live", q+"-fp-bomb"
	for _, stmt := range []string{
		"DELETE FROM headgate_job WHERE queue LIKE '" + q + "%'",
		"DELETE FROM headgate_quarantine WHERE fingerprint LIKE '" + q + "%'",
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	mk := func(queue, id, part, fp string, sched int64) headgate.Envelope {
		payload := []byte(`{"mode":"ok"}`)
		return headgate.Envelope{
			ID: id, Kind: "gmy:msg", Payload: payload, Queue: queue,
			PartitionKey: part, Fingerprint: fp,
			ScheduledAtMs: sched, RetentionMs: 86_400_000, MaxAttempts: 25,
		}
	}
	admitOne := func(queue, lease string) []headgate.AdmissionUnit {
		t.Helper()
		// A deliberately SHORT lease is the whole mechanism: the sweep below is not
		// mocked, it reads the store's own clock against lease_expires_at_ms.
		units, err := s.Admit(ctx, headgate.AdmitRequest{
			Worker: q + "-w", LeaseID: lease, Queues: []string{queue},
			Capacity: 1, Lease: 20 * time.Millisecond, Quantum: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		return units
	}
	claimed := func(units []headgate.AdmissionUnit) []string {
		var ids []string
		for _, u := range units {
			for _, c := range u.Claims {
				ids = append(ids, c.Envelope.ID)
			}
		}
		return ids
	}
	// mineOnly filters the GLOBAL sweep down to this test's jobs.
	mineOnly := func(rec []headgate.Reclaimed, prefix string) []headgate.Reclaimed {
		var out []headgate.Reclaimed
		for _, r := range rec {
			if strings.HasPrefix(r.JobID, prefix) {
				out = append(out, r)
			}
		}
		return out
	}

	// ----- crash attribution + the crash quarantine suspect-to-back re-stamp -----
	a, b, c := q+"-a", q+"-b1", q+"-c"
	if err := s.Enqueue(ctx, []headgate.Envelope{
		mk(q, a, "hol", liveFp, 1000),
		mk(q, b, "hol", liveFp, 1001),
		mk(q, c, "hol", liveFp, 1002),
	}); err != nil {
		t.Fatal(err)
	}
	if ids := claimed(admitOne(q, "RC1")); len(ids) != 1 || ids[0] != a {
		t.Fatalf("capacity-1 admit must take the partition's oldest job, got %v", ids)
	}
	beforeA := jobInt(t, s, a, "scheduled_at_ms")
	time.Sleep(120 * time.Millisecond) // past the 20ms lease
	rec, err := s.ReclaimExpired(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	mine := mineOnly(rec, q+"-")
	if len(mine) != 1 || mine[0].JobID != a {
		t.Fatalf("reclaim must report exactly this test's crashed job, got %+v", mine)
	}
	if mine[0].CrashAttempt != 1 || mine[0].Quarantined || mine[0].Fingerprint != liveFp {
		t.Fatalf("reclaimed record: %+v", mine[0])
	}
	// Invariant 3: an expired lease is LeaseLost, NEVER Retry — crash_attempt moves,
	// attempt does not. Quarantine depends on the distinction.
	if st := jobField(t, s, a, "state"); st != "retryable" {
		t.Fatalf("crashed job state: %s", st)
	}
	if n := jobInt(t, s, a, "attempt"); n != 0 {
		t.Fatalf("a crash must not consume an attempt: attempt=%d", n)
	}
	if n := jobInt(t, s, a, "crash_attempt"); n != 1 {
		t.Fatalf("crash_attempt=%d", n)
	}
	if errs := jobField(t, s, a, "errors"); !strings.Contains(errs, "lease_lost") {
		t.Fatalf("the attempt entry must record the lease_lost outcome: %s", errs)
	}
	// crash quarantine the suspect yields its queue position: reclaim re-stamps scheduled_at_ms to
	// the STORE clock plus the crash backoff, and that column is the gate's second sort
	// key. So the suspect goes BEHIND every same-priority sibling — this is what stops
	// a poison pill head-of-line-blocking its partition once per crash cycle.
	afterA := jobInt(t, s, a, "scheduled_at_ms")
	if afterA <= beforeA {
		t.Fatalf("reclaim must advance scheduled_at_ms: %d -> %d", beforeA, afterA)
	}
	if sib := jobInt(t, s, c, "scheduled_at_ms"); afterA <= sib {
		t.Fatalf("the suspect must land behind its siblings: %d vs %d", afterA, sib)
	}

	// ----- quarantine at the crash limit -----
	// The victim is pre-aged to one crash short of the limit rather than crash-looped,
	// so the test spends one lease expiry instead of CrashLimit of them.
	bomb := q + "-bomb"
	if err := s.Enqueue(ctx, []headgate.Envelope{
		mk(bombQueue, bomb, "pz", bombFp, 1000),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx,
		"UPDATE headgate_job SET crash_attempt = ? WHERE ulid = ?",
		s.opts.CrashLimit-1, bomb); err != nil {
		t.Fatal(err)
	}
	if ids := claimed(admitOne(bombQueue, "RC2")); len(ids) != 1 || ids[0] != bomb {
		t.Fatalf("bomb admit: %v", ids)
	}
	time.Sleep(120 * time.Millisecond)
	rec, err = s.ReclaimExpired(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var got *headgate.Reclaimed
	for i, r := range mineOnly(rec, q+"-") {
		if r.JobID == bomb {
			got = &mineOnly(rec, q+"-")[i]
		}
	}
	if got == nil {
		t.Fatalf("the bomb was not reclaimed: %+v", rec)
	}
	if !got.Quarantined || int64(got.CrashAttempt) != s.opts.CrashLimit {
		t.Fatalf("crash limit must quarantine: %+v (limit %d)", *got, s.opts.CrashLimit)
	}
	if st := jobField(t, s, bomb, "state"); st != "quarantined" {
		t.Fatalf("bomb state: %s", st)
	}
	if n := jobInt(t, s, bomb, "crash_attempt"); n != s.opts.CrashLimit {
		t.Fatalf("bomb crash_attempt=%d", n)
	}
	// The fingerprint is REGISTERED, not just the row parked — that registration is
	// what the gate and the enqueue path read.
	var crashCount int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT crash_count FROM headgate_quarantine WHERE fingerprint = ?", bombFp).
		Scan(&crashCount); err != nil {
		t.Fatalf("no quarantine row for %s: %v", bombFp, err)
	}
	if crashCount != s.opts.CrashLimit {
		t.Fatalf("quarantine crash_count=%d", crashCount)
	}
	// crash quarantine and therefore: enqueue of that fingerprint is refused until released.
	err = s.Enqueue(ctx, []headgate.Envelope{mk(bombQueue, q+"-again", "pz", bombFp, 1000)})
	if err == nil || !strings.Contains(err.Error(), "quarantined") {
		t.Fatalf("enqueue of a quarantined fingerprint must be refused, got %v", err)
	}
	// Hygiene: a leaked quarantine row silently rejects every later enqueue of this
	// fingerprint — and QuarantineRelease is the Inspect method that undoes it.
	if _, err := s.QuarantineRelease(ctx, bombFp); err != nil {
		t.Fatal(err)
	}
	if st := jobField(t, s, bomb, "state"); st != "available" {
		t.Fatalf("release must make the parked job available again: %s", st)
	}
}

func TestTheGoRuntimeRunsUnchangedOverGoMysql(t *testing.T) {
	s, ctx := testStore(t)
	q := "gmy-q"

	var downloads, failsLeft atomic.Int32
	failsLeft.Store(1)
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[gmMsg](reg, func(ctx context.Context, job *headgate.Job[gmMsg]) error {
		switch job.Args.Mode {
		case "ok":
			return nil
		case "panic":
			// attempt-log contract: lines logged BEFORE the panic still ride the retry ack.
			headgate.Log(ctx, "about to touch the wire")
			panic("kaboom")
		case "skip":
			return headgate.ErrSkipJob
		case "steps":
			// step replay over MySQL via the Go driver: the fence-gated checkpoint UPDATE.
			if err := headgate.Step(ctx, "download", func(context.Context) error {
				downloads.Add(1)
				return nil
			}); err != nil {
				return err
			}
			return headgate.Step(ctx, "transcode", func(context.Context) error {
				if failsLeft.Swap(0) > 0 {
					return errors.New("transcode failed")
				}
				return nil
			})
		default:
			return errors.New("unexpected mode " + job.Args.Mode)
		}
	})
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{q: {MaxWorkers: 4}},
		LeaseDuration: 30 * time.Second,
	}
	r := headgate.NewRunner(s, reg, cfg)

	if err := s.Enqueue(ctx, []headgate.Envelope{
		gmEnv(q, "gmy-ok", "ok"), gmEnv(q, "gmy-panic", "panic"),
		gmEnv(q, "gmy-skip", "skip"), gmEnv(q, "gmy-step", "steps"),
	}); err != nil {
		t.Fatal(err)
	}
	done, err := r.Drain(ctx, 10)
	if err != nil || len(done) != 4 {
		t.Fatalf("drain: %v %v", done, err)
	}
	if st := jobField(t, s, "gmy-ok", "state"); st != "completed" {
		t.Fatalf("ok: %s", st)
	}
	if st := jobField(t, s, "gmy-skip", "state"); st != "archived" {
		t.Fatalf("skip: %s", st)
	}
	if st := jobField(t, s, "gmy-panic", "state"); st != "retryable" {
		t.Fatalf("panic caught -> retryable, got %s", st)
	}
	if a := jobField(t, s, "gmy-panic", "attempt"); a != "1" {
		t.Fatalf("panic is a RETRY (attempt=1), got %s", a)
	}
	// attempt-log contract: the pre-panic log line landed INSIDE the attempt's entry.
	if errs := jobField(t, s, "gmy-panic", "errors"); !strings.Contains(errs, "about to touch the wire") {
		t.Fatalf("per-attempt logs must land in the entry: %s", errs)
	}
	if st := jobField(t, s, "gmy-step", "state"); st != "retryable" {
		t.Fatalf("step: %s", st)
	}
	if n := downloads.Load(); n != 1 {
		t.Fatalf("downloads=%d", n)
	}

	// Retry pass: the completed download step is SKIPPED, same as every backend.
	time.Sleep(30 * time.Millisecond)
	done, err = r.Drain(ctx, 10)
	if err != nil || len(done) != 2 {
		t.Fatalf("retry drain: %v %v", done, err)
	}
	if st := jobField(t, s, "gmy-step", "state"); st != "completed" {
		t.Fatalf("step retry: %s", st)
	}
	if n := downloads.Load(); n != 1 {
		t.Fatalf("checkpoint must skip the completed step; downloads=%d", n)
	}

	// runtime capability boundary capability honesty: Transactional (InnoDB) | Inspect (round 32c), never
	// Notifying. The method set and the Caps bit are asserted TOGETHER on purpose —
	// invariant 5's failure mode is a bit that claims what the methods cannot do, and
	// the reverse (methods present, bit missing) silently disables every duty the
	// runner gates on Inspect.
	if _, ok := any(s).(headgate.NotifyingStore); ok {
		t.Fatal("MySQL must not claim NotifyingStore — poll only, permanently")
	}
	if _, ok := any(s).(headgate.InspectStore); !ok {
		t.Fatal("the Go MySQL driver must implement InspectStore (round 32c)")
	}
	if s.Caps() != headgate.CapTransactional|headgate.CapInspect {
		t.Fatalf("caps: %b", s.Caps())
	}

	// Transactional enqueue + Once commit as one (the reason MySQL is in the PG tier).
	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueTx(ctx, tx, []headgate.Envelope{gmEnv(q, "gmy-tx1", "ok")}); err != nil {
		t.Fatal(err)
	}
	if err := s.RollbackTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	// Round 32h: `jobField` returns "" for a MISSING ROW and for a NULL column alike, so
	// "rollback discarded it" and "EnqueueTx silently wrote nothing" were the same
	// answer — and the latter is the exact failure this test exists to rule out. The
	// commit arm now runs FIRST, as the positive control that the path can write at all.
	tx, _ = s.BeginTx(ctx)
	if err := s.EnqueueTx(ctx, tx, []headgate.Envelope{gmEnv(q, "gmy-tx2", "ok")}); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if st := jobField(t, s, "gmy-tx2", "state"); st != "available" {
		t.Fatalf("control: a COMMITTED transactional enqueue must persist: %s", st)
	}
	if st := jobField(t, s, "gmy-tx1", "ulid"); st != "" {
		t.Fatal("rollback must discard the enqueue")
	}
}
