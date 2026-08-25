package headgatepgx

// The Go worker runtime over the pgx store — mirroring the Rust runtime tests: typed
// dispatch, panic recovery by default, control errors, step replay step replay with a stale
// step set parking as undecodable, cooperative lost-lease cancellation, and shutdown
// that releases (not abandons) in-flight work.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

type rtMsg struct {
	Mode string `json:"mode"`
}

func (rtMsg) Kind() string { return "gort:msg" }

func rtEnv(id, mode string) headgate.Envelope {
	payload := []byte(`{"mode":"` + mode + `"}`)
	return headgate.Envelope{
		ID: id, Kind: "gort:msg", Payload: payload,
		Queue: "gort", Fingerprint: headgate.Fingerprint("gort:msg", payload),
		ScheduledAtMs: 1, RetentionMs: 86_400_000,
	}
}

func rtRow(t *testing.T, s *PgxStore, id string) (state string, attempt, crash int32, errs string) {
	t.Helper()
	err := s.pool.QueryRow(context.Background(),
		`SELECT state::text, attempt, crash_attempt, errors::text
		 FROM headgate_job WHERE ulid = $1`, id).Scan(&state, &attempt, &crash, &errs)
	if err != nil {
		t.Fatalf("row %s: %v", id, err)
	}
	return
}

func TestGoRuntimeDrainStepsAndPanics(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gort'`); err != nil {
		t.Fatal(err)
	}
	s.opts.RetryBaseMs = 1

	var downloads, failsLeft atomic.Int32
	failsLeft.Store(1)
	reg := headgate.NewRegistry()
	err := headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
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
			if err := headgate.Step(ctx, "download", func(context.Context) error {
				downloads.Add(1)
				return nil
			}); err != nil {
				return err
			}
			return headgate.Step(ctx, "transcode", func(context.Context) error {
				if failsLeft.Swap(0) > 0 {
					return context.DeadlineExceeded // any error: consume an attempt
				}
				return nil
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"gort": {MaxWorkers: 10}},
		LeaseDuration: 30 * time.Second,
		DisableDuties: true,
	}
	r := headgate.NewRunner(s, reg, cfg)

	if err := s.Enqueue(ctx, []headgate.Envelope{
		rtEnv("gort-ok", "ok"), rtEnv("gort-panic", "panic"),
		rtEnv("gort-skip", "skip"), rtEnv("gort-step", "steps"),
	}); err != nil {
		t.Fatal(err)
	}
	done, err := r.Drain(ctx, 10)
	if err != nil || len(done) != 4 {
		t.Fatalf("drain: %v %v", done, err)
	}
	if st, _, _, _ := rtRow(t, s, "gort-ok"); st != "completed" {
		t.Fatalf("ok: %s", st)
	}
	if st, _, _, _ := rtRow(t, s, "gort-skip"); st != "archived" {
		t.Fatalf("skip: %s", st)
	}
	// panic-recovery contract panic caught by DEFAULT, recorded distinctly, counted as a returned error.
	st, attempt, crash, errs := rtRow(t, s, "gort-panic")
	if st != "retryable" || attempt != 1 || crash != 0 || !strings.Contains(errs, "panic: kaboom") {
		t.Fatalf("panic: %s a=%d c=%d errs=%s", st, attempt, crash, errs)
	}
	// attempt-log contract: the pre-panic log line landed INSIDE the attempt's entry.
	if !strings.Contains(errs, "about to touch the wire") {
		t.Fatalf("per-attempt logs must land in the entry: %s", errs)
	}

	// Retry pass: the completed download step is skipped — same checkpoint semantics
	// as the Rust runtime against the same store.
	time.Sleep(30 * time.Millisecond)
	if done, err = r.Drain(ctx, 10); err != nil || len(done) != 2 {
		t.Fatalf("drain2: %v %v", done, err)
	}
	if st, _, _, _ := rtRow(t, s, "gort-step"); st != "completed" {
		t.Fatalf("step: %s", st)
	}
	if n := downloads.Load(); n != 1 {
		t.Fatalf("download ran %d times; checkpoint must skip completed steps", n)
	}

	// A "deploy" that renames the steps: the checkpointed job parks as undecodable.
	reg2 := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg2, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		if job.Args.Mode != "steps" {
			return nil
		}
		if err := headgate.Step(ctx, "fetch", func(context.Context) error { return nil }); err != nil {
			return err
		}
		return headgate.Step(ctx, "encode", func(context.Context) error { return nil })
	})
	r2 := headgate.NewRunner(s, reg2, cfg)
	failsLeft.Store(1)
	if err := s.Enqueue(ctx, []headgate.Envelope{rtEnv("gort-stale", "steps")}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Drain(ctx, 10); err != nil { // fails at transcode with old steps
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := r2.Drain(ctx, 10); err != nil { // resumes under NEW steps
		t.Fatal(err)
	}
	if st, _, _, _ := rtRow(t, s, "gort-stale"); st != "undecodable" {
		t.Fatalf("stale step set must park undecodable, got %s", st)
	}
}

func TestGoRunnerCancelsLostLeasesAndReleasesOnShutdown(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gort2'`); err != nil {
		t.Fatal(err)
	}

	var sawCancel, finished atomic.Int32
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		if job.Args.Mode != "slow" {
			return nil
		}
		select {
		case <-ctx.Done():
			sawCancel.Add(1)
			return headgate.ErrLeaseLost // cooperative stop at the cancellation signal
		case <-time.After(30 * time.Second):
			finished.Add(1)
			return nil
		}
	})
	cfg := headgate.Config{
		Queues:          map[string]headgate.QueueConfig{"gort2": {MaxWorkers: 4}},
		LeaseDuration:   2 * time.Second,
		DutyInterval:    100 * time.Millisecond,
		ShutdownTimeout: 300 * time.Millisecond,
	}
	r := headgate.NewRunner(s, reg, cfg)
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(ctx) }()

	env := rtEnv("gort2-slow", "slow")
	env.Queue, env.ID = "gort2", "gort2-slow"
	if err := s.Enqueue(ctx, []headgate.Envelope{env}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		st, _, _, _ := rtRowSoft(s, "gort2-slow")
		return st == "running"
	})
	// Force expiry (re-forcing each poll: a heartbeat may legitimately renew first),
	// until the reclaimer sweeps and the heartbeat cancels the handler.
	waitFor(t, 15*time.Second, func() bool {
		_, _ = s.pool.Exec(context.Background(),
			`UPDATE headgate_job SET lease_expires_at_ms = 0
			 WHERE ulid = 'gort2-slow' AND state = 'running'`)
		_, _ = s.ReclaimExpired(context.Background(), 100)
		_, _, crash, _ := rtRowSoft(s, "gort2-slow")
		return crash >= 1 && sawCancel.Load() >= 1
	})
	if finished.Load() != 0 {
		t.Fatal("canceled handler must not finish")
	}

	// Park the reclaimed job, then prove shutdown releases a NEW in-flight job.
	if _, err := s.pool.Exec(ctx,
		`UPDATE headgate_job SET scheduled_at_ms = 9999999999999 WHERE ulid = 'gort2-slow'`); err != nil {
		t.Fatal(err)
	}
	env2 := rtEnv("gort2-rel", "slow")
	env2.Queue, env2.ID = "gort2", "gort2-rel"
	if err := s.Enqueue(ctx, []headgate.Envelope{env2}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		st, _, _, _ := rtRowSoft(s, "gort2-rel")
		return st == "running"
	})
	r.Shutdown()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
	st, attempt, crash, _ := rtRowSoft(s, "gort2-rel")
	if st != "available" || attempt != 0 || crash != 0 {
		t.Fatalf("shutdown must RELEASE (available, no counters); got %s a=%d c=%d", st, attempt, crash)
	}
}

// panic-recovery contract round 32 — panic ISOLATION, the Go half. Go's isolation is NATIVE and needs no
// mechanism: admitOnce gives every job its own goroutine and `invoke`'s deferred
// recover runs on that goroutine, so a panic unwinds one stack and no other. This
// asserts it rather than assuming it — a panicking handler and a healthy one run
// CONCURRENTLY in one live Runner: the healthy job completes, the panic is recorded as
// its own attempt entry, and the loop keeps admitting afterwards.
//
// The overlap is FORCED, not hoped for: the healthy handler signals that it is in
// flight, the panicking one waits for that signal before panicking, and the healthy one
// waits for the panic to have fired before finishing. Without real overlap nothing
// completes and the test fails on the wait.
func TestGoPanicIsolationDoesNotDisturbAConcurrentHealthyJob(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gort3'`); err != nil {
		t.Fatal(err)
	}

	var slowStarted, panicFired, slowFinished, overlapped atomic.Int32
	reg := headgate.NewRegistry()
	err := headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		switch job.Args.Mode {
		case "panic":
			for i := 0; i < 2000 && slowStarted.Load() == 0; i++ {
				time.Sleep(5 * time.Millisecond)
			}
			if slowStarted.Load() > 0 {
				overlapped.Store(1)
			}
			panicFired.Store(1)
			panic("isolate-kaboom")
		case "slow-ok":
			slowStarted.Store(1)
			for i := 0; i < 2000 && panicFired.Load() == 0; i++ {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond)
			slowFinished.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := headgate.Config{
		Queues:          map[string]headgate.QueueConfig{"gort3": {MaxWorkers: 4}},
		LeaseDuration:   30 * time.Second,
		DutyInterval:    100 * time.Millisecond,
		ShutdownTimeout: 300 * time.Millisecond,
	}
	r := headgate.NewRunner(s, reg, cfg)
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(ctx) }()

	// MaxAttempts 1 makes the recorded panic TERMINAL: a retry loop would keep
	// re-panicking under the live runner and race the assertion.
	pe := rtEnv("gort3-panic", "panic")
	pe.Queue, pe.MaxAttempts = "gort3", 1
	se := rtEnv("gort3-slow", "slow-ok")
	se.Queue = "gort3"
	if err := s.Enqueue(ctx, []headgate.Envelope{pe, se}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 40*time.Second, func() bool {
		st, _, _, _ := rtRowSoft(s, "gort3-slow")
		return st == "completed"
	})
	if slowFinished.Load() != 1 {
		t.Fatalf("healthy handler must run to completion beside a panicking sibling; finished=%d",
			slowFinished.Load())
	}
	if overlapped.Load() != 1 {
		t.Fatal("the two handlers must have been in flight at the same time")
	}
	st, attempt, crash, errs := rtRowSoft(s, "gort3-panic")
	if st != "archived" || attempt != 1 || crash != 0 {
		t.Fatalf("an isolated panic is a RECOVERED attempt, never a crash; got %s a=%d c=%d", st, attempt, crash)
	}
	if !strings.Contains(errs, "panic: isolate-kaboom") {
		t.Fatalf("the panic must be its own attempt entry: %s", errs)
	}

	// The run loop survived the panic: it still admits.
	ae := rtEnv("gort3-after", "ok")
	ae.Queue = "gort3"
	if err := s.Enqueue(ctx, []headgate.Envelope{ae}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 40*time.Second, func() bool {
		st, _, _, _ := rtRowSoft(s, "gort3-after")
		return st == "completed"
	})

	r.Shutdown()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
}

func rtRowSoft(s *PgxStore, id string) (state string, attempt, crash int32, errs string) {
	_ = s.pool.QueryRow(context.Background(),
		`SELECT state::text, attempt, crash_attempt, errors::text
		 FROM headgate_job WHERE ulid = $1`, id).Scan(&state, &attempt, &crash, &errs)
	return
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// transactional effects Job.Once: the effect, the effect-key claim, and the job's completion commit in
// ONE transaction — so an error AFTER Once cannot cause a retry that re-runs the
// effect, and the effect exists exactly once.
func TestJobOnceCommitsEffectsAtomicallyWithCompletion(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS hg_test_once_out (id text);
		DELETE FROM hg_test_once_out WHERE id LIKE 'gonce-%';
		DELETE FROM headgate_job WHERE queue = 'gonce';
		DELETE FROM headgate_effect WHERE key LIKE 'gonce-%'`); err != nil {
		t.Fatal(err)
	}
	reg2 := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg2, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		if err := job.Once(ctx, func(tx headgate.Tx) error {
			pgtx, err := unwrapTx(tx) // in-package; users would Unwrap().(pgx.Tx)
			if err != nil {
				return err
			}
			_, err = pgtx.Exec(ctx, `INSERT INTO hg_test_once_out VALUES ($1)`, job.ID)
			return err
		}); err != nil {
			return err
		}
		// The error AFTER Once must not undo anything: completion already committed.
		return context.DeadlineExceeded
	})
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"gonce": {MaxWorkers: 4}},
		DisableDuties: true,
	}
	r := headgate.NewRunner(s, reg2, cfg)

	e := rtEnv("gonce-1", "ok")
	e.Queue = "gonce"
	if err := s.Enqueue(ctx, []headgate.Envelope{e}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Drain(ctx, 10); err != nil {
		t.Fatal(err)
	}
	st, attempt, _, _ := rtRow(t, s, "gonce-1")
	if st != "completed" || attempt != 0 {
		t.Fatalf("Once must complete transactionally despite the later error; got %s a=%d", st, attempt)
	}
	var effects, outs int64
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM headgate_effect WHERE key = 'gonce-1'`).Scan(&effects)
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM hg_test_once_out WHERE id = 'gonce-1'`).Scan(&outs)
	if effects != 1 || outs != 1 {
		t.Fatalf("exactly one effect expected: effects=%d outs=%d", effects, outs)
	}
	// runtime capability boundary: the claim is idempotent forever — a fresh tx cannot re-claim.
	tx, _ := s.BeginTx(ctx)
	again, err := s.ClaimEffect(ctx, tx, "gonce-1")
	_ = s.RollbackTx(ctx, tx)
	if err != nil || again {
		t.Fatalf("effect key must stay claimed: again=%v err=%v", again, err)
	}
}

// ROUND 32L — the money path, Go side. transactional effects's guarantee is that the effect-key claim, the
// caller's writes and the FENCE-VERIFIED completion are ONE transaction, so a superseded
// holder's half-done writes never commit. Round 32l changed the ErrLeaseLost arm of
// `Once` from RollbackTx to CommitTx in BOTH languages — a post-effect failure that
// double-charges — and the whole gate stayed green: 462 shell assertions, 96 scenarios,
// both suites. TestJobOnceCommitsEffectsAtomicallyWithCompletion cannot see it, because
// its job is never stolen and the rejected-completion arm is therefore never taken.
//
// The steal happens INSIDE the closure, after the write, which is the production shape:
// the charge is in the transaction, and only then does the fence say the job is not ours.
// The un-stolen sibling is the CONTROL — without it a `Once` that wrote nothing at all
// would satisfy the real assertion and the test would be a tautology about empty tables.
func TestOnceRollsBackTheEffectWhenTheFenceRefusesTheCompletion(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS hg_test_once_out (id text);
		DELETE FROM hg_test_once_out WHERE id LIKE 'gofc-%';
		DELETE FROM headgate_job WHERE queue = 'gofc';
		DELETE FROM headgate_effect WHERE key LIKE 'gofc-%'`); err != nil {
		t.Fatal(err)
	}
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		steal := job.Args.Mode == "steal"
		err := job.Once(ctx, func(tx headgate.Tx) error {
			pgtx, uerr := unwrapTx(tx)
			if uerr != nil {
				return uerr
			}
			// The charge.
			if _, e := pgtx.Exec(ctx, `INSERT INTO hg_test_once_out VALUES ($1)`, job.ID); e != nil {
				return e
			}
			if steal {
				// ...and only now does the job stop being ours. A separate connection,
				// so this commits while the effect transaction is still open.
				if e := s.OperatorCancel(ctx, job.ID); e != nil {
					return e
				}
			}
			return nil
		})
		if steal && err == nil {
			return errors.New("BUG: Once completed a job whose fence was already gone")
		}
		return err
	})
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"gofc": {MaxWorkers: 4}},
		DisableDuties: true,
	}
	r := headgate.NewRunner(s, reg, cfg)

	stolen, kept := rtEnv("gofc-stolen", "steal"), rtEnv("gofc-kept", "keep")
	stolen.Queue, kept.Queue = "gofc", "gofc"
	if err := s.Enqueue(ctx, []headgate.Envelope{stolen, kept}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Drain(ctx, 10); err != nil {
		t.Fatal(err)
	}

	count := func(q, arg string) int64 {
		var n int64
		_ = s.pool.QueryRow(ctx, q, arg).Scan(&n)
		return n
	}
	keptOut := count(`SELECT count(*) FROM hg_test_once_out WHERE id = $1`, "gofc-kept")
	keptEff := count(`SELECT count(*) FROM headgate_effect WHERE key = $1`, "gofc-kept")
	if keptOut != 1 || keptEff != 1 {
		t.Fatalf("control: the un-stolen sibling must commit exactly one effect and one claim "+
			"(otherwise the assertion below passes on a Once that writes nothing); out=%d eff=%d",
			keptOut, keptEff)
	}
	stolenOut := count(`SELECT count(*) FROM hg_test_once_out WHERE id = $1`, "gofc-stolen")
	stolenEff := count(`SELECT count(*) FROM headgate_effect WHERE key = $1`, "gofc-stolen")
	if stolenOut != 0 || stolenEff != 0 {
		t.Fatalf("transactional effects: a completion the FENCE refused must roll the caller's writes back with it. "+
			"Committing them anyway is a double charge — the effect key is gone too, so the next "+
			"delivery re-runs the work and charges again; out=%d eff=%d", stolenOut, stolenEff)
	}
	if st, _, _, _ := rtRow(t, s, "gofc-stolen"); st != "cancelled" {
		t.Fatalf("the stolen job stays where its new owner put it, never completed by the loser; got %s", st)
	}
}

// step replay × transactional effects StepOnce: the charge and its step-completion marker are ONE commit, so a
// failure after the step retries the job but never the charge.
func TestStepOnceEffectsCommitExactlyOnceAcrossRetries(t *testing.T) {
	s, ctx := testStore(t)
	s.opts.RetryBaseMs = 1
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS hg_test_once_out (id text);
		DELETE FROM hg_test_once_out WHERE id LIKE 'gso-%';
		DELETE FROM headgate_job WHERE queue = 'gso';
		DELETE FROM headgate_effect WHERE key LIKE 'gso-%'`); err != nil {
		t.Fatal(err)
	}
	var failOnce, charges atomic.Int32
	failOnce.Store(1)
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		if err := headgate.StepOnce(ctx, "charge", func(ctx context.Context, tx headgate.Tx) error {
			charges.Add(1)
			pgtx, err := unwrapTx(tx)
			if err != nil {
				return err
			}
			_, err = pgtx.Exec(ctx, `INSERT INTO hg_test_once_out VALUES ('gso-1')`)
			return err
		}); err != nil {
			return err
		}
		if failOnce.Swap(0) > 0 {
			return context.DeadlineExceeded // post-charge failure on attempt 1
		}
		return nil
	})
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"gso": {MaxWorkers: 4}},
		DisableDuties: true,
	}
	r := headgate.NewRunner(s, reg, cfg)
	e := rtEnv("gso-1", "x")
	e.Queue = "gso"
	if err := s.Enqueue(ctx, []headgate.Envelope{e}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Drain(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if st, _, _, _ := rtRow(t, s, "gso-1"); st != "retryable" {
		t.Fatalf("attempt 1 must retry, got %s", st)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := r.Drain(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if st, _, _, _ := rtRow(t, s, "gso-1"); st != "completed" {
		t.Fatalf("attempt 2 must complete, got %s", st)
	}
	if n := charges.Load(); n != 1 {
		t.Fatalf("the charge ran %d times; StepOnce must run it exactly once", n)
	}
	var outs int64
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM hg_test_once_out WHERE id = 'gso-1'`).Scan(&outs)
	if outs != 1 {
		t.Fatalf("exactly one committed charge expected, got %d", outs)
	}
}

// INVARIANT 13 — "A checkpoint is durable BEFORE the step's side effects, never after the
// worker returns. And every step boundary re-verifies the fence."
//
// Round 32i mutation-tested this by moving the in-progress `persist` from before `fn(ctx)`
// to after it — River's exact mistake — in BOTH languages, and the entire suite stayed
// green: every Go test, every Rust test, all 364 conformance assertions.
// TestGoRuntimeDrainStepsAndPanics cannot see it, because what makes a completed step skip
// on the next attempt is the COMPLETION checkpoint, which that reordering leaves alone.
//
// What the order buys is that the checkpoint write IS the fence check, so a worker whose
// lease was stolen learns it at the boundary and stops BEFORE the next step's side effect.
// Persist-after inverts that: the side effect runs, and the worker finds out afterwards
// that it had no right to run it — the double execution step replay exists to prevent.
//
// The lease is therefore stolen BETWEEN two steps, from inside the handler, and the
// assertion is on the second step's SIDE EFFECT COUNTER. Nothing is timing-dependent: the
// theft is a synchronous UPDATE sequenced by the handler itself. The Rust twin is
// crates/headgate/tests/runtime.rs.
func TestAStepBoundaryStopsBeforeTheSideEffectWhenTheLeaseIsGone(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gort'`); err != nil {
		t.Fatal(err)
	}
	var firstRan, secondRan atomic.Int32

	reg := headgate.NewRegistry()
	if err := headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		if err := headgate.Step(ctx, "first", func(context.Context) error {
			firstRan.Add(1)
			return nil
		}); err != nil {
			return err
		}
		// ANOTHER worker takes the job. Writing the lease id directly is the smallest
		// faithful theft: Checkpoint gates on (job, lease_id, fence), so this is exactly
		// the state a real re-claim leaves behind.
		if _, err := s.pool.Exec(ctx,
			`UPDATE headgate_job SET lease_id = 'gort-thief' WHERE ulid = $1`, "gort-fence"); err != nil {
			return err
		}
		return headgate.Step(ctx, "second", func(context.Context) error {
			secondRan.Add(1)
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}

	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"gort": {MaxWorkers: 10}},
		LeaseDuration: 30 * time.Second,
		DisableDuties: true,
	}
	r := headgate.NewRunner(s, reg, cfg)
	if err := s.Enqueue(ctx, []headgate.Envelope{rtEnv("gort-fence", "steps-fence")}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Drain(ctx, 10); err != nil {
		t.Fatalf("drain: %v", err)
	}

	if got := firstRan.Load(); got != 1 {
		t.Fatalf("witness: the handler must have run and reached the first step, got %d — "+
			"without this the zero below is what a job that never dispatched also produces", got)
	}
	if got := secondRan.Load(); got != 0 {
		t.Fatalf("invariant 13: the second step's SIDE EFFECT ran %d time(s) with the lease "+
			"already gone. The checkpoint (and with it the fence check) must be durable "+
			"BEFORE the step's side effects, never after them", got)
	}
}
