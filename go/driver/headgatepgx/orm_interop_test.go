package headgatepgx

// caller-owned transaction contract the ORM-interop conformance matrix, Go × Postgres cell.
//
// Transactional enqueue is the headline feature and it is worth nothing if it cannot
// join the transaction the application already has open. "The port shape is right" was a
// claim until this file; the matrix is what turns it into a fact.
//
// The native handle here is pgx.Tx — opened by the TEST, never by headgate — and the
// entry point is the exported WrapTx, which is exactly what a GORM/Bun/pgx application
// calls. No ORM dependency is added: every Go ORM in the survey hands out either a
// *sql.Tx or a pgx.Tx underneath, and what a port can accept is decided by that driver
// type, not by the ORM's name (see docs/orm-interop.md).
//
// Opt-in via HG_TEST_PG; skips cleanly without it.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// ormScope is a per-run suffix. $-scoped so two runs (or a crashed previous run) can
// never collide, and everything it names is dropped when the test finishes.
func ormScope() string { return fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano()) }

func ormEnv(queue, id string) headgate.Envelope {
	payload := []byte(`{}`)
	return headgate.Envelope{
		ID: id, Kind: "orm:t", Payload: payload, Queue: queue,
		Fingerprint:   headgate.Fingerprint("orm:t", payload),
		ScheduledAtMs: 1, RetentionMs: 86_400_000,
	}
}

func ormAdmit(queue, worker, lease string) headgate.AdmitRequest {
	return headgate.AdmitRequest{
		Worker: worker, LeaseID: lease, Queues: []string{queue},
		Capacity: 10, Lease: 60 * time.Second, Quantum: 1000,
	}
}

// ormClean runs at START as well as at the end: a previous run that panicked mid-test
// leaves rows behind, and a matrix that only passes on a pristine database proves
// nothing.
func ormClean(t *testing.T, s *PgxStore, ctx context.Context, queue, app string) {
	t.Helper()
	_, _ = s.pool.Exec(ctx, `DROP TABLE IF EXISTS `+app)
	_, _ = s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = $1`, queue)
	_, _ = s.pool.Exec(ctx, `DELETE FROM headgate_active_partition WHERE queue = $1`, queue)
	_, _ = s.pool.Exec(ctx, `DELETE FROM headgate_effect WHERE key LIKE $1`, queue+"-%")
}

func ormCount(t *testing.T, s *PgxStore, ctx context.Context, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := s.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

// (a) COMMIT — one caller-owned transaction, an app write and an enqueue; both visible,
// and the job actually admittable afterwards.
func TestORMInteropCallerTxCommitIsVisibleAndAdmittable(t *testing.T) {
	s, ctx := testStore(t)
	sc := ormScope()
	queue := "ormgopg-a-" + sc
	app := "hg_orm_app_a_" + sc
	ormClean(t, s, ctx, queue, app)
	if _, err := s.pool.Exec(ctx, `CREATE TABLE `+app+` (id text primary key, note text)`); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	defer ormClean(t, s, ctx, queue, app)

	// THE POINT: the transaction is the application's. WrapTx lends it to headgate;
	// headgate never opens, commits, or owns anything here.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+app+` (id, note) VALUES ($1, $2)`, "order-1", "paid"); err != nil {
		t.Fatalf("app write: %v", err)
	}
	if err := s.EnqueueTx(ctx, WrapTx(tx), []headgate.Envelope{ormEnv(queue, queue+"-j1")}); err != nil {
		t.Fatalf("EnqueueTx on the caller's pgx.Tx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if n := ormCount(t, s, ctx, `SELECT count(*)::bigint FROM `+app); n != 1 {
		t.Fatalf("the app write must survive the commit; got %d", n)
	}
	if n := ormCount(t, s, ctx, `SELECT count(*)::bigint FROM headgate_job WHERE queue = $1`, queue); n != 1 {
		t.Fatalf("the enqueue must survive the same commit; got %d", n)
	}

	// Visible is not enough: the job has to pass the gate. An enqueue that commits but
	// is not admittable (wrong state, missing active-partition row) would be a silent
	// stall, so the matrix admits it for real.
	units, err := s.Admit(ctx, ormAdmit(queue, "orm-w", "ORM-GL1"))
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	var ids []string
	for _, u := range units {
		for _, c := range u.Claims {
			ids = append(ids, c.Envelope.ID)
		}
	}
	if len(ids) != 1 || ids[0] != queue+"-j1" {
		t.Fatalf("the committed job must be admittable; got %v", ids)
	}
}

// (b) ROLLBACK — the money assertion. If the app's transaction aborts, headgate's row
// must vanish with it. A queue that survives its caller's rollback has published a job
// for work that never happened, which is the exact failure transactional enqueue exists
// to prevent.
func TestORMInteropCallerTxRollbackLeavesNeither(t *testing.T) {
	s, ctx := testStore(t)
	sc := ormScope()
	queue := "ormgopg-b-" + sc
	app := "hg_orm_app_b_" + sc
	ormClean(t, s, ctx, queue, app)
	if _, err := s.pool.Exec(ctx, `CREATE TABLE `+app+` (id text primary key, note text)`); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	defer ormClean(t, s, ctx, queue, app)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+app+` (id, note) VALUES ($1, $2)`, "order-2", "pending"); err != nil {
		t.Fatalf("app write: %v", err)
	}
	if err := s.EnqueueTx(ctx, WrapTx(tx), []headgate.Envelope{ormEnv(queue, queue+"-j1")}); err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if n := ormCount(t, s, ctx, `SELECT count(*)::bigint FROM `+app); n != 0 {
		t.Fatalf("the app write must be gone; got %d", n)
	}
	if n := ormCount(t, s, ctx, `SELECT count(*)::bigint FROM headgate_job WHERE queue = $1`, queue); n != 0 {
		t.Fatalf("the enqueue must be gone WITH it — neither exists; got %d", n)
	}
	// Round 32h: this used to be `for _, u := range units { if len(u.Claims) > 0 {...} }`
	// over a slice that is ALWAYS empty on the pass path — dead code that an `Admit`
	// hard-wired to `return nil, nil` would satisfy. A committed sibling in the same
	// per-run queue is the positive control: the gate has to hand back j2 for the
	// absence of j1 to mean anything.
	tx2, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin commit arm: %v", err)
	}
	if err := s.EnqueueTx(ctx, WrapTx(tx2), []headgate.Envelope{ormEnv(queue, queue+"-j2")}); err != nil {
		t.Fatalf("EnqueueTx commit arm: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	units, err := s.Admit(ctx, ormAdmit(queue, "orm-w", "ORM-GL2"))
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	var admitted []string
	for _, u := range units {
		for _, c := range u.Claims {
			admitted = append(admitted, c.Envelope.ID)
		}
	}
	if len(admitted) != 1 || admitted[0] != queue+"-j2" {
		t.Fatalf("only the COMMITTED sibling may be admittable; got %v", admitted)
	}
}

// ormDeliver is one delivery of the job, shaped exactly like Job.Once: claim the effect
// key, do the application's writes, complete the job — all on ONE transaction the caller
// owns, so either all three commit or none do. Returns whether the effect ran.
func ormDeliver(t *testing.T, s *PgxStore, ctx context.Context, lease headgate.LeaseRef, key, app string) bool {
	t.Helper()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	htx := WrapTx(tx)
	claimed, err := s.ClaimEffect(ctx, htx, key)
	if err != nil {
		t.Fatalf("ClaimEffect: %v", err)
	}
	if !claimed {
		// a COMMITTED transaction already claimed it; the effect ran.
		_ = tx.Rollback(ctx)
		return false
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+app+` (id, note) VALUES ($1, $2)`, "charge-1", "applied"); err != nil {
		t.Fatalf("app write: %v", err)
	}
	if err := s.CompleteTx(ctx, htx, lease); err != nil {
		t.Fatalf("CompleteTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return true
}

// (c) Handler side: the effect-key claim, the app write, and the fence-verified
// completion in ONE caller transaction (transactional effects, the machinery behind Job.Once — the same
// ClaimEffect/CompleteTx pair TestJobOnceCommitsEffectsAtomicallyWithCompletion drives
// through the runtime, here driven directly so the app table is part of the commit). A
// crash after that commit re-delivers the job; the second pass must claim nothing and
// write nothing.
func TestORMInteropOnceInCallerTxDoesNotDoubleApply(t *testing.T) {
	s, ctx := testStore(t)
	sc := ormScope()
	queue := "ormgopg-c-" + sc
	app := "hg_orm_app_c_" + sc
	ormClean(t, s, ctx, queue, app)
	if _, err := s.pool.Exec(ctx, `CREATE TABLE `+app+` (id text primary key, note text)`); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	defer ormClean(t, s, ctx, queue, app)

	jobID := queue + "-j1"
	if err := s.Enqueue(ctx, []headgate.Envelope{ormEnv(queue, jobID)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	units, err := s.Admit(ctx, ormAdmit(queue, "orm-w", "ORM-GL3"))
	if err != nil || len(units) == 0 || len(units[0].Claims) == 0 {
		t.Fatalf("admit: %v units=%d", err, len(units))
	}
	c := units[0].Claims[0]
	lease := headgate.LeaseRef{JobID: c.Envelope.ID, LeaseID: c.LeaseID, Fence: c.Fence}
	key := queue + "-effect"

	if !ormDeliver(t, s, ctx, lease, key, app) {
		t.Fatalf("first delivery must run the effect")
	}
	// The crash: the worker died AFTER the commit and before it could report anything,
	// so the job is delivered again. Once is what makes that safe.
	if ormDeliver(t, s, ctx, lease, key, app) {
		t.Fatalf("a redelivery after a committed effect must skip the work entirely")
	}

	if n := ormCount(t, s, ctx, `SELECT count(*)::bigint FROM `+app); n != 1 {
		t.Fatalf("the app effect must be applied EXACTLY once; got %d", n)
	}
	if n := ormCount(t, s, ctx, `SELECT count(*)::bigint FROM headgate_effect WHERE key = $1`, key); n != 1 {
		t.Fatalf("one effect-key row, claimed once, forever; got %d", n)
	}
	var state string
	if err := s.pool.QueryRow(ctx, `SELECT state::text FROM headgate_job WHERE ulid = $1`, jobID).Scan(&state); err != nil {
		t.Fatalf("job row: %v", err)
	}
	if state != "completed" {
		t.Fatalf("completion must commit with the app write, not after it; got %s", state)
	}
}
