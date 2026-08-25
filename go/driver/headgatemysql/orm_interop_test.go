package headgatemysql

// caller-owned transaction contract the ORM-interop conformance matrix, Go × MySQL cell.
//
// Same three cases as the Postgres cell (go/driver/headgatepgx/orm_interop_test.go)
// against the other transactional backend, so the claim is about the PORT and not about
// the driver the reference implementation happens to use.
//
// The native handle here is database/sql *sql.Tx — opened by the TEST, never by
// headgate — and the entry point is the exported WrapTx. *sql.Tx is the handle GORM and
// Bun both sit on (tx.Statement.ConnPool.(*sql.Tx) and bun.Tx.Tx respectively), which is
// why this one cell covers every database/sql-based ORM without depending on any of
// them. See docs/orm-interop.md.
//
// Opt-in via HG_TEST_MYSQL. Run this package's tests one binary at a time — a
// default-config server has been wedged by full-parallel suites before.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
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
// nothing. One statement per call.
func ormClean(t *testing.T, s *MysqlStore, ctx context.Context, queue, app string) {
	t.Helper()
	_, _ = s.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+app)
	_, _ = s.db.ExecContext(ctx, `DELETE FROM headgate_job WHERE queue = ?`, queue)
	_, _ = s.db.ExecContext(ctx, `DELETE FROM headgate_active_partition WHERE queue = ?`, queue)
	_, _ = s.db.ExecContext(ctx, `DELETE FROM headgate_effect WHERE effect_key LIKE ?`, queue+"-%")
}

func ormCount(t *testing.T, s *MysqlStore, ctx context.Context, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRowContext(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

// (a) COMMIT — one caller-owned transaction, an app write and an enqueue; both visible,
// and the job actually admittable afterwards.
func TestORMInteropCallerTxCommitIsVisibleAndAdmittable(t *testing.T) {
	s, ctx := testStore(t)
	sc := ormScope()
	queue := "ormgomy-a-" + sc
	app := "hg_orm_app_a_" + sc
	ormClean(t, s, ctx, queue, app)
	if _, err := s.db.ExecContext(ctx,
		`CREATE TABLE `+app+` (id VARCHAR(64) PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB`); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	defer ormClean(t, s, ctx, queue, app)

	// THE POINT: the transaction is the application's. WrapTx lends it to headgate;
	// headgate never opens, commits, or owns anything here.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+app+` (id, note) VALUES (?, ?)`, "order-1", "paid"); err != nil {
		t.Fatalf("app write: %v", err)
	}
	if err := s.EnqueueTx(ctx, WrapTx(tx), []headgate.Envelope{ormEnv(queue, queue+"-j1")}); err != nil {
		t.Fatalf("EnqueueTx on the caller's *sql.Tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if n := ormCount(t, s, ctx, `SELECT count(*) FROM `+app); n != 1 {
		t.Fatalf("the app write must survive the commit; got %d", n)
	}
	if n := ormCount(t, s, ctx, `SELECT count(*) FROM headgate_job WHERE queue = ?`, queue); n != 1 {
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
	queue := "ormgomy-b-" + sc
	app := "hg_orm_app_b_" + sc
	ormClean(t, s, ctx, queue, app)
	if _, err := s.db.ExecContext(ctx,
		`CREATE TABLE `+app+` (id VARCHAR(64) PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB`); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	defer ormClean(t, s, ctx, queue, app)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+app+` (id, note) VALUES (?, ?)`, "order-2", "pending"); err != nil {
		t.Fatalf("app write: %v", err)
	}
	if err := s.EnqueueTx(ctx, WrapTx(tx), []headgate.Envelope{ormEnv(queue, queue+"-j1")}); err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if n := ormCount(t, s, ctx, `SELECT count(*) FROM `+app); n != 0 {
		t.Fatalf("the app write must be gone; got %d", n)
	}
	if n := ormCount(t, s, ctx, `SELECT count(*) FROM headgate_job WHERE queue = ?`, queue); n != 0 {
		t.Fatalf("the enqueue must be gone WITH it — neither exists; got %d", n)
	}
	// Round 32h: this used to be a loop over a slice that is ALWAYS empty on the pass
	// path — dead code an `Admit` hard-wired to `return nil, nil` would satisfy. A
	// committed sibling in the same per-run queue is the positive control.
	tx2, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin commit arm: %v", err)
	}
	if err := s.EnqueueTx(ctx, WrapTx(tx2), []headgate.Envelope{ormEnv(queue, queue+"-j2")}); err != nil {
		t.Fatalf("EnqueueTx commit arm: %v", err)
	}
	if err := tx2.Commit(); err != nil {
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
func ormDeliver(t *testing.T, s *MysqlStore, ctx context.Context, lease headgate.LeaseRef, key, app string) bool {
	t.Helper()
	tx, err := s.db.BeginTx(ctx, nil)
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
		_ = tx.Rollback()
		return false
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+app+` (id, note) VALUES (?, ?)`, "charge-1", "applied"); err != nil {
		t.Fatalf("app write: %v", err)
	}
	if err := s.CompleteTx(ctx, htx, lease); err != nil {
		t.Fatalf("CompleteTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return true
}

// (c) Handler side: the effect-key claim, the app write, and the fence-verified
// completion in ONE caller transaction (transactional effects, the ClaimEffect/CompleteTx pair behind
// Job.Once, driven directly so the app table is part of the commit). A crash after that
// commit re-delivers the job; the second pass must claim nothing and write nothing.
func TestORMInteropOnceInCallerTxDoesNotDoubleApply(t *testing.T) {
	s, ctx := testStore(t)
	sc := ormScope()
	queue := "ormgomy-c-" + sc
	app := "hg_orm_app_c_" + sc
	ormClean(t, s, ctx, queue, app)
	if _, err := s.db.ExecContext(ctx,
		`CREATE TABLE `+app+` (id VARCHAR(64) PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB`); err != nil {
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

	if n := ormCount(t, s, ctx, `SELECT count(*) FROM `+app); n != 1 {
		t.Fatalf("the app effect must be applied EXACTLY once; got %d", n)
	}
	if n := ormCount(t, s, ctx, `SELECT count(*) FROM headgate_effect WHERE effect_key = ?`, key); n != 1 {
		t.Fatalf("one effect-key row, claimed once, forever; got %d", n)
	}
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM headgate_job WHERE ulid = ?`, jobID).Scan(&state); err != nil {
		t.Fatalf("job row: %v", err)
	}
	if state != "completed" {
		t.Fatalf("completion must commit with the app write, not after it; got %s", state)
	}
}
