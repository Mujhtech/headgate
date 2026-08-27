package headgatepgx

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatemigrate"
	"github.com/mujhtech/headgate/go/headgatetest"
)

func multiInstanceEnvelope(kind string) headgate.Envelope {
	return headgate.Envelope{
		ID: "same-job-id", Kind: kind, Payload: []byte(kind), Queue: "same-queue",
		Fingerprint: "fp-" + kind, ScheduledAtMs: 1, RetentionMs: 86_400_000,
	}
}

func multiInstanceRequest(worker, lease string) headgate.AdmitRequest {
	return headgate.AdmitRequest{
		Worker: worker, LeaseID: lease, Queues: []string{"same-queue"},
		Capacity: 1, Lease: 30 * time.Second, Quantum: 1_000,
	}
}

func TestExplicitSchemasIsolateStoreDutiesAndMigrationsOnOnePool(t *testing.T) {
	conninfo := os.Getenv("HG_TEST_PG")
	if conninfo == "" {
		t.Skip("HG_TEST_PG not set")
	}
	ctx := context.Background()
	leftDB := headgatetest.RequirePostgresTestDatabase(t, ctx, conninfo)
	rightDB := headgatetest.RequirePostgresTestDatabase(t, ctx, conninfo)
	sharedPool, err := pgxpool.New(ctx, conninfo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sharedPool.Close)
	left, err := NewInSchema(sharedPool, leftDB.Schema)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewInSchema(sharedPool, rightDB.Schema)
	if err != nil {
		t.Fatal(err)
	}
	if left.Schema() != leftDB.Schema || right.Schema() != rightDB.Schema {
		t.Fatalf("schemas = %q, %q", left.Schema(), right.Schema())
	}

	if err := left.Enqueue(ctx, []headgate.Envelope{multiInstanceEnvelope("left-kind")}); err != nil {
		t.Fatal(err)
	}
	if err := right.Enqueue(ctx, []headgate.Envelope{multiInstanceEnvelope("right-kind")}); err != nil {
		t.Fatal(err)
	}
	leftJob, err := left.GetJob(ctx, "same-job-id", true)
	if err != nil || leftJob == nil || leftJob.Kind != "left-kind" {
		t.Fatalf("left job = %#v, %v", leftJob, err)
	}
	rightJob, err := right.GetJob(ctx, "same-job-id", true)
	if err != nil || rightJob == nil || rightJob.Kind != "right-kind" {
		t.Fatalf("right job = %#v, %v", rightJob, err)
	}
	leftUnits, err := left.Admit(ctx, multiInstanceRequest("left-worker", "left-lease"))
	if err != nil || len(leftUnits) != 1 || leftUnits[0].Claims[0].Envelope.Kind != "left-kind" {
		t.Fatalf("left admission = %#v, %v", leftUnits, err)
	}
	rightUnits, err := right.Admit(ctx, multiInstanceRequest("right-worker", "right-lease"))
	if err != nil || len(rightUnits) != 1 || rightUnits[0].Claims[0].Envelope.Kind != "right-kind" {
		t.Fatalf("right admission = %#v, %v", rightUnits, err)
	}
	if got, err := left.ClaimDuty(ctx, "same-duty", "left-holder", 30*time.Second); err != nil || !got {
		t.Fatalf("left duty = %t, %v", got, err)
	}
	if got, err := right.ClaimDuty(ctx, "same-duty", "right-holder", 30*time.Second); err != nil || !got {
		t.Fatalf("right duty = %t, %v", got, err)
	}

	admin, err := pgx.Connect(ctx, conninfo)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := headgatemigrate.MigratePostgresInSchema(
		ctx, admin, leftDB.Schema, headgatemigrate.Down, headgatemigrate.Options{},
	); err != nil {
		t.Fatalf("drop left installation: %v", err)
	}
	validation, err := headgatemigrate.ValidatePostgresInSchema(ctx, admin, rightDB.Schema)
	if err != nil || !validation.OK() {
		t.Fatalf("right validation = %#v, %v", validation, err)
	}
	rightJob, err = right.GetJob(ctx, "same-job-id", false)
	if err != nil || rightJob == nil {
		t.Fatalf("right job after left rollback = %#v, %v", rightJob, err)
	}
}
