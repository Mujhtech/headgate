package headgatemysql

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	headgate "github.com/mujhtech/headgate"
	"github.com/mujhtech/headgate/headgatemigrate"
	"github.com/mujhtech/headgate/headgatetest"
)

func isolatedMySQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ClientFoundRows = true
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mysqlMultiInstanceEnvelope(kind string) headgate.Envelope {
	return headgate.Envelope{
		ID: "same-job-id", Kind: kind, Payload: []byte(kind), Queue: "same-queue",
		Fingerprint: "fp-" + kind, ScheduledAtMs: 1, RetentionMs: 86_400_000,
	}
}

func mysqlMultiInstanceRequest(worker, lease string) headgate.AdmitRequest {
	return headgate.AdmitRequest{
		Worker: worker, LeaseID: lease, Queues: []string{"same-queue"},
		Capacity: 1, Lease: 30 * time.Second, Quantum: 1_000,
	}
}

func TestDatabasesIsolateStoreDutiesAndDestructiveMigrations(t *testing.T) {
	url := os.Getenv("HG_TEST_MYSQL")
	if url == "" {
		t.Skip("HG_TEST_MYSQL not set")
	}
	ctx := context.Background()
	leftDB := headgatetest.RequireMySQLTestDatabase(t, ctx, url)
	rightDB := headgatetest.RequireMySQLTestDatabase(t, ctx, url)
	leftSQL := isolatedMySQL(t, leftDB.DSN)
	rightSQL := isolatedMySQL(t, rightDB.DSN)
	left := New(leftSQL)
	right := New(rightSQL)

	if err := left.Enqueue(ctx, []headgate.Envelope{mysqlMultiInstanceEnvelope("left-kind")}); err != nil {
		t.Fatal(err)
	}
	if err := right.Enqueue(ctx, []headgate.Envelope{mysqlMultiInstanceEnvelope("right-kind")}); err != nil {
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
	leftUnits, err := left.Admit(ctx, mysqlMultiInstanceRequest("left-worker", "left-lease"))
	if err != nil || len(leftUnits) != 1 || leftUnits[0].Claims[0].Envelope.Kind != "left-kind" {
		t.Fatalf("left admission = %#v, %v", leftUnits, err)
	}
	rightUnits, err := right.Admit(ctx, mysqlMultiInstanceRequest("right-worker", "right-lease"))
	if err != nil || len(rightUnits) != 1 || rightUnits[0].Claims[0].Envelope.Kind != "right-kind" {
		t.Fatalf("right admission = %#v, %v", rightUnits, err)
	}
	if got, err := left.ClaimDuty(ctx, "same-duty", "left-holder", 30*time.Second); err != nil || !got {
		t.Fatalf("left duty = %t, %v", got, err)
	}
	if got, err := right.ClaimDuty(ctx, "same-duty", "right-holder", 30*time.Second); err != nil || !got {
		t.Fatalf("right duty = %t, %v", got, err)
	}

	if _, err := headgatemigrate.MigrateMySQL(
		ctx, leftSQL, headgatemigrate.Down, headgatemigrate.Options{},
	); err != nil {
		t.Fatalf("drop left installation: %v", err)
	}
	validation, err := headgatemigrate.ValidateMySQL(ctx, rightSQL)
	if err != nil || !validation.OK() {
		t.Fatalf("right validation = %#v, %v", validation, err)
	}
	rightJob, err = right.GetJob(ctx, "same-job-id", false)
	if err != nil || rightJob == nil {
		t.Fatalf("right job after left rollback = %#v, %v", rightJob, err)
	}
}
