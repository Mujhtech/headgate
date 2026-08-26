package headgatetest

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPostgresTestDatabasesMigrateIsolateParallelTestsAndCleanup(t *testing.T) {
	conninfo := os.Getenv("HG_TEST_PG")
	if conninfo == "" {
		t.Skip("HG_TEST_PG not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	type result struct {
		database *PostgresTestDatabase
		err      error
	}
	created := make(chan result, 2)
	for range 2 {
		go func() {
			database, err := CreatePostgresTestDatabase(ctx, conninfo)
			created <- result{database: database, err: err}
		}()
	}
	leftResult, rightResult := <-created, <-created
	if leftResult.err != nil {
		t.Fatal(leftResult.err)
	}
	if rightResult.err != nil {
		_ = leftResult.database.Cleanup(context.Background())
		t.Fatal(rightResult.err)
	}
	left, right := leftResult.database, rightResult.database
	t.Cleanup(func() { _ = left.Cleanup(context.Background()) })
	t.Cleanup(func() { _ = right.Cleanup(context.Background()) })
	if left.Schema == right.Schema {
		t.Fatalf("parallel helpers reused schema %q", left.Schema)
	}

	leftConn, err := left.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rightConn, err := right.Connect(ctx)
	if err != nil {
		leftConn.Close(ctx)
		t.Fatal(err)
	}
	defer rightConn.Close(context.Background())

	var leftInstalled, rightInstalled bool
	if err := leftConn.QueryRow(ctx, "SELECT to_regclass('headgate_job') IS NOT NULL").Scan(&leftInstalled); err != nil {
		t.Fatalf("left migration probe: %v", err)
	}
	if err := rightConn.QueryRow(ctx, "SELECT to_regclass('headgate_job') IS NOT NULL").Scan(&rightInstalled); err != nil {
		t.Fatalf("right migration probe: %v", err)
	}
	if !leftInstalled || !rightInstalled {
		t.Fatalf("schemas were not migrated: left=%t right=%t", leftInstalled, rightInstalled)
	}
	if _, err := leftConn.Exec(ctx, "INSERT INTO headgate_queue_state(queue) VALUES ('only-left')"); err != nil {
		t.Fatal(err)
	}
	var rightCount int
	if err := rightConn.QueryRow(ctx, "SELECT count(*) FROM headgate_queue_state WHERE queue = 'only-left'").Scan(&rightCount); err != nil {
		t.Fatal(err)
	}
	if rightCount != 0 {
		t.Fatalf("parallel schemas leaked data: right count = %d", rightCount)
	}

	leftConn.Close(ctx)
	if err := left.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	var rightStillInstalled bool
	if err := rightConn.QueryRow(ctx, "SELECT to_regclass('headgate_job') IS NOT NULL").Scan(&rightStillInstalled); err != nil {
		t.Fatal(err)
	}
	if !rightStillInstalled {
		t.Fatal("cleaning one test schema removed its sibling")
	}
}
