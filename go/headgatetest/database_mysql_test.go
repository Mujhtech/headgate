package headgatetest

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestMySQLTestDatabasesMigrateIsolateParallelTestsAndCleanup(t *testing.T) {
	url := os.Getenv("HG_TEST_MYSQL")
	if url == "" {
		t.Skip("HG_TEST_MYSQL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	type result struct {
		database *MySQLTestDatabase
		err      error
	}
	created := make(chan result, 2)
	for range 2 {
		go func() {
			database, err := CreateMySQLTestDatabase(ctx, url)
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
	if left.Database == right.Database {
		t.Fatalf("parallel helpers reused database %q", left.Database)
	}

	leftDB, err := left.Open()
	if err != nil {
		t.Fatal(err)
	}
	rightDB, err := right.Open()
	if err != nil {
		_ = leftDB.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := rightDB.Close(); err != nil {
			t.Errorf("close right database: %v", err)
		}
	}()
	var leftInstalled, rightInstalled int
	const installedSQL = `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = 'headgate_job'`
	if err := leftDB.QueryRowContext(ctx, installedSQL).Scan(&leftInstalled); err != nil {
		t.Fatalf("left migration probe: %v", err)
	}
	if err := rightDB.QueryRowContext(ctx, installedSQL).Scan(&rightInstalled); err != nil {
		t.Fatalf("right migration probe: %v", err)
	}
	if leftInstalled != 1 || rightInstalled != 1 {
		t.Fatalf("databases were not migrated: left=%d right=%d", leftInstalled, rightInstalled)
	}
	if _, err := leftDB.ExecContext(ctx, "INSERT INTO headgate_queue_state(queue) VALUES ('only-left')"); err != nil {
		t.Fatal(err)
	}
	var rightCount int
	if err := rightDB.QueryRowContext(ctx, "SELECT count(*) FROM headgate_queue_state WHERE queue = 'only-left'").Scan(&rightCount); err != nil {
		t.Fatal(err)
	}
	if rightCount != 0 {
		t.Fatalf("parallel databases leaked data: right count = %d", rightCount)
	}

	if err := leftDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := left.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	var rightStillInstalled int
	if err := rightDB.QueryRowContext(ctx, installedSQL).Scan(&rightStillInstalled); err != nil {
		t.Fatal(err)
	}
	if rightStillInstalled != 1 {
		t.Fatal("cleaning one test database removed its sibling")
	}
}
