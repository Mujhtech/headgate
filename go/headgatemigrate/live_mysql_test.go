package headgatemigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func testMySQLDSN(value, database string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", err
	}
	password, _ := parsed.User.Password()
	if database == "" {
		database = strings.TrimPrefix(parsed.Path, "/")
	}
	if parsed.Scheme != "mysql" || parsed.User.Username() == "" || parsed.Host == "" || database == "" {
		return "", fmt.Errorf("HG_TEST_MYSQL must be mysql://user:pass@host/database")
	}
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
		parsed.User.Username(), password, parsed.Host, database), nil
}

func TestLiveMySQLMigrationLifecycleAndDriftRejection(t *testing.T) {
	testURL := os.Getenv("HG_TEST_MYSQL")
	if testURL == "" {
		t.Skip("HG_TEST_MYSQL not set")
	}
	ctx := context.Background()
	adminDSN, err := testMySQLDSN(testURL, "")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	database := fmt.Sprintf("hg_migrate_go_%d", os.Getpid())
	var exists int
	if err := admin.QueryRowContext(ctx, `
SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?`, database).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatalf("refusing to reuse migrator test database %s", database)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+database); err != nil {
			t.Errorf("drop database: %v", err)
		}
	}()

	testDSN, err := testMySQLDSN(testURL, database)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := MigrateMySQL(ctx, db, Up, Options{})
	if err != nil || len(result.Steps) != 12 || result.Steps[0].Migration.Version != 1 || result.Steps[11].Migration.Version != 12 {
		t.Fatalf("fresh up = %#v, %v", result, err)
	}
	validation, err := ValidateMySQL(ctx, db)
	if err != nil || !validation.OK() || validation.CurrentVersion != 12 {
		t.Fatalf("fresh validation = %#v, %v", validation, err)
	}
	dry, err := MigrateMySQL(ctx, db, Down, Options{DryRun: true})
	if err != nil || !dry.DryRun || len(dry.Steps) != 12 {
		t.Fatalf("down dry-run = %#v, %v", dry, err)
	}
	downResult, err := MigrateMySQL(ctx, db, Down, Options{})
	if err != nil || len(downResult.Steps) != 12 {
		t.Fatalf("down = %#v, %v", downResult, err)
	}
	var jobExists, historyRows int
	if err := db.QueryRowContext(ctx, `
SELECT count(*) FROM information_schema.tables
 WHERE table_schema = DATABASE() AND table_name = 'headgate_job'`).Scan(&jobExists); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM headgate_schema_migration").Scan(&historyRows); err != nil {
		t.Fatal(err)
	}
	if jobExists != 0 || historyRows != 0 {
		t.Fatalf("down left job=%d history=%d", jobExists, historyRows)
	}

	if _, err := MigrateMySQL(ctx, db, Up, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
UPDATE headgate_schema_migration SET checksum = 'tampered' WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	validation, err = ValidateMySQL(ctx, db)
	if err != nil || validation.OK() || !containsMessage(validation.Messages, "checksum") {
		t.Fatalf("tampered validation = %#v, %v", validation, err)
	}

	if _, err := db.ExecContext(ctx, "DROP TABLE headgate_schema_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := MigrateMySQL(ctx, db, Up, Options{}); !errors.Is(err, ErrUnversionedSchema) {
		t.Fatalf("unversioned up error = %v", err)
	}
	adopted, err := AdoptMySQL(ctx, db)
	if err != nil || len(adopted) != 12 || adopted[11].Version != 12 {
		t.Fatalf("adopted = %#v, %v", adopted, err)
	}
	validation, err = ValidateMySQL(ctx, db)
	if err != nil || !validation.OK() {
		t.Fatalf("adopted validation = %#v, %v", validation, err)
	}
	if _, err := db.ExecContext(ctx, "DROP TRIGGER headgate_enqueue_depth_delete"); err != nil {
		t.Fatal(err)
	}
	validation, err = ValidateMySQL(ctx, db)
	if err != nil || validation.OK() || !containsMessage(validation.Messages, "missing trigger headgate_enqueue_depth_delete") {
		t.Fatalf("missing backpressure trigger validation = %#v, %v", validation, err)
	}

	if _, err := db.ExecContext(ctx, "DROP TABLE headgate_schema_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE headgate_queue_state DROP COLUMN dispatch_count"); err != nil {
		t.Fatal(err)
	}
	_, err = AdoptMySQL(ctx, db)
	var schemaError *SchemaError
	if !errors.As(err, &schemaError) || !containsMessage(schemaError.Messages, "headgate_queue_state.dispatch_count") {
		t.Fatalf("drifted adoption error = %T %v", err, err)
	}
}

func TestLiveMySQLConfiguredLockNamespaceAvoidsAnApplicationLock(t *testing.T) {
	testURL := os.Getenv("HG_TEST_MYSQL")
	if testURL == "" {
		t.Skip("HG_TEST_MYSQL not set")
	}
	ctx := context.Background()
	adminDSN, err := testMySQLDSN(testURL, "")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	database := fmt.Sprintf("hg_lock_go_%d", os.Getpid())
	var exists int
	if err := admin.QueryRowContext(ctx, `
SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?`, database).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatalf("refusing to reuse lock test database %s", database)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+database); err != nil {
			t.Errorf("drop database: %v", err)
		}
	}()

	applicationConn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applicationConn.Close() }()
	releaseAll := func() {
		var released int
		if err := applicationConn.QueryRowContext(ctx, "SELECT RELEASE_ALL_LOCKS()").Scan(&released); err != nil {
			t.Errorf("release fixture locks: %v", err)
		}
	}
	defer releaseAll()

	applicationLock, err := MySQLMigrationLockName(DefaultMySQLLockNamespace, database)
	if err != nil {
		t.Fatal(err)
	}
	configuredLock, err := MySQLMigrationLockName("billing", database)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{applicationLock, configuredLock} {
		var acquired sql.NullInt64
		if err := applicationConn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&acquired); err != nil {
			t.Fatal(err)
		}
		if !acquired.Valid || acquired.Int64 != 1 {
			t.Fatalf("fixture lock %q acquired = %#v", name, acquired)
		}
	}

	testDSN, err := testMySQLDSN(testURL, database)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		result Result
		err    error
	}
	done := make(chan outcome, 1)
	migrationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go func() {
		result, err := MigrateMySQLWithLockNamespace(
			migrationCtx, db, Up, Options{}, "billing",
		)
		done <- outcome{result: result, err: err}
	}()
	select {
	case early := <-done:
		t.Fatalf("migration finished while its configured lock was held: %#v, %v", early.result, early.err)
	case <-time.After(200 * time.Millisecond):
	}

	var released sql.NullInt64
	if err := applicationConn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", configuredLock).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if !released.Valid || released.Int64 != 1 {
		t.Fatalf("configured fixture lock release = %#v", released)
	}
	select {
	case migrated := <-done:
		if migrated.err != nil || len(migrated.result.Steps) != 12 {
			t.Fatalf("configured migration = %#v, %v", migrated.result, migrated.err)
		}
	case <-time.After(20 * time.Second):
		releaseAll()
		cancel()
		t.Fatal("migration stayed blocked after configured lock release")
	}

	var applicationFree sql.NullInt64
	if err := applicationConn.QueryRowContext(ctx, "SELECT IS_FREE_LOCK(?)", applicationLock).Scan(&applicationFree); err != nil {
		t.Fatal(err)
	}
	if !applicationFree.Valid || applicationFree.Int64 != 0 {
		t.Fatalf("application lock was not held through migration: %#v", applicationFree)
	}
}
