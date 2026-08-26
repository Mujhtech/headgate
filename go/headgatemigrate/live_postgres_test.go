package headgatemigrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestLivePostgresMigrationLifecycleAndDriftRejection(t *testing.T) {
	conninfo := os.Getenv("HG_TEST_PG")
	if conninfo == "" {
		t.Skip("HG_TEST_PG not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, conninfo)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("hg_migrate_go_%d", os.Getpid())
	var exists int
	if err := admin.QueryRow(ctx, `
SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1`, schema).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatalf("refusing to reuse migrator test schema %s", schema)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	}()

	conn, err := pgx.Connect(ctx, conninfo)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET search_path TO "+schema); err != nil {
		t.Fatal(err)
	}

	result, err := MigratePostgres(ctx, conn, Up, Options{})
	if err != nil || len(result.Steps) != 11 || result.Steps[0].Migration.Version != 1 || result.Steps[10].Migration.Version != 11 {
		t.Fatalf("fresh up = %#v, %v", result, err)
	}
	validation, err := ValidatePostgres(ctx, conn)
	if err != nil || !validation.OK() || validation.CurrentVersion != 11 {
		t.Fatalf("fresh validation = %#v, %v", validation, err)
	}
	dry, err := MigratePostgres(ctx, conn, Down, Options{DryRun: true})
	if err != nil || !dry.DryRun || len(dry.Steps) != 11 {
		t.Fatalf("down dry-run = %#v, %v", dry, err)
	}
	downResult, err := MigratePostgres(ctx, conn, Down, Options{})
	if err != nil || len(downResult.Steps) != 11 {
		t.Fatalf("down = %#v, %v", downResult, err)
	}
	var jobExists bool
	var historyRows int
	if err := conn.QueryRow(ctx, `
SELECT to_regclass('headgate_job') IS NOT NULL,
       (SELECT count(*) FROM headgate_schema_migration)`).Scan(&jobExists, &historyRows); err != nil {
		t.Fatal(err)
	}
	if jobExists || historyRows != 0 {
		t.Fatalf("down left job=%t history=%d", jobExists, historyRows)
	}

	if _, err := MigratePostgres(ctx, conn, Up, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
UPDATE headgate_schema_migration SET checksum = 'tampered' WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	validation, err = ValidatePostgres(ctx, conn)
	if err != nil || validation.OK() || !containsMessage(validation.Messages, "checksum") {
		t.Fatalf("tampered validation = %#v, %v", validation, err)
	}

	if _, err := conn.Exec(ctx, "DROP TABLE headgate_schema_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := MigratePostgres(ctx, conn, Up, Options{}); !errors.Is(err, ErrUnversionedSchema) {
		t.Fatalf("unversioned up error = %v", err)
	}
	adopted, err := AdoptPostgres(ctx, conn)
	if err != nil || len(adopted) != 11 || adopted[10].Version != 11 {
		t.Fatalf("adopted = %#v, %v", adopted, err)
	}
	validation, err = ValidatePostgres(ctx, conn)
	if err != nil || !validation.OK() {
		t.Fatalf("adopted validation = %#v, %v", validation, err)
	}
	if _, err := conn.Exec(ctx, "DROP TRIGGER headgate_enqueue_depth_delete ON headgate_job"); err != nil {
		t.Fatal(err)
	}
	validation, err = ValidatePostgres(ctx, conn)
	if err != nil || validation.OK() || !containsMessage(validation.Messages, "missing trigger headgate_enqueue_depth_delete") {
		t.Fatalf("missing backpressure trigger validation = %#v, %v", validation, err)
	}

	if _, err := conn.Exec(ctx, "DROP TABLE headgate_schema_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "ALTER TABLE headgate_queue_state DROP COLUMN dispatch_count"); err != nil {
		t.Fatal(err)
	}
	_, err = AdoptPostgres(ctx, conn)
	var schemaError *SchemaError
	if !errors.As(err, &schemaError) || !containsMessage(schemaError.Messages, "headgate_queue_state.dispatch_count") {
		t.Fatalf("drifted adoption error = %T %v", err, err)
	}
}

func containsMessage(messages []string, substring string) bool {
	for _, message := range messages {
		if strings.Contains(message, substring) {
			return true
		}
	}
	return false
}
