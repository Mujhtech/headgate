package main

import (
	"strings"
	"testing"

	"github.com/mujhtech/headgate/go/headgatemigrate"
)

func noenv(string) string { return "" }

func TestDestructiveCommandsRequireConfirmation(t *testing.T) {
	_, err := parseCLI([]string{
		"--backend", "postgres", "--database-url", "postgres://localhost/x", "down",
	}, noenv)
	if err == nil || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("error = %v", err)
	}
	dry, err := parseCLI([]string{
		"--backend", "postgres", "--database-url", "postgres://localhost/x", "down", "--dry-run",
	}, noenv)
	if err != nil || !dry.options.DryRun {
		t.Fatalf("dry = %#v, %v", dry, err)
	}
}

func TestGetNeedsOnlyBackend(t *testing.T) {
	config, err := parseCLI([]string{
		"--backend", "mysql", "get", "--version", "1", "--down",
	}, noenv)
	if err != nil {
		t.Fatal(err)
	}
	if config.backend != headgatemigrate.MySQL || config.getDir != headgatemigrate.Down || config.databaseURL != "" {
		t.Fatalf("config = %#v", config)
	}
}

func TestPostgresSchemaIsValidatedAndMySQLRejectsIt(t *testing.T) {
	config, err := parseCLI([]string{
		"--backend", "postgres", "--schema", `tenant-"blue`,
		"get", "--version", "1",
	}, noenv)
	if err != nil {
		t.Fatal(err)
	}
	if config.schema != `tenant-"blue` {
		t.Fatalf("schema = %q", config.schema)
	}
	_, err = parseCLI([]string{
		"--backend", "mysql", "--schema", "tenant", "get", "--version", "1",
	}, noenv)
	if err == nil || !strings.Contains(err.Error(), "Postgres-only") {
		t.Fatalf("error = %v", err)
	}
	_, err = parseCLI([]string{
		"--backend", "postgres", "--schema", strings.Repeat("x", 64),
		"get", "--version", "1",
	}, noenv)
	if err == nil || !strings.Contains(err.Error(), "63") {
		t.Fatalf("error = %v", err)
	}
}

func TestMySQLLockNamespaceIsScopedValidatedAndCommandSpecific(t *testing.T) {
	config, err := parseCLI([]string{
		"--backend", "mysql", "--database-url", "mysql://localhost/jobs",
		"up", "--lock-namespace", "billing.v2",
	}, noenv)
	if err != nil {
		t.Fatal(err)
	}
	if config.lockNS != "billing.v2" {
		t.Fatalf("lock namespace = %q", config.lockNS)
	}

	_, err = parseCLI([]string{
		"--backend", "postgres", "--database-url", "postgres://localhost/jobs",
		"up", "--lock-namespace", "billing",
	}, noenv)
	if err == nil || !strings.Contains(err.Error(), "MySQL-only") {
		t.Fatalf("postgres error = %v", err)
	}

	_, err = parseCLI([]string{
		"--backend", "mysql", "get", "--version", "1",
		"--lock-namespace", "billing",
	}, noenv)
	if err == nil || !strings.Contains(err.Error(), "up, down, and adopt") {
		t.Fatalf("read-only error = %v", err)
	}

	_, err = parseCLI([]string{
		"--backend", "mysql", "--database-url", "mysql://localhost/jobs",
		"up", "--lock-namespace", "bad:scope",
	}, noenv)
	if err == nil || !strings.Contains(err.Error(), "1-31 ASCII bytes") {
		t.Fatalf("invalid error = %v", err)
	}
}

func TestMySQLURLConversion(t *testing.T) {
	dsn, err := mysqlDSN("mysql://root:hg@127.0.0.1:3307/example")
	if err != nil {
		t.Fatal(err)
	}
	if dsn != "root:hg@tcp(127.0.0.1:3307)/example?parseTime=true" {
		t.Fatalf("dsn = %q", dsn)
	}
}
