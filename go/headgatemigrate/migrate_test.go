package headgatemigrate

import (
	"errors"
	"strings"
	"testing"
)

func applied(backend Backend, version int) AppliedMigration {
	migration, _ := GetMigration(backend, version)
	return AppliedMigration{Version: version, Name: migration.Name, Checksum: Checksum(migration), AppliedAtMS: 1}
}

func TestPlanUpDownAndCurrentNoop(t *testing.T) {
	steps, err := Plan(Postgres, nil, Up, Options{})
	if err != nil || len(steps) != 12 || steps[0].Migration.Version != 1 || steps[11].Migration.Version != 12 {
		t.Fatalf("up plan = %#v, %v", steps, err)
	}
	current := []AppliedMigration{applied(Postgres, 1), applied(Postgres, 2), applied(Postgres, 3), applied(Postgres, 4), applied(Postgres, 5), applied(Postgres, 6), applied(Postgres, 7), applied(Postgres, 8), applied(Postgres, 9), applied(Postgres, 10), applied(Postgres, 11), applied(Postgres, 12)}
	steps, err = Plan(Postgres, current, Up, Options{})
	if err != nil || len(steps) != 0 {
		t.Fatalf("current plan = %#v, %v", steps, err)
	}
	steps, err = Plan(Postgres, current, Down, Options{})
	if err != nil || len(steps) != 12 || steps[0].Direction != Down || steps[0].Migration.Version != 12 {
		t.Fatalf("down plan = %#v, %v", steps, err)
	}
}

func TestChecksumAndHistoryGapFailPlanning(t *testing.T) {
	bad := applied(Postgres, 1)
	bad.Checksum = "tampered"
	if _, err := Plan(Postgres, []AppliedMigration{bad}, Up, Options{DryRun: true}); err == nil {
		t.Fatal("tampered checksum planned successfully")
	} else {
		var history *HistoryError
		if !errors.As(err, &history) {
			t.Fatalf("error = %T %v", err, err)
		}
	}
	future := AppliedMigration{Version: 12, Name: "future", Checksum: "x", AppliedAtMS: 1}
	if err := ValidateHistory(Postgres, []AppliedMigration{future}); err == nil {
		t.Fatal("history gap accepted")
	}
}

func TestTargetsAndMaxStepsAreBounded(t *testing.T) {
	thirteen := 13
	if _, err := Plan(Postgres, nil, Up, Options{TargetVersion: &thirteen}); err == nil {
		t.Fatal("future target accepted")
	}
	zero := 0
	steps, err := Plan(Postgres, nil, Up, Options{MaxSteps: &zero})
	if err != nil || len(steps) != 0 {
		t.Fatalf("max zero = %#v, %v", steps, err)
	}
}

func TestMySQLLockNamesPreserveDefaultAndSeparateNamespaces(t *testing.T) {
	name, err := MySQLMigrationLockName(DefaultMySQLLockNamespace, "jobs")
	if err != nil || name != "headgate:migrate:jobs" {
		t.Fatalf("default name = %q, %v", name, err)
	}
	billing, err := MySQLMigrationLockName("billing", "jobs")
	if err != nil {
		t.Fatal(err)
	}
	email, err := MySQLMigrationLockName("email", "jobs")
	if err != nil || billing == email {
		t.Fatalf("names = %q, %q, %v", billing, email, err)
	}
	long, err := MySQLMigrationLockName("billing", strings.Repeat("x", 64))
	if err != nil || !strings.HasPrefix(long, "billing:h:") || len(long) > 64 {
		t.Fatalf("long lock = %q, %v", long, err)
	}
	for _, invalid := range []string{"", "-bad", "bad:scope", "white space"} {
		if _, err := MySQLMigrationLockName(invalid, "jobs"); err == nil {
			t.Fatalf("namespace %q accepted", invalid)
		}
	}
}
