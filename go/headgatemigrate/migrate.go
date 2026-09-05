// Package headgatemigrate provides versioned, embedded schema migrations for
// headgate's Postgres and MySQL stores.
//
// Historical migration checksums are immutable. Postgres applies each version and its
// history row transactionally. MySQL DDL auto-commits, so each statement is resumable,
// a connection-scoped lock serializes callers, and history is recorded only after the
// resulting schema passes the current manifest.
package headgatemigrate

import (
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
)

type Backend string

const (
	Postgres Backend = "postgres"
	MySQL    Backend = "mysql"
)

type Direction string

const (
	Up   Direction = "up"
	Down Direction = "down"
)

type Migration struct {
	Version    int
	Name       string
	UpSQL      string
	DownSQL    string
	OnlineSafe bool
}

//go:embed migrations/postgres/0001_init.up.sql
var postgresInitialUp string

//go:embed migrations/postgres/0001_init.down.sql
var postgresInitialDown string

//go:embed migrations/postgres/0002_enqueue_backpressure.up.sql
var postgresBackpressureUp string

//go:embed migrations/postgres/0002_enqueue_backpressure.down.sql
var postgresBackpressureDown string

//go:embed migrations/postgres/0003_job_results.up.sql
var postgresResultsUp string

//go:embed migrations/postgres/0003_job_results.down.sql
var postgresResultsDown string

//go:embed migrations/postgres/0004_mid_run_output.up.sql
var postgresOutputUp string

//go:embed migrations/postgres/0004_mid_run_output.down.sql
var postgresOutputDown string

//go:embed migrations/postgres/0005_job_progress.up.sql
var postgresProgressUp string

//go:embed migrations/postgres/0005_job_progress.down.sql
var postgresProgressDown string

//go:embed migrations/postgres/0006_periodic_origin.up.sql
var postgresPeriodicOriginUp string

//go:embed migrations/postgres/0006_periodic_origin.down.sql
var postgresPeriodicOriginDown string

//go:embed migrations/postgres/0007_scheduler_events.up.sql
var postgresSchedulerEventsUp string

//go:embed migrations/postgres/0007_scheduler_events.down.sql
var postgresSchedulerEventsDown string

//go:embed migrations/postgres/0008_pending_tags_metrics.up.sql
var postgresPendingTagsMetricsUp string

//go:embed migrations/postgres/0008_pending_tags_metrics.down.sql
var postgresPendingTagsMetricsDown string

//go:embed migrations/postgres/0009_pending_tags_metrics.up.sql
var postgresPendingTagsMetricsV9Up string

//go:embed migrations/postgres/0009_pending_tags_metrics.down.sql
var postgresPendingTagsMetricsV9Down string

//go:embed migrations/postgres/0010_sticky_routing.up.sql
var postgresStickyRoutingUp string

//go:embed migrations/postgres/0010_sticky_routing.down.sql
var postgresStickyRoutingDown string

//go:embed migrations/postgres/0011_partitioned_archive.up.sql
var postgresPartitionedArchiveUp string

//go:embed migrations/postgres/0011_partitioned_archive.down.sql
var postgresPartitionedArchiveDown string

//go:embed migrations/postgres/0012_worker_control_state.up.sql
var postgresWorkerControlStateUp string

//go:embed migrations/postgres/0012_worker_control_state.down.sql
var postgresWorkerControlStateDown string

//go:embed migrations/postgres/0013_durable_events.up.sql
var postgresDurableEventsUp string

//go:embed migrations/postgres/0013_durable_events.down.sql
var postgresDurableEventsDown string

//go:embed migrations/mysql/0001_init.up.sql
var mysqlInitialUp string

//go:embed migrations/mysql/0001_init.down.sql
var mysqlInitialDown string

//go:embed migrations/mysql/0002_enqueue_backpressure.up.sql
var mysqlBackpressureUp string

//go:embed migrations/mysql/0002_enqueue_backpressure.down.sql
var mysqlBackpressureDown string

//go:embed migrations/mysql/0003_job_results.up.sql
var mysqlResultsUp string

//go:embed migrations/mysql/0003_job_results.down.sql
var mysqlResultsDown string

//go:embed migrations/mysql/0004_mid_run_output.up.sql
var mysqlOutputUp string

//go:embed migrations/mysql/0004_mid_run_output.down.sql
var mysqlOutputDown string

//go:embed migrations/mysql/0005_job_progress.up.sql
var mysqlProgressUp string

//go:embed migrations/mysql/0005_job_progress.down.sql
var mysqlProgressDown string

//go:embed migrations/mysql/0006_periodic_origin.up.sql
var mysqlPeriodicOriginUp string

//go:embed migrations/mysql/0006_periodic_origin.down.sql
var mysqlPeriodicOriginDown string

//go:embed migrations/mysql/0007_scheduler_events.up.sql
var mysqlSchedulerEventsUp string

//go:embed migrations/mysql/0007_scheduler_events.down.sql
var mysqlSchedulerEventsDown string

//go:embed migrations/mysql/0008_pending_tags_metrics.up.sql
var mysqlPendingTagsMetricsUp string

//go:embed migrations/mysql/0008_pending_tags_metrics.down.sql
var mysqlPendingTagsMetricsDown string

//go:embed migrations/mysql/0009_pending_tags_metrics.up.sql
var mysqlPendingTagsMetricsV9Up string

//go:embed migrations/mysql/0009_pending_tags_metrics.down.sql
var mysqlPendingTagsMetricsV9Down string

//go:embed migrations/mysql/0010_sticky_routing.up.sql
var mysqlStickyRoutingUp string

//go:embed migrations/mysql/0010_sticky_routing.down.sql
var mysqlStickyRoutingDown string

//go:embed migrations/mysql/0011_partitioned_archive.up.sql
var mysqlPartitionedArchiveUp string

//go:embed migrations/mysql/0011_partitioned_archive.down.sql
var mysqlPartitionedArchiveDown string

//go:embed migrations/mysql/0012_worker_control_state.up.sql
var mysqlWorkerControlStateUp string

//go:embed migrations/mysql/0012_worker_control_state.down.sql
var mysqlWorkerControlStateDown string

//go:embed migrations/mysql/0013_durable_events.up.sql
var mysqlDurableEventsUp string

//go:embed migrations/mysql/0013_durable_events.down.sql
var mysqlDurableEventsDown string

var byBackend = map[Backend][]Migration{
	Postgres: {
		{Version: 1, Name: "initial_schema", UpSQL: postgresInitialUp, DownSQL: postgresInitialDown, OnlineSafe: false},
		{Version: 2, Name: "enqueue_backpressure", UpSQL: postgresBackpressureUp, DownSQL: postgresBackpressureDown, OnlineSafe: false},
		{Version: 3, Name: "job_results", UpSQL: postgresResultsUp, DownSQL: postgresResultsDown, OnlineSafe: true},
		{Version: 4, Name: "mid_run_output", UpSQL: postgresOutputUp, DownSQL: postgresOutputDown, OnlineSafe: true},
		{Version: 5, Name: "job_progress", UpSQL: postgresProgressUp, DownSQL: postgresProgressDown, OnlineSafe: true},
		{Version: 6, Name: "periodic_origin", UpSQL: postgresPeriodicOriginUp, DownSQL: postgresPeriodicOriginDown, OnlineSafe: true},
		{Version: 7, Name: "scheduler_events", UpSQL: postgresSchedulerEventsUp, DownSQL: postgresSchedulerEventsDown, OnlineSafe: true},
		{Version: 8, Name: "pending_state", UpSQL: postgresPendingTagsMetricsUp, DownSQL: postgresPendingTagsMetricsDown, OnlineSafe: false},
		{Version: 9, Name: "pending_tags_metrics", UpSQL: postgresPendingTagsMetricsV9Up, DownSQL: postgresPendingTagsMetricsV9Down, OnlineSafe: false},
		{Version: 10, Name: "sticky_routing", UpSQL: postgresStickyRoutingUp, DownSQL: postgresStickyRoutingDown, OnlineSafe: false},
		{Version: 11, Name: "partitioned_archive", UpSQL: postgresPartitionedArchiveUp, DownSQL: postgresPartitionedArchiveDown, OnlineSafe: true},
		{Version: 12, Name: "worker_control_state", UpSQL: postgresWorkerControlStateUp, DownSQL: postgresWorkerControlStateDown, OnlineSafe: true},
		{Version: 13, Name: "durable_events", UpSQL: postgresDurableEventsUp, DownSQL: postgresDurableEventsDown, OnlineSafe: true},
	},
	MySQL: {
		{Version: 1, Name: "initial_schema", UpSQL: mysqlInitialUp, DownSQL: mysqlInitialDown, OnlineSafe: false},
		{Version: 2, Name: "enqueue_backpressure", UpSQL: mysqlBackpressureUp, DownSQL: mysqlBackpressureDown, OnlineSafe: false},
		{Version: 3, Name: "job_results", UpSQL: mysqlResultsUp, DownSQL: mysqlResultsDown, OnlineSafe: true},
		{Version: 4, Name: "mid_run_output", UpSQL: mysqlOutputUp, DownSQL: mysqlOutputDown, OnlineSafe: true},
		{Version: 5, Name: "job_progress", UpSQL: mysqlProgressUp, DownSQL: mysqlProgressDown, OnlineSafe: true},
		{Version: 6, Name: "periodic_origin", UpSQL: mysqlPeriodicOriginUp, DownSQL: mysqlPeriodicOriginDown, OnlineSafe: true},
		{Version: 7, Name: "scheduler_events", UpSQL: mysqlSchedulerEventsUp, DownSQL: mysqlSchedulerEventsDown, OnlineSafe: true},
		{Version: 8, Name: "pending_state_barrier", UpSQL: mysqlPendingTagsMetricsUp, DownSQL: mysqlPendingTagsMetricsDown, OnlineSafe: false},
		{Version: 9, Name: "pending_tags_metrics", UpSQL: mysqlPendingTagsMetricsV9Up, DownSQL: mysqlPendingTagsMetricsV9Down, OnlineSafe: false},
		{Version: 10, Name: "sticky_routing", UpSQL: mysqlStickyRoutingUp, DownSQL: mysqlStickyRoutingDown, OnlineSafe: false},
		{Version: 11, Name: "partitioned_archive", UpSQL: mysqlPartitionedArchiveUp, DownSQL: mysqlPartitionedArchiveDown, OnlineSafe: false},
		{Version: 12, Name: "worker_control_state", UpSQL: mysqlWorkerControlStateUp, DownSQL: mysqlWorkerControlStateDown, OnlineSafe: false},
		{Version: 13, Name: "durable_events", UpSQL: mysqlDurableEventsUp, DownSQL: mysqlDurableEventsDown, OnlineSafe: true},
	},
}

func Migrations(backend Backend) []Migration {
	source := byBackend[backend]
	result := make([]Migration, len(source))
	copy(result, source)
	return result
}

func GetMigration(backend Backend, version int) (Migration, bool) {
	for _, migration := range byBackend[backend] {
		if migration.Version == version {
			return migration, true
		}
	}
	return Migration{}, false
}

func LatestVersion(backend Backend) int {
	all := byBackend[backend]
	if len(all) == 0 {
		return 0
	}
	return all[len(all)-1].Version
}

func Checksum(migration Migration) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(migration.UpSQL)))
}

type AppliedMigration struct {
	Version     int
	Name        string
	Checksum    string
	AppliedAtMS int64
}

type Options struct {
	TargetVersion *int
	MaxSteps      *int
	DryRun        bool
}

type Step struct {
	Direction Direction
	Migration Migration
}

type Result struct {
	DryRun bool
	Steps  []Step
}

type InstallationState string

const (
	Empty       InstallationState = "empty"
	Unversioned InstallationState = "unversioned"
	Versioned   InstallationState = "versioned"
)

var ErrUnversionedSchema = errors.New("headgate tables exist without migration history; validate and adopt the current schema before migrating")

type HistoryError struct{ Message string }

func (e *HistoryError) Error() string { return "invalid migration history: " + e.Message }

type SchemaError struct{ Messages []string }

func (e *SchemaError) Error() string {
	message := "schema validation failed"
	for i, item := range e.Messages {
		if i == 0 {
			message += ": " + item
		} else {
			message += "; " + item
		}
	}
	return message
}

func ValidateHistory(backend Backend, applied []AppliedMigration) error {
	known := byBackend[backend]
	for index, row := range applied {
		expectedVersion := index + 1
		if row.Version != expectedVersion {
			return &HistoryError{Message: fmt.Sprintf(
				"versions must be contiguous from 1; found %d where %d was expected",
				row.Version, expectedVersion,
			)}
		}
		if row.Version > len(known) {
			return &HistoryError{Message: fmt.Sprintf("database has unknown future version %d", row.Version)}
		}
		migration := known[row.Version-1]
		if row.Name != migration.Name {
			return &HistoryError{Message: fmt.Sprintf(
				"version %d is named %q, expected %q", row.Version, row.Name, migration.Name,
			)}
		}
		expectedChecksum := Checksum(migration)
		if row.Checksum != expectedChecksum {
			return &HistoryError{Message: fmt.Sprintf(
				"version %d checksum is %s, expected %s",
				row.Version, row.Checksum, expectedChecksum,
			)}
		}
	}
	return nil
}

func Plan(backend Backend, applied []AppliedMigration, direction Direction, options Options) ([]Step, error) {
	if backend != Postgres && backend != MySQL {
		return nil, fmt.Errorf("unknown migration backend %q", backend)
	}
	if direction != Up && direction != Down {
		return nil, fmt.Errorf("unknown migration direction %q", direction)
	}
	if err := ValidateHistory(backend, applied); err != nil {
		return nil, err
	}
	current := 0
	if len(applied) != 0 {
		current = applied[len(applied)-1].Version
	}
	latest := LatestVersion(backend)
	target := latest
	if direction == Down {
		target = 0
	}
	if options.TargetVersion != nil {
		target = *options.TargetVersion
	}
	if target < 0 || target > latest {
		return nil, fmt.Errorf("target version %d is outside 0..%d", target, latest)
	}
	if direction == Up && target < current {
		return nil, fmt.Errorf("target version %d is below current version %d; use down", target, current)
	}
	if direction == Down && target > current {
		return nil, fmt.Errorf("target version %d is above current version %d; use up", target, current)
	}

	steps := make([]Step, 0)
	all := byBackend[backend]
	if direction == Up {
		for _, migration := range all {
			if migration.Version > current && migration.Version <= target {
				steps = append(steps, Step{Direction: direction, Migration: migration})
			}
		}
	} else {
		for index := len(all) - 1; index >= 0; index-- {
			migration := all[index]
			if migration.Version > target && migration.Version <= current {
				steps = append(steps, Step{Direction: direction, Migration: migration})
			}
		}
	}
	if options.MaxSteps != nil {
		if *options.MaxSteps < 0 {
			return nil, errors.New("max steps must be non-negative")
		}
		if len(steps) > *options.MaxSteps {
			steps = steps[:*options.MaxSteps]
		}
	}
	return steps, nil
}
