//! Versioned, embedded schema migrations for headgate's SQL backends.
//!
//! The migration history is data, not a guess made from whichever columns happen to
//! exist. Every applied version records the SHA-256 of its immutable up SQL. A changed
//! historical migration therefore fails validation instead of silently turning two
//! installations at "version 1" into different schemas.
//!
//! Postgres applies each version and its history row in one transaction. MySQL DDL
//! commits implicitly, so its migrations must be resumable: a connection-scoped lock
//! serializes migrators, every statement in an up migration is idempotent, and the
//! version row is written only after the resulting schema passes the current manifest.

use std::fmt;

use sha2::{Digest, Sha256};

pub use headgate_sql::PostgresNamespace;

mod mysql;
mod postgres;
mod schema;

pub use mysql::{
    DEFAULT_MYSQL_LOCK_NAMESPACE, MysqlValidation, adopt_mysql, adopt_mysql_with_lock_namespace,
    applied_mysql, migrate_mysql, migrate_mysql_with_lock_namespace, mysql_migration_lock_name,
    validate_mysql,
};
pub use postgres::{
    PostgresValidation, adopt_postgres, adopt_postgres_in_schema, applied_postgres,
    applied_postgres_in_schema, migrate_postgres, migrate_postgres_in_schema, validate_postgres,
    validate_postgres_in_schema,
};

/// The two stores with durable schemas. Redis key layouts are versioned by code and Lua,
/// not by a DDL migrator, so claiming a Redis migration backend would be dishonest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    Postgres,
    Mysql,
}

impl Backend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Up,
    Down,
}

impl Direction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A checked-in migration. Existing versions are immutable: add a new version instead
/// of editing an applied one, even when the edit appears additive.
#[derive(Clone, Copy, Debug)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub up_sql: &'static str,
    pub down_sql: &'static str,
    /// Whether the UP direction may run while workers and clients continue using the
    /// schema. DOWN is always operator-confirmed and offline.
    pub online_safe: bool,
}

const POSTGRES_MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial_schema",
        up_sql: include_str!("../migrations/postgres/0001_init.up.sql"),
        down_sql: include_str!("../migrations/postgres/0001_init.down.sql"),
        // This is a fresh install containing non-concurrent index creation. There is no
        // prior application traffic it needs to preserve, so call it offline explicitly.
        online_safe: false,
    },
    Migration {
        version: 2,
        name: "enqueue_backpressure",
        up_sql: include_str!("../migrations/postgres/0002_enqueue_backpressure.up.sql"),
        down_sql: include_str!("../migrations/postgres/0002_enqueue_backpressure.down.sql"),
        // Baseline + trigger installation is one cut-over; stop producers first.
        online_safe: false,
    },
    Migration {
        version: 3,
        name: "job_results",
        up_sql: include_str!("../migrations/postgres/0003_job_results.up.sql"),
        down_sql: include_str!("../migrations/postgres/0003_job_results.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 4,
        name: "mid_run_output",
        up_sql: include_str!("../migrations/postgres/0004_mid_run_output.up.sql"),
        down_sql: include_str!("../migrations/postgres/0004_mid_run_output.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 5,
        name: "job_progress",
        up_sql: include_str!("../migrations/postgres/0005_job_progress.up.sql"),
        down_sql: include_str!("../migrations/postgres/0005_job_progress.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 6,
        name: "periodic_origin",
        up_sql: include_str!("../migrations/postgres/0006_periodic_origin.up.sql"),
        down_sql: include_str!("../migrations/postgres/0006_periodic_origin.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 7,
        name: "scheduler_events",
        up_sql: include_str!("../migrations/postgres/0007_scheduler_events.up.sql"),
        down_sql: include_str!("../migrations/postgres/0007_scheduler_events.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 8,
        name: "pending_state",
        up_sql: include_str!("../migrations/postgres/0008_pending_tags_metrics.up.sql"),
        down_sql: include_str!("../migrations/postgres/0008_pending_tags_metrics.down.sql"),
        // Forward-only enum extension: schedule an operator-reviewed maintenance window.
        online_safe: false,
    },
    Migration {
        version: 9,
        name: "pending_tags_metrics",
        up_sql: include_str!("../migrations/postgres/0009_pending_tags_metrics.up.sql"),
        down_sql: include_str!("../migrations/postgres/0009_pending_tags_metrics.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 10,
        name: "sticky_routing",
        up_sql: include_str!("../migrations/postgres/0010_sticky_routing.up.sql"),
        down_sql: include_str!("../migrations/postgres/0010_sticky_routing.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 11,
        name: "partitioned_archive",
        up_sql: include_str!("../migrations/postgres/0011_partitioned_archive.up.sql"),
        down_sql: include_str!("../migrations/postgres/0011_partitioned_archive.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 12,
        name: "worker_control_state",
        up_sql: include_str!("../migrations/postgres/0012_worker_control_state.up.sql"),
        down_sql: include_str!("../migrations/postgres/0012_worker_control_state.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 13,
        name: "durable_events",
        up_sql: include_str!("../migrations/postgres/0013_durable_events.up.sql"),
        down_sql: include_str!("../migrations/postgres/0013_durable_events.down.sql"),
        online_safe: true,
    },
];

const MYSQL_MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial_schema",
        up_sql: include_str!("../migrations/mysql/0001_init.up.sql"),
        down_sql: include_str!("../migrations/mysql/0001_init.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 2,
        name: "enqueue_backpressure",
        up_sql: include_str!("../migrations/mysql/0002_enqueue_backpressure.up.sql"),
        down_sql: include_str!("../migrations/mysql/0002_enqueue_backpressure.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 3,
        name: "job_results",
        up_sql: include_str!("../migrations/mysql/0003_job_results.up.sql"),
        down_sql: include_str!("../migrations/mysql/0003_job_results.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 4,
        name: "mid_run_output",
        up_sql: include_str!("../migrations/mysql/0004_mid_run_output.up.sql"),
        down_sql: include_str!("../migrations/mysql/0004_mid_run_output.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 5,
        name: "job_progress",
        up_sql: include_str!("../migrations/mysql/0005_job_progress.up.sql"),
        down_sql: include_str!("../migrations/mysql/0005_job_progress.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 6,
        name: "periodic_origin",
        up_sql: include_str!("../migrations/mysql/0006_periodic_origin.up.sql"),
        down_sql: include_str!("../migrations/mysql/0006_periodic_origin.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 7,
        name: "scheduler_events",
        up_sql: include_str!("../migrations/mysql/0007_scheduler_events.up.sql"),
        down_sql: include_str!("../migrations/mysql/0007_scheduler_events.down.sql"),
        online_safe: true,
    },
    Migration {
        version: 8,
        name: "pending_state_barrier",
        up_sql: include_str!("../migrations/mysql/0008_pending_tags_metrics.up.sql"),
        down_sql: include_str!("../migrations/mysql/0008_pending_tags_metrics.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 9,
        name: "pending_tags_metrics",
        up_sql: include_str!("../migrations/mysql/0009_pending_tags_metrics.up.sql"),
        down_sql: include_str!("../migrations/mysql/0009_pending_tags_metrics.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 10,
        name: "sticky_routing",
        up_sql: include_str!("../migrations/mysql/0010_sticky_routing.up.sql"),
        down_sql: include_str!("../migrations/mysql/0010_sticky_routing.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 11,
        name: "partitioned_archive",
        up_sql: include_str!("../migrations/mysql/0011_partitioned_archive.up.sql"),
        down_sql: include_str!("../migrations/mysql/0011_partitioned_archive.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 12,
        name: "worker_control_state",
        up_sql: include_str!("../migrations/mysql/0012_worker_control_state.up.sql"),
        down_sql: include_str!("../migrations/mysql/0012_worker_control_state.down.sql"),
        online_safe: false,
    },
    Migration {
        version: 13,
        name: "durable_events",
        up_sql: include_str!("../migrations/mysql/0013_durable_events.up.sql"),
        down_sql: include_str!("../migrations/mysql/0013_durable_events.down.sql"),
        online_safe: true,
    },
];

pub const fn migrations(backend: Backend) -> &'static [Migration] {
    match backend {
        Backend::Postgres => POSTGRES_MIGRATIONS,
        Backend::Mysql => MYSQL_MIGRATIONS,
    }
}

pub fn migration(backend: Backend, version: u32) -> Option<&'static Migration> {
    migrations(backend).iter().find(|m| m.version == version)
}

pub fn latest_version(backend: Backend) -> u32 {
    migrations(backend).last().map_or(0, |m| m.version)
}

/// The checksum stored in `headgate_schema_migration`. It covers the UP SQL because that
/// is the schema an applied version claims was installed; changing DOWN SQL is caught by
/// source parity tests and review, while it cannot make an existing schema differ.
pub fn checksum(migration: &Migration) -> String {
    let digest = Sha256::digest(migration.up_sql.as_bytes());
    format!("{digest:x}")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedMigration {
    pub version: u32,
    pub name: String,
    pub checksum: String,
    pub applied_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MigrateOptions {
    /// Desired schema version after the call. `None` means latest for UP and zero for
    /// DOWN. Migrating to the current version is an idempotent no-op.
    pub target_version: Option<u32>,
    /// Bound work in one invocation. `None` means all required versions.
    pub max_steps: Option<usize>,
    /// Plan and return exact SQL without changing either schema or history.
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct MigrationStep {
    pub direction: Direction,
    pub migration: &'static Migration,
}

impl PartialEq for MigrationStep {
    fn eq(&self, other: &Self) -> bool {
        self.direction == other.direction && self.migration.version == other.migration.version
    }
}

impl Eq for MigrationStep {}

#[derive(Clone, Debug, Default)]
pub struct MigrateResult {
    pub dry_run: bool,
    pub steps: Vec<MigrationStep>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationState {
    Empty,
    Unversioned,
    Versioned,
}

#[derive(Debug)]
pub enum MigrationError {
    Invalid(String),
    UnversionedSchema,
    History(String),
    Schema(Vec<String>),
    Postgres(tokio_postgres::Error),
    Mysql(mysql_async::Error),
}

impl MigrationError {
    pub(crate) fn schema(messages: Vec<String>) -> Self {
        Self::Schema(messages)
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid migration request: {message}"),
            Self::UnversionedSchema => write!(
                f,
                "headgate tables exist without migration history; validate and adopt the current schema before migrating"
            ),
            Self::History(message) => write!(f, "invalid migration history: {message}"),
            Self::Schema(messages) => {
                write!(f, "schema validation failed: {}", messages.join("; "))
            }
            Self::Postgres(error) => write!(f, "postgres migration failed: {error}"),
            Self::Mysql(error) => write!(f, "mysql migration failed: {error}"),
        }
    }
}

impl std::error::Error for MigrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postgres(error) => Some(error),
            Self::Mysql(error) => Some(error),
            _ => None,
        }
    }
}

impl From<tokio_postgres::Error> for MigrationError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::Postgres(value)
    }
}

impl From<mysql_async::Error> for MigrationError {
    fn from(value: mysql_async::Error) -> Self {
        Self::Mysql(value)
    }
}

/// Validate history independently of a database. This is also the planner's first step,
/// so a checksum mismatch cannot be bypassed with `--dry-run` or a target version.
pub fn validate_history(
    backend: Backend,
    applied: &[AppliedMigration],
) -> Result<(), MigrationError> {
    let known = migrations(backend);
    for (index, row) in applied.iter().enumerate() {
        let expected_version = (index + 1) as u32;
        if row.version != expected_version {
            return Err(MigrationError::History(format!(
                "versions must be contiguous from 1; found {} where {} was expected",
                row.version, expected_version
            )));
        }
        let Some(migration) = known.iter().find(|m| m.version == row.version) else {
            return Err(MigrationError::History(format!(
                "database has unknown future version {}",
                row.version
            )));
        };
        if row.name != migration.name {
            return Err(MigrationError::History(format!(
                "version {} is named {:?}, expected {:?}",
                row.version, row.name, migration.name
            )));
        }
        let expected_checksum = checksum(migration);
        if row.checksum != expected_checksum {
            return Err(MigrationError::History(format!(
                "version {} checksum is {}, expected {}",
                row.version, row.checksum, expected_checksum
            )));
        }
    }
    Ok(())
}

pub fn plan(
    backend: Backend,
    applied: &[AppliedMigration],
    direction: Direction,
    options: MigrateOptions,
) -> Result<Vec<MigrationStep>, MigrationError> {
    validate_history(backend, applied)?;
    let all = migrations(backend);
    let current = applied.last().map_or(0, |m| m.version);
    let latest = latest_version(backend);
    let target = options.target_version.unwrap_or(match direction {
        Direction::Up => latest,
        Direction::Down => 0,
    });
    if target > latest {
        return Err(MigrationError::Invalid(format!(
            "target version {target} is newer than embedded latest version {latest}"
        )));
    }
    match direction {
        Direction::Up if target < current => {
            return Err(MigrationError::Invalid(format!(
                "target version {target} is below current version {current}; use down"
            )));
        }
        Direction::Down if target > current => {
            return Err(MigrationError::Invalid(format!(
                "target version {target} is above current version {current}; use up"
            )));
        }
        _ => {}
    }

    let mut steps: Vec<_> = match direction {
        Direction::Up => all
            .iter()
            .filter(|m| m.version > current && m.version <= target)
            .map(|migration| MigrationStep {
                direction,
                migration,
            })
            .collect(),
        Direction::Down => all
            .iter()
            .rev()
            .filter(|m| m.version > target && m.version <= current)
            .map(|migration| MigrationStep {
                direction,
                migration,
            })
            .collect(),
    };
    if let Some(max_steps) = options.max_steps {
        steps.truncate(max_steps);
    }
    Ok(steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn applied(version: u32) -> AppliedMigration {
        let migration = migration(Backend::Postgres, version).unwrap();
        AppliedMigration {
            version,
            name: migration.name.to_owned(),
            checksum: checksum(migration),
            applied_at_ms: 1,
        }
    }

    #[test]
    fn plans_up_down_targets_and_idempotent_current() {
        let up = plan(
            Backend::Postgres,
            &[],
            Direction::Up,
            MigrateOptions::default(),
        )
        .unwrap();
        assert_eq!(
            up.iter().map(|s| s.migration.version).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        );

        let current = [
            applied(1),
            applied(2),
            applied(3),
            applied(4),
            applied(5),
            applied(6),
            applied(7),
            applied(8),
            applied(9),
            applied(10),
            applied(11),
            applied(12),
            applied(13),
        ];
        assert!(
            plan(
                Backend::Postgres,
                &current,
                Direction::Up,
                MigrateOptions::default()
            )
            .unwrap()
            .is_empty()
        );
        let down = plan(
            Backend::Postgres,
            &current,
            Direction::Down,
            MigrateOptions::default(),
        )
        .unwrap();
        assert_eq!(
            down.iter().map(|s| s.migration.version).collect::<Vec<_>>(),
            [13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        );
    }

    #[test]
    fn checksum_or_gap_in_history_is_fatal_even_to_planning() {
        let mut bad = applied(1);
        bad.checksum = "tampered".into();
        assert!(matches!(
            plan(
                Backend::Postgres,
                &[bad],
                Direction::Up,
                MigrateOptions {
                    dry_run: true,
                    ..MigrateOptions::default()
                }
            ),
            Err(MigrationError::History(_))
        ));

        let future = AppliedMigration {
            version: 11,
            name: "future".into(),
            checksum: "x".into(),
            applied_at_ms: 1,
        };
        assert!(matches!(
            validate_history(Backend::Postgres, &[future]),
            Err(MigrationError::History(_))
        ));
    }

    #[test]
    fn wrong_direction_and_future_targets_are_rejected() {
        let current = [applied(1)];
        assert!(matches!(
            plan(
                Backend::Postgres,
                &current,
                Direction::Up,
                MigrateOptions {
                    target_version: Some(0),
                    ..MigrateOptions::default()
                }
            ),
            Err(MigrationError::Invalid(_))
        ));
        assert!(matches!(
            plan(
                Backend::Postgres,
                &[],
                Direction::Up,
                MigrateOptions {
                    target_version: Some(14),
                    ..MigrateOptions::default()
                }
            ),
            Err(MigrationError::Invalid(_))
        ));
    }
}
