use std::collections::HashSet;

use mysql_async::Conn;
use mysql_async::prelude::*;
use sha2::{Digest, Sha256};

use crate::schema::{MYSQL_COLUMNS, MYSQL_INDEXES, MYSQL_TRIGGERS, STATES, TABLES};
use crate::{
    AppliedMigration, Backend, Direction, InstallationState, MigrateOptions, MigrateResult,
    MigrationError, checksum, latest_version, migrations, plan,
};

const CREATE_HISTORY: &str = "
CREATE TABLE IF NOT EXISTS headgate_schema_migration (
  line          VARCHAR(64)  NOT NULL DEFAULT 'main',
  version       BIGINT       NOT NULL,
  name          VARCHAR(255) NOT NULL,
  checksum      CHAR(64)     NOT NULL,
  applied_at_ms BIGINT       NOT NULL,
  PRIMARY KEY (line, version)
) ENGINE=InnoDB";

/// Backward-compatible with the lock name shipped before namespaces were configurable.
pub const DEFAULT_MYSQL_LOCK_NAMESPACE: &str = "headgate";

/// Build the connection-scoped MySQL migration lock name. The readable form preserves
/// the historical `headgate:migrate:<database>` default. Only an overlong database is
/// hashed under a distinct `:h:` marker, keeping the result below MySQL's
/// 64-byte GET_LOCK limit without aliasing a short literal database name.
pub fn mysql_migration_lock_name(
    namespace: &str,
    database: &str,
) -> Result<String, MigrationError> {
    let valid = !namespace.is_empty()
        && namespace.len() <= 31
        && namespace.as_bytes()[0].is_ascii_alphanumeric()
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
    if !valid {
        return Err(MigrationError::Invalid(
            "MySQL lock namespace must be 1-31 ASCII bytes, start alphanumeric, and contain only [A-Za-z0-9_.-]"
                .into(),
        ));
    }
    if database.is_empty() {
        return Err(MigrationError::Invalid(
            "MySQL migrations require a selected database".into(),
        ));
    }
    let readable = format!("{namespace}:migrate:{database}");
    if readable.len() <= 64 {
        return Ok(readable);
    }
    let digest = format!("{:x}", Sha256::digest(database.as_bytes()));
    Ok(format!("{namespace}:h:{}", &digest[..30]))
}

#[derive(Clone, Debug)]
pub struct MysqlValidation {
    pub state: InstallationState,
    pub current_version: u32,
    pub latest_version: u32,
    pub applied: Vec<AppliedMigration>,
    pub messages: Vec<String>,
}

impl MysqlValidation {
    pub fn is_ok(&self) -> bool {
        self.messages.is_empty()
    }
}

async fn table_exists(conn: &mut Conn, table: &str) -> Result<bool, MigrationError> {
    let n: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.tables
              WHERE table_schema = DATABASE() AND table_name = ?",
            (table,),
        )
        .await?;
    Ok(n.unwrap_or(0) != 0)
}

async fn read_history(conn: &mut Conn) -> Result<Vec<AppliedMigration>, MigrationError> {
    let rows: Vec<(i64, String, String, i64)> = conn
        .query(
            "SELECT version, name, checksum, applied_at_ms
               FROM headgate_schema_migration
              WHERE line = 'main'
              ORDER BY version",
        )
        .await?;
    rows.into_iter()
        .map(|(version, name, checksum, applied_at_ms)| {
            let version = u32::try_from(version).map_err(|_| {
                MigrationError::History(format!("version {version} is outside u32"))
            })?;
            Ok(AppliedMigration {
                version,
                name,
                checksum,
                applied_at_ms,
            })
        })
        .collect()
}

pub async fn applied_mysql(
    conn: &mut Conn,
) -> Result<(InstallationState, Vec<AppliedMigration>), MigrationError> {
    let has_history = table_exists(conn, "headgate_schema_migration").await?;
    let has_schema = table_exists(conn, "headgate_job").await?;
    if !has_history {
        return Ok((
            if has_schema {
                InstallationState::Unversioned
            } else {
                InstallationState::Empty
            },
            Vec::new(),
        ));
    }
    let applied = read_history(conn).await?;
    let state = if applied.is_empty() && !has_schema {
        InstallationState::Empty
    } else if applied.is_empty() && has_schema {
        InstallationState::Unversioned
    } else {
        InstallationState::Versioned
    };
    Ok((state, applied))
}

async fn missing_schema(conn: &mut Conn) -> Result<Vec<String>, MigrationError> {
    let mut missing = Vec::new();
    let tables: HashSet<String> = conn
        .query(
            "SELECT table_name
               FROM information_schema.tables
              WHERE table_schema = DATABASE()",
        )
        .await?
        .into_iter()
        .collect();
    for table in TABLES {
        if !tables.contains(*table) {
            missing.push(format!("missing table {table}"));
        }
    }

    let columns: HashSet<(String, String)> = conn
        .query(
            "SELECT table_name, column_name
               FROM information_schema.columns
              WHERE table_schema = DATABASE()",
        )
        .await?
        .into_iter()
        .collect();
    for required in MYSQL_COLUMNS {
        if !columns.contains(&(required.table.to_owned(), required.column.to_owned())) {
            missing.push(format!(
                "missing column {}.{}",
                required.table, required.column
            ));
        }
    }

    let indexes: HashSet<String> = conn
        .query(
            "SELECT DISTINCT index_name
               FROM information_schema.statistics
              WHERE table_schema = DATABASE()",
        )
        .await?
        .into_iter()
        .collect();
    for index in MYSQL_INDEXES {
        if !indexes.contains(*index) {
            missing.push(format!("missing index {index}"));
        }
    }

    let triggers: HashSet<String> = conn
        .query(
            "SELECT trigger_name FROM information_schema.triggers
              WHERE trigger_schema = DATABASE()",
        )
        .await?
        .into_iter()
        .collect();
    for trigger in MYSQL_TRIGGERS {
        if !triggers.contains(*trigger) {
            missing.push(format!("missing trigger {trigger}"));
        }
    }

    let state_type: Option<String> = conn
        .exec_first(
            "SELECT column_type FROM information_schema.columns
              WHERE table_schema = DATABASE()
                AND table_name = 'headgate_job' AND column_name = 'state'",
            (),
        )
        .await?;
    let labels = state_type.as_deref().map(enum_labels).unwrap_or_default();
    let actual: HashSet<_> = labels.into_iter().collect();
    let expected: HashSet<_> = STATES.iter().map(|state| (*state).to_owned()).collect();
    if actual != expected {
        missing.push(format!(
            "headgate_job.state labels are {:?}, expected {:?}",
            actual, expected
        ));
    }

    let saturation_type: Option<String> = conn
        .exec_first(
            "SELECT column_type FROM information_schema.columns
              WHERE table_schema = DATABASE()
                AND table_name = 'headgate_concurrency_limit'
                AND column_name = 'on_saturated'",
            (),
        )
        .await?;
    let actual: HashSet<_> = saturation_type
        .as_deref()
        .map(enum_labels)
        .unwrap_or_default()
        .into_iter()
        .collect();
    let expected: HashSet<_> = ["queue", "discard", "cancel_running", "cancel_incoming"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    if actual != expected {
        missing.push(format!(
            "headgate_concurrency_limit.on_saturated labels are {:?}, expected {:?}",
            actual, expected
        ));
    }
    Ok(missing)
}

fn enum_labels(column_type: &str) -> Vec<String> {
    let Some(body) = column_type
        .strip_prefix("enum(")
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Vec::new();
    };
    body.split(',')
        .map(|part| part.trim().trim_matches('\'').replace("''", "'"))
        .collect()
}

pub async fn validate_mysql(conn: &mut Conn) -> Result<MysqlValidation, MigrationError> {
    let (state, applied) = applied_mysql(conn).await?;
    let current_version = applied.last().map_or(0, |row| row.version);
    let latest = latest_version(Backend::Mysql);
    let mut messages = Vec::new();
    match state {
        InstallationState::Empty => messages.push("headgate schema is not installed".into()),
        InstallationState::Unversioned => {
            messages.push("headgate schema exists without migration history".into());
            messages.extend(missing_schema(conn).await?);
        }
        InstallationState::Versioned => {
            if let Err(error) = crate::validate_history(Backend::Mysql, &applied) {
                messages.push(error.to_string());
            }
            if current_version != latest {
                messages.push(format!(
                    "schema is at version {current_version}, embedded latest is {latest}"
                ));
            } else {
                messages.extend(missing_schema(conn).await?);
            }
        }
    }
    Ok(MysqlValidation {
        state,
        current_version,
        latest_version: latest,
        applied,
        messages,
    })
}

async fn lock_name(conn: &mut Conn, namespace: &str) -> Result<String, MigrationError> {
    let database: Option<String> = conn.query_first("SELECT COALESCE(DATABASE(), '')").await?;
    mysql_migration_lock_name(namespace, database.as_deref().unwrap_or_default())
}

async fn acquire_lock(conn: &mut Conn, namespace: &str) -> Result<String, MigrationError> {
    let name = lock_name(conn, namespace).await?;
    let acquired: Option<i64> = conn.exec_first("SELECT GET_LOCK(?, 30)", (&name,)).await?;
    if acquired != Some(1) {
        return Err(MigrationError::Invalid(
            "timed out acquiring the MySQL migration lock".into(),
        ));
    }
    Ok(name)
}

async fn release_lock(conn: &mut Conn, name: &str) -> Result<(), MigrationError> {
    let released: Option<i64> = conn.exec_first("SELECT RELEASE_LOCK(?)", (name,)).await?;
    if released != Some(1) {
        return Err(MigrationError::Invalid(
            "MySQL migration lock was not held at release".into(),
        ));
    }
    Ok(())
}

pub async fn migrate_mysql(
    conn: &mut Conn,
    direction: Direction,
    options: MigrateOptions,
) -> Result<MigrateResult, MigrationError> {
    migrate_mysql_with_lock_namespace(conn, direction, options, DEFAULT_MYSQL_LOCK_NAMESPACE).await
}

pub async fn migrate_mysql_with_lock_namespace(
    conn: &mut Conn,
    direction: Direction,
    options: MigrateOptions,
    lock_namespace: &str,
) -> Result<MigrateResult, MigrationError> {
    // Validate even for a no-op/dry-run so a misspelled production namespace is never
    // accepted by one command and rejected only during the next real migration.
    let _ = mysql_migration_lock_name(lock_namespace, "validation")?;
    let (state, applied) = applied_mysql(conn).await?;
    if state == InstallationState::Unversioned {
        return Err(MigrationError::UnversionedSchema);
    }
    let planned = plan(Backend::Mysql, &applied, direction, options)?;
    if options.dry_run {
        return Ok(MigrateResult {
            dry_run: true,
            steps: planned,
        });
    }
    conn.query_drop(CREATE_HISTORY).await?;
    let lock_name = acquire_lock(conn, lock_namespace).await?;
    let result = migrate_mysql_locked(conn, direction, options).await;
    let release = release_lock(conn, &lock_name).await;
    match (result, release) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(result), Ok(())) => Ok(result),
    }
}

async fn migrate_mysql_locked(
    conn: &mut Conn,
    direction: Direction,
    options: MigrateOptions,
) -> Result<MigrateResult, MigrationError> {
    let live = read_history(conn).await?;
    let planned = plan(Backend::Mysql, &live, direction, options)?;
    let mut executed = Vec::new();
    for step in planned {
        for statement in split_statements(match direction {
            Direction::Up => step.migration.up_sql,
            Direction::Down => step.migration.down_sql,
        })? {
            conn.query_drop(statement).await?;
        }
        match direction {
            Direction::Up => {
                if step.migration.version == latest_version(Backend::Mysql) {
                    let schema_errors = missing_schema(conn).await?;
                    if !schema_errors.is_empty() {
                        return Err(MigrationError::schema(schema_errors));
                    }
                }
                let migration_checksum = checksum(step.migration);
                conn.exec_drop(
                    "INSERT INTO headgate_schema_migration
                       (line, version, name, checksum, applied_at_ms)
                     VALUES ('main', ?, ?, ?,
                             CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED))",
                    (
                        i64::from(step.migration.version),
                        step.migration.name,
                        migration_checksum,
                    ),
                )
                .await?;
            }
            Direction::Down => {
                conn.exec_drop(
                    "DELETE FROM headgate_schema_migration
                      WHERE line = 'main' AND version = ?",
                    (i64::from(step.migration.version),),
                )
                .await?;
            }
        }
        executed.push(step);
    }
    Ok(MigrateResult {
        dry_run: false,
        steps: executed,
    })
}

pub async fn adopt_mysql(conn: &mut Conn) -> Result<Vec<AppliedMigration>, MigrationError> {
    adopt_mysql_with_lock_namespace(conn, DEFAULT_MYSQL_LOCK_NAMESPACE).await
}

pub async fn adopt_mysql_with_lock_namespace(
    conn: &mut Conn,
    lock_namespace: &str,
) -> Result<Vec<AppliedMigration>, MigrationError> {
    let _ = mysql_migration_lock_name(lock_namespace, "validation")?;
    let (state, existing) = applied_mysql(conn).await?;
    if state == InstallationState::Versioned {
        crate::validate_history(Backend::Mysql, &existing)?;
        return Ok(existing);
    }
    if state == InstallationState::Empty {
        return Err(MigrationError::Invalid(
            "cannot adopt an empty database; migrate up instead".into(),
        ));
    }
    conn.query_drop(CREATE_HISTORY).await?;
    let lock_name = acquire_lock(conn, lock_namespace).await?;
    let result = adopt_mysql_locked(conn).await;
    let release = release_lock(conn, &lock_name).await;
    match (result, release) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(result), Ok(())) => Ok(result),
    }
}

async fn adopt_mysql_locked(conn: &mut Conn) -> Result<Vec<AppliedMigration>, MigrationError> {
    let live = read_history(conn).await?;
    if !live.is_empty() {
        crate::validate_history(Backend::Mysql, &live)?;
        return Ok(live);
    }
    let schema_errors = missing_schema(conn).await?;
    if !schema_errors.is_empty() {
        return Err(MigrationError::schema(schema_errors));
    }
    for migration in migrations(Backend::Mysql) {
        let migration_checksum = checksum(migration);
        conn.exec_drop(
            "INSERT INTO headgate_schema_migration
               (line, version, name, checksum, applied_at_ms)
             VALUES ('main', ?, ?, ?,
                     CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED))",
            (
                i64::from(migration.version),
                migration.name,
                migration_checksum,
            ),
        )
        .await?;
    }
    read_history(conn).await
}

/// Split MySQL migration assets without requiring `CLIENT_MULTI_STATEMENTS`. Comments,
/// quoted strings and backtick identifiers may contain semicolons; only a top-level
/// semicolon terminates a statement.
fn split_statements(sql: &str) -> Result<Vec<String>, MigrationError> {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum Mode {
        Normal,
        Single,
        Double,
        Backtick,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut mode = Mode::Normal;
    let mut statement = String::new();
    let mut statements = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        let next = bytes.get(i + 1).copied().map(char::from);
        match mode {
            Mode::Normal if ch == '-' && next == Some('-') => {
                mode = Mode::LineComment;
                i += 2;
                continue;
            }
            Mode::Normal if ch == '/' && next == Some('*') => {
                mode = Mode::BlockComment;
                i += 2;
                continue;
            }
            Mode::LineComment if ch == '\n' => {
                mode = Mode::Normal;
                statement.push('\n');
            }
            Mode::LineComment => {}
            Mode::BlockComment if ch == '*' && next == Some('/') => {
                mode = Mode::Normal;
                i += 2;
                continue;
            }
            Mode::BlockComment => {}
            Mode::Normal if ch == '\'' => {
                mode = Mode::Single;
                statement.push(ch);
            }
            Mode::Normal if ch == '"' => {
                mode = Mode::Double;
                statement.push(ch);
            }
            Mode::Normal if ch == '`' => {
                mode = Mode::Backtick;
                statement.push(ch);
            }
            Mode::Single | Mode::Double | Mode::Backtick => {
                statement.push(ch);
                let quote = match mode {
                    Mode::Single => '\'',
                    Mode::Double => '"',
                    Mode::Backtick => '`',
                    _ => unreachable!(),
                };
                if ch == '\\'
                    && let Some(next) = next
                {
                    statement.push(next);
                    i += 2;
                    continue;
                }
                if ch == quote {
                    if next == Some(quote) {
                        statement.push(quote);
                        i += 2;
                        continue;
                    }
                    mode = Mode::Normal;
                }
            }
            Mode::Normal if ch == ';' => {
                let value = statement.trim();
                if !value.is_empty() {
                    statements.push(value.to_owned());
                }
                statement.clear();
            }
            Mode::Normal => statement.push(ch),
        }
        i += 1;
    }
    if matches!(
        mode,
        Mode::Single | Mode::Double | Mode::Backtick | Mode::BlockComment
    ) {
        return Err(MigrationError::Invalid(
            "unterminated quote or block comment in embedded MySQL migration".into(),
        ));
    }
    let value = statement.trim();
    if !value.is_empty() {
        statements.push(value.to_owned());
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_splitter_ignores_comments_and_quoted_semicolons() {
        let sql = "-- a ; comment\nCREATE TABLE `semi;colon` (v text DEFAULT 'a;b');\n/* ; */ DROP TABLE `semi;colon`;";
        let statements = split_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("'a;b'"));
        assert!(statements[1].starts_with("DROP TABLE"));
    }

    #[test]
    fn embedded_mysql_migrations_split_into_expected_statements() {
        let migration = &migrations(Backend::Mysql)[0];
        assert_eq!(split_statements(migration.up_sql).unwrap().len(), 15);
        assert_eq!(split_statements(migration.down_sql).unwrap().len(), 15);
    }

    #[test]
    fn mysql_lock_names_preserve_the_default_and_separate_namespaces() {
        assert_eq!(
            mysql_migration_lock_name(DEFAULT_MYSQL_LOCK_NAMESPACE, "jobs").unwrap(),
            "headgate:migrate:jobs"
        );
        assert_ne!(
            mysql_migration_lock_name("billing", "jobs").unwrap(),
            mysql_migration_lock_name("email", "jobs").unwrap()
        );
        let long = mysql_migration_lock_name("billing", &"x".repeat(64)).unwrap();
        assert!(long.starts_with("billing:h:"), "{long}");
        assert!(long.len() <= 64, "{long}");
        for invalid in ["", "-bad", "bad:scope", "white space"] {
            assert!(mysql_migration_lock_name(invalid, "jobs").is_err());
        }
    }
}
