use std::collections::HashSet;

use headgate_sql::{PostgresNamespace, quote_identifier};
use tokio_postgres::{Client, GenericClient};

use crate::schema::{POSTGRES_COLUMNS, POSTGRES_INDEXES, POSTGRES_TRIGGERS, STATES, TABLES};
use crate::{
    AppliedMigration, Backend, Direction, InstallationState, MigrateOptions, MigrateResult,
    MigrationError, checksum, latest_version, migrations, plan,
};

const CREATE_HISTORY: &str = "
CREATE TABLE IF NOT EXISTS headgate_schema_migration (
  line          text   NOT NULL DEFAULT 'main',
  version       bigint NOT NULL,
  name          text   NOT NULL,
  checksum      text   NOT NULL,
  applied_at_ms bigint NOT NULL,
  PRIMARY KEY (line, version)
)";

#[derive(Clone, Debug)]
pub struct PostgresValidation {
    pub state: InstallationState,
    pub current_version: u32,
    pub latest_version: u32,
    pub applied: Vec<AppliedMigration>,
    pub messages: Vec<String>,
}

impl PostgresValidation {
    pub fn is_ok(&self) -> bool {
        self.messages.is_empty()
    }
}

async fn namespace_for(
    client: &Client,
    schema: Option<&str>,
) -> Result<PostgresNamespace, MigrationError> {
    let name = match schema {
        Some(name) => name.to_owned(),
        None => client
            .query_one("SELECT current_schema()", &[])
            .await?
            .get(0),
    };
    let namespace = PostgresNamespace::explicit(&name).map_err(MigrationError::Invalid)?;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
            &[&name],
        )
        .await?
        .get(0);
    if !exists {
        return Err(MigrationError::Invalid(format!(
            "Postgres schema {name:?} does not exist; create it before migrating"
        )));
    }
    Ok(namespace)
}

fn relation_name(namespace: &PostgresNamespace, relation: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier(namespace.name().expect("resolved namespace")),
        quote_identifier(relation)
    )
}

async fn relation_exists<C>(
    client: &C,
    namespace: &PostgresNamespace,
    relation: &str,
) -> Result<bool, MigrationError>
where
    C: GenericClient + Sync,
{
    let relation = relation_name(namespace, relation);
    Ok(client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&relation])
        .await?
        .get(0))
}

async fn read_history<C>(
    client: &C,
    namespace: &PostgresNamespace,
) -> Result<Vec<AppliedMigration>, MigrationError>
where
    C: GenericClient + Sync,
{
    let sql = namespace.render(
        "SELECT version, name, checksum, applied_at_ms
           FROM headgate_schema_migration
          WHERE line = 'main'
          ORDER BY version",
    );
    let rows = client.query(sql.as_ref(), &[]).await?;
    rows.into_iter()
        .map(|row| {
            let version: i64 = row.get(0);
            let version = u32::try_from(version).map_err(|_| {
                MigrationError::History(format!("version {version} is outside u32"))
            })?;
            Ok(AppliedMigration {
                version,
                name: row.get(1),
                checksum: row.get(2),
                applied_at_ms: row.get(3),
            })
        })
        .collect()
}

async fn applied_postgres_scoped(
    client: &Client,
    namespace: &PostgresNamespace,
) -> Result<(InstallationState, Vec<AppliedMigration>), MigrationError> {
    let has_history = relation_exists(client, namespace, "headgate_schema_migration").await?;
    let has_schema = relation_exists(client, namespace, "headgate_job").await?;
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
    let applied = read_history(client, namespace).await?;
    let state = if applied.is_empty() && !has_schema {
        InstallationState::Empty
    } else if applied.is_empty() && has_schema {
        InstallationState::Unversioned
    } else {
        InstallationState::Versioned
    };
    Ok((state, applied))
}

pub async fn applied_postgres(
    client: &Client,
) -> Result<(InstallationState, Vec<AppliedMigration>), MigrationError> {
    let namespace = namespace_for(client, None).await?;
    applied_postgres_scoped(client, &namespace).await
}

pub async fn applied_postgres_in_schema(
    client: &Client,
    schema: &str,
) -> Result<(InstallationState, Vec<AppliedMigration>), MigrationError> {
    let namespace = namespace_for(client, Some(schema)).await?;
    applied_postgres_scoped(client, &namespace).await
}

async fn missing_schema<C>(
    client: &C,
    namespace: &PostgresNamespace,
) -> Result<Vec<String>, MigrationError>
where
    C: GenericClient + Sync,
{
    let schema = namespace.name().expect("resolved namespace");
    let mut missing = Vec::new();
    let tables: HashSet<String> = client
        .query(
            "SELECT table_name
               FROM information_schema.tables
              WHERE table_schema = $1 AND table_name LIKE 'headgate_%'",
            &[&schema],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for table in TABLES {
        if !tables.contains(*table) {
            missing.push(format!("missing table {table}"));
        }
    }

    let columns: HashSet<(String, String)> = client
        .query(
            "SELECT table_name, column_name
               FROM information_schema.columns
              WHERE table_schema = $1 AND table_name LIKE 'headgate_%'",
            &[&schema],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    for required in POSTGRES_COLUMNS {
        if !columns.contains(&(required.table.to_owned(), required.column.to_owned())) {
            missing.push(format!(
                "missing column {}.{}",
                required.table, required.column
            ));
        }
    }

    let indexes: HashSet<String> = client
        .query(
            "SELECT indexname FROM pg_indexes
              WHERE schemaname = $1 AND indexname LIKE 'headgate_%'",
            &[&schema],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for index in POSTGRES_INDEXES {
        if !indexes.contains(*index) {
            missing.push(format!("missing index {index}"));
        }
    }

    let triggers: HashSet<String> = client
        .query(
            "SELECT t.tgname
               FROM pg_trigger t
               JOIN pg_class c ON c.oid = t.tgrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = $1 AND NOT t.tgisinternal",
            &[&schema],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for trigger in POSTGRES_TRIGGERS {
        if !triggers.contains(*trigger) {
            missing.push(format!("missing trigger {trigger}"));
        }
    }

    let states: Vec<String> = client
        .query(
            "SELECT e.enumlabel
               FROM pg_type t
               JOIN pg_namespace n ON n.oid = t.typnamespace
               JOIN pg_enum e ON e.enumtypid = t.oid
              WHERE n.nspname = $1 AND t.typname = 'headgate_state'
              ORDER BY e.enumsortorder",
            &[&schema],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let expected_states: Vec<String> = STATES.iter().map(|state| (*state).to_owned()).collect();
    if states != expected_states {
        missing.push(format!(
            "headgate_state labels are {:?}, expected {:?}",
            states, expected_states
        ));
    }
    Ok(missing)
}

async fn validate_postgres_scoped(
    client: &Client,
    namespace: &PostgresNamespace,
) -> Result<PostgresValidation, MigrationError> {
    let (state, applied) = applied_postgres_scoped(client, namespace).await?;
    let current_version = applied.last().map_or(0, |row| row.version);
    let latest = latest_version(Backend::Postgres);
    let mut messages = Vec::new();
    match state {
        InstallationState::Empty => messages.push("headgate schema is not installed".into()),
        InstallationState::Unversioned => {
            messages.push("headgate schema exists without migration history".into());
            messages.extend(missing_schema(client, namespace).await?);
        }
        InstallationState::Versioned => {
            if let Err(error) = crate::validate_history(Backend::Postgres, &applied) {
                messages.push(error.to_string());
            }
            if current_version != latest {
                messages.push(format!(
                    "schema is at version {current_version}, embedded latest is {latest}"
                ));
            } else {
                messages.extend(missing_schema(client, namespace).await?);
            }
        }
    }
    Ok(PostgresValidation {
        state,
        current_version,
        latest_version: latest,
        applied,
        messages,
    })
}

pub async fn validate_postgres(client: &Client) -> Result<PostgresValidation, MigrationError> {
    let namespace = namespace_for(client, None).await?;
    validate_postgres_scoped(client, &namespace).await
}

pub async fn validate_postgres_in_schema(
    client: &Client,
    schema: &str,
) -> Result<PostgresValidation, MigrationError> {
    let namespace = namespace_for(client, Some(schema)).await?;
    validate_postgres_scoped(client, &namespace).await
}

async fn migrate_postgres_scoped(
    client: &mut Client,
    namespace: &PostgresNamespace,
    direction: Direction,
    options: MigrateOptions,
) -> Result<MigrateResult, MigrationError> {
    let (state, applied) = applied_postgres_scoped(client, namespace).await?;
    if state == InstallationState::Unversioned {
        return Err(MigrationError::UnversionedSchema);
    }
    let planned = plan(Backend::Postgres, &applied, direction, options)?;
    if options.dry_run {
        return Ok(MigrateResult {
            dry_run: true,
            steps: planned,
        });
    }
    let create_history = namespace.render(CREATE_HISTORY);
    client.batch_execute(create_history.as_ref()).await?;
    let mut executed = Vec::new();
    for intended in planned {
        let tx = client.transaction().await?;
        let lock =
            namespace.render("LOCK TABLE headgate_schema_migration IN ACCESS EXCLUSIVE MODE");
        tx.batch_execute(lock.as_ref()).await?;
        let live = read_history(&tx, namespace).await?;
        crate::validate_history(Backend::Postgres, &live)?;
        let current = live.last().map_or(0, |row| row.version);
        match direction {
            Direction::Up => {
                if current >= intended.migration.version {
                    tx.commit().await?;
                    continue;
                }
                if current + 1 != intended.migration.version {
                    return Err(MigrationError::History(format!(
                        "concurrent migration moved current version to {current}; expected {}",
                        intended.migration.version - 1
                    )));
                }
                let up_sql = namespace.render(intended.migration.up_sql);
                tx.batch_execute(up_sql.as_ref()).await?;
                // The manifest describes LATEST, not every historical intermediate.
                // Validate it only after the step that is supposed to produce latest;
                // otherwise adding v2 makes a fresh v1 transaction fail for lacking v2.
                if intended.migration.version == latest_version(Backend::Postgres) {
                    let schema_errors = missing_schema(&tx, namespace).await?;
                    if !schema_errors.is_empty() {
                        return Err(MigrationError::schema(schema_errors));
                    }
                }
                let version = i64::from(intended.migration.version);
                let migration_checksum = checksum(intended.migration);
                let insert = namespace.render(
                    "INSERT INTO headgate_schema_migration
                       (line, version, name, checksum, applied_at_ms)
                     VALUES ('main', $1, $2, $3,
                             (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint)",
                );
                tx.execute(
                    insert.as_ref(),
                    &[&version, &intended.migration.name, &migration_checksum],
                )
                .await?;
            }
            Direction::Down => {
                if current < intended.migration.version {
                    tx.commit().await?;
                    continue;
                }
                if current != intended.migration.version {
                    return Err(MigrationError::History(format!(
                        "cannot migrate down version {}; current version is {current}",
                        intended.migration.version
                    )));
                }
                let down_sql = namespace.render(intended.migration.down_sql);
                tx.batch_execute(down_sql.as_ref()).await?;
                let version = i64::from(intended.migration.version);
                let delete = namespace.render(
                    "DELETE FROM headgate_schema_migration
                      WHERE line = 'main' AND version = $1",
                );
                tx.execute(delete.as_ref(), &[&version]).await?;
            }
        }
        tx.commit().await?;
        executed.push(intended);
    }
    Ok(MigrateResult {
        dry_run: false,
        steps: executed,
    })
}

pub async fn migrate_postgres(
    client: &mut Client,
    direction: Direction,
    options: MigrateOptions,
) -> Result<MigrateResult, MigrationError> {
    let namespace = namespace_for(client, None).await?;
    migrate_postgres_scoped(client, &namespace, direction, options).await
}

pub async fn migrate_postgres_in_schema(
    client: &mut Client,
    schema: &str,
    direction: Direction,
    options: MigrateOptions,
) -> Result<MigrateResult, MigrationError> {
    let namespace = namespace_for(client, Some(schema)).await?;
    migrate_postgres_scoped(client, &namespace, direction, options).await
}

async fn adopt_postgres_scoped(
    client: &mut Client,
    namespace: &PostgresNamespace,
) -> Result<Vec<AppliedMigration>, MigrationError> {
    let (state, existing) = applied_postgres_scoped(client, namespace).await?;
    if state == InstallationState::Versioned {
        crate::validate_history(Backend::Postgres, &existing)?;
        return Ok(existing);
    }
    if state == InstallationState::Empty {
        return Err(MigrationError::Invalid(
            "cannot adopt an empty database; migrate up instead".into(),
        ));
    }
    let create_history = namespace.render(CREATE_HISTORY);
    client.batch_execute(create_history.as_ref()).await?;
    let tx = client.transaction().await?;
    let lock = namespace.render("LOCK TABLE headgate_schema_migration IN ACCESS EXCLUSIVE MODE");
    tx.batch_execute(lock.as_ref()).await?;
    let live = read_history(&tx, namespace).await?;
    if !live.is_empty() {
        crate::validate_history(Backend::Postgres, &live)?;
        tx.commit().await?;
        return Ok(live);
    }
    let schema_errors = missing_schema(&tx, namespace).await?;
    if !schema_errors.is_empty() {
        return Err(MigrationError::schema(schema_errors));
    }
    let insert = namespace.render(
        "INSERT INTO headgate_schema_migration
           (line, version, name, checksum, applied_at_ms)
         VALUES ('main', $1, $2, $3,
                 (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint)",
    );
    for migration in migrations(Backend::Postgres) {
        let version = i64::from(migration.version);
        let migration_checksum = checksum(migration);
        tx.execute(
            insert.as_ref(),
            &[&version, &migration.name, &migration_checksum],
        )
        .await?;
    }
    tx.commit().await?;
    applied_postgres_scoped(client, namespace)
        .await
        .map(|(_, applied)| applied)
}

pub async fn adopt_postgres(client: &mut Client) -> Result<Vec<AppliedMigration>, MigrationError> {
    let namespace = namespace_for(client, None).await?;
    adopt_postgres_scoped(client, &namespace).await
}

pub async fn adopt_postgres_in_schema(
    client: &mut Client,
    schema: &str,
) -> Result<Vec<AppliedMigration>, MigrationError> {
    let namespace = namespace_for(client, Some(schema)).await?;
    adopt_postgres_scoped(client, &namespace).await
}
