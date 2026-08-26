use std::error::Error;

use headgate_migrate::{
    DEFAULT_MYSQL_LOCK_NAMESPACE, Direction, MigrateOptions, MigrationError, adopt_mysql,
    adopt_postgres, migrate_mysql, migrate_mysql_with_lock_namespace, migrate_postgres,
    mysql_migration_lock_name, validate_mysql, validate_postgres,
};

fn test_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(std::io::Error::other(message.into()))
}

#[tokio::test]
async fn live_postgres_migration_lifecycle_and_drift_rejection() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping postgres migrator test");
        return;
    };
    let schema = format!("hg_migrate_rust_{}", std::process::id());
    let (admin, admin_driver) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .expect("admin connect");
    let admin_task = tokio::spawn(async move { admin_driver.await });
    let exists: i64 = admin
        .query_one(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1",
            &[&schema],
        )
        .await
        .expect("schema probe")
        .get(0);
    assert_eq!(exists, 0, "refusing to reuse migrator test schema {schema}");
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create test schema");

    let (mut client, driver) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .expect("test connect");
    let driver_task = tokio::spawn(async move { driver.await });
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .await
        .expect("set search_path");

    let result: Result<(), Box<dyn Error>> = async {
        let up = migrate_postgres(&mut client, Direction::Up, MigrateOptions::default()).await?;
        if up.steps.len() != 11
            || up.steps[0].migration.version != 1
            || up.steps[10].migration.version != 11
        {
            return Err(test_error(format!("fresh up steps = {:?}", up.steps)));
        }
        let validation = validate_postgres(&client).await?;
        if !validation.is_ok() || validation.current_version != 11 {
            return Err(test_error(format!("fresh validation = {validation:?}")));
        }
        let dry = migrate_postgres(
            &mut client,
            Direction::Down,
            MigrateOptions {
                dry_run: true,
                ..MigrateOptions::default()
            },
        )
        .await?;
        if !dry.dry_run || dry.steps.len() != 11 {
            return Err(test_error(format!("down dry run = {:?}", dry.steps)));
        }
        let down =
            migrate_postgres(&mut client, Direction::Down, MigrateOptions::default()).await?;
        if down.steps.len() != 11 {
            return Err(test_error(format!("down steps = {:?}", down.steps)));
        }
        let row = client
            .query_one(
                "SELECT to_regclass('headgate_job') IS NOT NULL,
                        (SELECT count(*) FROM headgate_schema_migration)",
                &[],
            )
            .await?;
        let job_exists: bool = row.get(0);
        let history_rows: i64 = row.get(1);
        if job_exists || history_rows != 0 {
            return Err(test_error(format!(
                "down left job={job_exists} history={history_rows}"
            )));
        }

        migrate_postgres(&mut client, Direction::Up, MigrateOptions::default()).await?;
        client
            .execute(
                "UPDATE headgate_schema_migration SET checksum = 'tampered' WHERE version = 1",
                &[],
            )
            .await?;
        let validation = validate_postgres(&client).await?;
        if validation.is_ok()
            || !validation
                .messages
                .iter()
                .any(|message| message.contains("checksum"))
        {
            return Err(test_error(format!(
                "tampered checksum validation = {validation:?}"
            )));
        }

        client
            .batch_execute("DROP TABLE headgate_schema_migration")
            .await?;
        if !matches!(
            migrate_postgres(&mut client, Direction::Up, MigrateOptions::default()).await,
            Err(MigrationError::UnversionedSchema)
        ) {
            return Err(test_error(
                "unversioned Postgres schema was migrated as fresh",
            ));
        }
        let adopted = adopt_postgres(&mut client).await?;
        if adopted.last().map(|row| row.version) != Some(11) {
            return Err(test_error(format!("adopted history = {adopted:?}")));
        }
        if !validate_postgres(&client).await?.is_ok() {
            return Err(test_error("adopted Postgres schema did not validate"));
        }

        client
            .batch_execute("DROP TRIGGER headgate_enqueue_depth_delete ON headgate_job")
            .await?;
        let validation = validate_postgres(&client).await?;
        if validation.is_ok()
            || !validation
                .messages
                .iter()
                .any(|message| message == "missing trigger headgate_enqueue_depth_delete")
        {
            return Err(test_error(format!(
                "missing Postgres backpressure trigger validation = {validation:?}"
            )));
        }

        client
            .batch_execute(
                "DROP TABLE headgate_schema_migration;
                 ALTER TABLE headgate_queue_state DROP COLUMN dispatch_count;",
            )
            .await?;
        match adopt_postgres(&mut client).await {
            Err(MigrationError::Schema(messages))
                if messages.iter().any(|message| {
                    message == "missing column headgate_queue_state.dispatch_count"
                }) => {}
            other => {
                return Err(test_error(format!(
                    "drifted Postgres adoption result = {other:?}"
                )));
            }
        }
        Ok(())
    }
    .await;

    drop(client);
    let _ = driver_task.await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .expect("drop test schema");
    drop(admin);
    let _ = admin_task.await;
    result.expect("Postgres migration lifecycle");
}

#[tokio::test]
async fn live_mysql_migration_lifecycle_and_drift_rejection() {
    use mysql_async::prelude::*;

    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping mysql migrator test");
        return;
    };
    let database = format!("hg_migrate_rust_{}", std::process::id());
    let base_opts = mysql_async::Opts::from_url(&url).expect("mysql test URL");
    let admin_pool = mysql_async::Pool::new(base_opts.clone());
    let mut admin = admin_pool.get_conn().await.expect("admin connect");
    let exists: Option<u64> = admin
        .exec_first(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?",
            (&database,),
        )
        .await
        .expect("database probe");
    assert_eq!(exists, Some(0), "refusing to reuse migrator test database");
    admin
        .query_drop(format!("CREATE DATABASE {database}"))
        .await
        .expect("create test database");

    let test_opts = mysql_async::OptsBuilder::from_opts(base_opts).db_name(Some(database.clone()));
    let pool = mysql_async::Pool::new(test_opts);
    let mut conn = pool.get_conn().await.expect("test connect");
    let result: Result<(), Box<dyn Error>> = async {
        let up = migrate_mysql(&mut conn, Direction::Up, MigrateOptions::default()).await?;
        if up.steps.len() != 11
            || up.steps[0].migration.version != 1
            || up.steps[10].migration.version != 11
        {
            return Err(test_error(format!("fresh up steps = {:?}", up.steps)));
        }
        let validation = validate_mysql(&mut conn).await?;
        if !validation.is_ok() || validation.current_version != 11 {
            return Err(test_error(format!("fresh validation = {validation:?}")));
        }
        let dry = migrate_mysql(
            &mut conn,
            Direction::Down,
            MigrateOptions {
                dry_run: true,
                ..MigrateOptions::default()
            },
        )
        .await?;
        if !dry.dry_run || dry.steps.len() != 11 {
            return Err(test_error(format!("down dry run = {:?}", dry.steps)));
        }
        let down = migrate_mysql(&mut conn, Direction::Down, MigrateOptions::default()).await?;
        if down.steps.len() != 11 {
            return Err(test_error(format!("down steps = {:?}", down.steps)));
        }
        let job_exists: Option<u64> = conn
            .query_first(
                "SELECT count(*) FROM information_schema.tables
                  WHERE table_schema = DATABASE() AND table_name = 'headgate_job'",
            )
            .await?;
        let history_rows: Option<u64> = conn
            .query_first("SELECT count(*) FROM headgate_schema_migration")
            .await?;
        if job_exists != Some(0) || history_rows != Some(0) {
            return Err(test_error(format!(
                "down left job={job_exists:?} history={history_rows:?}"
            )));
        }

        migrate_mysql(&mut conn, Direction::Up, MigrateOptions::default()).await?;
        conn.query_drop(
            "UPDATE headgate_schema_migration SET checksum = 'tampered' WHERE version = 1",
        )
        .await?;
        let validation = validate_mysql(&mut conn).await?;
        if validation.is_ok()
            || !validation
                .messages
                .iter()
                .any(|message| message.contains("checksum"))
        {
            return Err(test_error(format!(
                "tampered checksum validation = {validation:?}"
            )));
        }

        conn.query_drop("DROP TABLE headgate_schema_migration")
            .await?;
        if !matches!(
            migrate_mysql(&mut conn, Direction::Up, MigrateOptions::default()).await,
            Err(MigrationError::UnversionedSchema)
        ) {
            return Err(test_error("unversioned MySQL schema was migrated as fresh"));
        }
        let adopted = adopt_mysql(&mut conn).await?;
        if adopted.last().map(|row| row.version) != Some(11) {
            return Err(test_error(format!("adopted history = {adopted:?}")));
        }
        if !validate_mysql(&mut conn).await?.is_ok() {
            return Err(test_error("adopted MySQL schema did not validate"));
        }

        conn.query_drop("DROP TRIGGER headgate_enqueue_depth_delete")
            .await?;
        let validation = validate_mysql(&mut conn).await?;
        if validation.is_ok()
            || !validation
                .messages
                .iter()
                .any(|message| message == "missing trigger headgate_enqueue_depth_delete")
        {
            return Err(test_error(format!(
                "missing MySQL backpressure trigger validation = {validation:?}"
            )));
        }

        conn.query_drop("DROP TABLE headgate_schema_migration")
            .await?;
        conn.query_drop("ALTER TABLE headgate_queue_state DROP COLUMN dispatch_count")
            .await?;
        match adopt_mysql(&mut conn).await {
            Err(MigrationError::Schema(messages))
                if messages.iter().any(|message| {
                    message == "missing column headgate_queue_state.dispatch_count"
                }) => {}
            other => {
                return Err(test_error(format!(
                    "drifted MySQL adoption result = {other:?}"
                )));
            }
        }
        Ok(())
    }
    .await;

    drop(conn);
    let _ = pool.disconnect().await;
    admin
        .query_drop(format!("DROP DATABASE {database}"))
        .await
        .expect("drop test database");
    drop(admin);
    let _ = admin_pool.disconnect().await;
    result.expect("MySQL migration lifecycle");
}

#[tokio::test]
async fn live_mysql_configured_lock_namespace_avoids_an_application_lock() {
    use std::time::Duration;

    use mysql_async::prelude::*;

    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping mysql lock namespace test");
        return;
    };
    let database = format!("hg_lock_rust_{}", std::process::id());
    let base_opts = mysql_async::Opts::from_url(&url).expect("mysql test URL");
    let admin_pool = mysql_async::Pool::new(base_opts.clone());
    let mut admin = admin_pool.get_conn().await.expect("admin connect");
    let exists: Option<u64> = admin
        .exec_first(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?",
            (&database,),
        )
        .await
        .expect("database probe");
    assert_eq!(exists, Some(0), "refusing to reuse lock test database");
    admin
        .query_drop(format!("CREATE DATABASE {database}"))
        .await
        .expect("create test database");

    let test_opts = mysql_async::OptsBuilder::from_opts(base_opts).db_name(Some(database.clone()));
    let pool = mysql_async::Pool::new(test_opts);
    let mut conn = pool.get_conn().await.expect("test connect");
    let result: Result<(), Box<dyn Error>> = async {
        let application_lock =
            mysql_migration_lock_name(DEFAULT_MYSQL_LOCK_NAMESPACE, &database)?;
        let configured_lock = mysql_migration_lock_name("billing", &database)?;
        let application_acquired: Option<i64> = admin
            .exec_first("SELECT GET_LOCK(?, 0)", (&application_lock,))
            .await?;
        let configured_acquired: Option<i64> = admin
            .exec_first("SELECT GET_LOCK(?, 0)", (&configured_lock,))
            .await?;
        if application_acquired != Some(1) || configured_acquired != Some(1) {
            return Err(test_error(format!(
                "lock fixture application={application_acquired:?} configured={configured_acquired:?}"
            )));
        }

        let mut migration = Box::pin(migrate_mysql_with_lock_namespace(
            &mut conn,
            Direction::Up,
            MigrateOptions::default(),
            "billing",
        ));
        if let Ok(early) = tokio::time::timeout(Duration::from_millis(200), &mut migration).await {
            return Err(test_error(format!(
                "migration finished while its configured lock was held: {early:?}"
            )));
        }

        let released: Option<i64> = admin
            .exec_first("SELECT RELEASE_LOCK(?)", (&configured_lock,))
            .await?;
        if released != Some(1) {
            return Err(test_error(format!(
                "configured fixture lock release = {released:?}"
            )));
        }
        // Nine real DDL migrations can exceed five seconds when this live test binary
        // runs its Postgres/MySQL lifecycle siblings concurrently. This deadline proves
        // the named lock was released; it is not a migration-performance assertion.
        let migrated = tokio::time::timeout(Duration::from_secs(30), &mut migration)
            .await
            .map_err(|_| test_error("migration still blocked after configured lock release"))??;
        if migrated.steps.len() != 11 {
            return Err(test_error(format!(
                "configured migration steps = {:?}",
                migrated.steps
            )));
        }

        let application_still_held: Option<i64> = admin
            .exec_first("SELECT IS_FREE_LOCK(?)", (&application_lock,))
            .await?;
        if application_still_held != Some(0) {
            return Err(test_error(format!(
                "application lock was not held through migration: {application_still_held:?}"
            )));
        }
        Ok(())
    }
    .await;

    let _: Result<Option<u64>, _> = admin.query_first("SELECT RELEASE_ALL_LOCKS()").await;
    drop(conn);
    let _ = pool.disconnect().await;
    admin
        .query_drop(format!("DROP DATABASE {database}"))
        .await
        .expect("drop test database");
    drop(admin);
    let _ = admin_pool.disconnect().await;
    result.expect("configured MySQL migration lock namespace");
}
