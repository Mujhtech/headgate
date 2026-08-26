//! Per-test live-store isolation. SQL namespaces are installed through
//! `headgate-migrate`; Redis uses a generated key prefix because its schema is the
//! keyspace, not DDL.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use headgate_migrate::{Direction, MigrateOptions, migrate_mysql, migrate_postgres_in_schema};
use mysql_async::prelude::*;

static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(1);

fn unique_name(backend: &str) -> String {
    // Generated from trusted components only, so it is safe to interpolate as a SQL
    // identifier. Keeping it under 63 bytes also satisfies Postgres's identifier limit.
    format!(
        "hg_test_{backend}_{}_{}",
        std::process::id(),
        NEXT_NAMESPACE.fetch_add(1, Ordering::Relaxed)
    )
}

#[derive(Debug)]
pub struct TestDatabaseError(String);

impl TestDatabaseError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for TestDatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TestDatabaseError {}

async fn pg_connect(
    config: &tokio_postgres::Config,
) -> Result<
    (
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    ),
    TestDatabaseError,
> {
    let (client, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .map_err(|error| TestDatabaseError::new(format!("postgres connect: {error}")))?;
    Ok((client, tokio::spawn(connection)))
}

/// One migrated Postgres schema. Use [`config`](Self::config) to construct a pool/store;
/// it carries the schema in the startup `search_path`, so every pooled connection sees
/// the same isolated namespace.
pub struct PostgresTestDatabase {
    schema: String,
    admin_config: tokio_postgres::Config,
    test_config: tokio_postgres::Config,
}

impl PostgresTestDatabase {
    pub async fn create(conninfo: &str) -> Result<Self, TestDatabaseError> {
        let admin_config: tokio_postgres::Config = conninfo
            .parse()
            .map_err(|error| TestDatabaseError::new(format!("bad Postgres conninfo: {error}")))?;
        let schema = unique_name("pg");
        let (mut admin, admin_task) = pg_connect(&admin_config).await?;
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .map_err(|error| TestDatabaseError::new(format!("create schema {schema}: {error}")))?;

        let mut test_config = admin_config.clone();
        test_config.options(&format!("-c search_path={schema}"));
        let migrated = migrate_postgres_in_schema(
            &mut admin,
            &schema,
            Direction::Up,
            MigrateOptions::default(),
        )
        .await
        .map_err(|error| TestDatabaseError::new(format!("{error}: {error:?}")));
        if let Err(error) = migrated {
            let _ = admin
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await;
            drop(admin);
            let _ = admin_task.await;
            return Err(error);
        }
        drop(admin);
        let _ = admin_task.await;
        Ok(Self {
            schema,
            admin_config,
            test_config,
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn config(&self) -> tokio_postgres::Config {
        self.test_config.clone()
    }

    /// Drop only this helper's generated schema and every object the migrator installed
    /// in it. The helper consumes itself so cleanup cannot accidentally run twice.
    pub async fn cleanup(self) -> Result<(), TestDatabaseError> {
        let (admin, task) = pg_connect(&self.admin_config).await?;
        let result = admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .map_err(|error| TestDatabaseError::new(format!("drop schema: {error}")));
        drop(admin);
        let _ = task.await;
        result
    }
}

/// One migrated MySQL database. [`opts`](Self::opts) can be cloned into a
/// `mysql_async::Pool` without re-parsing or exposing credentials.
pub struct MysqlTestDatabase {
    database: String,
    admin_opts: mysql_async::Opts,
    test_opts: mysql_async::Opts,
}

impl MysqlTestDatabase {
    pub async fn create(url: &str) -> Result<Self, TestDatabaseError> {
        let admin_opts = mysql_async::Opts::from_url(url)
            .map_err(|error| TestDatabaseError::new(format!("bad MySQL URL: {error}")))?;
        let database = unique_name("mysql");
        let admin_pool = mysql_async::Pool::new(admin_opts.clone());
        let mut admin = admin_pool
            .get_conn()
            .await
            .map_err(|error| TestDatabaseError::new(format!("mysql connect: {error}")))?;
        admin
            .query_drop(format!("CREATE DATABASE {database}"))
            .await
            .map_err(|error| {
                TestDatabaseError::new(format!("create database {database}: {error}"))
            })?;

        let test_opts: mysql_async::Opts = mysql_async::OptsBuilder::from_opts(admin_opts.clone())
            .db_name(Some(database.clone()))
            .into();
        let pool = mysql_async::Pool::new(test_opts.clone());
        let migrated = async {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|error| TestDatabaseError::new(format!("mysql connect: {error}")))?;
            migrate_mysql(&mut conn, Direction::Up, MigrateOptions::default())
                .await
                .map_err(|error| TestDatabaseError::new(error.to_string()))
        }
        .await;
        let _ = pool.disconnect().await;
        if let Err(error) = migrated {
            let _ = admin.query_drop(format!("DROP DATABASE {database}")).await;
            drop(admin);
            let _ = admin_pool.disconnect().await;
            return Err(error);
        }
        drop(admin);
        let _ = admin_pool.disconnect().await;
        Ok(Self {
            database,
            admin_opts,
            test_opts,
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn opts(&self) -> mysql_async::Opts {
        self.test_opts.clone()
    }

    pub async fn cleanup(self) -> Result<(), TestDatabaseError> {
        let pool = mysql_async::Pool::new(self.admin_opts);
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|error| TestDatabaseError::new(format!("mysql connect: {error}")))?;
        let result = conn
            .query_drop(format!("DROP DATABASE {}", self.database))
            .await
            .map_err(|error| TestDatabaseError::new(format!("drop database: {error}")));
        drop(conn);
        let _ = pool.disconnect().await;
        result
    }
}

/// A process-unique Redis key prefix. Redis's schema boundary is already an explicit
/// prefix in both drivers, so tests do not need a scarce numbered logical database and
/// never call `FLUSHALL`/`FLUSHDB`.
pub struct RedisTestNamespace {
    prefix: String,
    client: redis::Client,
}

impl RedisTestNamespace {
    pub async fn create(url: &str) -> Result<Self, TestDatabaseError> {
        let client = redis::Client::open(url)
            .map_err(|error| TestDatabaseError::new(format!("bad Redis URL: {error}")))?;
        let namespace = Self {
            prefix: unique_name("redis"),
            client,
        };
        // A generated name should be empty. Checking through SCAN catches the astronomic
        // collision case without introducing KEYS (which is exactly the production trap
        // the queue avoids).
        if namespace.scan_keys().await?.is_empty() {
            Ok(namespace)
        } else {
            Err(TestDatabaseError::new(format!(
                "generated Redis prefix {} already exists",
                namespace.prefix
            )))
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn client(&self) -> redis::Client {
        self.client.clone()
    }

    pub async fn connection_manager(
        &self,
    ) -> Result<redis::aio::ConnectionManager, TestDatabaseError> {
        self.client
            .get_connection_manager()
            .await
            .map_err(|error| TestDatabaseError::new(format!("redis connect: {error}")))
    }

    async fn scan_keys(&self) -> Result<Vec<String>, TestDatabaseError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| TestDatabaseError::new(format!("redis connect: {error}")))?;
        let mut cursor = 0_u64;
        let mut keys = Vec::new();
        loop {
            let (next, mut page): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{}:*", self.prefix))
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .map_err(|error| TestDatabaseError::new(format!("redis scan: {error}")))?;
            keys.append(&mut page);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(keys)
    }

    pub async fn cleanup(self) -> Result<(), TestDatabaseError> {
        let keys = self.scan_keys().await?;
        if keys.is_empty() {
            return Ok(());
        }
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| TestDatabaseError::new(format!("redis connect: {error}")))?;
        for page in keys.chunks(100) {
            redis::cmd("DEL")
                .arg(page)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|error| TestDatabaseError::new(format!("redis cleanup: {error}")))?;
        }
        Ok(())
    }
}
