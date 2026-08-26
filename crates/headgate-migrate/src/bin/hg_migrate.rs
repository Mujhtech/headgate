use std::env;

use headgate_migrate::{
    Backend, DEFAULT_MYSQL_LOCK_NAMESPACE, Direction, MigrateOptions, MigrationError,
    PostgresNamespace, adopt_mysql_with_lock_namespace, adopt_postgres, adopt_postgres_in_schema,
    applied_mysql, applied_postgres, applied_postgres_in_schema, checksum,
    migrate_mysql_with_lock_namespace, migrate_postgres, migrate_postgres_in_schema, migration,
    migrations, mysql_migration_lock_name, validate_mysql, validate_postgres,
    validate_postgres_in_schema,
};
use mysql_async::{Opts, Pool};
use tokio_postgres::NoTls;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Up,
    Down,
    Validate,
    List,
    Get,
    Version,
    Adopt,
}

#[derive(Debug)]
struct Cli {
    backend: Backend,
    database_url: Option<String>,
    schema: Option<String>,
    lock_namespace: Option<String>,
    command: Command,
    options: MigrateOptions,
    get_version: Option<u32>,
    get_direction: Direction,
}

fn usage() -> &'static str {
    "hg-migrate [--database-url URL] [--backend postgres|mysql] [--schema NAME] [--lock-namespace NAME] COMMAND [OPTIONS]\n\
\n\
Commands:\n\
  up          apply versions (default target: latest)\n\
  down        roll versions back; requires --confirm unless --dry-run\n\
  validate    verify history checksums and the current schema manifest\n\
  list        list embedded and applied versions\n\
  get         print one embedded migration's SQL\n\
  version     print current and latest versions\n\
  adopt       record a validated unversioned current schema; requires --confirm\n\
\n\
Options:\n\
  --schema NAME        explicit Postgres schema (qualified; never search_path)\n\
  --lock-namespace N   MySQL migration lock namespace (up/down/adopt only)\n\
  --target-version N   desired version after up/down\n\
  --max-steps N        bound versions applied by this invocation\n\
  --dry-run            plan without creating history or changing schema\n\
  --version N          migration version for get\n\
  --up | --down        SQL direction for get (default: up)\n\
  --confirm            acknowledge a destructive down or schema adoption\n\
\n\
HG_DATABASE_URL and DATABASE_URL are used when --database-url is omitted."
}

fn take_value(args: &mut Vec<String>, index: usize, flag: &str) -> Result<String, String> {
    if index + 1 >= args.len() {
        return Err(format!("{flag} requires a value"));
    }
    args.remove(index);
    Ok(args.remove(index))
}

fn parse_backend(value: &str) -> Result<Backend, String> {
    match value {
        "postgres" | "postgresql" | "pg" => Ok(Backend::Postgres),
        "mysql" => Ok(Backend::Mysql),
        _ => Err(format!("unknown backend {value:?}; want postgres or mysql")),
    }
}

fn infer_backend(url: &str) -> Option<Backend> {
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Some(Backend::Postgres)
    } else if url.starts_with("mysql://") {
        Some(Backend::Mysql)
    } else {
        None
    }
}

fn parse_cli(mut args: Vec<String>) -> Result<Cli, String> {
    if args.is_empty() || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Err(usage().into());
    }
    let mut database_url = None;
    let mut backend = None;
    let mut schema = None;
    let mut lock_namespace = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--database-url" => {
                database_url = Some(take_value(&mut args, index, "--database-url")?)
            }
            "--backend" => {
                let value = take_value(&mut args, index, "--backend")?;
                backend = Some(parse_backend(&value)?);
            }
            "--schema" => schema = Some(take_value(&mut args, index, "--schema")?),
            "--lock-namespace" => {
                lock_namespace = Some(take_value(&mut args, index, "--lock-namespace")?)
            }
            _ => index += 1,
        }
    }
    if database_url.is_none() {
        database_url = env::var("HG_DATABASE_URL")
            .ok()
            .or_else(|| env::var("DATABASE_URL").ok());
    }

    let Some(command_name) = args.first().cloned() else {
        return Err(format!("missing command\n\n{}", usage()));
    };
    args.remove(0);
    let command = match command_name.as_str() {
        "up" => Command::Up,
        "down" => Command::Down,
        "validate" => Command::Validate,
        "list" => Command::List,
        "get" => Command::Get,
        "version" => Command::Version,
        "adopt" => Command::Adopt,
        _ => return Err(format!("unknown command {command_name:?}\n\n{}", usage())),
    };

    let mut options = MigrateOptions::default();
    let mut get_version = None;
    let mut get_direction = Direction::Up;
    let mut confirm = false;
    let index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--target-version" => {
                let value = take_value(&mut args, index, "--target-version")?;
                options.target_version = Some(value.parse().map_err(|_| {
                    format!("invalid --target-version {value:?}; want a non-negative integer")
                })?);
            }
            "--max-steps" => {
                let value = take_value(&mut args, index, "--max-steps")?;
                options.max_steps = Some(value.parse().map_err(|_| {
                    format!("invalid --max-steps {value:?}; want a non-negative integer")
                })?);
            }
            "--version" => {
                let value = take_value(&mut args, index, "--version")?;
                get_version = Some(value.parse().map_err(|_| {
                    format!("invalid --version {value:?}; want a positive integer")
                })?);
            }
            "--dry-run" => {
                options.dry_run = true;
                args.remove(index);
            }
            "--confirm" => {
                confirm = true;
                args.remove(index);
            }
            "--up" => {
                get_direction = Direction::Up;
                args.remove(index);
            }
            "--down" => {
                get_direction = Direction::Down;
                args.remove(index);
            }
            unknown => return Err(format!("unknown option {unknown:?}")),
        }
    }

    if command == Command::Get && get_version.is_none() {
        return Err("get requires --version N".into());
    }
    if command == Command::Down && !options.dry_run && !confirm {
        return Err("down is destructive; pass --confirm (or inspect it with --dry-run)".into());
    }
    if command == Command::Adopt && !confirm {
        return Err(
            "adopt writes migration history; pass --confirm after reviewing validate".into(),
        );
    }
    let backend = backend
        .or_else(|| database_url.as_deref().and_then(infer_backend))
        .ok_or_else(|| "cannot infer backend; pass --backend postgres|mysql".to_string())?;
    if command != Command::Get && database_url.is_none() {
        return Err("missing --database-url (or HG_DATABASE_URL / DATABASE_URL)".into());
    }
    if schema.is_some() && backend != Backend::Postgres {
        return Err("--schema is Postgres-only; select a MySQL database in its URL".into());
    }
    if let Some(name) = &schema {
        PostgresNamespace::explicit(name)?;
    }
    if lock_namespace.is_some() && backend != Backend::Mysql {
        return Err(
            "--lock-namespace is MySQL-only; Postgres uses schema-local table locks".into(),
        );
    }
    if lock_namespace.is_some() && !matches!(command, Command::Up | Command::Down | Command::Adopt)
    {
        return Err("--lock-namespace applies only to MySQL up, down, and adopt".into());
    }
    if let Some(namespace) = &lock_namespace {
        mysql_migration_lock_name(namespace, "validation").map_err(|error| error.to_string())?;
    }
    Ok(Cli {
        backend,
        database_url,
        schema,
        lock_namespace,
        command,
        options,
        get_version,
        get_direction,
    })
}

fn print_steps(result: &headgate_migrate::MigrateResult) {
    if result.steps.is_empty() {
        println!("no-op: already at target version");
        return;
    }
    for step in &result.steps {
        println!(
            "{} version={} name={} online_safe={}{}",
            step.direction,
            step.migration.version,
            step.migration.name,
            step.migration.online_safe,
            if result.dry_run { " dry_run=true" } else { "" }
        );
    }
}

async fn run_postgres(cli: &Cli, url: &str) -> Result<(), MigrationError> {
    let (mut client, connection) = tokio_postgres::connect(url, NoTls).await?;
    let driver = tokio::spawn(async move { connection.await });
    match cli.command {
        Command::Up => {
            let result = match cli.schema.as_deref() {
                Some(schema) => {
                    migrate_postgres_in_schema(&mut client, schema, Direction::Up, cli.options)
                        .await?
                }
                None => migrate_postgres(&mut client, Direction::Up, cli.options).await?,
            };
            print_steps(&result)
        }
        Command::Down => {
            let result = match cli.schema.as_deref() {
                Some(schema) => {
                    migrate_postgres_in_schema(&mut client, schema, Direction::Down, cli.options)
                        .await?
                }
                None => migrate_postgres(&mut client, Direction::Down, cli.options).await?,
            };
            print_steps(&result)
        }
        Command::Validate => {
            let validation = match cli.schema.as_deref() {
                Some(schema) => validate_postgres_in_schema(&client, schema).await?,
                None => validate_postgres(&client).await?,
            };
            if !validation.is_ok() {
                return Err(MigrationError::Schema(validation.messages));
            }
            println!(
                "ok backend=postgres current={} latest={}",
                validation.current_version, validation.latest_version
            );
        }
        Command::List => {
            let (_, applied) = match cli.schema.as_deref() {
                Some(schema) => applied_postgres_in_schema(&client, schema).await?,
                None => applied_postgres(&client).await?,
            };
            for item in migrations(Backend::Postgres) {
                println!(
                    "version={} name={} checksum={} online_safe={} applied={}",
                    item.version,
                    item.name,
                    checksum(item),
                    item.online_safe,
                    applied.iter().any(|row| row.version == item.version)
                );
            }
        }
        Command::Version => {
            let (_, applied) = match cli.schema.as_deref() {
                Some(schema) => applied_postgres_in_schema(&client, schema).await?,
                None => applied_postgres(&client).await?,
            };
            println!(
                "current={} latest={}",
                applied.last().map_or(0, |row| row.version),
                headgate_migrate::latest_version(Backend::Postgres)
            );
        }
        Command::Adopt => {
            let adopted = match cli.schema.as_deref() {
                Some(schema) => adopt_postgres_in_schema(&mut client, schema).await?,
                None => adopt_postgres(&mut client).await?,
            };
            println!(
                "adopted version={}",
                adopted.last().map_or(0, |row| row.version)
            );
        }
        Command::Get => unreachable!(),
    }
    drop(client);
    driver
        .await
        .map_err(|error| MigrationError::Invalid(format!("postgres connection task: {error}")))??;
    Ok(())
}

async fn run_mysql(cli: &Cli, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::from_url(url)?;
    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    let lock_namespace = cli
        .lock_namespace
        .as_deref()
        .unwrap_or(DEFAULT_MYSQL_LOCK_NAMESPACE);
    match cli.command {
        Command::Up => print_steps(
            &migrate_mysql_with_lock_namespace(
                &mut conn,
                Direction::Up,
                cli.options,
                lock_namespace,
            )
            .await?,
        ),
        Command::Down => print_steps(
            &migrate_mysql_with_lock_namespace(
                &mut conn,
                Direction::Down,
                cli.options,
                lock_namespace,
            )
            .await?,
        ),
        Command::Validate => {
            let validation = validate_mysql(&mut conn).await?;
            if !validation.is_ok() {
                return Err(MigrationError::Schema(validation.messages).into());
            }
            println!(
                "ok backend=mysql current={} latest={}",
                validation.current_version, validation.latest_version
            );
        }
        Command::List => {
            let (_, applied) = applied_mysql(&mut conn).await?;
            for item in migrations(Backend::Mysql) {
                println!(
                    "version={} name={} checksum={} online_safe={} applied={}",
                    item.version,
                    item.name,
                    checksum(item),
                    item.online_safe,
                    applied.iter().any(|row| row.version == item.version)
                );
            }
        }
        Command::Version => {
            let (_, applied) = applied_mysql(&mut conn).await?;
            println!(
                "current={} latest={}",
                applied.last().map_or(0, |row| row.version),
                headgate_migrate::latest_version(Backend::Mysql)
            );
        }
        Command::Adopt => {
            let adopted = adopt_mysql_with_lock_namespace(&mut conn, lock_namespace).await?;
            println!(
                "adopted version={}",
                adopted.last().map_or(0, |row| row.version)
            );
        }
        Command::Get => unreachable!(),
    }
    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = match parse_cli(env::args().skip(1).collect()) {
        Ok(cli) => cli,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(if message == usage() { 0 } else { 2 });
        }
    };
    if cli.command == Command::Get {
        let item = migration(cli.backend, cli.get_version.unwrap()).unwrap_or_else(|| {
            eprintln!(
                "unknown {} migration version {}",
                cli.backend,
                cli.get_version.unwrap()
            );
            std::process::exit(2);
        });
        let sql = match cli.get_direction {
            Direction::Up => item.up_sql,
            Direction::Down => item.down_sql,
        };
        if let Some(schema) = &cli.schema {
            print!(
                "{}",
                PostgresNamespace::explicit(schema).unwrap().render(sql)
            );
        } else {
            print!("{sql}");
        }
        return;
    }
    let url = cli.database_url.as_deref().unwrap();
    let result: Result<(), Box<dyn std::error::Error>> = match cli.backend {
        Backend::Postgres => run_postgres(&cli, url).await.map_err(Into::into),
        Backend::Mysql => run_mysql(&cli, url).await,
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destructive_commands_require_explicit_confirmation() {
        let error = parse_cli(vec![
            "--backend".into(),
            "postgres".into(),
            "--database-url".into(),
            "postgres://localhost/x".into(),
            "down".into(),
        ])
        .unwrap_err();
        assert!(error.contains("--confirm"));

        let dry = parse_cli(vec![
            "--backend".into(),
            "postgres".into(),
            "--database-url".into(),
            "postgres://localhost/x".into(),
            "down".into(),
            "--dry-run".into(),
        ])
        .unwrap();
        assert!(dry.options.dry_run);
    }

    #[test]
    fn get_needs_only_an_explicit_backend() {
        let cli = parse_cli(vec![
            "--backend".into(),
            "mysql".into(),
            "get".into(),
            "--version".into(),
            "1".into(),
            "--down".into(),
        ])
        .unwrap();
        assert_eq!(cli.backend, Backend::Mysql);
        assert_eq!(cli.get_direction, Direction::Down);
        assert!(cli.database_url.is_none());
    }

    #[test]
    fn postgres_schema_is_validated_and_mysql_rejects_it() {
        let cli = parse_cli(vec![
            "--backend".into(),
            "postgres".into(),
            "--schema".into(),
            "tenant-\"blue".into(),
            "get".into(),
            "--version".into(),
            "1".into(),
        ])
        .unwrap();
        assert_eq!(cli.schema.as_deref(), Some("tenant-\"blue"));

        let mysql = parse_cli(vec![
            "--backend".into(),
            "mysql".into(),
            "--schema".into(),
            "tenant".into(),
            "get".into(),
            "--version".into(),
            "1".into(),
        ])
        .unwrap_err();
        assert!(mysql.contains("Postgres-only"));

        let too_long = parse_cli(vec![
            "--backend".into(),
            "postgres".into(),
            "--schema".into(),
            "x".repeat(64),
            "get".into(),
            "--version".into(),
            "1".into(),
        ])
        .unwrap_err();
        assert!(too_long.contains("63"));
    }

    #[test]
    fn mysql_lock_namespace_is_scoped_validated_and_command_specific() {
        let cli = parse_cli(vec![
            "--backend".into(),
            "mysql".into(),
            "--database-url".into(),
            "mysql://localhost/jobs".into(),
            "up".into(),
            "--lock-namespace".into(),
            "billing.v2".into(),
        ])
        .unwrap();
        assert_eq!(cli.lock_namespace.as_deref(), Some("billing.v2"));

        let postgres = parse_cli(vec![
            "--backend".into(),
            "postgres".into(),
            "--database-url".into(),
            "postgres://localhost/jobs".into(),
            "up".into(),
            "--lock-namespace".into(),
            "billing".into(),
        ])
        .unwrap_err();
        assert!(postgres.contains("MySQL-only"));

        let read_only = parse_cli(vec![
            "--backend".into(),
            "mysql".into(),
            "get".into(),
            "--version".into(),
            "1".into(),
            "--lock-namespace".into(),
            "billing".into(),
        ])
        .unwrap_err();
        assert!(read_only.contains("up, down, and adopt"));

        let invalid = parse_cli(vec![
            "--backend".into(),
            "mysql".into(),
            "--database-url".into(),
            "mysql://localhost/jobs".into(),
            "up".into(),
            "--lock-namespace".into(),
            "bad:scope".into(),
        ])
        .unwrap_err();
        assert!(invalid.contains("1-31 ASCII bytes"));
    }
}
