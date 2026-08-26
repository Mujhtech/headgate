# Schema migrations

headgate ships the same embedded migration line in two independently usable packages:

- Rust: `headgate-migrate`, with the `hg-migrate` binary.
- Go: `github.com/mujhtech/headgate/headgatemigrate`, with
  `go run ./cmd/hg-migrate` from that module.

Both packages support Postgres and MySQL. Redis has no DDL schema and is deliberately not
presented as a migration backend.

## The contract

`headgate_schema_migration` records a line (`main`), monotonically increasing version,
name, immutable SHA-256 checksum, and the store-clock application time. Validation fails
when:

- an applied version is missing, out of order, or newer than the binary;
- a historical migration's name or bytes changed;
- the database is behind the embedded latest version; or
- a required table, column, index, trigger, state value, or saturation value is absent.

Postgres applies each version and its history row in one transaction while holding an
exclusive lock on the history table. No fixed advisory-lock number is used, so the
migrator cannot collide with an application's advisory-lock namespace.

MySQL DDL commits implicitly. The MySQL runner therefore acquires a connection-scoped
lock named for the configured lock namespace and current database, executes idempotent
statements, validates the resulting schema, and only then records the version. A crash
before the history insert resumes safely; it is never disguised as a successful migration.

Migration SQL is copied below each independently publishable package root. This is a
packaging necessity, not permission to drift: `scripts/check-migrations.py` requires the
driver, Rust, and Go assets to remain byte-identical and the up/down version sets to remain
contiguous.

## CLI

Rust examples:

```bash
hg-migrate --database-url "$DATABASE_URL" up
hg-migrate --database-url "$DATABASE_URL" validate
hg-migrate --database-url "$DATABASE_URL" list
hg-migrate --backend postgres get --version 1 --up
hg-migrate --database-url "$DATABASE_URL" down --target-version 0 --dry-run
hg-migrate --database-url "$DATABASE_URL" down --target-version 0 --confirm
```

The Go command accepts the same flags and output contract:

```bash
go run ./cmd/hg-migrate --database-url "$DATABASE_URL" up
```

`HG_DATABASE_URL` and `DATABASE_URL` are fallback environment variables. The backend is
inferred from `postgres://`, `postgresql://`, or `mysql://`; libpq keyword conninfo and
native MySQL DSNs require `--backend`.

Common controls:

- `--target-version N` chooses the version after the operation.
- `--max-steps N` bounds work in one invocation.
- `--dry-run` returns the exact ordered plan without creating history or changing DDL.
- `get --version N --up|--down` emits raw SQL for Atlas, Flyway, or review.
- a real `down` requires `--confirm`; down migrations are destructive and offline.

## Migration lock namespace

Postgres uses a lock on the installation's qualified migration-history table. It takes no
advisory lock, so there is no process-global integer key that can collide with application
code; the CLI rejects `--lock-namespace` for Postgres instead of pretending to configure a
lock that does not exist.

MySQL uses `GET_LOCK`, whose names share a server-wide namespace with application locks.
Its backward-compatible default is `headgate:migrate:<database>`. If an application or a
second headgate installation could use that prefix, choose one stable namespace per
installation:

```bash
hg-migrate --database-url "$MYSQL_URL" --lock-namespace billing-jobs up
hg-migrate --database-url "$MYSQL_URL" --lock-namespace billing-jobs down --confirm
hg-migrate --database-url "$MYSQL_URL" --lock-namespace billing-jobs adopt --confirm
```

The namespace is 1–31 ASCII bytes, begins with an alphanumeric byte, and otherwise accepts
letters, digits, `_`, `-`, and `.`. The database remains part of the key, so the same
namespace may be reused across distinct MySQL databases. A name longer than MySQL's
64-byte limit uses a bounded SHA-256 form with a distinct `:h:` marker; it cannot alias the
readable key of a short database.

Every concurrent migrator for one installation must use the same namespace. Deliberately
different namespaces are deliberately different locks; using them against the same MySQL
database would bypass serialization. The flag is therefore accepted only by `up`, `down`,
and `adopt`, and invalid/read-only/cross-backend uses fail during CLI parsing rather than
being silently ignored.

For an explicitly isolated Postgres installation, create the schema first and pass the
same name to every migration command:

```bash
psql "$DATABASE_URL" -c 'CREATE SCHEMA "billing-jobs"'
hg-migrate --database-url "$DATABASE_URL" --schema 'billing-jobs' up
hg-migrate --database-url "$DATABASE_URL" --schema 'billing-jobs' validate
hg-migrate --backend postgres --schema 'billing-jobs' get --version 1 --up
```

`--schema` is Postgres-only. It explicitly quotes and qualifies every headgate relation
and type; it does not set or trust `search_path`, so it is safe when connections are
shared or transaction-pooled. The schema must already exist and names longer than
Postgres's 63-byte identifier limit are rejected instead of silently truncated. MySQL
instances are selected by the database in the URL/DSN.

Re-running `up` at the current version is a successful no-op. Concurrent runners
serialize and re-read history after acquiring the lock, so a waiter also becomes a no-op
instead of applying the same version twice.

## Existing unversioned installations

The migrator refuses to apply version 1 over existing `headgate_*` tables. Treating an
unknown hand-installed schema as fresh would either fail halfway (Postgres) or, worse,
let `CREATE TABLE IF NOT EXISTS` bless a partial MySQL table.

Use this sequence:

```bash
hg-migrate --database-url "$DATABASE_URL" validate
hg-migrate --database-url "$DATABASE_URL" adopt --confirm
hg-migrate --database-url "$DATABASE_URL" validate
```

`adopt` writes history only when the complete current manifest passes. It does not repair
or infer missing DDL. For example, an older schema without
`headgate_queue_state.dispatch_count` is rejected by name. Repair or restore the schema
deliberately, validate again, then adopt. This strictness is the direct regression guard
for the round-32m MySQL drift incident.

## Online-safety ledger

| Backend | Version | Name | Up online-safe? | Down online-safe? | Reason |
|---|---:|---|---|---|---|
| Postgres | 1 | `initial_schema` | No | No | Fresh install; creates the enum, tables, and non-concurrent indexes |
| MySQL | 1 | `initial_schema` | No | No | Fresh install; DDL auto-commits and down drops all headgate data tables |
| Postgres | 2 | `enqueue_backpressure` | No | No | One-time unfinished-depth backfill plus trigger installation; run with producers stopped |
| MySQL | 2 | `enqueue_backpressure` | No | No | One-time unfinished-depth backfill and auto-committing trigger DDL; run with producers stopped |

An online-safe future version must say why in this table and in both languages'
`Migration.OnlineSafe` metadata. Additive does not automatically mean online-safe: a
table rewrite, blocking index build, or new non-null column without a safe backfill is
offline even when no object is dropped.

## Library calls

Rust exposes `migrate_postgres`, `migrate_mysql`, `validate_*`, `applied_*`, and
`adopt_*`, plus the pure `plan` function and embedded `Migration` metadata. Go exposes
the corresponding `MigratePostgres`, `MigrateMySQL`, `Validate*`, `Applied*`,
`Adopt*`, and `Plan` functions. Postgres has matching `*_in_schema` Rust calls and
`*PostgresInSchema` Go calls for every applied/validate/migrate/adopt operation.
MySQL callers that need a non-default named-lock boundary use Rust
`migrate_mysql_with_lock_namespace` / `adopt_mysql_with_lock_namespace` or Go
`MigrateMySQLWithLockNamespace` / `AdoptMySQLWithLockNamespace`; the original calls
delegate to the documented `headgate` default.

See [`multi-instance.md`](multi-instance.md) for store construction and the isolation
contract across Postgres, MySQL, and Redis.

The live tests create isolated temporary namespaces and exercise fresh up, validation,
dry-run, destructive down, reinstall, checksum tampering, unversioned refusal, adoption,
and missing-column rejection in all four language/backend cells:

```bash
HG_TEST_PG='host=127.0.0.1 port=5433 user=postgres dbname=hg' \
HG_TEST_MYSQL='mysql://root:password@127.0.0.1:3306/hg' \
cargo test -p headgate-migrate --test live

cd go/headgatemigrate
HG_TEST_PG='host=127.0.0.1 port=5433 user=postgres dbname=hg' \
HG_TEST_MYSQL='mysql://root:password@127.0.0.1:3306/hg' \
go test -run TestLive -v ./...
```
