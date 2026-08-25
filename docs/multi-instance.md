# Multiple headgate instances

An instance boundary includes jobs, policy state, singleton duties, schedules, workers,
operations, migration history, and wakeups. Configuring only the job table is not
isolation. headgate uses one backend-native boundary and applies it to every one of those
objects.

## Postgres: one schema per instance

Create and migrate each schema explicitly:

```bash
psql "$DATABASE_URL" <<'SQL'
CREATE SCHEMA "billing-jobs";
CREATE SCHEMA "email-jobs";
SQL

hg-migrate --database-url "$DATABASE_URL" --schema billing-jobs up
hg-migrate --database-url "$DATABASE_URL" --schema email-jobs up
```

Construct the store with the same schema. Rust offers
`PgStore::in_schema`, `with_options_in_schema`, `connect_in_schema`, and
`connect_in_schema_with_options`; Go offers `headgatepgx.NewInSchema` and
`ConnectInSchema`.

```rust
let billing = PgStore::connect_in_schema(&conninfo, 16, "billing-jobs")?;
```

```go
pool, err := pgxpool.New(ctx, conninfo)
if err != nil { return err }
billing, err := headgatepgx.NewInSchema(pool, "billing-jobs")
```

The same caller-owned pool may be passed to stores for different schemas. Every durable
object is explicitly quoted and qualified at the query boundary; no connection's
`search_path` is changed or trusted. This avoids connection-state leakage through shared
pools and remains correct behind PgBouncer transaction pooling. A schema-specific hashed
LISTEN channel also prevents one instance's enqueue from waking another instance.

The schema must already exist. A name may contain characters that require quoting, but it
must be non-empty, contain no NUL, and fit Postgres's 63-byte identifier limit. Invalid
names fail at construction/CLI parsing rather than being truncated onto another instance.

Run every migration operation with `--schema`, including `validate`, `adopt`, and `down`.
Rolling down one schema never operates on another schema's history or objects.

## MySQL: one database per instance

MySQL's instance boundary is the selected database. Give each instance a distinct
database and construct its store from that database's URL/DSN:

```text
mysql://headgate:secret@db:3306/billing_jobs
mysql://headgate:secret@db:3306/email_jobs
```

Migration locks are named from `DATABASE()`, schema validation filters that exact
database, and all unqualified store SQL resolves inside the connection's selected
database. There is deliberately no table-prefix mode that would leave duties or migration
history ambiguous.

## Redis: one key prefix per instance

Redis has no DDL schema. Its existing explicit prefix is the complete boundary:

```rust
let billing = RedisStore::connect(&url, "billing-jobs").await?;
```

```go
billing, err := headgateredis.Connect(url, "billing-jobs")
```

Use a unique, stable prefix per production instance. Do not use `FLUSHDB` for cleanup;
the test helpers clean only `{prefix}:*` keys with cursor-based scans.

## Permissions and security boundary

The Postgres migration role needs permission to create/drop objects in each configured
schema; the runtime role needs access only to its schema. The MySQL role needs access to
its selected database. Redis ACLs should restrict each deployment to its prefix where the
server supports key patterns.

Separate schemas/databases/prefixes prevent accidental queue-state collision. They are
not, by themselves, a hostile-tenant security boundary when the same credentials can read
all instances. Use distinct least-privilege credentials when cross-instance reads must be
forbidden even after an application compromise.

The live regression tests use identical job IDs, queue names, and duty names in two
instances, admit independently, then destructively roll down one SQL installation and
verify the sibling remains valid and readable in all four Rust/Go × Postgres/MySQL cells.
