# Testing headgate

There are two deliberately different test boundaries:

| Need | Rust | Go |
|---|---|---|
| Fast handler, retry, fence, clock, or runner test with no services | `headgate_testkit::MemStore` | `headgatetest.MemStore` |
| Backend behavior, SQL transactions, migrations, or Redis scripts | `PostgresTestDatabase`, `MysqlTestDatabase`, `RedisTestNamespace` | `Create*TestDatabase` / `Require*TestDatabase`, `CreateRedisTestNamespace` / `RequireRedisTestNamespace` |

The in-memory store is not a pretend SQL server. Its capability mask honestly omits
transactions, inspection, and notifications. Use a live helper when the behavior under
test depends on one of those capabilities or on a backend's atomic gate.

## Isolation contract

Each helper creates one boundary that no sibling test owns:

- PostgreSQL: a generated schema, migrated through the migrator's explicit-schema API,
  plus connection configuration that sets that schema as the startup `search_path` for
  arbitrary test SQL. Production stores use explicit qualification instead.
- MySQL: a generated database, migrated with `headgate-migrate`, plus ready-to-use
  connection options or a DSN.
- Redis: a generated key prefix and client. Cleanup uses `SCAN` and batches of at most
  100 `DEL` arguments over `{prefix}:*`. It never runs `KEYS`, `FLUSHDB`, or `FLUSHALL`.

Generated names contain the process id and an atomic process-local sequence. A stale SQL
namespace therefore makes creation fail rather than silently sharing state. All six live
language/backend tests create two helpers concurrently, write into one, clean it, and
prove the sibling remains usable.

The PostgreSQL role needs `CREATE` on the database and permission to create/drop schemas.
The MySQL account needs `CREATE` and `DROP` database privileges. The Redis account needs
`SCAN` and key-level `DEL` permission for the chosen server. Use a dedicated test server
or account; these helpers are test infrastructure, not tenant isolation.

## Rust

```rust
use headgate_testkit::PostgresTestDatabase;

let database = PostgresTestDatabase::create(&postgres_conninfo).await?;
let config = database.config(); // give this to the test's pool/store

// Drop every pool/connection first, then remove only this generated schema.
database.cleanup().await?;
```

`MysqlTestDatabase::opts()` returns `mysql_async::Opts` for a pool.
`RedisTestNamespace::client()` and `prefix()` plug directly into
`headgate_redis::RedisStore::new`; `connection_manager()` is a convenience when the store
should own a manager. Rust cleanup consumes the helper, making double cleanup impossible.

## Go

```go
database := headgatetest.RequirePostgresTestDatabase(t, ctx, os.Getenv("HG_TEST_PG"))
conn, err := database.Connect(ctx)
if err != nil {
    t.Fatal(err)
}
defer conn.Close(ctx)
```

The `Require*` forms register idempotent cleanup with `testing.TB.Cleanup`; use the
`Create*` forms when setup errors need custom handling. `MySQLTestDatabase.Open` returns a
`database/sql` handle. `RedisTestNamespace.Client` and `Prefix` plug into
`headgateredis.New`.

## Running the live helper tests

The tests skip explicitly when their backend variable is absent:

```bash
HG_TEST_PG='host=127.0.0.1 port=5433 user=postgres dbname=hg' \
HG_TEST_MYSQL='mysql://root:password@127.0.0.1:3307/hg' \
HG_TEST_REDIS='redis://127.0.0.1:6380' \
cargo test -p headgate-testkit --test database_postgres \
  --test database_mysql --test database_redis

cd go/headgatetest
HG_TEST_PG='host=127.0.0.1 port=5433 user=postgres dbname=hg' \
HG_TEST_MYSQL='mysql://root:password@127.0.0.1:3307/hg' \
HG_TEST_REDIS='redis://127.0.0.1:6380' \
go test -v ./...
```

Do not add raw schema-file loading to individual tests. The helpers intentionally call
the production migration libraries so a test database cannot drift from an installed
database.
