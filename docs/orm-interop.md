# ORM interop (§9.4b)

Transactional enqueue is the headline feature, and it is worth nothing if it cannot join
the transaction the application already has open. A service on GORM, Bun, or SeaORM has
its transaction *first* and reaches for the queue second; a queue that insists on opening
the transaction itself has not solved the problem, it has moved it.

This document records what the `Transactional` port actually accepts, per ecosystem. Every
claim below is backed by a test in the conformance matrix:

| | Postgres | MySQL |
|---|---|---|
| **Rust** | [`crates/headgate-postgres/tests/orm_interop.rs`](../crates/headgate-postgres/tests/orm_interop.rs) | [`crates/headgate-mysql/tests/orm_interop.rs`](../crates/headgate-mysql/tests/orm_interop.rs) |
| **Go** | [`go/driver/headgatepgx/orm_interop_test.go`](../go/driver/headgatepgx/orm_interop_test.go) | [`go/driver/headgatemysql/orm_interop_test.go`](../go/driver/headgatemysql/orm_interop_test.go) |

Each cell runs the same three cases:

- **(a) commit** — an application-table write and an enqueue in one caller-owned
  transaction; after `COMMIT` both rows exist *and* the job is actually admitted by the
  gate. Visible is not enough: a row that commits but never passes admission is a silent
  stall.
- **(b) rollback** — the same transaction aborted. Neither the application row nor the job
  exists, and the job is not admittable. This is the assertion the feature exists for: a
  job that survives its caller's rollback has published work that never happened.
- **(c) handler side** — the effect-key claim, an application write, and the
  fence-verified completion in one caller transaction (§5.6, the machinery behind `Once`).
  A crash *after* that commit re-delivers the job; the redelivery claims nothing and
  writes nothing, so the effect is applied exactly once.

## The unit of compatibility is the driver handle, not the ORM

No ORM crate or module is a dependency of headgate, and none is needed. Every ORM in the
survey is a layer over a driver handle, and that handle is what the port accepts. If your
ORM can hand you its `*sql.Tx` / `pgx.Tx` / `tokio_postgres::Transaction`, it works; if it
cannot, no amount of adapter code in headgate will help.

## Go

Both drivers export a `WrapTx` that lends the application's transaction to headgate.
Ownership does not transfer — the caller still commits or rolls back.

```go
// database/sql (MySQL)
tx, _ := db.BeginTx(ctx, nil)
tx.ExecContext(ctx, `INSERT INTO orders ...`)
store.EnqueueTx(ctx, headgatemysql.WrapTx(tx), batch)
tx.Commit()          // or tx.Rollback(): the job goes with it

// pgx (Postgres)
tx, _ := pool.Begin(ctx)
tx.Exec(ctx, `INSERT INTO orders ...`)
store.EnqueueTx(ctx, headgatepgx.WrapTx(tx), batch)
tx.Commit(ctx)
```

Reaching the handle from each ORM:

| Library | How to get the handle | Then |
|---|---|---|
| **database/sql** | you already have `*sql.Tx` | `headgatemysql.WrapTx(tx)` |
| **pgx** | you already have `pgx.Tx` | `headgatepgx.WrapTx(tx)` |
| **GORM** | `tx.Statement.ConnPool.(*sql.Tx)` | `headgatemysql.WrapTx(...)` |
| **Bun** | `bunTx.Tx` (a `bun.Tx` embeds `*sql.Tx`) | `headgatemysql.WrapTx(...)` |
| **sqlc** | generated methods take a `DBTX` interface satisfied by `*sql.Tx` / `pgx.Tx` | pass the same handle to sqlc and to `WrapTx` |

`headgatemysql.WrapTx` was added with this matrix. `EnqueueTx` previously accepted only a
handle headgate had opened itself, so the Go×MySQL cell — the GORM/Bun case — could not be
expressed at all. The pgx driver has exported `WrapTx` since it landed.

For Postgres, GORM and Bun can also be run over the `pgx` stdlib driver, in which case the
handle is a `*sql.Tx` rather than a `pgx.Tx`. **The matrix does not cover that path** —
`headgatepgx` accepts `pgx.Tx` only. A `database/sql`-over-pgx application on Postgres has
to use `pgx` directly for the transactions that enqueue.

## Rust

The interop path is the public generic `enqueue_on`, which is the same code the plain
`enqueue` and the `dyn`-path `enqueue_tx` run:

```rust
// tokio-postgres
let tx = client.transaction().await?;                 // the caller's transaction
tx.execute("INSERT INTO orders ...", &[]).await?;
store.enqueue_on(&tx, &batch).await?;                 // headgate joins it
tx.commit().await?;                                   // or rollback()

// mysql_async
let mut tx = conn.start_transaction(TxOpts::default()).await?;
tx.exec_drop("INSERT INTO orders ...", ()).await?;
store.enqueue_on(&mut tx, &batch).await?;
tx.commit().await?;
```

`PgStore::enqueue_on` is generic over `tokio_postgres::GenericClient` and
`MysqlStore::enqueue_on` over `mysql_async::prelude::Queryable`, so any handle those
traits cover works — pooled connection, raw connection, or transaction.

**The handler-side surface is narrower, and the matrix says so.** `complete_tx`,
`claim_effect`, and `checkpoint_tx` take `&mut dyn TxHandle` and downcast to headgate's own
`PgTx` / `MysqlTx`; there is no `WrapTx` for them. Case (c) therefore opens the transaction
with `store.begin()` and does the application's writes through the native handle those
types expose — `PgTx::client() -> &tokio_postgres::Client` and
`MysqlTx::conn() -> &mut mysql_async::Conn`. The caller still owns commit and rollback, so
the atomicity claim holds; what is *not* available in Rust is joining a transaction the
application opened itself for the completion half. Enqueue-side interop is unrestricted.

### sqlx and SeaORM: honestly, no

`sqlx` implements the Postgres and MySQL wire protocols itself. It does not wrap
tokio-postgres or mysql_async, and `sqlx::Transaction` / `PgConnection` expose no accessor
that yields one — so there is no raw-connection unwrap to perform, and none of the code
above can be reached from a sqlx transaction. SeaORM is built on sqlx, so
`DatabaseTransaction` inherits the same limitation.

**The matrix makes no claim about sqlx or SeaORM.** The two honest options for those stacks
are:

1. Use `tokio-postgres` or `mysql_async` for the specific transactions that must include an
   enqueue, and keep sqlx/SeaORM for everything else. Two pools, one of them small.
2. Enqueue *after* the commit, on a separate connection, and accept that a crash in the
   window loses the job — which is precisely the failure transactional enqueue exists to
   remove. Do not write your own `INSERT INTO headgate_job`: uniqueness (§4.4), the
   quarantine check (§5.2), active-partition maintenance (§5.3/§13), and the arrival
   counters (§5.5) are all part of `enqueue`, and a hand-rolled insert silently skips them.

Closing this properly means a headgate adapter written against sqlx, not a shim inside the
existing ones. That is not in this round and is not claimed anywhere.

## What the matrix does not prove

- No ORM's own API is exercised — only the driver handles they expose. The GORM and Bun
  rows above are documentation of where the handle lives, not tests.
- Redis is absent by design: it declines `Transactional` rather than approximating it
  (§3.1).
- `database/sql`-over-pgx on Postgres, and sqlx/SeaORM on either backend, are named as
  gaps above rather than covered.
