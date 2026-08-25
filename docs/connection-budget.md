# Connection budget

headgate borrows from caller-owned pools; it never creates one connection per worker or
per handler. The production sizing rule for either SQL backend is:

```text
T = maximum simultaneous transaction-holding handler callbacks sharing the pool
P = T + 2                         recommended command-pool size

Postgres physical connections = P + L
L = 1 per notifying store, or 0 for a poll-only store

MySQL physical connections = P
```

`T` includes `once`, `step_once`, and application transactions kept open while handler
code runs. Sum it across every worker that shares the pool; do not multiply the two spare
slots per worker. For example, one process whose shared store can run at most two
transactional callbacks at once should configure a pool of four. Ten ordinary handlers do
not add ten: admission, checkpoints, acks, renewal, heartbeat registration, and duties all
borrow transiently and return the connection before acquiring another.

The two spare slots have different jobs:

1. one keeps lease renewal and worker heartbeat able to move while callbacks hold `T`
   transactions;
2. one carries admission, enqueue, checkpoints, acks, duties, inspectors, and API calls.

This is a reliability budget, not a claim that a smaller pool always deadlocks. The store
has no internal path that holds a pooled connection while acquiring a second one, so a
smaller pool queues callers. But if all slots are held by long transactions, renewal may
wait past lease expiry even though the process is technically making progress. The
existing two-connection stress tests prove deadlock freedom; the `T + 2` tests prove the
lease-safe operating rule.

## Backend accounting

Postgres push wakeup uses one dedicated `LISTEN` connection outside the pool and fans it
out to every worker using that store. `PgStore::new` / `headgatepgx.New` is poll-only and
has `L = 0`; `connect`, `with_listen`, or `WithListen` has `L = 1`. The increment is per
store instance, not per worker. Share one store when workers share one pool; constructing
five notifying stores over it spends five listeners.

MySQL has no push wakeup and no connection outside its pool. Its migration runner pins one
connection while it holds `GET_LOCK`; run migrations from a deployment connection or
include that slot if deliberately sharing the runtime pool.

Redis has no transactional handler surface. In Rust, ordinary commands share the supplied
multiplexed `ConnectionManager` and wakeup adds one dedicated pub/sub connection. In Go,
ordinary commands are bounded by the supplied go-redis client's `PoolSize`; `WithWake`
uses the separately supplied wake client and holds one subscription connection. These are
caller-client budgets, not SQL's `T + 2` formula.

## Configuration examples

Rust Postgres (pool four, plus one listener):

```rust
let manager = deadpool_postgres::Manager::new(pg_config.clone(), tokio_postgres::NoTls);
let pool = deadpool_postgres::Pool::builder(manager).max_size(4).build()?;
let store = headgate_postgres::PgStore::new(pool).with_listen(pg_config);
```

Go Postgres:

```go
cfg, _ := pgxpool.ParseConfig(databaseURL)
cfg.MaxConns = 4
pool, _ := pgxpool.NewWithConfig(ctx, cfg)
store := headgatepgx.New(pool).WithListen(databaseURL)
```

Rust MySQL:

```rust
let limits = mysql_async::PoolConstraints::new(0, 4).unwrap();
let pool_opts = mysql_async::PoolOpts::default().with_constraints(limits);
let opts = mysql_async::OptsBuilder::from_opts(opts)
    .client_found_rows(true)
    .pool_opts(pool_opts);
let store = headgate_mysql::MysqlStore::new(mysql_async::Pool::new(opts));
```

Go MySQL:

```go
db.SetMaxOpenConns(4)
db.SetMaxIdleConns(4)
store := headgatemysql.New(db) // DSN must also set clientFoundRows=true
```

Use `deadpool_postgres::Pool::status`, `pgxpool.Pool.Stat`,
`mysql_async::Pool::metrics`, or `sql.DB.Stats` to alert on sustained waiters and wait
duration. A cap prevents connection explosion; it does not make a saturated pool healthy.

## Transaction callback rule

Inside `once` / `step_once`, use the transaction handle supplied to the callback for
application writes and transactional enqueue. Calling a normal store method from inside
the callback asks the same pool for another connection while retaining one. That is an
application-created nested acquisition and can deadlock a fully occupied pool; either use
the supplied transaction or budget explicitly for the nested call.

## Live proof

The Rust and Go Postgres tests and the Rust and Go MySQL tests each run six jobs at
capacity six through a pool capped at four. Two `once` callbacks retain connections for
2.5 seconds with a 900 ms lease. Once both callbacks hold their transaction slots, each
test captures the current store-issued lease deadline, waits for the store clock to pass
that exact deadline, and then requires:

- both held jobs are still running with lease deadlines later than both the captured
  deadline and the current store clock;
- the four plain/step siblings have already completed, proving their acks landed;
- all six singleton duties were acquired by the worker; and
- sampled physical connections never exceeded four (Postgres additionally observes
  exactly one `LISTEN` session and a total no greater than five).

All six jobs must then become durable `completed` rows. A test with no locking, no renewal,
a blocked ack lane, a missing duty loop, or an extra unpooled connection fails a different
witness.
