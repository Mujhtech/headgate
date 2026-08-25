//! caller-owned transaction contract the ORM-interop conformance matrix, Rust × MySQL cell.
//!
//! Same three cases as the Postgres cell (crates/headgate-postgres/tests/orm_interop.rs)
//! against the other transactional backend, so the claim is about the PORT and not about
//! the driver the reference implementation happens to use.
//!
//! The native handle here is `mysql_async::Transaction` — opened by the TEST on its own
//! connection — and the entry point is the public [`MysqlStore::enqueue_on`], generic
//! over `mysql_async::prelude::Queryable`. No ORM crate is added; see docs/orm-interop.md
//! for why a driver handle is the right unit of compatibility.
//!
//! Opt-in via HG_TEST_MYSQL. Run this crate's test binaries ONE AT A TIME — a
//! default-config server has been wedged by full-parallel suites before.

use std::time::Duration;

use headgate_core::{AdmitRequest, Envelope, Store, Transactional};
use headgate_mysql::MysqlStore;
use mysql_async::prelude::*;

fn url() -> Option<String> {
    match std::env::var("HG_TEST_MYSQL") {
        Ok(s) => Some(s),
        Err(_) => {
            eprintln!("HG_TEST_MYSQL not set; skipping ORM interop matrix (mysql)");
            None
        }
    }
}

fn store(url: &str) -> MysqlStore {
    MysqlStore::connect(url).expect("connect")
}

/// A per-run application table. $-scoped so two runs (or a crashed previous run) can
/// never collide, and dropped at the end of the test that made it.
fn scope() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}_{nanos}", std::process::id())
}

/// A raw driver connection the test owns — the "application's own pool" of caller-owned transaction contract.
/// headgate never sees it except as a `&mut Transaction`.
async fn app_conn(url: &str) -> mysql_async::Conn {
    let pool = mysql_async::Pool::new(mysql_async::Opts::from_url(url).expect("url"));
    pool.get_conn().await.expect("app connection")
}

fn env(queue: &str, id: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: "orm:t".into(),
        payload: b"{}".to_vec(),
        queue: queue.into(),
        fingerprint: headgate_core::fingerprint("orm:t", b"{}"),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

/// Clean at START as well as at the end: a previous run that panicked mid-test leaves
/// rows behind, and a matrix that only passes on a pristine database proves nothing.
/// One statement per call — MySQL rejects multi-statement bodies on this connection.
async fn clean(c: &mut mysql_async::Conn, queue: &str, app_table: &str) {
    let _ = c
        .query_drop(format!("DROP TABLE IF EXISTS {app_table}"))
        .await;
    let _ = c
        .exec_drop("DELETE FROM headgate_job WHERE queue = ?", (queue,))
        .await;
    let _ = c
        .exec_drop(
            "DELETE FROM headgate_active_partition WHERE queue = ?",
            (queue,),
        )
        .await;
    let _ = c
        .exec_drop(
            "DELETE FROM headgate_effect WHERE effect_key LIKE ?",
            (format!("{queue}-%"),),
        )
        .await;
}

fn admit_req(queue: &str, worker: &str, lease: &str) -> AdmitRequest {
    AdmitRequest {
        worker: worker.into(),
        lease_id: lease.into(),
        queues: vec![queue.into()],
        capacity: 10,
        lease: Duration::from_secs(60),
        quantum: 1000,
    }
}

async fn count(c: &mut mysql_async::Conn, sql: &str) -> i64 {
    let v: Option<i64> = c.query_first(sql).await.expect("count");
    v.unwrap_or(0)
}

// ---------------------------------------------------------------------------
// (a) COMMIT — one caller-owned transaction, an app write and an enqueue, both visible,
//     and the job actually admittable afterwards.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn caller_tx_commit_makes_the_app_row_and_the_job_visible_and_admittable() {
    let Some(url) = url() else { return };
    let store = store(&url);
    let s = scope();
    let queue = format!("ormmy-a-{s}");
    let app = format!("hg_orm_app_a_{s}");
    let mut c = app_conn(&url).await;
    clean(&mut c, &queue, &app).await;
    c.query_drop(format!(
        "CREATE TABLE {app} (id VARCHAR(64) PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB"
    ))
    .await
    .expect("create app table");

    // THE POINT: the transaction is the application's. headgate is handed a borrow of
    // it and writes into it; it never opens, commits, or owns anything here.
    let mut tx = c
        .start_transaction(mysql_async::TxOpts::default())
        .await
        .expect("begin");
    tx.exec_drop(
        format!("INSERT INTO {app} (id, note) VALUES (?, ?)"),
        ("order-1", "paid"),
    )
    .await
    .expect("app write");
    store
        .enqueue_on(&mut tx, &[env(&queue, &format!("{queue}-j1"))])
        .await
        .expect("enqueue_on the caller's mysql_async::Transaction");
    tx.commit().await.expect("commit");

    assert_eq!(
        count(&mut c, &format!("SELECT count(*) FROM {app}")).await,
        1,
        "the app write must survive the commit"
    );
    assert_eq!(
        count(
            &mut c,
            &format!("SELECT count(*) FROM headgate_job WHERE queue = '{queue}'")
        )
        .await,
        1,
        "the enqueue must survive the same commit"
    );

    // Visible is not enough: the job has to pass the gate. An enqueue that commits but
    // is not admittable (wrong state, missing active-partition row) would be a silent
    // stall, so the matrix admits it for real.
    let units = store
        .admit(admit_req(&queue, "orm-w", "ORM-L1"))
        .await
        .expect("admit");
    let ids: Vec<&str> = units
        .iter()
        .flat_map(|u| &u.claims)
        .map(|c| c.envelope.id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec![format!("{queue}-j1")],
        "the committed job must be admittable"
    );

    clean(&mut c, &queue, &app).await;
}

// ---------------------------------------------------------------------------
// (b) ROLLBACK — the money assertion. If the app's transaction aborts, headgate's row
//     must vanish with it. A queue that survives its caller's rollback has published a
//     job for work that never happened.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn caller_tx_rollback_leaves_neither_the_app_row_nor_the_job() {
    let Some(url) = url() else { return };
    let store = store(&url);
    let s = scope();
    let queue = format!("ormmy-b-{s}");
    let app = format!("hg_orm_app_b_{s}");
    let mut c = app_conn(&url).await;
    clean(&mut c, &queue, &app).await;
    c.query_drop(format!(
        "CREATE TABLE {app} (id VARCHAR(64) PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB"
    ))
    .await
    .expect("create app table");

    let mut tx = c
        .start_transaction(mysql_async::TxOpts::default())
        .await
        .expect("begin");
    tx.exec_drop(
        format!("INSERT INTO {app} (id, note) VALUES (?, ?)"),
        ("order-2", "pending"),
    )
    .await
    .expect("app write");
    store
        .enqueue_on(&mut tx, &[env(&queue, &format!("{queue}-j1"))])
        .await
        .expect("enqueue_on the caller's transaction");
    tx.rollback().await.expect("rollback");

    assert_eq!(
        count(&mut c, &format!("SELECT count(*) FROM {app}")).await,
        0,
        "the app write must be gone"
    );
    assert_eq!(
        count(
            &mut c,
            &format!("SELECT count(*) FROM headgate_job WHERE queue = '{queue}'")
        )
        .await,
        0,
        "the enqueue must be gone WITH it — neither exists"
    );

    let units = store
        .admit(admit_req(&queue, "orm-w", "ORM-L2"))
        .await
        .expect("admit");
    assert!(
        units.iter().flat_map(|u| &u.claims).next().is_none(),
        "a rolled-back enqueue must never be admittable"
    );

    clean(&mut c, &queue, &app).await;
}

// ---------------------------------------------------------------------------
// (c) Handler side: the effect-key claim, the app write, and the fence-verified
//     completion in ONE caller transaction (transactional effects, the machinery behind `Once`). A crash
//     after that commit re-delivers the job; the second pass must claim nothing and
//     write nothing.
// ---------------------------------------------------------------------------

/// One delivery of the job, shaped exactly like `JobCtx::once`: claim the effect key,
/// do the application's writes, complete the job — all on ONE transaction the caller
/// owns, so either all three commit or none do. Returns whether the effect ran.
async fn deliver(
    store: &MysqlStore,
    txs: &dyn Transactional,
    lease: &headgate_core::LeaseRef,
    key: &str,
    app: &str,
) -> bool {
    let mut tx = store.begin().await.expect("begin");
    let claimed = txs.claim_effect(&mut tx, key).await.expect("claim_effect");
    if !claimed {
        txs.rollback_tx(Box::new(tx)).await.expect("rollback");
        return false; // a COMMITTED transaction already claimed it; the effect ran.
    }
    tx.conn()
        .expect("conn")
        .exec_drop(
            format!("INSERT INTO {app} (id, note) VALUES (?, ?)"),
            ("charge-1", "applied"),
        )
        .await
        .expect("app write");
    txs.complete_tx(&mut tx, lease).await.expect("complete_tx");
    txs.commit_tx(Box::new(tx)).await.expect("commit");
    true
}

#[tokio::test]
async fn once_in_a_caller_tx_does_not_double_apply_after_a_crash() {
    let Some(url) = url() else { return };
    let store = store(&url);
    let txs = store.as_transactional().expect("mysql is transactional");
    let s = scope();
    let queue = format!("ormmy-c-{s}");
    let app = format!("hg_orm_app_c_{s}");
    let mut c = app_conn(&url).await;
    clean(&mut c, &queue, &app).await;
    c.query_drop(format!(
        "CREATE TABLE {app} (id VARCHAR(64) PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB"
    ))
    .await
    .expect("create app table");

    let job_id = format!("{queue}-j1");
    store
        .enqueue(&[env(&queue, &job_id)])
        .await
        .expect("enqueue");
    let units = store
        .admit(admit_req(&queue, "orm-w", "ORM-L3"))
        .await
        .expect("admit");
    let lease = units[0].claims[0].lease_ref();
    let effect_key = format!("{queue}-effect");

    assert!(
        deliver(&store, txs, &lease, &effect_key, &app).await,
        "first delivery runs the effect"
    );

    // The crash: the worker died AFTER the commit and before it could report anything,
    // so the job is delivered again. `Once` is what makes that safe.
    assert!(
        !deliver(&store, txs, &lease, &effect_key, &app).await,
        "a redelivery after a committed effect must skip the work entirely"
    );

    assert_eq!(
        count(&mut c, &format!("SELECT count(*) FROM {app}")).await,
        1,
        "the app effect must be applied EXACTLY once"
    );
    assert_eq!(
        count(
            &mut c,
            &format!("SELECT count(*) FROM headgate_effect WHERE effect_key = '{effect_key}'")
        )
        .await,
        1,
        "one effect-key row, claimed once, forever"
    );
    let state: Option<String> = c
        .exec_first("SELECT state FROM headgate_job WHERE ulid = ?", (&job_id,))
        .await
        .expect("job row");
    assert_eq!(
        state.as_deref(),
        Some("completed"),
        "completion committed with the app write, not after it"
    );

    clean(&mut c, &queue, &app).await;
}
