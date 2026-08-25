//! caller-owned transaction contract the ORM-interop conformance matrix, Rust × Postgres cell.
//!
//! Transactional enqueue is the headline feature and it is worth nothing if it cannot
//! join the transaction the application already has open. "The port shape is right" was
//! a claim until this file; the matrix is what turns it into a fact.
//!
//! The native handle here is `tokio_postgres::Transaction` — opened by the TEST, never
//! by headgate — and the entry point is the public [`PgStore::enqueue_on`], which is the
//! same code path `enqueue` and `enqueue_tx` run. No ORM crate is added: sqlx, SeaORM,
//! Diesel and friends all sit on top of a driver handle, and what a port can accept is
//! decided by the driver type, not by the ORM's name (see docs/orm-interop.md).
//!
//! Opt-in via HG_TEST_PG; skips cleanly without it, like every other live test here.

use std::time::Duration;

use headgate_core::{AdmitRequest, Envelope, Store, Transactional};
use headgate_postgres::PgStore;

fn conninfo() -> Option<String> {
    match std::env::var("HG_TEST_PG") {
        Ok(s) => Some(s),
        Err(_) => {
            eprintln!("HG_TEST_PG not set; skipping ORM interop matrix (postgres)");
            None
        }
    }
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

/// A raw driver connection the test owns — this is the "application's own pool" that
/// caller-owned transaction contract is about. headgate never sees it except as a `&Transaction`.
async fn app_client(conninfo: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(conninfo, tokio_postgres::NoTls)
        .await
        .expect("app connection");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
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
async fn clean(c: &tokio_postgres::Client, queue: &str, app_table: &str) {
    let _ = c
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {app_table};
             DELETE FROM headgate_job WHERE queue = '{queue}';
             DELETE FROM headgate_active_partition WHERE queue = '{queue}';
             DELETE FROM headgate_effect WHERE key LIKE '{queue}-%'"
        ))
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

// ---------------------------------------------------------------------------
// (a) COMMIT — one caller-owned transaction, an app write and an enqueue, both visible,
//     and the job actually admittable afterwards.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn caller_tx_commit_makes_the_app_row_and_the_job_visible_and_admittable() {
    let Some(conninfo) = conninfo() else { return };
    let store = PgStore::connect(&conninfo, 2).expect("connect");
    let s = scope();
    let queue = format!("ormpg-a-{s}");
    let app = format!("hg_orm_app_a_{s}");
    let mut client = app_client(&conninfo).await;
    clean(&client, &queue, &app).await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {app} (id text primary key, note text)"
        ))
        .await
        .expect("create app table");

    // THE POINT: the transaction is the application's. headgate is handed a borrow of
    // it and writes into it; it never opens, commits, or owns anything here.
    let tx = client.transaction().await.expect("begin (caller-owned)");
    tx.execute(
        &format!("INSERT INTO {app} (id, note) VALUES ($1, $2)"),
        &[&"order-1", &"paid"],
    )
    .await
    .expect("app write");
    store
        .enqueue_on(&tx, &[env(&queue, &format!("{queue}-j1"))])
        .await
        .expect("enqueue_on the caller's tokio_postgres::Transaction");
    tx.commit().await.expect("commit");

    let app_rows: i64 = client
        .query_one(&format!("SELECT count(*)::bigint FROM {app}"), &[])
        .await
        .expect("count app")
        .get(0);
    assert_eq!(app_rows, 1, "the app write must survive the commit");

    let jobs: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM headgate_job WHERE queue = $1",
            &[&queue],
        )
        .await
        .expect("count jobs")
        .get(0);
    assert_eq!(jobs, 1, "the enqueue must survive the same commit");

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

    clean(&client, &queue, &app).await;
}

// ---------------------------------------------------------------------------
// (b) ROLLBACK — the money assertion. If the app's transaction aborts, headgate's row
//     must vanish with it. A queue that survives its caller's rollback has published a
//     job for work that never happened, which is the exact failure transactional
//     enqueue exists to prevent.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn caller_tx_rollback_leaves_neither_the_app_row_nor_the_job() {
    let Some(conninfo) = conninfo() else { return };
    let store = PgStore::connect(&conninfo, 2).expect("connect");
    let s = scope();
    let queue = format!("ormpg-b-{s}");
    let app = format!("hg_orm_app_b_{s}");
    let mut client = app_client(&conninfo).await;
    clean(&client, &queue, &app).await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {app} (id text primary key, note text)"
        ))
        .await
        .expect("create app table");

    let tx = client.transaction().await.expect("begin (caller-owned)");
    tx.execute(
        &format!("INSERT INTO {app} (id, note) VALUES ($1, $2)"),
        &[&"order-2", &"pending"],
    )
    .await
    .expect("app write");
    store
        .enqueue_on(&tx, &[env(&queue, &format!("{queue}-j1"))])
        .await
        .expect("enqueue_on the caller's transaction");
    tx.rollback().await.expect("rollback");

    let app_rows: i64 = client
        .query_one(&format!("SELECT count(*)::bigint FROM {app}"), &[])
        .await
        .expect("count app")
        .get(0);
    assert_eq!(app_rows, 0, "the app write must be gone");

    let jobs: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM headgate_job WHERE queue = $1",
            &[&queue],
        )
        .await
        .expect("count jobs")
        .get(0);
    assert_eq!(jobs, 0, "the enqueue must be gone WITH it — neither exists");

    let units = store
        .admit(admit_req(&queue, "orm-w", "ORM-L2"))
        .await
        .expect("admit");
    assert!(
        units.iter().flat_map(|u| &u.claims).next().is_none(),
        "a rolled-back enqueue must never be admittable"
    );

    clean(&client, &queue, &app).await;
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
    store: &PgStore,
    txs: &dyn Transactional,
    lease: &headgate_core::LeaseRef,
    key: &str,
    app: &str,
) -> bool {
    let mut tx = store.begin().await.expect("begin");
    let claimed = txs.claim_effect(&mut tx, key).await.expect("claim_effect");
    if !claimed {
        tx.rollback().await.expect("rollback");
        return false; // a COMMITTED transaction already claimed it; the effect ran.
    }
    tx.client()
        .expect("client")
        .execute(
            &format!("INSERT INTO {app} (id, note) VALUES ($1, $2)"),
            &[&"charge-1", &"applied"],
        )
        .await
        .expect("app write");
    txs.complete_tx(&mut tx, lease).await.expect("complete_tx");
    tx.commit().await.expect("commit");
    true
}

#[tokio::test]
async fn once_in_a_caller_tx_does_not_double_apply_after_a_crash() {
    let Some(conninfo) = conninfo() else { return };
    let store = PgStore::connect(&conninfo, 2).expect("connect");
    let txs = store.as_transactional().expect("postgres is transactional");
    let s = scope();
    let queue = format!("ormpg-c-{s}");
    let app = format!("hg_orm_app_c_{s}");
    let client = app_client(&conninfo).await;
    clean(&client, &queue, &app).await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {app} (id text primary key, note text)"
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

    let charges: i64 = client
        .query_one(&format!("SELECT count(*)::bigint FROM {app}"), &[])
        .await
        .expect("count app")
        .get(0);
    assert_eq!(charges, 1, "the app effect must be applied EXACTLY once");

    let effects: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM headgate_effect WHERE key = $1",
            &[&effect_key],
        )
        .await
        .expect("count effects")
        .get(0);
    assert_eq!(effects, 1, "one effect-key row, claimed once, forever");

    let state: String = client
        .query_one(
            "SELECT state::text FROM headgate_job WHERE ulid = $1",
            &[&job_id],
        )
        .await
        .expect("job row")
        .get(0);
    assert_eq!(
        state, "completed",
        "completion committed with the app write, not after it"
    );

    clean(&client, &queue, &app).await;
}
