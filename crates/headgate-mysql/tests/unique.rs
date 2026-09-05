//! job uniqueness uniqueness on push wakeups's GENERATED COLUMNS — the corners the Postgres scenarios cover
//! and MySQL never got. MySQL has no partial indexes, so both modes ride a generated
//! column that is NULL when the key is not held (`unique_active` for LIFECYCLE,
//! `unique_throttle` for THROTTLE) and unique indexes over those columns; MySQL treats
//! NULLs as distinct, which is the whole mechanism.
//!
//! What is asserted here, and why each corner earns a test:
//!
//!   1. LIFECYCLE dedup ACROSS STATES. `unique_active` names four live states, so the
//!      key must survive `scheduled -> available -> running -> retryable` and be
//!      released by EVERY terminal state — not just by the completed one the existing
//!      store test happens to exercise. A generated column that named the wrong state
//!      set would look correct in a one-state test and leak keys forever in production.
//!   2. THROTTLE lazy release at window expiry, and its defining difference from
//!      LIFECYCLE: the key is held ACROSS the terminal state that releases a lifecycle
//!      key, because `unique_throttle` reads `unique_expires_at_ms` and never `state`.
//!   3. THROTTLE + `retention_ms = 0` — the OPEN CORNER the register records on
//!      Postgres. This test RECORDS what MySQL does; it does not change the semantic.
//!   4. job uniqueness vs idempotent enqueue identity: a DIFFERENT id carrying the SAME unique key is the Duplicate
//!      path, never the id-conflict one, and the two classifications have a fixed
//!      ORDER (id first). Blurring them is exactly how a 409 condition once shipped as
//!      a 400.
//!
//! Opt-in via HG_TEST_MYSQL; skips cleanly without it.

use std::sync::Arc;
use std::time::Duration;

use headgate_core::{AdmitRequest, Envelope, Inspect, Outcome, Store, StoreError};
use headgate_mysql::{MysqlStore, MysqlStoreOptions};

fn store() -> Option<Arc<MysqlStore>> {
    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping mysql uniqueness tests");
        return None;
    };
    let opts = MysqlStoreOptions {
        crash_limit: 3,
        retry_base_ms: 1,
        ..Default::default()
    };
    // failure classification caller-supplied pool — the caller carries the CLIENT_FOUND_ROWS requirement.
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).expect("url"))
            .client_found_rows(true),
    );
    Some(Arc::new(MysqlStore::with_options(pool, opts)))
}

async fn raw() -> mysql_async::Conn {
    let url = std::env::var("HG_TEST_MYSQL").unwrap();
    let pool = mysql_async::Pool::new(mysql_async::Opts::from_url(&url).unwrap());
    pool.get_conn().await.unwrap()
}

/// Per-test, per-run scope. Libtest runs this file's tests concurrently, so a shared PID
/// prefix would let one test's `DELETE ... WHERE queue LIKE '<prefix>%'` erase a sibling's
/// fixture. The trailing separator also keeps a short case name from prefix-matching a
/// longer one.
fn scope(case: &str) -> String {
    format!("mu{}-{case}-", std::process::id())
}

async fn clean(prefix: &str) {
    use mysql_async::prelude::*;
    let mut c = raw().await;
    // Do not DELETE through a prefix range. Under InnoDB's default REPEATABLE READ that
    // takes next-key locks on neighbouring test queues; a sibling admission can then hold
    // the neighbouring PRIMARY record while waiting for this range, forming a deadlock
    // even though the fixtures are logically disjoint. Discover non-lockingly, then delete
    // exact primary keys in a stable order.
    let ids: Vec<u64> = c
        .exec(
            "SELECT id FROM headgate_job WHERE queue LIKE ? ORDER BY id",
            (format!("{prefix}%"),),
        )
        .await
        .unwrap();
    for id in ids {
        c.exec_drop("DELETE FROM headgate_job WHERE id = ?", (id,))
            .await
            .unwrap();
    }
    let fingerprints: Vec<String> = c
        .exec(
            "SELECT fingerprint FROM headgate_quarantine
             WHERE fingerprint LIKE ? ORDER BY fingerprint",
            (format!("{prefix}%"),),
        )
        .await
        .unwrap();
    for fingerprint in fingerprints {
        c.exec_drop(
            "DELETE FROM headgate_quarantine WHERE fingerprint = ?",
            (fingerprint,),
        )
        .await
        .unwrap();
    }
}

async fn field(id: &str, col: &str) -> String {
    use mysql_async::prelude::*;
    let mut c = raw().await;
    let v: Option<String> = c
        .exec_first(
            format!("SELECT CAST({col} AS CHAR) FROM headgate_job WHERE ulid = ?"),
            (id,),
        )
        .await
        .unwrap()
        .flatten();
    v.unwrap_or_default()
}

/// Reads the GENERATED columns themselves, not merely their effect. "Enqueue was
/// refused" and "the key is held" are different statements, and only one of them
/// survives a rewrite of the enqueue path.
async fn generated(id: &str) -> (bool, bool) {
    use mysql_async::prelude::*;
    let mut c = raw().await;
    type GeneratedUniqueKeys = (Option<Vec<u8>>, Option<Vec<u8>>);
    let row: Option<GeneratedUniqueKeys> = c
        .exec_first(
            "SELECT unique_active, unique_throttle FROM headgate_job WHERE ulid = ?",
            (id,),
        )
        .await
        .unwrap();
    match row {
        None => (false, false),
        Some((a, t)) => (a.is_some(), t.is_some()),
    }
}

fn env(queue: &str, id: &str, payload: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: "mu:msg".into(),
        payload: payload.as_bytes().to_vec(),
        queue: queue.into(),
        fingerprint: headgate_core::fingerprint("mu:msg", payload.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

fn unique_env(queue: &str, id: &str, key: &str, window_ms: i64) -> Envelope {
    Envelope {
        unique_key: Some(key.as_bytes().to_vec()),
        unique_window_ms: window_ms,
        ..env(queue, id, "{}")
    }
}

fn req(queue: &str, lease_id: &str, capacity: u32) -> AdmitRequest {
    AdmitRequest {
        worker: "muw".into(),
        lease_id: lease_id.into(),
        queues: vec![queue.into()],
        capacity,
        lease: Duration::from_millis(600_000),
        quantum: 1000,
    }
}

/// Admits exactly one job and returns its LeaseRef.
async fn claim_one(s: &Arc<MysqlStore>, queue: &str, lease_id: &str) -> headgate_core::LeaseRef {
    let units = s.admit(req(queue, lease_id, 1)).await.unwrap();
    if let Some(lease) = units
        .iter()
        .flat_map(|u| &u.claims)
        .map(|c| c.lease_ref())
        .next()
    {
        return lease;
    }

    // Empty admission is never expected in this file. Keep its durable witnesses in the
    // panic so a concurrency failure explains whether the job vanished, changed state,
    // or lost its maintained active-partition entry.
    use mysql_async::prelude::*;
    let mut c = raw().await;
    let jobs: Vec<(String, String, i64, String)> = c
        .exec(
            "SELECT ulid, CAST(state AS CHAR), scheduled_at_ms, partition_key
               FROM headgate_job WHERE queue = ? ORDER BY id",
            (queue,),
        )
        .await
        .unwrap_or_default();
    let parts: Vec<(String, String)> = c
        .exec(
            "SELECT queue, partition_key FROM headgate_active_partition WHERE queue = ?",
            (queue,),
        )
        .await
        .unwrap_or_default();
    panic!("nothing admitted from {queue}; jobs={jobs:?} active_partitions={parts:?}")
}

// ---------------------------------------------------------------------------
// 1. LIFECYCLE: held through every LIVE state, released by every TERMINAL one.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lifecycle_unique_is_held_through_every_live_state() {
    let Some(s) = store() else { return };
    let sc = scope("life");
    let q = format!("{sc}-life");
    clean(&sc).await;

    let key = format!("{sc}-K-live");
    // Enqueued into the FUTURE: the row lands 'scheduled', the first of the four live
    // states `unique_active` names.
    let mut first = unique_env(&q, &format!("{sc}-l1"), &key, 0);
    first.scheduled_at_ms = 99_999_999_999_999;
    s.enqueue(&[first]).await.unwrap();
    assert_eq!(field(&format!("{sc}-l1"), "state").await, "scheduled");

    let refused = |state: &str| {
        let s = s.clone();
        let q = q.clone();
        let key = key.clone();
        let sc = sc.clone();
        let state = state.to_string();
        async move {
            let dup = unique_env(&q, &format!("{sc}-dup-{state}"), &key, 0);
            match s.enqueue(&[dup]).await {
                Err(StoreError::Duplicate { existing_id, .. }) => {
                    assert_eq!(
                        existing_id,
                        format!("{sc}-l1"),
                        "the duplicate must name the WINNER, not guess (state {state})"
                    );
                }
                other => panic!("unique key must still be held in state {state}: {other:?}"),
            }
        }
    };
    refused("scheduled").await;
    assert_eq!(
        generated(&format!("{sc}-l1")).await,
        (true, false),
        "scheduled: unique_active holds the key, unique_throttle is NULL"
    );

    // scheduled -> available
    // The fixture started deliberately far in the future so enqueue had to choose the
    // scheduled state. Make that same row due before asking the store-clock promoter to
    // move it; promote_due must never be made dependent on the test process's clock.
    {
        use mysql_async::prelude::*;
        let mut c = raw().await;
        c.exec_drop(
            "UPDATE headgate_job SET scheduled_at_ms = 0 WHERE ulid = ?",
            (format!("{sc}-l1"),),
        )
        .await
        .unwrap();
    }
    s.promote_due(10_000).await.unwrap();
    assert_eq!(field(&format!("{sc}-l1"), "state").await, "available");
    refused("available").await;

    // available -> running
    let lease = claim_one(&s, &q, "MUL1").await;
    assert_eq!(field(&format!("{sc}-l1"), "state").await, "running");
    refused("running").await;
    assert_eq!(
        generated(&format!("{sc}-l1")).await,
        (true, false),
        "running: a claimed job still holds its lifecycle key"
    );

    // running -> retryable
    s.ack(&lease, Outcome::Retry, Some("boom"), None)
        .await
        .unwrap();
    assert_eq!(field(&format!("{sc}-l1"), "state").await, "retryable");
    refused("retryable").await;
    assert_eq!(
        generated(&format!("{sc}-l1")).await,
        (true, false),
        "retryable: the key survives a failed attempt — that is the point of LIFECYCLE"
    );
    clean(&sc).await;
}

#[tokio::test]
async fn lifecycle_unique_is_released_by_every_terminal_state() {
    let Some(s) = store() else { return };
    let sc = scope("terminal");
    clean(&sc).await;

    // (label, how to drive the job terminal, expected terminal state)
    // Each case gets its OWN key and queue: the release is a property of one row's
    // generated column, and sharing a key would let one case mask another.
    for (label, expect) in [
        ("completed", "completed"),
        ("archived", "archived"),
        ("undecodable", "undecodable"),
    ] {
        let q = format!("{sc}-{label}");
        let key = format!("{sc}-K-{label}");
        let id = format!("{sc}-{label}-1");
        s.enqueue(&[unique_env(&q, &id, &key, 0)]).await.unwrap();
        let lease = claim_one(&s, &q, &format!("MUT-{label}")).await;
        let outcome = match label {
            "completed" => Outcome::Success,
            "archived" => Outcome::Skip,
            _ => Outcome::Undecodable,
        };
        s.ack(&lease, outcome, Some("done"), None).await.unwrap();
        assert_eq!(field(&id, "state").await, expect, "{label}");
        assert_eq!(
            generated(&id).await,
            (false, false),
            "{label} is TERMINAL: unique_active must be NULL"
        );
        // ...and therefore a new job may take the key.
        s.enqueue(&[unique_env(&q, &format!("{sc}-{label}-2"), &key, 0)])
            .await
            .unwrap();
    }

    // cancelled — an OPERATOR terminal, reached without ever running. The generated
    // column must not care how the row got there.
    {
        let insp: &dyn Inspect = s.as_ref().as_inspect().expect("caps say INSPECT");
        let q = format!("{sc}-cancelled");
        let key = format!("{sc}-K-cancelled");
        let id = format!("{sc}-cancelled-1");
        s.enqueue(&[unique_env(&q, &id, &key, 0)]).await.unwrap();
        insp.operator_cancel(&id).await.unwrap();
        assert_eq!(field(&id, "state").await, "cancelled");
        assert_eq!(
            generated(&id).await,
            (false, false),
            "cancelled releases the key"
        );
        s.enqueue(&[unique_env(&q, &format!("{sc}-cancelled-2"), &key, 0)])
            .await
            .unwrap();
    }

    // quarantined — crash quarantine's terminal-and-VISIBLE state, reached by the sweeper. This is
    // the case the sweeper's own comment claims ("the generated column releases any
    // lifecycle unique key these jobs held") and nothing asserted.
    {
        use mysql_async::prelude::*;
        let insp: &dyn Inspect = s.as_ref().as_inspect().expect("caps say INSPECT");
        let q = format!("{sc}-quarantined");
        let key = format!("{sc}-K-quarantined");
        let id = format!("{sc}-quarantined-1");
        let mut e = unique_env(&q, &id, &key, 0);
        // Its own fingerprint, so the GLOBAL sweep touches nothing but this row.
        e.fingerprint = format!("{sc}-fp-q");
        s.enqueue(&[e]).await.unwrap();
        {
            let mut c = raw().await;
            c.exec_drop(
                "INSERT INTO headgate_quarantine
                   (fingerprint, kind, crash_count, quarantined_at_ms, reason)
                 VALUES (?, 'mu:msg', 3, 1, 'test') AS new
                 ON DUPLICATE KEY UPDATE crash_count = new.crash_count",
                (format!("{sc}-fp-q"),),
            )
            .await
            .unwrap();
        }
        insp.quarantine_sweep(10_000).await.unwrap();
        assert_eq!(field(&id, "state").await, "quarantined");
        assert_eq!(
            generated(&id).await,
            (false, false),
            "quarantined is terminal: the sweeper's comment, now asserted"
        );
        // A NEW job may take the key — but only under a clean fingerprint, since crash quarantine
        // rejects an enqueue of a quarantined one before uniqueness is ever consulted.
        let mut next = unique_env(&q, &format!("{sc}-quarantined-2"), &key, 0);
        next.fingerprint = format!("{sc}-fp-ok");
        s.enqueue(&[next]).await.unwrap();
    }
    clean(&sc).await;
}

// ---------------------------------------------------------------------------
// 2. THROTTLE: released by the CLOCK, held across the terminal state.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn throttle_unique_is_held_across_completion_and_released_lazily_at_expiry() {
    let Some(s) = store() else { return };
    let sc = scope("throttle");
    let q = format!("{sc}-q");
    clean(&sc).await;
    const WINDOW_MS: i64 = 600_000;

    let key = format!("{sc}-K-throttle");
    let id = format!("{sc}-t1");
    // Keep the window far beyond test-runtime jitter while proving terminal state does
    // not release it. Exact duration arithmetic has its own timing-free conformance
    // assertions; this case is about state independence and the lazy conflict path.
    s.enqueue(&[unique_env(&q, &id, &key, WINDOW_MS)])
        .await
        .unwrap();
    assert_eq!(
        generated(&id).await,
        (false, true),
        "THROTTLE writes unique_throttle and leaves unique_active NULL — the two modes \
         must not both hold the same key, or releasing one would not release it"
    );

    let dup = unique_env(&q, &format!("{sc}-t2"), &key, WINDOW_MS);
    assert!(
        matches!(s.enqueue(&[dup]).await, Err(StoreError::Duplicate { ref existing_id, .. })
                 if *existing_id == id),
        "throttle blocks within the window"
    );

    // THE DIFFERENCE FROM LIFECYCLE: drive the job to a terminal state that WOULD have
    // released a lifecycle key, and the throttle key must still be held — because
    // `unique_throttle` reads unique_expires_at_ms and never `state`.
    let lease = claim_one(&s, &q, "MUH1").await;
    s.ack(&lease, Outcome::Success, None, None).await.unwrap();
    assert_eq!(field(&id, "state").await, "completed");
    assert_eq!(
        generated(&id).await,
        (false, true),
        "a COMPLETED job still holds its throttle key"
    );
    let dup = unique_env(&q, &format!("{sc}-t3"), &key, WINDOW_MS);
    assert!(
        matches!(s.enqueue(&[dup]).await, Err(StoreError::Duplicate { .. })),
        "throttle survives completion; only the clock releases it"
    );

    // Make the deadline unambiguously past using the STORE clock, not a guessed sleep.
    // Nothing sweeps it: the conflicting enqueue must clear this expired holder and
    // retry once.
    {
        use mysql_async::prelude::*;
        let mut c = raw().await;
        c.exec_drop(
            "UPDATE headgate_job
                SET unique_expires_at_ms = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED) - 1
              WHERE ulid = ?",
            (&id,),
        )
        .await
        .unwrap();
    }
    s.enqueue(&[unique_env(&q, &format!("{sc}-t4"), &key, WINDOW_MS)])
        .await
        .unwrap();
    assert_eq!(
        generated(&id).await,
        (false, false),
        "the lazy release must actually CLEAR the old holder's unique_expires_at_ms, \
         not merely let the new row in"
    );
    assert_eq!(field(&format!("{sc}-t4"), "state").await, "available");
    clean(&sc).await;
}

// ---------------------------------------------------------------------------
// 3. THE OPEN CORNER: throttle + retention_ms = 0.
// ---------------------------------------------------------------------------

/// The register records this on Postgres as an open corner: "throttle + retention_ms =
/// 0 deletes the row and its key before the window ends". MySQL reaches it through a
/// GENERATED COLUMN rather than a partial index, but the cause is identical — the key
/// lives IN the row, and `retention_ms = 0` deletes the row at ack (retention policy's ephemeral
/// jobs). So the window is released EARLY.
///
/// This test RECORDS the behavior; it does not change it. Written that way on purpose:
/// if the semantic is ever decided differently, this assertion is where the decision
/// becomes visible, instead of a corner nobody re-derives.
#[tokio::test]
async fn throttle_with_retention_zero_loses_the_window_with_the_row() {
    let Some(s) = store() else { return };
    let sc = scope("retention-zero");
    let q = format!("{sc}-q");
    clean(&sc).await;

    let key = format!("{sc}-K-eph");
    let id = format!("{sc}-z1");
    // A TEN MINUTE window, so nothing here can be mistaken for the window expiring.
    let mut e = unique_env(&q, &id, &key, 600_000);
    e.retention_ms = 0;
    s.enqueue(&[e]).await.unwrap();
    assert_eq!(generated(&id).await, (false, true));
    let dup = unique_env(&q, &format!("{sc}-z2"), &key, 600_000);
    assert!(
        matches!(s.enqueue(&[dup]).await, Err(StoreError::Duplicate { .. })),
        "the window holds while the row exists"
    );

    let lease = claim_one(&s, &q, "MUZ1").await;
    s.ack(&lease, Outcome::Success, None, None).await.unwrap();
    // retention policy / state_machine.yaml: success -> deleted when retention_ms == 0.
    assert_eq!(
        field(&id, "ulid").await,
        "",
        "retention 0 deletes the row at ack"
    );

    // OBSERVED: the throttle key went with it, ~10 minutes early.
    let next = unique_env(&q, &format!("{sc}-z3"), &key, 600_000);
    s.enqueue(&[next]).await.expect(
        "OPEN CORNER (job uniqueness, register row \"Unique / dedup\"): an ephemeral job's throttle \
         window dies with its row. If this ever starts failing, the semantic was CHANGED \
         somewhere and the register row needs updating with it.",
    );
    assert_eq!(field(&format!("{sc}-z3"), "state").await, "available");
    clean(&sc).await;
}

// ---------------------------------------------------------------------------
// 4. job uniqueness (Duplicate) vs idempotent enqueue identity (IdConflict) — two contracts, one enqueue.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_different_id_with_the_same_unique_key_is_duplicate_not_id_conflict() {
    let Some(s) = store() else { return };
    let sc = scope("classification");
    let q = format!("{sc}-q");
    clean(&sc).await;

    let key = format!("{sc}-K-mix");
    let first = format!("{sc}-c1");
    s.enqueue(&[unique_env(&q, &first, &key, 0)]).await.unwrap();

    // A DIFFERENT id carrying the SAME unique key is the Duplicate path. It is opt-in,
    // releasable uniqueness over a caller-chosen key and it names the WINNER; the
    // id-conflict contract is the never-released primary key and names the id you
    // asked for. Folding one into the other is how a 409 condition shipped as a 400.
    match s
        .enqueue(&[unique_env(&q, &format!("{sc}-c2"), &key, 0)])
        .await
    {
        Err(StoreError::Duplicate { existing_id, .. }) => assert_eq!(existing_id, first),
        other => panic!("same key + different id must be Duplicate, got {other:?}"),
    }

    // The SAME id with the same content is idempotent success — idempotent enqueue identity's id pass runs
    // BEFORE the insert, so the unique index never sees this call at all.
    s.enqueue(&[unique_env(&q, &first, &key, 0)]).await.unwrap();

    // ...and the SAME id with DIFFERENT content is the id conflict, NOT a duplicate:
    // the classification ORDER is fixed (id first), which is what makes the API's 409
    // reachable for a caller who also happens to use a unique key.
    let mut changed = unique_env(&q, &first, &key, 0);
    changed.payload = b"{\"n\":2}".to_vec();
    changed.fingerprint = headgate_core::fingerprint("mu:msg", &changed.payload);
    match s.enqueue(&[changed]).await {
        Err(StoreError::IdConflict { job_id }) => assert_eq!(job_id, first),
        other => panic!("same id + different content must be IdConflict, got {other:?}"),
    }

    // Exactly two rows exist: the original and nothing the refusals wrote.
    use mysql_async::prelude::*;
    let mut c = raw().await;
    let n: Option<i64> = c
        .exec_first("SELECT COUNT(*) FROM headgate_job WHERE queue = ?", (&q,))
        .await
        .unwrap();
    assert_eq!(n.unwrap_or(0), 1, "no refusal may leave a row behind");
    clean(&sc).await;
}
