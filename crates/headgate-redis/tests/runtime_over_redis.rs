//! architecture thesis3's decisive check: the SAME worker runtime, unchanged, over the Redis store. If
//! this needs backend-specific code, the store port failed its second implementation.
//! Opt-in via HG_TEST_REDIS (e.g. redis://127.0.0.1:6380); skips cleanly without it.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use headgate::{
    Control, JobCtx, PeriodicEnqueueHookEvent, PeriodicEnqueueHookFn, Registry, WorkerConfig,
    testing,
};
use headgate_core::{BoxError, CodecError, Envelope, Store, Task};
use headgate_redis::{RedisStore, RedisStoreOptions};

struct Msg(String);

impl Task for Msg {
    const TYPE: &'static str = "rr:msg";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Msg(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn env_for(queue: &str, id: &str, payload: &str) -> Envelope {
    headgate::prepare_envelope(Envelope {
        id: id.into(),
        kind: Msg::TYPE.into(),
        payload: payload.as_bytes().to_vec(),
        queue: queue.into(),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    })
    .expect("prepare")
}

async fn state_of(url: &str, prefix: &str, id: &str) -> (String, i64, i64) {
    let client = redis::Client::open(url).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let vals: Vec<Option<String>> = redis::cmd("HMGET")
        .arg(format!("{prefix}:job:{id}"))
        .arg("state")
        .arg("attempt")
        .arg("crash_attempt")
        .query_async(&mut c)
        .await
        .unwrap();
    (
        vals[0].clone().unwrap_or_default(),
        vals[1].as_deref().and_then(|v| v.parse().ok()).unwrap_or(0),
        vals[2].as_deref().and_then(|v| v.parse().ok()).unwrap_or(0),
    )
}

#[tokio::test]
async fn the_worker_runtime_runs_unchanged_over_redis() {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping redis runtime test");
        return;
    };
    let prefix = "rrt";
    {
        // clean this test's keyspace
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut c = client.get_multiplexed_async_connection().await.unwrap();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{prefix}:*"))
            .query_async(&mut c)
            .await
            .unwrap();
        if !keys.is_empty() {
            let _: i64 = redis::cmd("DEL")
                .arg(&keys)
                .query_async(&mut c)
                .await
                .unwrap();
        }
    }
    let opts = RedisStoreOptions {
        retry_base_ms: 1,
        ..Default::default()
    };
    let client = redis::Client::open(url.as_str()).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let store = Arc::new(RedisStore::with_options(conn, prefix, opts));

    let q = "rr-q";
    let downloads = Arc::new(AtomicU32::new(0));
    let fails_left = Arc::new(AtomicU32::new(1));
    let mut reg = Registry::new();
    {
        let (d, f) = (downloads.clone(), fails_left.clone());
        reg.register::<Msg, _, _>(move |ctx: JobCtx, m: Msg| {
            let (d, f) = (d.clone(), f.clone());
            async move {
                match m.0.as_str() {
                    "ok" => Ok(()),
                    "panic" => panic!("kaboom"),
                    "skip" => Err(Control::Skip.into()),
                    "steps" => {
                        // step replay over Redis: fence-gated checkpoint via checkpoint.lua.
                        ctx.step("download", || async {
                            d.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .await?;
                        ctx.step("transcode", || async {
                            if f.swap(0, Ordering::SeqCst) > 0 {
                                return Err::<(), BoxError>("transcode failed".into());
                            }
                            Ok(())
                        })
                        .await?;
                        Ok(())
                    }
                    other => Err(format!("unexpected payload {other}").into()),
                }
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = WorkerConfig {
        queues: vec![q.into()],
        ..Default::default()
    };

    store
        .enqueue(&[
            env_for(q, "rr-ok", "ok"),
            env_for(q, "rr-panic", "panic"),
            env_for(q, "rr-skip", "skip"),
            env_for(q, "rr-step", "steps"),
        ])
        .await
        .expect("enqueue");

    // Same testing::drain as Postgres — that is the point.
    let done = testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(done.len(), 4);
    assert_eq!(state_of(&url, prefix, "rr-ok").await.0, "completed");
    assert_eq!(state_of(&url, prefix, "rr-skip").await.0, "archived");
    let (st, attempt, crash) = state_of(&url, prefix, "rr-panic").await;
    assert_eq!(
        (st.as_str(), attempt, crash),
        ("retryable", 1, 0),
        "panic caught, recorded as retry"
    );
    assert_eq!(state_of(&url, prefix, "rr-step").await.0, "retryable");
    assert_eq!(downloads.load(Ordering::SeqCst), 1);

    // Retry pass: the completed download step is SKIPPED on Redis exactly as on PG.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let done = testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(done.len(), 2, "panic + step jobs re-admitted: {done:?}");
    assert_eq!(state_of(&url, prefix, "rr-step").await.0, "completed");
    assert_eq!(
        downloads.load(Ordering::SeqCst),
        1,
        "checkpoint skipped the completed step"
    );

    // runtime capability boundary the capability claims are honest: Inspect yes (src/inspect.rs), never
    // Transactional (structurally impossible on Redis), Notifying not yet (no pub/sub).
    assert!(
        store.as_transactional().is_none(),
        "Redis must not claim Transactional"
    );
    assert!(
        store.as_inspect().is_some(),
        "Redis claims Inspect and must answer it"
    );
    assert!(
        store.as_notifying().is_none(),
        "Redis must not claim Notifying yet"
    );
    assert_eq!(store.caps(), headgate_core::Caps::INSPECT);
}

/// surveyed policy behavior over Redis: with Inspect answered, the worker's SCHEDULER duty activates —
/// the same leaderless enqueue-then-CAS-advance loop the Postgres tests prove, driven
/// through a real Worker (not a drain), against sched.lua's due/advance.
#[tokio::test]
async fn the_scheduler_duty_fires_over_redis() {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping redis scheduler test");
        return;
    };
    let prefix = "rrs";
    {
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut c = client.get_multiplexed_async_connection().await.unwrap();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{prefix}:*"))
            .query_async(&mut c)
            .await
            .unwrap();
        if !keys.is_empty() {
            let _: i64 = redis::cmd("DEL")
                .arg(&keys)
                .query_async(&mut c)
                .await
                .unwrap();
        }
    }
    let client = redis::Client::open(url.as_str()).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let store = Arc::new(RedisStore::new(conn, prefix));
    let insp = store.as_inspect().unwrap();

    let q = "rrs-q";
    insp.upsert_schedule(&headgate_core::Schedule {
        id: "rrs-s1".into(),
        kind: Msg::TYPE.into(),
        payload: b"ok".to_vec(),
        queue: q.into(),
        partition_key: String::new(),
        rate_class: String::new(),
        priority: 0,
        max_attempts: 25,
        retention_ms: 86_400_000,
        spec: "@every:300".into(),
        next_run_ms: 1,
        last_enqueued_ms: None,
        on_missed: headgate_core::MissedPolicy::Skip,
        backfill_limit: 0,
        paused: false,
    })
    .await
    .unwrap();

    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(|_ctx: JobCtx, _m: Msg| async { Ok(()) })
        .unwrap();
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let captured = hook_events.clone();
    let periodic_hook: Arc<dyn headgate::PeriodicEnqueueHook> = Arc::new(
        PeriodicEnqueueHookFn::new(move |event: PeriodicEnqueueHookEvent<'_>| {
            let attempt = event.attempt();
            if attempt.schedule_id() == "rrs-s1" {
                captured.lock().unwrap().push((
                    matches!(event, PeriodicEnqueueHookEvent::Begin { .. }),
                    attempt.schedule_id().to_string(),
                    attempt.tick_ms(),
                ));
            }
        }),
    );
    let cfg = WorkerConfig {
        queues: vec![q.into()],
        worker_id: Some("rrs-w".into()),
        duty_interval: Duration::from_millis(100),
        periodic_enqueue_hooks: vec![periodic_hook],
        ..Default::default()
    };
    let (worker, handle) = headgate::Worker::new(store.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let done = insp
            .counts(Some(q))
            .await
            .unwrap()
            .counts
            .iter()
            .find(|(s, _)| s == "completed")
            .map(|(_, n)| *n)
            .unwrap_or(0);
        if done >= 2 {
            break; // two distinct ticks fired and ran to completion
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduler never fired twice"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let hook_events = hook_events.lock().unwrap().clone();
    assert!(hook_events.len() >= 4, "two ticks need begin/end hooks");
    for pair in hook_events.chunks_exact(2) {
        assert!(pair[0].0, "pre-enqueue hook must run first");
        assert!(!pair[1].0, "post-enqueue hook must run second");
        assert_eq!(pair[0].1, "rrs-s1");
        assert_eq!(pair[0].2, pair[1].2, "both events identify one tick");
    }
    // Hygiene: leave nothing ticking in the shared keyspace.
    insp.delete_schedule("rrs-s1").await.unwrap();
    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}
