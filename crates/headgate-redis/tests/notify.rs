//! push wakeups push wakeup over Redis pub/sub — the mirror of the Postgres LISTEN test: a
//! waiting subscriber is woken by an enqueue's PUBLISH with the queue's name. A missed
//! notification costs latency, never correctness, so the test enqueues until the
//! wakeup lands. Opt-in via HG_TEST_REDIS; skips cleanly without it.

use std::time::Duration;

use headgate_core::{Caps, Envelope, Store};
use headgate_redis::RedisStore;

#[tokio::test]
async fn enqueue_publish_wakes_a_waiting_subscriber() {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping redis notify test");
        return;
    };
    {
        // clean this test's keyspace — pid-unique ids never collide, but keys otherwise
        // accumulate run over run
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut c = client.get_multiplexed_async_connection().await.unwrap();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("rnfy:*")
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
    let store = std::sync::Arc::new(RedisStore::connect(&url, "rnfy").await.expect("connect"));
    assert!(
        store.caps().has(Caps::NOTIFYING),
        "connect() must enable pub/sub"
    );
    let notifying = store.as_notifying().expect("as_notifying");

    // Prime the lazy subscriber; the first window may elapse before SUBSCRIBE is up.
    let _ = notifying
        .wait_wakeup(&["rnfy-q".into()], Duration::from_millis(300))
        .await;

    let waiter = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .as_notifying()
                .unwrap()
                .wait_wakeup(&["rnfy-q".into()], Duration::from_secs(10))
                .await
        })
    };
    let started = std::time::Instant::now();
    let mut i = 0;
    let woke = loop {
        i += 1;
        store
            .enqueue(&[Envelope {
                id: format!("rnfy-{}-{i}", std::process::id()),
                kind: "nfy".into(),
                payload: vec![0],
                queue: "rnfy-q".into(),
                fingerprint: "fp-rnfy".into(),
                scheduled_at_ms: 1,
                ..Default::default()
            }])
            .await
            .expect("enqueue");
        tokio::time::sleep(Duration::from_millis(150)).await;
        if waiter.is_finished() {
            break waiter.await.unwrap().expect("wait_wakeup");
        }
        assert!(
            started.elapsed() < Duration::from_secs(9),
            "no wakeup after repeated publishes"
        );
    };
    assert_eq!(
        woke.as_deref(),
        Some("rnfy-q"),
        "subscriber must wake with the queue name"
    );
}
