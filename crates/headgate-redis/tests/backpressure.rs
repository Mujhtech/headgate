use std::sync::Arc;

use headgate_redis::RedisStore;
use headgate_testkit::RedisTestNamespace;

#[tokio::test]
async fn enqueue_backpressure_is_atomic_exact_and_work_conserving_under_contention() {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping Redis backpressure proof");
        return;
    };
    let namespace = RedisTestNamespace::create(&url)
        .await
        .expect("Redis namespace");
    let conn = namespace
        .connection_manager()
        .await
        .expect("Redis connection");
    let store = Arc::new(RedisStore::new(conn, namespace.prefix()));
    headgate_testkit::assert_enqueue_backpressure(store, "redis-backpressure").await;
    namespace.cleanup().await.expect("namespace cleanup");
}
