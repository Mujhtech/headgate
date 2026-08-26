use headgate_testkit::RedisTestNamespace;

#[tokio::test]
async fn redis_test_namespaces_isolate_parallel_tests_and_cleanup_without_flushall() {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping Redis test-namespace helper");
        return;
    };
    let (left, right) = tokio::join!(
        RedisTestNamespace::create(&url),
        RedisTestNamespace::create(&url)
    );
    let left = left.expect("left namespace");
    let right = right.expect("right namespace");
    assert_ne!(left.prefix(), right.prefix());
    let left_key = format!("{}:probe", left.prefix());
    let right_key = format!("{}:probe", right.prefix());
    let mut conn = left
        .client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&left_key)
        .arg("left")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&right_key)
        .arg("right")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    left.cleanup().await.expect("cleanup left");
    let left_value: Option<String> = redis::cmd("GET")
        .arg(&left_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    let right_value: Option<String> = redis::cmd("GET")
        .arg(&right_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(left_value, None);
    assert_eq!(right_value.as_deref(), Some("right"));
    right.cleanup().await.expect("cleanup right");
}
