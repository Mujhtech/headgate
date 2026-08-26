use headgate_testkit::MysqlTestDatabase;
use mysql_async::prelude::*;

#[tokio::test]
async fn mysql_test_databases_migrate_isolate_parallel_tests_and_cleanup() {
    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping MySQL test-database helper");
        return;
    };
    let (left, right) = tokio::join!(
        MysqlTestDatabase::create(&url),
        MysqlTestDatabase::create(&url)
    );
    let left = left.expect("left database");
    let right = right.expect("right database");
    assert_ne!(left.database(), right.database());

    let left_pool = mysql_async::Pool::new(left.opts());
    let right_pool = mysql_async::Pool::new(right.opts());
    let mut left_conn = left_pool.get_conn().await.unwrap();
    let mut right_conn = right_pool.get_conn().await.unwrap();
    let left_installed: Option<u64> = left_conn
        .query_first(
            "SELECT count(*) FROM information_schema.tables
              WHERE table_schema = DATABASE() AND table_name = 'headgate_job'",
        )
        .await
        .unwrap();
    let right_installed: Option<u64> = right_conn
        .query_first(
            "SELECT count(*) FROM information_schema.tables
              WHERE table_schema = DATABASE() AND table_name = 'headgate_job'",
        )
        .await
        .unwrap();
    assert_eq!(left_installed, Some(1));
    assert_eq!(right_installed, Some(1));
    left_conn
        .query_drop("INSERT INTO headgate_queue_state(queue) VALUES ('only-left')")
        .await
        .unwrap();
    let right_count: Option<u64> = right_conn
        .query_first("SELECT count(*) FROM headgate_queue_state WHERE queue = 'only-left'")
        .await
        .unwrap();
    assert_eq!(
        right_count,
        Some(0),
        "parallel databases cannot see each other"
    );

    drop(left_conn);
    let _ = left_pool.disconnect().await;
    left.cleanup().await.expect("cleanup left");
    let still_live: Option<u64> = right_conn
        .query_first(
            "SELECT count(*) FROM information_schema.tables
              WHERE table_schema = DATABASE() AND table_name = 'headgate_job'",
        )
        .await
        .unwrap();
    assert_eq!(
        still_live,
        Some(1),
        "cleaning one test cannot drop its sibling"
    );
    drop(right_conn);
    let _ = right_pool.disconnect().await;
    right.cleanup().await.expect("cleanup right");
}
