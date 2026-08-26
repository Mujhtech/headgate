use headgate_testkit::PostgresTestDatabase;

async fn connect(
    config: tokio_postgres::Config,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("connect isolated schema");
    (client, tokio::spawn(connection))
}

#[tokio::test]
async fn postgres_test_databases_migrate_isolate_parallel_tests_and_cleanup() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping Postgres test-database helper");
        return;
    };
    let (left, right) = tokio::join!(
        PostgresTestDatabase::create(&conninfo),
        PostgresTestDatabase::create(&conninfo)
    );
    let left = left.expect("left database");
    let right = right.expect("right database");
    assert_ne!(left.schema(), right.schema());

    let (left_client, left_task) = connect(left.config()).await;
    let (right_client, right_task) = connect(right.config()).await;
    let left_installed: bool = left_client
        .query_one("SELECT to_regclass('headgate_job') IS NOT NULL", &[])
        .await
        .unwrap()
        .get(0);
    let right_installed: bool = right_client
        .query_one("SELECT to_regclass('headgate_job') IS NOT NULL", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        left_installed && right_installed,
        "both schemas are migrated"
    );
    left_client
        .execute(
            "INSERT INTO headgate_queue_state(queue) VALUES ('only-left')",
            &[],
        )
        .await
        .unwrap();
    let right_count: i64 = right_client
        .query_one(
            "SELECT count(*) FROM headgate_queue_state WHERE queue = 'only-left'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(right_count, 0, "parallel schemas cannot see each other");

    drop(left_client);
    let _ = left_task.await;
    left.cleanup().await.expect("cleanup left");
    let still_live: bool = right_client
        .query_one("SELECT to_regclass('headgate_job') IS NOT NULL", &[])
        .await
        .unwrap()
        .get(0);
    assert!(still_live, "cleaning one test must not drop its sibling");
    drop(right_client);
    let _ = right_task.await;
    right.cleanup().await.expect("cleanup right");
}
