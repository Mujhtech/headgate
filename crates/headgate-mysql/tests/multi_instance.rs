//! Two databases on one MySQL server. Identical logical keys prove that database
//! selection, singleton duties, and destructive migration scope do not cross instances.

use std::time::Duration;

use headgate_core::{AdmitRequest, Envelope, Store};
use headgate_migrate::{Direction, MigrateOptions, migrate_mysql, validate_mysql};
use headgate_mysql::MysqlStore;
use headgate_testkit::MysqlTestDatabase;
use mysql_async::{OptsBuilder, Pool};

fn pool(database: &MysqlTestDatabase) -> Pool {
    Pool::new(OptsBuilder::from_opts(database.opts()).client_found_rows(true))
}

fn envelope(kind: &str) -> Envelope {
    Envelope {
        id: "same-job-id".into(),
        kind: kind.into(),
        payload: kind.as_bytes().to_vec(),
        queue: "same-queue".into(),
        fingerprint: format!("fp-{kind}"),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

fn request(worker: &str, lease_id: &str) -> AdmitRequest {
    AdmitRequest {
        worker: worker.into(),
        lease_id: lease_id.into(),
        queues: vec!["same-queue".into()],
        capacity: 1,
        lease: Duration::from_secs(30),
        quantum: 1_000,
    }
}

#[tokio::test]
async fn databases_isolate_store_duties_and_destructive_migrations() {
    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping MySQL multi-instance proof");
        return;
    };
    let (left_db, right_db) = tokio::join!(
        MysqlTestDatabase::create(&url),
        MysqlTestDatabase::create(&url),
    );
    let left_db = left_db.expect("left database");
    let right_db = right_db.expect("right database");
    let left_pool = pool(&left_db);
    let right_pool = pool(&right_db);
    let left = MysqlStore::new(left_pool.clone());
    let right = MysqlStore::new(right_pool.clone());

    left.enqueue(&[envelope("left-kind")])
        .await
        .expect("left enqueue");
    right
        .enqueue(&[envelope("right-kind")])
        .await
        .expect("right enqueue");
    let left_job = left
        .as_inspect()
        .unwrap()
        .get_job("same-job-id", true)
        .await
        .expect("left inspect")
        .expect("left job");
    let right_job = right
        .as_inspect()
        .unwrap()
        .get_job("same-job-id", true)
        .await
        .expect("right inspect")
        .expect("right job");
    assert_eq!(left_job.kind, "left-kind");
    assert_eq!(right_job.kind, "right-kind");

    let left_units = left
        .admit(request("left-worker", "left-lease"))
        .await
        .unwrap();
    let right_units = right
        .admit(request("right-worker", "right-lease"))
        .await
        .unwrap();
    assert_eq!(left_units[0].claims[0].envelope.kind, "left-kind");
    assert_eq!(right_units[0].claims[0].envelope.kind, "right-kind");
    assert!(
        left.claim_duty("same-duty", "left-holder", Duration::from_secs(30))
            .await
            .unwrap()
    );
    assert!(
        right
            .claim_duty("same-duty", "right-holder", Duration::from_secs(30))
            .await
            .unwrap()
    );

    let mut left_connection = left_pool.get_conn().await.expect("left migration conn");
    migrate_mysql(
        &mut left_connection,
        Direction::Down,
        MigrateOptions::default(),
    )
    .await
    .expect("drop only left installation");
    drop(left_connection);
    let mut right_connection = right_pool.get_conn().await.expect("right validation conn");
    let right_validation = validate_mysql(&mut right_connection)
        .await
        .expect("validate right");
    assert!(right_validation.is_ok(), "{:?}", right_validation.messages);
    drop(right_connection);
    assert!(
        right
            .as_inspect()
            .unwrap()
            .get_job("same-job-id", false)
            .await
            .unwrap()
            .is_some(),
        "rolling back the left database must not touch the right database"
    );

    drop(left);
    drop(right);
    left_pool.disconnect().await.expect("close left pool");
    right_pool.disconnect().await.expect("close right pool");
    left_db.cleanup().await.expect("clean left database");
    right_db.cleanup().await.expect("clean right database");
}
