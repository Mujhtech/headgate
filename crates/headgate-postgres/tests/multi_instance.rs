//! One physical Postgres database, one caller-owned pool, two independently migrated
//! headgate schemas. Identical logical keys are deliberate collision witnesses.

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use headgate_core::{AdmitRequest, Envelope, Store};
use headgate_migrate::{
    Direction, MigrateOptions, migrate_postgres_in_schema, validate_postgres_in_schema,
};
use headgate_postgres::PgStore;
use headgate_testkit::PostgresTestDatabase;
use tokio_postgres::NoTls;

fn pool(conninfo: &str) -> Pool {
    let config: tokio_postgres::Config = conninfo.parse().expect("Postgres config");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(2)
        .build()
        .expect("shared pool")
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
async fn explicit_schemas_isolate_store_duties_and_migrations_on_one_pool() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping Postgres multi-instance proof");
        return;
    };
    let (left_db, right_db) = tokio::join!(
        PostgresTestDatabase::create(&conninfo),
        PostgresTestDatabase::create(&conninfo),
    );
    let left_db = left_db.expect("left schema");
    let right_db = right_db.expect("right schema");

    let shared_pool = pool(&conninfo);
    let left = PgStore::in_schema(shared_pool.clone(), left_db.schema()).expect("left store");
    let right = PgStore::in_schema(shared_pool.clone(), right_db.schema()).expect("right store");
    assert_eq!(left.schema(), Some(left_db.schema()));
    assert_eq!(right.schema(), Some(right_db.schema()));

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

    let (mut admin, connection) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .expect("admin connect");
    let driver = tokio::spawn(connection);
    migrate_postgres_in_schema(
        &mut admin,
        left_db.schema(),
        Direction::Down,
        MigrateOptions::default(),
    )
    .await
    .expect("drop only left installation");
    let right_validation = validate_postgres_in_schema(&admin, right_db.schema())
        .await
        .expect("validate right");
    assert!(right_validation.is_ok(), "{:?}", right_validation.messages);
    assert!(
        right
            .as_inspect()
            .unwrap()
            .get_job("same-job-id", false)
            .await
            .unwrap()
            .is_some(),
        "rolling back the left schema must not touch the right schema"
    );
    drop(admin);
    driver.await.unwrap().unwrap();

    drop(left);
    drop(right);
    drop(shared_pool);
    left_db.cleanup().await.expect("clean left schema");
    right_db.cleanup().await.expect("clean right schema");
}
