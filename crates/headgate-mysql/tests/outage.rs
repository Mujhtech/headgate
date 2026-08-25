use headgate_core::{Envelope, Store, StoreError};
use headgate_mysql::MysqlStore;

#[tokio::test]
async fn enqueue_classifies_an_unreachable_mysql_without_masking_input_errors() {
    // TCP/1 is reserved and closed on the supported test platforms; using a fixed
    // refused endpoint also keeps this test runnable in sandboxes that deny bind(2).
    let store =
        MysqlStore::connect("mysql://headgate@127.0.0.1:1/headgate").expect("construct lazy pool");
    let valid = Envelope {
        id: "mysql-outage".into(),
        kind: "outage".into(),
        ..Default::default()
    };

    let err = store
        .enqueue(std::slice::from_ref(&valid))
        .await
        .expect_err("refused connection must fail");
    assert!(
        matches!(err, StoreError::Unavailable(_)),
        "refused enqueue must be typed unavailable, got {err:?}"
    );

    let mut invalid = valid.clone();
    invalid.id.clear();
    let err = store
        .enqueue(&[invalid])
        .await
        .expect_err("invalid envelope must fail");
    assert!(
        matches!(err, StoreError::Invalid(_)),
        "invalid envelope while down changed taxonomy: {err:?}"
    );

    let err = store
        .enqueue(&[valid.clone(), valid])
        .await
        .expect_err("duplicate id must fail");
    assert!(
        matches!(err, StoreError::IdConflict { .. }),
        "duplicate id while down changed taxonomy: {err:?}"
    );
}
