use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use headgate::{CodecError, Envelope, JobCtx, Registry, Store, Task, WorkerConfig, testing};
use headgate_core::Inspect;
use headgate_crypto::{StaticKeyring, encrypt_envelope, register_encrypted};
use headgate_postgres::PgStore;

struct Secret(String);
impl Task for Secret {
    const TYPE: &'static str = "crypto:secret";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

#[tokio::test]
async fn live_store_holds_ciphertext_while_handler_receives_plaintext() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping encrypted payload proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 2).unwrap());
    let id = format!("crypto-live-{}", std::process::id());
    let queue = id.clone();
    let plaintext = b"production secret".to_vec();
    let ring = Arc::new(
        StaticKeyring::new("current", BTreeMap::from([("current".into(), [42u8; 32])])).unwrap(),
    );
    let encrypted = encrypt_envelope(
        ring.as_ref(),
        Envelope {
            id: id.clone(),
            kind: Secret::TYPE.into(),
            schema_version: 1,
            payload: plaintext.clone(),
            queue: queue.clone(),
            retention_ms: 86_400_000,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        !encrypted
            .payload
            .windows(plaintext.len())
            .any(|w| w == plaintext)
    );
    store
        .enqueue(std::slice::from_ref(&encrypted))
        .await
        .unwrap();
    let stored = store
        .get_job(&id, true)
        .await
        .unwrap()
        .unwrap()
        .payload
        .unwrap();
    assert_eq!(stored, encrypted.payload);
    assert_ne!(stored, plaintext);

    let seen = Arc::new(Mutex::new(String::new()));
    let mut registry = Registry::new();
    let output = seen.clone();
    register_encrypted::<Secret, _, _>(&mut registry, ring, move |_ctx: JobCtx, task: Secret| {
        let output = output.clone();
        async move {
            *output.lock().unwrap() = task.0;
            Ok(())
        }
    })
    .unwrap();
    let cfg = WorkerConfig {
        queues: vec![queue],
        ..Default::default()
    };
    let outcomes = testing::drain(&store, &Arc::new(registry), &cfg, 1).await;
    assert_eq!(outcomes, [id]);
    assert_eq!(&*seen.lock().unwrap(), "production secret");
}
