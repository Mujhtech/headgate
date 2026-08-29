use std::collections::BTreeMap;

use headgate::{Envelope, Task};
use headgate_crypto::{StaticKeyring, decrypt_envelope, encrypt_envelope};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Task)]
#[task(kind = "example:secret-report", version = 1)]
struct SecretReport {
    account: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let keys = StaticKeyring::new("2026-08", BTreeMap::from([("2026-08".into(), [7u8; 32])]))?;
    let task = SecretReport {
        account: "customer-42".into(),
    };
    let plaintext = task.encode()?;
    let encrypted = encrypt_envelope(
        &keys,
        Envelope {
            id: "secret-report-1".into(),
            kind: SecretReport::TYPE.into(),
            schema_version: SecretReport::VERSION,
            payload: plaintext.clone(),
            queue: "reports".into(),
            ..Default::default()
        },
    )?;

    assert_ne!(encrypted.payload, plaintext);
    assert_eq!(decrypt_envelope(&keys, &encrypted)?, plaintext);
    println!("payload encrypted before enqueue and authenticated on decode");
    Ok(())
}
