use std::io;
use std::sync::{Arc, Mutex};

use headgate::{CodecError, Envelope, JobCtx, Registry, Task, WorkerConfig, testing};
use headgate_testkit::MemStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct WelcomeEmail {
    email: String,
    locale: String,
}

impl WelcomeEmail {
    fn validate(self) -> Result<Self, CodecError> {
        if self.email.is_empty() || self.locale.is_empty() {
            return Err(CodecError::Malformed(
                "email and locale must be nonempty".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Deserialize)]
struct WelcomeV1 {
    address: String,
}

#[derive(Deserialize)]
struct WelcomeV2 {
    email: String,
}

fn malformed(error: serde_json::Error) -> CodecError {
    CodecError::Malformed(error.to_string())
}

impl Task for WelcomeEmail {
    const TYPE: &'static str = "email:welcome";
    const VERSION: u32 = 3;

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(malformed)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice::<Self>(bytes)
            .map_err(malformed)?
            .validate()
    }

    fn upcast(version: u32, bytes: &[u8]) -> Result<Self, CodecError> {
        let current = match version {
            1 => {
                let old: WelcomeV1 = serde_json::from_slice(bytes).map_err(malformed)?;
                Self {
                    email: old.address,
                    locale: "en".into(),
                }
            }
            2 => {
                let old: WelcomeV2 = serde_json::from_slice(bytes).map_err(malformed)?;
                Self {
                    email: old.email,
                    locale: "en".into(),
                }
            }
            Self::VERSION => return Self::decode(bytes),
            other => return Err(CodecError::UnknownVersion(other)),
        };
        current.validate()
    }
}

fn fixtures() -> Vec<(&'static str, u32, &'static str, Option<WelcomeEmail>)> {
    vec![
        (
            "v1",
            1,
            r#"{"address":"ada@example.com"}"#,
            Some(WelcomeEmail {
                email: "ada@example.com".into(),
                locale: "en".into(),
            }),
        ),
        (
            "v2",
            2,
            r#"{"email":"ada@example.com"}"#,
            Some(WelcomeEmail {
                email: "ada@example.com".into(),
                locale: "en".into(),
            }),
        ),
        (
            "v3",
            3,
            r#"{"email":"ada@example.com","locale":"fr"}"#,
            Some(WelcomeEmail {
                email: "ada@example.com".into(),
                locale: "fr".into(),
            }),
        ),
        (
            "future",
            4,
            r#"{"email":"ada@example.com","locale":"fr"}"#,
            None,
        ),
        ("missing-locale", 3, r#"{"email":"ada@example.com"}"#, None),
        ("malformed-v1", 1, r#"{"address":42}"#, None),
    ]
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemStore::new());
    let client = headgate::Client::new(store.clone());
    let mut registry = Registry::new();
    let handled = Arc::new(Mutex::new(Vec::new()));
    let seen = handled.clone();
    registry
        .register::<WelcomeEmail, _, _>(move |_: JobCtx, task| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(task);
                Ok(())
            }
        })
        .map_err(io::Error::other)?;
    let registry = Arc::new(registry);
    let config = WorkerConfig {
        queues: vec!["mail".into()],
        run_duties: false,
        ..Default::default()
    };

    for (name, version, payload, want) in fixtures() {
        let id = format!("versioning-{name}");
        client
            .enqueue(&[Envelope {
                id: id.clone(),
                kind: WelcomeEmail::TYPE.into(),
                schema_version: version,
                payload: payload.as_bytes().to_vec(),
                queue: "mail".into(),
                max_attempts: 3,
                retention_ms: 60_000,
                ..Default::default()
            }])
            .await?;
        let before = handled.lock().unwrap().len();
        let result = testing::perform_job(&store, &registry, &config)
            .await
            .ok_or_else(|| io::Error::other(format!("{name}: job was not admitted")))?;
        let (outcome, state) = if want.is_some() {
            ("success", "completed")
        } else {
            ("undecodable", "undecodable")
        };
        assert_eq!(result.job_id, id);
        assert_eq!(result.outcome, outcome, "{name}");
        let (stored, stored_state) = store.job_state(&id).expect("job must be retained");
        assert_eq!(stored_state, state, "{name}");
        assert_eq!(stored.schema_version, version);
        assert_eq!(stored.payload, payload.as_bytes());
        let handled = handled.lock().unwrap();
        if let Some(want) = want {
            assert_eq!(handled.len(), before + 1);
            assert_eq!(handled[before], want);
        } else {
            assert_eq!(handled.len(), before, "invalid payload reached handler");
        }
        println!("{name} -> {outcome}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_stored_versions_and_rejects_invalid_payloads() {
        for (name, version, payload, want) in fixtures() {
            let got = WelcomeEmail::upcast(version, payload.as_bytes());
            match want {
                Some(want) => assert_eq!(got.unwrap(), want, "{name}"),
                None => {
                    assert!(got.is_err(), "{name}: invalid payload decoded");
                    if version == 4 {
                        assert!(matches!(got, Err(CodecError::UnknownVersion(4))));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn versioned_jobs_use_the_runtime_and_preserve_stored_payloads() {
        run().await.unwrap();
    }
}
