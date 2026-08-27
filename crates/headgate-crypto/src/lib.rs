//! Client-side AES-256-GCM payload encryption. Stores only ever see ciphertext.

use std::{collections::BTreeMap, sync::Arc};

use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use headgate::{CodecError, Envelope, JobCtx, JobError, Registry, Task};
use rand::RngCore;

const MAGIC: &[u8; 5] = b"HGEC\x01";
const NONCE_LEN: usize = 12;

#[derive(Debug)]
pub struct CryptoError(String);
impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for CryptoError {}

pub trait KeyProvider: Send + Sync + 'static {
    fn active_key(&self) -> Result<(String, [u8; 32]), CryptoError>;
    fn key(&self, id: &str) -> Result<[u8; 32], CryptoError>;
}

#[derive(Clone)]
pub struct StaticKeyring {
    active: String,
    keys: BTreeMap<String, [u8; 32]>,
}

impl StaticKeyring {
    pub fn new(
        active: impl Into<String>,
        keys: BTreeMap<String, [u8; 32]>,
    ) -> Result<Self, CryptoError> {
        let active = active.into();
        if !keys.contains_key(&active) {
            return Err(CryptoError("active encryption key is missing".into()));
        }
        Ok(Self { active, keys })
    }
}

impl KeyProvider for StaticKeyring {
    fn active_key(&self) -> Result<(String, [u8; 32]), CryptoError> {
        Ok((self.active.clone(), self.keys[&self.active]))
    }
    fn key(&self, id: &str) -> Result<[u8; 32], CryptoError> {
        self.keys
            .get(id)
            .copied()
            .ok_or_else(|| CryptoError(format!("encryption key `{id}` is unavailable")))
    }
}

/// Encrypt an envelope in place. The poison-pill fingerprint is derived from plaintext
/// before the randomized nonce is introduced, so identical bad jobs still quarantine
/// together without revealing their payload to the store.
pub fn encrypt_envelope(
    keys: &dyn KeyProvider,
    mut env: Envelope,
) -> Result<Envelope, CryptoError> {
    if env.fingerprint.is_empty() {
        env.fingerprint = headgate::fingerprint(&env.kind, &env.payload);
    }
    let (key_id, key) = keys.active_key()?;
    if key_id.is_empty() || key_id.len() > u16::MAX as usize {
        return Err(CryptoError(
            "encryption key id must be 1..65535 bytes".into(),
        ));
    }
    let mut nonce = [0u8; NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce);
    env.payload = seal(&key_id, &key, &nonce, &aad(&env), &env.payload)?;
    Ok(env)
}

pub fn decrypt_envelope(keys: &dyn KeyProvider, env: &Envelope) -> Result<Vec<u8>, CryptoError> {
    if env.payload.len() < MAGIC.len() + 2 + NONCE_LEN + 16 || &env.payload[..MAGIC.len()] != MAGIC
    {
        return Err(CryptoError(
            "payload is not a headgate encrypted envelope".into(),
        ));
    }
    let id_len = u16::from_be_bytes([env.payload[5], env.payload[6]]) as usize;
    let id_start = 7;
    let nonce_start = id_start + id_len;
    if nonce_start + NONCE_LEN + 16 > env.payload.len() {
        return Err(CryptoError("encrypted payload header is truncated".into()));
    }
    let key_id = std::str::from_utf8(&env.payload[id_start..nonce_start])
        .map_err(|_| CryptoError("encryption key id is not UTF-8".into()))?;
    let key = keys.key(key_id)?;
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|_| CryptoError("invalid AES key".into()))?;
    cipher
        .decrypt(
            Nonce::from_slice(&env.payload[nonce_start..nonce_start + NONCE_LEN]),
            Payload {
                msg: &env.payload[nonce_start + NONCE_LEN..],
                aad: &aad(env),
            },
        )
        .map_err(|_| CryptoError("encrypted payload authentication failed".into()))
}

fn seal(
    key_id: &str,
    key: &[u8; 32],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher =
        Aes256Gcm::new_from_slice(key).map_err(|_| CryptoError("invalid AES key".into()))?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| CryptoError("payload encryption failed".into()))?;
    let mut out = Vec::with_capacity(MAGIC.len() + 2 + key_id.len() + NONCE_LEN + ciphertext.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(key_id.len() as u16).to_be_bytes());
    out.extend_from_slice(key_id.as_bytes());
    out.extend_from_slice(nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn aad(env: &Envelope) -> Vec<u8> {
    let mut out = Vec::with_capacity(env.id.len() + env.kind.len() + 12);
    out.extend_from_slice(&(env.id.len() as u32).to_be_bytes());
    out.extend_from_slice(env.id.as_bytes());
    out.extend_from_slice(&(env.kind.len() as u32).to_be_bytes());
    out.extend_from_slice(env.kind.as_bytes());
    out.extend_from_slice(&env.schema_version.to_be_bytes());
    out
}

pub fn register_encrypted<T, F, Fut>(
    registry: &mut Registry,
    keys: Arc<dyn KeyProvider>,
    handler: F,
) -> Result<(), String>
where
    T: Task,
    F: Fn(JobCtx, T) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
{
    registry.register_raw::<T, _, _>(move |ctx, mut env| {
        let keys = keys.clone();
        let plaintext =
            decrypt_envelope(keys.as_ref(), &env).map_err(|e| CodecError::Malformed(e.to_string()));
        let task = plaintext.and_then(|bytes| T::upcast(env.schema_version, &bytes));
        env.payload.clear();
        let future = task.map(|task| handler(ctx, task));
        async move {
            match future {
                Ok(future) => future.await,
                Err(error) => Err(Box::new(error) as JobError),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring() -> StaticKeyring {
        StaticKeyring::new("k1", BTreeMap::from([("k1".into(), [7u8; 32])])).unwrap()
    }

    fn env() -> Envelope {
        Envelope {
            id: "job-1".into(),
            kind: "secret:task".into(),
            schema_version: 1,
            payload: b"top secret".to_vec(),
            queue: "default".into(),
            ..Default::default()
        }
    }

    #[test]
    fn round_trip_binds_identity_and_preserves_plaintext_fingerprint() {
        let a = encrypt_envelope(&ring(), env()).unwrap();
        let b = encrypt_envelope(&ring(), env()).unwrap();
        assert_ne!(a.payload, b.payload, "fresh nonce per enqueue");
        assert_eq!(
            a.fingerprint, b.fingerprint,
            "fingerprint must not depend on nonce"
        );
        assert_eq!(decrypt_envelope(&ring(), &a).unwrap(), b"top secret");
        let mut moved = a;
        moved.id = "job-2".into();
        assert!(
            decrypt_envelope(&ring(), &moved).is_err(),
            "job id is authenticated AAD"
        );
    }

    #[test]
    fn tampering_and_missing_keys_fail_authentication() {
        let mut encrypted = encrypt_envelope(&ring(), env()).unwrap();
        *encrypted.payload.last_mut().unwrap() ^= 1;
        assert!(decrypt_envelope(&ring(), &encrypted).is_err());
        let missing =
            StaticKeyring::new("other", BTreeMap::from([("other".into(), [9u8; 32])])).unwrap();
        assert!(decrypt_envelope(&missing, &encrypt_envelope(&ring(), env()).unwrap()).is_err());
    }

    #[test]
    fn wire_vector_matches_go_byte_for_byte() {
        let nonce = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let env = env();
        let wire = seal("k1", &[7u8; 32], &nonce, &aad(&env), b"top secret").unwrap();
        let hex: String = wire.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "484745430100026b31000102030405060708090a0b6cee99506e6cba3b12c6527e0b794110389ff91129360bd1446d"
        );
    }
}
