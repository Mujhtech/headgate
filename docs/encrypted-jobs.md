# Client-side encrypted job payloads

`headgate-crypto` (Rust) and `headgatecrypto` (Go) encrypt an envelope's payload before it
reaches a Headgate store and decrypt it only inside the registered handler. They use
AES-256-GCM with a fresh 96-bit nonce for every encryption. The wire format is versioned
and carries a key identifier so old jobs remain readable during key rotation.

This is payload encryption, not opaque jobs. The store and control plane still see the
job id, kind, schema version, queue, partition, rate class, tags, headers, schedule and
other policy metadata. Results, progress, output and attempt errors are not encrypted by
this layer. Applications that put secrets in those fields need a separate protection
strategy.

The authenticated additional data binds ciphertext to the job id, kind and schema
version. Moving ciphertext to another job or changing its decoder identity therefore
fails authentication and the runtime records the job as `undecodable`; it does not retry
an authentication failure forever. The fingerprint is derived from plaintext before the
random nonce is added, preserving poison-pill grouping and uniqueness behavior. That also
means the fingerprint can reveal that two encrypted payloads are equal.

Use `register_encrypted` in Rust or `headgatecrypto.RegisterEncrypted` in Go for an encrypted
task kind. Registering the normal typed handler for that kind would try to decode the
ciphertext directly. Encryption increases payload size, and backend payload limits apply
to the ciphertext.

## Key rotation

The provider names one current key for writes and may retain historical keys for reads.
Rotate by installing the new key alongside the old keys, switching the current key id,
then retaining every old key until no queued, retryable, scheduled, quarantined or
retained job can reference it. Removing a referenced key intentionally makes those jobs
undecodable. Key storage, access control and rotation delivery belong to the embedding
application or its KMS adapter; Headgate does not persist keys.

The Rust and Go implementations share a deterministic wire vector, so either language
can decrypt a payload produced by the other when given the same key.
