# Replace on unique conflict

An enqueue with a `unique_key` may ask headgate to update selected fields on the job that
already holds that key. The insert still returns the ordinary typed duplicate result and
the existing job id; `replaced` says whether any requested field was actually applied.
Callers can therefore join the winner without confusing replacement with a second insert.

The request-only `unique_replace` mask is deliberately small:

| Bit | Constant | Replaced fields |
|---:|---|---|
| 1 | `UNIQUE_REPLACE_PAYLOAD` / `UniqueReplacePayload` | schema version, payload bytes, fingerprint |
| 2 | `UNIQUE_REPLACE_SCHEDULED_AT` / `UniqueReplaceScheduledAt` | scheduled time, only while the holder is `scheduled` |
| 4 | `UNIQUE_REPLACE_PRIORITY` / `UniqueReplacePriority` | priority |
| 8 | `UNIQUE_REPLACE_MAX_ATTEMPTS` / `UniqueReplaceMaxAttempts` | maximum attempts |

The HTTP field uses the same `0..15` bitmask and requires a caller-supplied `unique_key`;
the API's implicit Idempotency-Key dedup key cannot be used as a replacement target. A 409 response is shaped as
`{"error":"duplicate unique key","existing_id":"…","replaced":true|false}`.

Replacement requires a non-empty uniqueness request and a single-job enqueue. Unknown
bits and replacement in a batch are invalid. Queue, partition, kind, rate class, unique
window/states, headers, weight, timeout, deadline, retention, and periodic origin cannot
be changed through conflict replacement. Add a new reviewed bit rather than widening an
existing one when another field becomes safe to replace.

Only `scheduled`, `available`, and `retryable` holders are mutable. A running holder is
never edited, even when the request asks only for a seemingly harmless field; terminal
holders do not hold lifecycle uniqueness. Scheduled-time replacement does not perform a
state transition: it applies only to a holder that is already scheduled. Use the normal
reschedule/admin operation when transition semantics are intended.

The mutation is atomic with conflict resolution. SQL adapters lock the holder and update
inside the enqueue transaction. A plain enqueue commits an applied replacement before
returning the duplicate result; caller-owned transactional enqueue leaves commit or
rollback to its caller. Redis performs the decision, hash mutation, fingerprint-index
maintenance, and scheduling-index maintenance in one Lua invocation.
