# Enqueue during a store outage

headgate does not keep a hidden local enqueue buffer. If the configured store cannot
accept a job, `enqueue` returns a typed unavailable error and the job is not retained for
later replay.

That is a deliberate reliability boundary. An in-process buffer is neither durable nor
fleet-wide: a restart loses it, two producers recover independently, and memory pressure
silently becomes an admission policy. A durable sidecar or outbox can be a valid product
choice, but it is another queue and must be operated as one. headgate therefore leaves
that choice with the application instead of changing delivery semantics implicitly.

## Error contract

The same categories are exposed in both languages:

| Condition | Rust | Go | HTTP |
|---|---|---|---:|
| Connection refused, reset, timed out, or closed pool | `StoreError::Unavailable` | `*UnavailableError` / `ErrUnavailable` | 503 |
| Invalid envelope or duration | `StoreError::Invalid` | `*InvalidError` / `ErrInvalid` | 400 |
| Caller-supplied id conflicts | `StoreError::IdConflict` | `*IDConflictError` | 409 |
| Unique-key holder already exists | `StoreError::Duplicate` | `*DuplicateError` | 409 |

Every driver validates the batch before acquiring a connection. Consequently, a malformed
job remains a caller error even while the store is down; transport state never masks it as
a retryable outage. Errors that require reading durable state, such as collision with an
existing row, can only be decided while that state is reachable.

The Go drivers normalize concrete `pgx`, `database/sql`, MySQL, and go-redis transport
errors at the outer enqueue boundary. Typed validation, conflict, uniqueness, and
quarantine errors pass through unchanged. The Rust drivers perform the same classification
in their connection/query adapters.

## Caller choices

On unavailable, choose explicitly:

- Fail the surrounding request and let an idempotent caller retry.
- Degrade the feature and record that no job was created.
- Write to an application-owned durable outbox and drain it deliberately.

Do not retry indefinitely inside a request. If the application retries, use a stable
caller-supplied job id or `Idempotency-Key`; a retry after an ambiguous network failure can
then join the original job instead of creating another.

On Postgres or MySQL, transactional enqueue is usually the cleanest answer. Put the
business write and enqueue in the same caller transaction. If the store is unavailable,
the business write fails too, leaving no split state to reconcile.

## Operational checks

Readiness reports the same unavailable taxonomy as enqueue. Alert on sustained HTTP 503s
or typed unavailable errors, but do not count validation, uniqueness, quarantine, or
policy rejection as store outages. The optional
[enqueue circuit breaker](enqueue-circuit-breaker.md) uses exactly that classifier;
backpressure remains a separate store policy. Neither changes the no-buffer rule.
