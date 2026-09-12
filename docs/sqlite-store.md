# SQLite store implementation contract

SQLite is the fourth Headgate backend. It is an embedded, durable worker store, not a
shortcut around the admission gate and not a persistence wrapper around either in-memory
test store.

Current status: complete in Rust and Go. The packages implement the mandatory `Store`
contract plus result, output, progress, transactional, and bounded inspection/control
surfaces. Both advertise `TRANSACTIONAL | INSPECT`, and both capability declarations are
validated against their runtime interfaces. The standalone control APIs accept
`HG_STORE=sqlite`; `HG_SQLITE` selects the database file.

## Release slices

1. **Complete.** Implement the complete mandatory `Store` port in Rust and Go: enqueue, atomic
   admission, fenced ack and renewal, checkpoints, lease reclamation, due promotion,
   retention eviction, and duty leases.
2. **Complete.** Add `ResultStore`, `OutputStore`, and `ProgressStore`, each with fence and
   readback tests.
3. **Complete.** Add `Transactional` using a physical connection owned by the transaction
   handle, including transactional enqueue, completion, effects, and checkpoints.
4. **Complete.** Add bounded `Inspect`, control operations, schedules, workers, events,
   results/output/progress/checkpoint inspection, and wire the shared control API/console.
5. **Complete.** Add Rust and Go SQLite cells to the cross-language scenario corpus;
   migrations remain embedded and byte-gated.

A slice is not released merely because it compiles. Its capability register row changes
only after the corresponding live scenarios run in both languages.

## Atomic admission

SQLite has no `SKIP LOCKED`, so writers serialize. Admission starts `BEGIN IMMEDIATE`,
reads store time once, evaluates policies from durable tables, and claims selected jobs
with a fenced update that writes state, lease id, fence, and lease expiry together.
Policy-rejected rows are never updated. Busy or locked errors are surfaced as store
unavailability; callers use their normal bounded poll/backoff path.

The single-writer limitation is an explicit deployment tradeoff. It is suitable for an
embedded process and small local fleets, not a claim that SQLite scales like PostgreSQL.

## Connection rules

- File databases enable foreign keys on every connection and use a bounded busy timeout.
- WAL is enabled for file databases, never assumed for memory databases.
- Ordinary `:memory:` databases use exactly one physical connection. A pool of
  `:memory:` connections is a pool of separate databases.
- Store time comes from SQLite inside the transaction. Caller time never participates in
  leasing, refill, scheduling, or duty ownership.
- Integer ranges and state values are constrained explicitly because SQLite's dynamic
  typing is not a substitute for boundary validation.

## Capabilities and interface drift

Rust's `headgate_core::Store` trait and Go's `headgate.Store` interface remain the one
mandatory contract. SQLite does not gain a bespoke worker method. Optional surfaces stay
separate, and SQLite implements the same interfaces consumed by the shared runtime and
control API rather than adding SQLite-only operations.

`headgate_core::validate_store_capabilities` checks Rust capability bits against runtime
upcasts in both directions. Go's `headgate.ValidateAdvertisedCapabilities` checks that
every advertised bit has its corresponding optional interface; the reverse is not valid
in Go because a type may support notifications while a particular instance was created
without the connection state needed to enable them.

SQLite deliberately does not advertise `NOTIFYING`: cross-process notification would be
polling disguised as push. Workers use the normal bounded poll/backoff path. This is an
honest deployment limitation, not a missing SQLite-specific interface.
