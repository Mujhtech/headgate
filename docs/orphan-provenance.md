# Orphan provenance

The inspection API exposes `orphaned: true` after a job has been reclaimed from an
expired worker lease. It is durable provenance derived from `crash_attempt > 0`; it is
not another job state. The job remains `retryable`, `available`, `running`, or terminal
according to the normal state machine, while operators can still see that an earlier
holder disappeared.

Returned handler errors do not set `orphaned`, because they increment `attempt` rather
than `crash_attempt`. The field remains true through later retries and completion for as
long as the retained job row exists.
