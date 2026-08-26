# Rolling restarts and the memory guard

headgate workers support two distinct shutdown paths. The operator command `restart`
stops admission, releases the worker's singleton duties, and waits without a deadline for
every running handler to finish. The ordinary `terminate` command and the programmatic
shutdown handle retain the configured bounded drain and release work that outlives that
deadline.

The runtime does not fork or launch its replacement. A process supervisor should start a
new worker, wait until it is healthy, then send `restart` to the old worker through
`POST /workers/{worker_id}/signal`. The command is consume-once. The old worker clears it
before draining so reuse of a stable worker id cannot immediately stop its replacement.

## Memory guard

Set `memory_limit_bytes` and `memory_check_interval` in Rust, or `MemoryLimitBytes` and
`MemoryCheckInterval` in Go. A zero/absent limit disables sampling. Every successful
sample emits a `WorkerMemory` / `worker_memory` telemetry event with the measured bytes,
limit, and whether that sample requested a restart.

Crossing the limit stops admission and uses the ordinary **bounded** drain. A leaking
process must not remain alive forever merely because one handler never returns; the
supervisor is responsible for replacing the exited process. Sampling errors are logged
and retried at the next interval rather than taking down a healthy worker.

The built-in Unix sampler reads the process resident-set high-water mark (`getrusage`),
reported as bytes after the platform's unit conversion. Both runtimes accept an injected
sampler for deterministic tests and for deployments that prefer a current-RSS or
container-aware measurement.

## Recommended rollout

1. Start the replacement worker and wait for its readiness check.
2. Send `restart` to the old worker.
3. Observe the old worker's in-flight count fall to zero; it releases singleton duties
   immediately so the replacement can acquire them during the drain.
4. Let the supervisor reap the exited old process.

Use `terminate` when completing every long-running handler is less important than a
bounded deployment window. Use `restart` when long jobs must be allowed to finish.
