# Runnable examples

These examples need no database or services. They use the in-memory store and exit with
an error if the demonstrated behavior changes.

| Language | Example | Scenario | Run |
| --- | --- | --- | --- |
| Rust | `basic` | Typed registration, enqueue, worker drain, completion | `cargo run --manifest-path examples/rust/Cargo.toml --bin basic` |
| Rust | `job-result` | Persisting a result only after successful completion | `cargo run --manifest-path examples/rust/Cargo.toml --bin job-result` |
| Rust | `retry` | Returned errors, retry attempts, deterministic backoff | `cargo run --manifest-path examples/rust/Cargo.toml --bin retry` |
| Rust | `uniqueness` | Lifecycle uniqueness and allowlisted field replacement | `cargo run --manifest-path examples/rust/Cargo.toml --bin uniqueness` |
| Rust | `tenant-fairness` | Work-conserving service across noisy and quiet partitions | `cargo run --manifest-path examples/rust/Cargo.toml --bin tenant-fairness` |
| Rust | `progress` | Fenced progress updates retained with a completed job | `cargo run --manifest-path examples/rust/Cargo.toml --bin progress` |
| Rust | `snooze` | Delayed re-scheduling without consuming a retry attempt | `cargo run --manifest-path examples/rust/Cargo.toml --bin snooze` |
| Rust | `workflow` | Validated fan-out/fan-in DAG prepared as one atomic enqueue batch | `cargo run --manifest-path examples/rust/Cargo.toml --bin workflow` |
| Go | `basic` | Typed registration, enqueue, worker drain, completion | `(cd examples/go && GOWORK=off go run ./basic)` |
| Go | `rate-limit` | Fleet token bucket and non-failure rate-limit requeue | `(cd examples/go && GOWORK=off go run ./rate_limit)` |
| Go | `retry` | Returned errors, retry attempts, deterministic backoff | `(cd examples/go && GOWORK=off go run ./retry)` |
| Go | `uniqueness` | Trailing-edge debounce with duplicate winner identity | `(cd examples/go && GOWORK=off go run ./uniqueness)` |
| Go | `priority` | Priority ordering within one tenant partition | `(cd examples/go && GOWORK=off go run ./priority)` |
| Go | `progress` | Fenced progress updates retained with a completed job | `(cd examples/go && GOWORK=off go run ./progress)` |
| Go | `sticky-routing` | Strict worker affinity plus unpinned work | `(cd examples/go && GOWORK=off go run ./sticky_routing)` |
| Go | `workflow` | Validated fan-out/fan-in DAG prepared as one atomic enqueue batch | `(cd examples/go && GOWORK=off go run ./workflow)` |
| UI | `ui-demo` | Every console view with realistic read-only jobs, policies, workers, schedules, and a workflow DAG | `(cd examples/go && GOWORK=off go run ./ui_demo)` |

The UI example serves the actual embedded dashboard at
[`http://127.0.0.1:8080`](http://127.0.0.1:8080). Its API data is intentionally
in-memory and read-only, so you can inspect every route without starting Postgres, Redis,
or MySQL. Use `-addr` to choose another listener, for example
`go run ./ui_demo -addr 127.0.0.1:9090`.

Run every example and its compile checks with:

```bash
scripts/test-examples.sh
```

The in-memory store preserves lifecycle, fencing, retry, uniqueness, fairness, and rate
limit behavior. Use the backend-specific setup in the [getting-started guide](../docs/getting-started.md)
when testing database connectivity, migrations, or transactional enqueue.

More focused usage is documented in [testing](../docs/testing.md),
[job progress](../docs/job-progress.md), [job results](../docs/job-results.md),
[insert and await](../docs/insert-and-await.md), [workflows](../docs/workflows.md), and
[the embedded console](../docs/console.md).
