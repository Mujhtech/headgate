# Go 1.27 measurements and dependency refresh

Measured on September 2, 2026 with Go 1.27.1, macOS arm64, Apple M2 Pro,
GOMAXPROCS=1. Each case uses a typed job with a 1 KiB ASCII message. Ten samples
per configuration, 200 ms per sample, interleaved serially after compiling all
binaries. No verification or lint process ran during the timed measurements.

## JSON engine

The comparison uses the same source, dependencies, and Go 1.27.1 compiler:
`GOEXPERIMENT=nojsonv2` versus the default. Production calls remain
`encoding/json`. This isolates the engine switch; it is not a comparison of
complete Go 1.24 and 1.27 applications.

| Operation | Legacy engine | Default engine | Time change | Allocations, old → new |
| --- | ---: | ---: | ---: | ---: |
| Typed payload decode | 5.981 µs | 1.778 µs | −70.28% | 8 → 3 |
| Typed handler dispatch | 6.042 µs | 1.836 µs | −69.61% | 9 → 4 |
| Payload encode | 1.448 µs | 1.188 µs | −17.99% | 2 → 3 |

All three time differences are significant in benchstat (reported p=0.000,
n=10). Encoding allocates 1,248 rather than 1,200 bytes per operation: faster
does not mean lower allocation cost in every direction. Payloads with maps,
custom marshalers, binary data, or different sizes can behave differently.
Dispatch here measures decode and the registered handler adapter; it does not
include store I/O, leases, polling, or the complete worker loop. No fleet
throughput or tail-latency improvement is claimed.

## Allocator

Compared `GOEXPERIMENT=nosizespecializedmalloc` with the default while retaining
the default JSON engine. None of the three cases showed a statistically
significant time difference (p=0.493, 0.055, and 0.361). Allocations and bytes per
operation were unchanged. The new allocator stays enabled; these workloads do
not establish a benefit from it independently of JSON.

Green Tea GC and container-aware GOMAXPROCS remain enabled by runtime defaults.
This benchmark does not isolate either one's effect.

## Reproduce

From the repository root, with Go 1.27.1 and `benchstat` on PATH:

```sh
bash scripts/bench-go-runtime.sh /tmp/headgate-runtime-results
```

The script builds all configurations before timing and runs them serially. Use
an otherwise idle machine. Raw samples and benchstat reports for this run are
in [benchmarks/go127-20260902](benchmarks/go127-20260902/).

## Dependencies

All existing module paths keep their current major versions. Local sibling
`replace` directives remain because they wire this multi-module checkout; none
were external compatibility forks or Go 1.24 workarounds.
The workspace selects toolchain Go 1.27.1 so CI and release builds use the patch
version validated here; module language minimums remain Go 1.27.0.

| Dependency | Previous | Updated |
| --- | --- | --- |
| pgx | 5.7.2 | 5.10.0 |
| go-redis | 9.7.0 | 9.22.0 |
| MySQL driver | 1.9.3 | 1.10.1 |
| OpenTelemetry APIs and SDKs | 1.41.0 | 1.46.0 |
| Cobra / pflag | 1.10.1 / 1.0.9 | 1.10.2 / 1.0.10 |
| x/sync | 0.10.0 | 0.22.0 |
| x/sys | 0.41.0 | 0.47.0 |
| x/text | 0.21.0 | 0.41.0 |
| testify (dependency tests) | 1.11.1 | 1.12.1 |

The old x/crypto requirement disappears after the pgx refresh and module tidy.
pgx 5.9 raised its Go minimum to 1.25; pgx 5.8 had already removed x/crypto.
Not every update required raising our Go minimum. Protobuf was already current
at 1.36.12. Transitive requirements and checksums were resolved for each module;
core still has no database driver or exporter dependency.

Upstream release notes: [pgx](https://github.com/jackc/pgx/blob/v5.10.0/CHANGELOG.md),
[Redis](https://github.com/redis/go-redis/releases/tag/v9.22.0),
[MySQL](https://github.com/go-sql-driver/mysql/releases/tag/v1.10.1),
[OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go/releases/tag/v1.46.0).

## Verification after dependency updates

The complete `scripts/verify.sh` run passed with disposable PostgreSQL 17,
Redis 7.4, and MySQL 8.4: 1,058 admission/API assertions, 36 shared scenarios
(96 assertions), and 741 resolved evidence citations with zero evidence debt.
Rust and Go database tests had zero skips. Two existing MySQL pending-command
read-path checks remain announced skips in the admission corpus. CLI and
OpenTelemetry are now included in the main vet/build/test gate.

Race checks passed for core, API, CLI, workflows, OpenTelemetry, and shared JSON
contracts. golangci-lint 2.13.2 reported zero issues for changed packages;
gopls 0.23.0 accepted the generic methods. `govulncheck` reported no known
vulnerabilities across all workspace modules. All 13 modules, including
examples, passed `go mod verify`.
