#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== Rust examples =="
cargo fmt --manifest-path examples/rust/Cargo.toml --all -- --check
cargo check --manifest-path examples/rust/Cargo.toml --all-targets
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin basic
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin job-result
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin retry
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin uniqueness
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin tenant-fairness
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin progress
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin snooze
cargo run --quiet --manifest-path examples/rust/Cargo.toml --bin workflow

echo "== Go examples =="
unformatted=$(find examples/go -name '*.go' -print0 | xargs -0 gofmt -l)
test -z "$unformatted" || { printf '%s\n' "$unformatted"; exit 1; }
(
  cd examples/go
  GOWORK=off go vet ./...
  GOWORK=off go test ./...
  GOWORK=off go run ./basic
  GOWORK=off go run ./rate_limit
  GOWORK=off go run ./retry
  GOWORK=off go run ./uniqueness
  GOWORK=off go run ./priority
  GOWORK=off go run ./progress
  GOWORK=off go run ./sticky_routing
  GOWORK=off go run ./workflow
)
