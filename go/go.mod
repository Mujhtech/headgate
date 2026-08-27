module github.com/mujhtech/headgate/go

go 1.24.0

// Core has no driver or exporter dependencies.
// This is a CI gate, not a guideline: scripts/check-deps.sh fails the build if
// pgx, go-sql-driver, go-redis, or a metrics exporter appears in `go list -m all`.

require google.golang.org/protobuf v1.36.12
