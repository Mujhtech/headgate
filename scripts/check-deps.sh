#!/usr/bin/env bash
# §8.4 invariant 8: core links against no driver and no exporter.
set -euo pipefail
fail=0
if command -v cargo >/dev/null; then
  if cargo tree -p headgate-core 2>/dev/null | grep -qE 'tokio-postgres|sqlx|redis|mysql|prometheus|opentelemetry-sdk'; then
    echo "FAIL: headgate-core pulled in a driver or exporter"; fail=1
  else echo "ok: headgate-core is clean"; fi
fi
if command -v go >/dev/null && [ -d go ]; then
  # GOWORK=off: the workspace legitimately contains driver modules (that is the whole
  # point of the multi-module layout); the invariant is about the CORE module's go.mod.
  if (cd go && GOWORK=off go list -m all 2>/dev/null | grep -qE 'pgx|go-sql-driver|go-redis|prometheus'); then
    echo "FAIL: go core pulled in a driver or exporter"; fail=1
  else echo "ok: go core is clean"; fi
fi
exit $fail
