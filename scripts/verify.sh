#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Homebrew installs libpq as keg-only, so psql may be available without being on PATH.
# Keep the normal PATH authoritative and add the standard Apple Silicon location only
# when command discovery would otherwise fail.
if ! command -v psql >/dev/null 2>&1 && [ -x /opt/homebrew/opt/libpq/bin/psql ]; then
  export PATH="/opt/homebrew/opt/libpq/bin:$PATH"
fi

# Keep live database tests serial. Retention and archive tests deliberately exercise
# fleet-wide sweeps, so concurrent test processes sharing one verification database can
# consume each other's fixtures even though the store behavior is correct.
#
# Output remains uncaptured because live tests are optional. The ledger below counts every
# announced skip and rejects a skip when its corresponding gate variable is set; a green
# run must say which backends it actually exercised.
LEDGER_DIR=${TMPDIR:-/tmp}/hg-verify-$$
mkdir -p "$LEDGER_DIR"
trap 'rm -rf "$LEDGER_DIR"' EXIT
RUSTLOG="$LEDGER_DIR/rust.log"; GOLOG="$LEDGER_DIR/go.log"

echo "== gate posture =="
for v in HG_TEST_PG HG_TEST_REDIS HG_TEST_MYSQL HG_MYSQL; do
  eval "val=\${$v:-}"
  if [ -n "$val" ]; then echo "  set:   $v"; else echo "  UNSET: $v  (the tests behind it will SKIP, not pass)"; fi
done

echo "== proto =="        && protoc --proto_path=proto --descriptor_set_out=/dev/null proto/headgate.proto && echo ok
echo "== specs =="        && python3 -c "
import yaml,glob
for f in ['api/headgate.openapi.yaml','conformance/state_machine.yaml']+glob.glob('conformance/scenarios/*.yaml'):
    yaml.safe_load(open(f)); print(' ok', f)"
echo "== ui =="           && pnpm --dir ui check
# Preserve skip announcements in the transcript so a skipped capability cannot look
# like a passing one.
echo "== rust =="         && RUST_TEST_THREADS=1 cargo test --workspace -q -- --nocapture 2>&1 | tee "$RUSTLOG"
# Go only reports skip lines in verbose mode. Keep the complete transcript while showing
# a compact summary in the terminal.
echo "== go =="           && (cd go && go vet ./... ./driver/headgatepgx/... ./driver/headgatemysql/... ./driver/headgateredis/... ./headgateapi/... ./headgatemigrate/... ./headgatetest/... ./headgateui/... \
                                    && go build ./... ./driver/headgatepgx/... ./driver/headgatemysql/... ./driver/headgateredis/... ./headgateapi/... ./headgatemigrate/... ./headgatetest/... ./headgateui/... \
                                    && go test -p 1 -v ./... ./driver/headgatepgx/... ./driver/headgatemysql/... ./driver/headgateredis/... ./headgateapi/... ./headgatemigrate/... ./headgatetest/... ./headgateui/... 2>&1 \
                                       | tee "$GOLOG" \
                                       | awk '/^(---|===) (SKIP|FAIL)/ || /^(ok|FAIL|\?)[ \t]/ || /^ *--- SKIP/ {print}')
echo "== examples =="     && ./scripts/test-examples.sh
echo "== shared sql ==" && cmp crates/headgate-postgres/queries/admit.sql go/driver/headgatepgx/admit.sql \
                          && cmp crates/headgate-postgres/queries/admit_direct.sql go/driver/headgatepgx/admit_direct.sql \
                          && cmp crates/headgate-mysql/queries/eligible.sql go/driver/headgatemysql/eligible.sql \
                          && echo " ok admit.sql + admit_direct.sql + eligible.sql copies identical"
echo "== shared ui ==" && diff -qr ui/dist go/headgateui/dist \
                          && echo " ok console SPA copies identical"
echo "== shared lua =="   && for f in admit enqueue ack renew checkpoint reclaim promote duty admin sched worker explain output progress; do \
                               cmp "crates/headgate-redis/lua/$f.lua" "go/driver/headgateredis/lua/$f.lua" \
                                 || { echo "DRIFT: $f.lua copies differ"; exit 1; }; \
                             done && echo " ok lua copies identical (14 scripts)"
echo "== migrations =="    && python3 ./scripts/check-migrations.py
echo "== deps =="         && ./scripts/check-deps.sh
# The deleted-test guard runs before the live corpus. Whether a test vanished is a property
# of the tree, and a suite must not go green merely because its assertions disappeared.
echo "== test inventory ==" && python3 ./scripts/check-inventory.py
echo "== admission =="    && ./scripts/test-admission.sh
# The scenario runner follows the admission suite because that suite builds the four
# language/backend harness binaries it drives.
echo "== scenarios =="    && python3 ./scripts/run-scenarios.py
# Resolve every declared capability to a named assertion or test in the transcripts above.
# A capability cannot be declared unless its evidence ran in this verification pass.
echo "== register evidence ==" && python3 ./scripts/check-evidence.py

echo "== skip ledger =="
# Every gate in both languages announces itself as "HG_TEST_<X> not set" — the Rust
# ones through eprintln! (visible only under --nocapture), the Go ones through t.Skip
# (visible only under -v). Counted from those exact bytes rather than from a heuristic.
rust_skips=$(grep -c 'HG_TEST_[A-Z]* not set' "$RUSTLOG" 2>/dev/null || true)
go_skips=$(grep -c -- '--- SKIP' "$GOLOG" 2>/dev/null || true)
echo "  rust tests that skipped: ${rust_skips:-0}"
echo "  go tests that skipped:   ${go_skips:-0}"
ledger_fail=0
for v in HG_TEST_PG HG_TEST_REDIS HG_TEST_MYSQL; do
  eval "val=\${$v:-}"
  [ -n "$val" ] || continue
  # A gate variable that IS set must leave nothing skipped on its account.
  n=$(cat "$RUSTLOG" "$GOLOG" 2>/dev/null | grep -c "$v not set" || true)
  if [ "${n:-0}" -gt 0 ]; then
    echo "  ❌ $v is SET but $n test(s) still announced skipping on it:"
    grep -h "$v not set" "$RUSTLOG" "$GOLOG" 2>/dev/null | sort -u | sed 's/^/       /'
    ledger_fail=1
  fi
done
if [ "${rust_skips:-0}" -gt 0 ] || [ "${go_skips:-0}" -gt 0 ]; then
  echo "  ⏭  NOT ALL GREEN MEANS NOT ALL RUN. The skipped tests above proved nothing in"
  echo "     this run; conformance/MYSQL_VERIFICATION.md is the ledger for the MySQL half."
fi
[ "$ledger_fail" -eq 0 ] || { echo; echo "FAILED: skip ledger"; exit 1; }
echo; echo "ALL GREEN"
