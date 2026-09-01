#!/usr/bin/env bash
# Reproduces the verification the admission path already passed — now THROUGH the Rust
# store (crates/headgate-postgres), not raw psql. Every admit goes through Store::admit,
# every seed through Store::enqueue (the batch/unnest path); psql remains only for
# fixture resets, policy rows, and read-only assertions. The FIRST Redis section still
# drives admit.lua directly (that is the raw-gate regression corpus); the sections after
# it drive the Redis store port, which has existed in both languages since round 21.
# Requires a Postgres and a Redis. Override with PGHOST/PGPORT/PGDATABASE/REDIS_PORT. MySQL is
# OPTIONAL and gated: set HG_MYSQL, and both MySQL sections soft-skip without it.
# Also covered here, beyond the raw gate: both store ports in both languages, the
# cross-language sections, and §10.1 API parity over 6 server configurations
# (2 languages x 3 backends). No expected total is hardcoded — the gate is failed=0.
set -uo pipefail
cd "$(dirname "$0")/.."
PGH=${PGHOST:-/tmp}; PGP=${PGPORT:-5433}; PGD=${PGDATABASE:-hg}; RP=${REDIS_PORT:-6380}
PSQL="psql -h $PGH -p $PGP -U postgres -d $PGD -qtA"
RED="redis-cli -p $RP"
export HG_PG="host=$PGH port=$PGP user=postgres dbname=$PGD"
H=target/debug/hg-pg-harness
pass=0; fail=0; skip=0; guarded=0

# Preflight. A suite that goes GREEN because the store is unreachable is worse than
# no suite: `redis-cli` on a dead server prints nothing, and `grep -c bomb` on nothing
# returns 0, which is exactly the value the quarantine assertion wants.
$RED ping >/dev/null 2>&1 || { echo "FATAL: no Redis at port $RP"; exit 2; }
$PSQL -c 'select 1' >/dev/null 2>&1 || { echo "FATAL: no Postgres at $PGH:$PGP db=$PGD"; exit 2; }
command -v cargo >/dev/null || { echo "FATAL: cargo not found; the Postgres suite runs through the Rust store"; exit 2; }
cargo build -q -p headgate-postgres --bin hg-pg-harness || { echo "FATAL: harness build failed"; exit 2; }
# ===================== THE ANTI-VACUITY HARNESS (round 32h) =====================
# An assertion that can pass when the thing it tests is ABSENT is not an assertion.
# This sweep found three separate cases of exactly that, all of them shaped the same:
#
#   * round 32g — `reset_pg` never truncated `headgate_queue_state`, so on every run
#     after the first a duplicate-key INSERT rolled the WHOLE API seed back (psql wraps
#     a multi-statement `-c` in one transaction). `GET /rate-classes` and
#     `GET /quarantine` had been diffing `[]` against `[]` for many rounds.
#   * round 31 — a stray sweep quarantined the shared `fp` fingerprint, so six MySQL
#     assertions silently "got 0/empty".
#   * round 31 — `distinct_kinds(100)`'s bounded sample filled with strays.
#
# Three mechanisms, so the class cannot come back silently:
#
#  1. `chk` REFUSES a trivial expectation. If `want` is "", 0, [], {}, null, none,
#     false, not_found or "-", the assertion FAILS with an UNGUARDED-ZERO diagnostic
#     telling you to use `chk0`. There is no way to write a bare zero-comparison any
#     more; a future round has to supply a witness or the suite goes red.
#  2. `chk0` is that guarded form: it asserts a WITNESS is non-trivial FIRST, then the
#     zero. A fixture that never landed fails on the witness, before the zero it would
#     otherwise have satisfied. Both halves count as assertions.
#  3. `seed` runs a multi-statement psql seed under ON_ERROR_STOP and makes a ROLLBACK
#     a failed assertion. The round-32g bug would have been caught on its first run:
#     the duplicate-key print four rounds recorded as harmless noise was the symptom.
#
# Plus: skips are COUNTED and announced, so a section that soft-skipped can never be
# mistaken for a section that passed.
#
# Trim BOTH sides before comparing — macOS `wc` pads with spaces, GNU does not; trusting
# any platform's whitespace is how the double-claim guard once failed on formatting.
trim(){ echo "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'; }
# The value shapes an assertion can hold while proving nothing. `-` is here because
# `ranset()` prints it for an empty store, and two empty ranset() results compare equal.
trivial(){ case "$(trim "$1")" in ""|0|-|"[]"|"{}"|null|none|false|not_found|0.0) return 0;; *) return 1;; esac; }
# ===================== THE ASSERTION TRANSCRIPT (round 32j) =====================
# Every assertion this run EXECUTES is recorded here, one line per assertion, as
# `<PASS|FAIL|SKIP>\t<label>`. It is the ground truth `scripts/check-evidence.sh` resolves
# the capability register's evidence citations against.
#
# Why a transcript and not a static grep of this file: a label that exists in the source
# and a label that RAN are different facts, and the register's ✅ rows are claims about
# the second one. The MySQL sections are the case that makes the difference load-bearing —
# ~55 assertions in this file have never executed against a server, and a source-only
# linter would resolve their citations exactly as happily as it resolves a Postgres one.
# The transcript also carries the run's gate posture, so the linter knows whether "this
# label did not run" means "MySQL was down" or "your citation is a fiction".
TRANSCRIPT=${HG_ASSERT_TRANSCRIPT:-target/conformance/assertions.tsv}
mkdir -p "$(dirname "$TRANSCRIPT")"
: > "$TRANSCRIPT"
printf '#\tstarted_at_ms\t%s\n' "$(date +%s)000" >> "$TRANSCRIPT"
printf '#\tHG_MYSQL\t%s\n' "${HG_MYSQL:+set}" >> "$TRANSCRIPT"
# Set to `yes` by the MySQL store section's watchdog probe below. Read by
# check-evidence.sh: it is the difference between "this MySQL evidence did not run
# because no server was reachable" (expected, ledgered) and "this citation is a
# fiction" (hard failure).
printf '#\tmysql_live\tno\n' >> "$TRANSCRIPT"
# Labels are single-line by construction (they are shell string literals in this file), so
# a tab-separated line needs no escaping beyond flattening any stray whitespace.
rec(){ printf '%s\t%s\n' "$1" "$(printf '%s' "$2" | tr '\t\n' '  ')" >> "$TRANSCRIPT"; }
chk_(){ a=$(trim "$2"); b=$(trim "$3"); if [ "$a" = "$b" ]; then echo "  ✅ $1 ($a)"; pass=$((pass+1)); rec PASS "$1"; else echo "  ❌ $1: got $a want $b"; fail=$((fail+1)); rec FAIL "$1"; fi }
chk(){ if trivial "$3"; then
         echo "  ❌ $1: UNGUARDED-ZERO expectation \"$(trim "$3")\" — this assertion would pass on an absent fixture. Use: chk0 <label> <got> <want> <witness-label> <witness>"
         fail=$((fail+1)); rec FAIL "$1"; return; fi
       chk_ "$@"; }
# chk0: a zero/empty/identical expectation WITH the witness that makes it an assertion.
# args: label got want witness-label witness-value
chk0(){ if trivial "$5"; then
          echo "  ❌ $4: VACUITY GUARD — witness is trivial (\"$(trim "$5")\"), so \"$1\" would pass on nothing"
          fail=$((fail+1)); rec FAIL "$4"
        else echo "  ✅ $4 ($(trim "$5"))"; pass=$((pass+1)); rec PASS "$4"; fi
        guarded=$((guarded+1)); chk_ "$1" "$2" "$3"; }
# A section that did not run is not a section that passed. Announced loudly, counted
# separately, and never folded into `passed=`.
skipped(){ echo "  ⏭  SKIPPED: $1${2:+ — $2}"; skip=$((skip+1)); rec SKIP "$1"; }
# A multi-statement psql seed, with the rollback made visible. See mechanism 3 above.
seed(){ local out
  if out=$($PSQL -v ON_ERROR_STOP=1 -c "$2" 2>&1); then echo "  ✅ seed: $1"; pass=$((pass+1)); rec PASS "seed: $1"
  else echo "  ❌ seed: $1 — ROLLED BACK: $(echo "$out" | tr '\n' ' ' | cut -c1-160)"; fail=$((fail+1)); rec FAIL "seed: $1"; fi; }
# Round 32h: five more tables. `headgate_queue_state` is the one round 32g's rolled-back
# seed turned on; `headgate_schedule`, `headgate_operation`, `headgate_worker` and
# `headgate_effect` are the rest of the same residue class — none of them were ever
# reset, so `GET /periodic` and `GET /queues` were diffing over eight schedules and five
# queue rows left by earlier runs and by the Rust/Go unit tests. Round 32t adds the enqueue
# policy and its monotonic counters to the same reset. TRUNCATE does not fire DELETE
# triggers; retaining those counters made a repeated run report yesterday's unfinished
# depth and doubled time-to-drain. Run-to-run determinism is the point: a snapshot whose
# content depends on what ran before it cannot be a regression corpus.
reset_pg(){ $PSQL -c "TRUNCATE headgate_job_tag, headgate_job, headgate_queue_sample, headgate_rate_bucket, headgate_quarantine, headgate_partition_deficit, headgate_concurrency_limit, headgate_queue_counter, headgate_partition_counter, headgate_active_partition, headgate_inflight, headgate_queue_state, headgate_enqueue_policy, headgate_enqueue_counter, headgate_schedule, headgate_operation, headgate_worker, headgate_effect;" >/dev/null; }
# §5.9 the ONE kind-format rule's message, defined once: every backend, both languages,
# and both API servers must produce these exact bytes. (Single-quoted so the backticks
# stay literal.)
KINDMSG='invalid kind `bad kind`: 1-128 characters, first [A-Za-z0-9_], rest [A-Za-z0-9_] or one of -[]<>/.:+'
# §11.2 round 32, the per-schedule timezone contract's exact bytes. The mutation DIFF
# proves the two servers agree; these literals prove they agree on the RIGHT thing —
# two servers can match each other while both being wrong, which is the one failure a
# diff cannot see.
TZSPEC='CRON_TZ=America/New_York 0 9 * * *'
TZMSG='unknown timezone `Mars/Phobos`'
TZEVERY='`@every` is epoch-aligned UTC and takes no CRON_TZ:'
# §5.2 round 32f, the ONE quarantine_release not-found contract. Rust gets the `not
# found: ` prefix from StoreError::NotFound's Display; Go has no NotFoundError type and
# hardcodes the prefix into the literal, which is also how headgateapi.storeErr knows to
# answer 404 instead of falling through to 400. Both halves are asserted, because the
# STATUS is the half that drifted and the message is what decides it.
QRELMSG='not found: fingerprint apim-never-quarantined is not quarantined'
QRELSTATUS='status: 404'
# §10.1 round 32g. Round 32f audited every API-reachable error path and found the
# mutation diff covered TEN of ~70. These are the exact bytes the newly-covered ones
# must produce on BOTH servers — asserted as literals beside the diff for the same
# reason the tz and quarantine contracts are: two servers can match each other while
# both being wrong, and a diff cannot see that.
G_MISSFIELD='missing field `scheduled_at_ms`'
G_SIGNALMSG='command must be quiet, resume, restart, terminate, or resign'
G_MEDIAMSG='expected Content-Type: application/json'
G_QUERYMSG='invalid query parameter `limit`'
G_NOQUERYMSG='missing query parameter `queue`'
G_ACTIONMSG='unknown action `nope`'
G_CRONMSG='bad cron `notacron`: want 5 fields, or 6 with seconds'
G_RELEASEHDR='x-released-jobs: 0'
# The two shapes that must NEVER appear. `invalid request: ` is StoreError::Invalid's
# Display prefix, which /jobs/actions was the one route still shipping; `u003e` is
# encoding/json's HTML escape of `>`, invisible to a diff that pipes through jq.
G_NEVER_PREFIX='invalid request: '
G_NEVER_ESCAPE='u003e'
# How many requests api_mutate issues. NOT an expected total for the suite (the gate is
# still failed=0) — it is the witness that a mutation snapshot is a real transcript and
# not an empty file, which is what made the two "must never appear" counts vacuous.
REQCOUNT=100

# round 32g: the literal-bytes half of the new coverage, over BOTH snapshots on every
# backend. Ten assertions; the diff proves the two servers agree, these prove they agree
# on the right bytes.
g_asserts(){ # $1 = rust snapshot, $2 = go snapshot, $3 = label
  local R="$1" G="$2" L="$3"
  gpair(){ echo "$(grep -Fc -- "$1" "$R")|$(grep -Fc -- "$1" "$G")"; }
  chk "$L 32g: a missing required field is 422 NAMING it, never a silent mutation" \
      "$(gpair "$G_MISSFIELD")" "1|1"
  chk "$L 32g: an explicit empty signal command is 400, never a silent clear" \
      "$(gpair "$G_SIGNALMSG")" "2|2"
  chk "$L 32g: a bodied request with no Content-Type is 415, never an enqueue" \
      "$(gpair "$G_MEDIAMSG")" "1|1"
  chk "$L 32g: an unparseable query parameter is 400 NAMING it, never a silent default" \
      "$(gpair "$G_QUERYMSG")" "1|1"
  chk "$L 32g: GET /partitions without queue is 400, never an empty list" \
      "$(gpair "$G_NOQUERYMSG")" "1|1"
  chk "$L 32g: unknown action is backticked on every backend (headgatepgx used %q)" \
      "$(gpair "$G_ACTIONMSG")" "2|2"
  chk "$L 32g: a cron rejection backticks the spec (Go used %q double quotes)" \
      "$(gpair "$G_CRONMSG")" "1|1"
  chk "$L 32g: DELETE /quarantine emits x-released-jobs — the header nothing compared" \
      "$(gpair "$G_RELEASEHDR")" "1|1"
  # Round 32h: the two "must never appear" counts are the exact shape this sweep exists
  # to kill — `grep -c` on a MISSING or EMPTY snapshot is also 0|0, on both sides, so a
  # mutation sequence that never ran would satisfy both. The witness is the request count
  # itself: 87 statuses per snapshot, and none of them the `000` curl prints for a dead
  # server. Every other assertion in this function is guarded by demanding 1|1 or 2|2;
  # these two were not.
  chk0 "$L 32g: no route ships Display's \"invalid request: \" prefix (/jobs/actions did)" \
      "$(gpair "$G_NEVER_PREFIX")" "0|0" \
      "$L 32g: ...witness: both snapshots carry every request status, none of them 000" \
      "$(echo "$(grep -c '^status: ' "$R")|$(grep -c '^status: ' "$G")|$(grep -c '^status: 000' "$R")$(grep -c '^status: 000' "$G")" | grep -x "$REQCOUNT|$REQCOUNT|00")"
  # Both halves of the byte-level pair, read from the `raw-body:` / `bytes:` lines the
  # RAW requests emit — jq has decoded the escape everywhere else in these files, which
  # is precisely why the escaping survived twelve rounds unseen.
  chk0 "$L 32g: no body HTML-escapes \`>\` — the byte divergence jq was hiding" \
      "$(gpair "$G_NEVER_ESCAPE")" "0|0" \
      "$L 32g: ...witness: the RAW requests really emitted bodies to grep for the escape in" \
      "$(grep -c '^raw-body: ' "$R")"
  # Round 32h: g25/g26 are the ONLY coverage of "an explicit \"\" queue stays \"\" and an
  # explicit 0 max_attempts stays 0", and the only thing asserting them was a diff of two
  # `map(select(.id == "apig-opt"))` results — which is `[]` on both servers if the PUT
  # failed on both. Pinned as literals, on both snapshots.
  chk "$L 32h: an explicit empty periodic queue survives as \"\" (Go defaulted it)" \
      "$(gpair '"queue": ""')" "1|1"
  chk "$L 32h: ...and an explicit max_attempts 0 survives as 0 (Go made it 25)" \
      "$(gpair '"max_attempts": 0')" "1|1"
  chk "$L 32g: ...and no body carries a trailing newline serde_json would not write" \
      "$(grep -c '^bytes: ' "$R")|$(grep -A0 -c '^bytes: ' "$G")|$(diff <(grep '^bytes: ' "$R") <(grep '^bytes: ' "$G") >/dev/null && echo same || echo DIFFERENT)" \
      "5|5|same"
}

echo "== Postgres (through the Rust store) =="
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('stripe',5,5,5,1000,1000000);" >/dev/null
$H enqueue count=20 prefix=u queue=default rate=stripe fp=fp sched=1000 >/dev/null
r=$($H admit queues=default capacity=100 lease_ms=30000 worker=w1 lease=L1 quantum=1000 | wc -l | tr -d ' ')
chk "fleet rate limit caps at bucket size" "$r" "5"

# Round 32, maintainer decision FAIL OPEN. An UNCONFIGURED rate class — a name with no
# bucket row — is UNLIMITED on every gate now. It used to admit NOTHING on the two SQL
# gates (`COALESCE(b.avail, 0)`), which turned a typo'd rate_class into a silent permanent
# stall while the Lua gate ran the same envelope unthrottled. A limit nobody wrote is not
# a limit; the divergence is closed on the Redis semantic.
reset_pg
$H enqueue count=20 prefix=uc queue=default rate=nosuchclass fp=fp sched=1000 >/dev/null
r=$($H admit queues=default capacity=100 lease_ms=30000 worker=w1 lease=LFO quantum=1000 | wc -l | tr -d ' ')
chk "unconfigured rate class is UNLIMITED (fail open)" "$r" "20"
# A gate that admitted NOTHING also mints no bucket row, so the count-zero is only an
# assertion beside the admit count that proves the fail-open path actually ran.
chk0 "...and mints no bucket row for it" "$($PSQL -c "SELECT count(*) FROM headgate_rate_bucket;")" "0" \
     "...witness: the fail-open admit really admitted" "$r"

# Invariant 16's kill switch must SURVIVE fail-open: a CONFIGURED class with limit 0 and
# an empty bucket is a paused class, and paused still means nothing gets through. This is
# the assertion that proves fail-open reads "no row", not "no budget".
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('killed',0,5,0,1000,1000);" >/dev/null
$H enqueue count=10 prefix=ks queue=default rate=killed fp=fp sched=1000 >/dev/null
r=$($H admit queues=default capacity=100 lease_ms=30000 worker=w1 lease=LKS quantum=1000 | wc -l | tr -d ' ')
# "admits nothing" is also what an empty queue and a broken enqueue produce. The witness
# is the seeded backlog: ten rows are there, waiting, and the KILL SWITCH is why none ran.
chk0 "invariant-16 kill switch still admits nothing (limit 0 + empty bucket)" "$r" "0" \
     "...witness: ten jobs are actually waiting on the killed class" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='available' AND rate_class='killed';")"

# ----- TRAP 0, THE STORE CLOCK (round 32i) -----
# "Time comes from the store, never the caller." `now_ms` used to be a parameter, which
# made every fleet-wide limit a function of the CALLING worker's clock: a worker 60s fast
# computed 60 extra seconds of token refill and admitted a second full bucket in the same
# real second — 10 admitted against a limit of 5 — and skewed lease expiry into either
# early expiry (double-claim) or late expiry (stranded job).
#
# conformance/scenarios/admission.yaml carries two regression scenarios for this
# (`a_skewed_worker_cannot_inflate_the_limit`, `lease_expiry_ignores_worker_clocks`).
# ROUND 32j: THAT FILE IS EXECUTED NOW — `scripts/run-scenarios.py` runs it against a live
# Postgres and a live Redis through both languages' store ports, and verify.sh runs it. It
# was executed by NOTHING when the paragraph below was written. The two guards are
# deliberately kept in BOTH places: this section drives the Rust store directly and reads
# `clock_timestamp()` in the same query, while the scenario runner reads the store clock
# from a second connection through four (language, backend) cells — different blind spots.
# Round 32i mutation-
# tested the trap by restoring `$3::bigint AS now_ms` and having both drivers pass their
# own clock 60 seconds fast: every behavioural assertion in this section stayed green,
# including `fleet rate limit caps at bucket size`. The suite went red only where the
# verdict helper happens to bind $3 = 0, which is an accident of a debug harness rather
# than a guard on the invariant.
#
# Both halves are asserted here, against the STORE's own clock read in the same query.
reset_pg
$H enqueue count=1 prefix=t0 queue=default fp=fp sched=1000 >/dev/null
$H admit queues=default capacity=1 lease_ms=30000 worker=w1 lease=LT0 quantum=1000 >/dev/null
# 5s of slack absorbs a process spawn; the skew this exists to catch is 60s, twelve times
# outside it. `t`/`f` rather than a raw delta so a NULL lease (no claim) is not "small".
r=$($PSQL -c "SELECT (lease_id IS NOT NULL AND abs((lease_expires_at_ms - 30000) - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint) < 5000)::text FROM headgate_job WHERE ulid='t01';")
chk "trap 0: lease_expires_at_ms is stamped from STORE time, never the calling worker's clock" "$r" "true"
# The refill half. The bucket is stamped EMPTY at STORE now, so any gate measuring elapsed
# time against the store reads ~0ms of refill and admits nothing; a gate measuring against
# a worker 60s fast reads 60000 * 5 / 10000 = 30 tokens, caps at burst, and admits a whole
# second bucket. Window 10s is chosen so real spawn latency (~200ms) refills exactly zero.
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket SELECT 't0rc', 0, 5, 5, 10000, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint;" >/dev/null
$H enqueue count=10 prefix=t0r queue=default rate=t0rc fp=fp sched=1000 >/dev/null
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LT0R quantum=1000 | wc -l | tr -d ' ')
chk0 "trap 0: a bucket emptied at STORE now refills ~nothing — a 60s-fast worker would admit a whole second bucket" \
     "$r" "0" \
     "trap 0: ...witness: ten jobs are waiting on that class, so the empty bucket is why none ran" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='available' AND rate_class='t0rc';")"

reset_pg
for batch in 0 1 2 3 4; do
  $H enqueue count=1000 prefix=n$batch- queue=default partition=noisy fp=fp sched=1000 >/dev/null
done
$H enqueue count=3    prefix=a queue=default partition=A     fp=fp sched=1000 >/dev/null
$H enqueue count=3    prefix=b queue=default partition=B     fp=fp sched=1000 >/dev/null
r=$($H admit queues=default capacity=9 lease_ms=30000 worker=w1 lease=L1 quantum=3 | cut -d'|' -f4 | sort -u | wc -l | tr -d ' ')
chk "fairness spans partitions under a 5000-job flood" "$r" "3"

# §5.3 PREFETCH SEMANTICS (round 32). `capacity` is a CEILING; the per-partition share in
# ONE admit is deficit + quantum. So a single admit is balanced round-robin exactly when
# the quantum BINDS (quantum * active_partitions >= capacity), and not otherwise. Asserted
# as a sorted count multiset so it does not depend on which partition the gate reached
# first — the split shape is the contract, the partition identity is not.
split(){ cut -d'|' -f4 | sort | uniq -c | awk '{print $1}' | sort -rn | paste -sd, -; }
# §5.2 the claimed-id SET of one admit, sorted. Deliberately a set, not a sequence: the
# claim read-back is `ORDER BY id` on the SQL backends, so printed order is insertion
# order and says nothing about the DRAW order. What a capacity-limited admit *returns*
# does — which is why the head-of-line assertions below bound capacity instead.
idset(){ cut -d'|' -f1 | sort | paste -sd, -; }
reset_pg
for p in A B C; do $H enqueue count=4 prefix=pf$p queue=default partition=$p fp=fp sched=1000 >/dev/null; done
r=$($H admit queues=default capacity=6 lease_ms=30000 worker=w1 lease=LP1 quantum=2 | split)
chk "prefetch: capacity 6, quantum 2, 3 partitions -> 2 per partition" "$r" "2,2,2"
reset_pg
for p in A B C; do $H enqueue count=4 prefix=pf$p queue=default partition=$p fp=fp sched=1000 >/dev/null; done
r=$($H admit queues=default capacity=6 lease_ms=30000 worker=w1 lease=LP2 quantum=1000 | split)
chk "prefetch: a non-binding quantum lets one partition fill the batch" "$r" "4,2"

# ----- INVARIANT 11, THE MAKE-UP ROUND (round 32i) -----
# "Fairness is work-conserving. If capacity remains after every other partition is served,
# the noisy partition gets it. Idling a worker to punish a tenant is a throughput bug
# wearing a policy costume."
#
# §5.3's work-conservation is ROUND-SCOPED: a partition that had candidates and was not
# served accrues `quantum - claimed`, and SPENDS it on a later admit as `deficit + quantum`.
# The suite asserted the ACCRUAL (`fast path: ...the deficit is still charged`, which reads
# the table) and never once asserted the SPEND. Mutation-tested in round 32i by replacing
# `deficit + quantum` with `quantum` in BOTH arms — the fast arm's draw bound and the
# policy arm's `rank_part` clause — so credit is charged forever and never redeemable:
# 359 of 359 assertions stayed green while a starved partition could never catch up and a
# 10-slot worker took one job per poll.
#
# The credit is seeded directly rather than earned over three rounds, so the assertion is
# one admit and cannot drift with the draw order; the EARNING half is already covered by
# the fast-path deficit assertion. The pair is the point: identical fixture, identical
# capacity, the ONLY difference is the credit — so a gate that ignores it fails the second
# and a gate that ignores capacity fails the first.
reset_pg
$H enqueue count=6 prefix=wc queue=default partition=A fp=fp sched=1000 >/dev/null
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LW0 quantum=1 | wc -l | tr -d ' ')
chk "invariant 11: with NO accrued credit one quantum is all a partition draws (the contrast)" "$r" "1"
reset_pg
$H enqueue count=6 prefix=wc queue=default partition=A fp=fp sched=1000 >/dev/null
$PSQL -c "INSERT INTO headgate_partition_deficit (queue, partition_key, deficit, updated_at_ms) VALUES ('default','A',3,1000);" >/dev/null
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LW1 quantum=1 | wc -l | tr -d ' ')
chk "invariant 11: 3 rounds of accrued credit are SPENT on the next admit, never idled (fast arm)" "$r" "4"
# ...and again through the POLICY arm, which evaluates the fair share as a `rank_part`
# clause rather than as a draw bound: an unrelated rate bucket is enough to switch arms,
# and the answer must be the same number.
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('wc-unrelated',1000,1000,1000,1000,1000);" >/dev/null
$H enqueue count=6 prefix=wc queue=default partition=A fp=fp sched=1000 >/dev/null
$PSQL -c "INSERT INTO headgate_partition_deficit (queue, partition_key, deficit, updated_at_ms) VALUES ('default','A',3,1000);" >/dev/null
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LW2 quantum=1 | wc -l | tr -d ' ')
chk "invariant 11: ...and the POLICY arm redeems the same credit, same number (one gate, two arms)" "$r" "4"

# ----- PRIORITY: A REAL ORDERING KEY ON SQL, IGNORED BY THE REDIS GATE (round 32j) -----
# Round 32i's finding, verbatim: "`priority` is a real ordering key on the SQL gates
# (`ORDER BY priority DESC, scheduled_at_ms, id`) and is IGNORED ENTIRELY by the Redis
# gate, whose `pending` zset is scored by `scheduled_at_ms` alone; no assertion in the
# corpus sets a non-default priority on any backend, and no harness accepts a `priority=`
# argument." Every envelope the suite had ever enqueued carried priority 0, so a gate that
# sorts by it and a gate that has never heard of it produced identical answers on every
# fixture in the file. The register's `Priority` row was ✅ on that.
#
# IMPLEMENTING per-job priority in the Redis draw is an ask-first maintainer decision and
# is NOT done here. What is done is PINNING the actual behavior of each gate so the
# divergence cannot change unnoticed in either direction — a Redis gate that starts
# honoring priority fails here just as loudly as a SQL gate that stops.
#
# The fixture is built so the two orders are DIFFERENT ANSWERS rather than the same answer
# reached twice: priority runs OPPOSITE to scheduled_at_ms, and ids are assigned in
# scheduled_at order. A priority-ordering gate draws pb1, pc1, pa1; a gate ordering by
# scheduled_at alone draws pa1, pb1, pc1; a gate ordering by id draws pa1, pb1, pc1 too.
#
# capacity=1, three times, because the claim read-back is `ORDER BY id` on both SQL
# backends — asserting on the printed order of a multi-row claim would measure INSERTION
# order and prove nothing (it fooled the first draft of the §5.2 suspect-order assertions
# for exactly this reason). One row per admit is the only way to read the DRAW order.
reset_pg
$H enqueue count=1 prefix=pa queue=default partition=P fp=fp sched=1000 priority=0 >/dev/null
$H enqueue count=1 prefix=pb queue=default partition=P fp=fp sched=1001 priority=9 >/dev/null
$H enqueue count=1 prefix=pc queue=default partition=P fp=fp sched=1002 priority=5 >/dev/null
ord=""
for i in 1 2 3; do
  ord="$ord$($H admit queues=default capacity=1 lease_ms=30000 worker=wp lease=LP$i quantum=1000 | cut -d'|' -f1) "
done
chk "priority: the SQL gate draws priority DESC first, ahead of scheduled_at_ms" \
    "$(trim "$ord")" "pb1 pc1 pa1"
# ...and the column really holds what the harness said it does. Without this the assertion
# above would also pass on a harness that silently dropped `priority=` and a gate that
# happened to order by id descending.
chk "priority: ...and the stored column carries the non-default values (9/5/0)" \
    "$($PSQL -c "SELECT string_agg(priority::text, ',' ORDER BY ulid) FROM headgate_job WHERE queue='default';")" \
    "0,9,5"
# The FAST arm evaluates the same fixture with the policy machinery skipped (§13, round
# 32e). Ordering is NOT policy, so it must survive the fast path — a fast arm that dropped
# the sort key would be a different admitted set, which is the one thing that arm may
# never be. The block above already runs policy-free; this one forces the POLICY arm with
# an unrelated bucket and demands the identical order.
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('pri-unrelated',1000,1000,1000,1000,1000);" >/dev/null
$H enqueue count=1 prefix=pa queue=default partition=P fp=fp sched=1000 priority=0 >/dev/null
$H enqueue count=1 prefix=pb queue=default partition=P fp=fp sched=1001 priority=9 >/dev/null
$H enqueue count=1 prefix=pc queue=default partition=P fp=fp sched=1002 priority=5 >/dev/null
ord=""
for i in 1 2 3; do
  ord="$ord$($H admit queues=default capacity=1 lease_ms=30000 worker=wp lease=LQ$i quantum=1000 | cut -d'|' -f1) "
done
chk "priority: ...and the POLICY arm draws the identical priority order (one gate, two arms)" \
    "$(trim "$ord")" "pb1 pc1 pa1"

reset_pg
$H enqueue count=500 prefix=j queue=default fp=fp sched=1000 >/dev/null
rm -f /tmp/hgout*; for i in $(seq 1 8); do
  $H admit queues=default capacity=80 lease_ms=30000 worker=w$i lease=L$i quantum=1000 | cut -d'|' -f1 > /tmp/hgout$i &
done; wait
dup=$(cat /tmp/hgout* | sort | uniq -d | wc -l | tr -d ' ')
claimed=$(cat /tmp/hgout* | wc -l | tr -d ' ')
# THE headline regression assertion of this whole file, and until round 32h it was the
# purest vacuous shape in it: eight workers that admitted NOTHING have zero duplicates,
# and so does a gate that crashed, and so does an enqueue that silently failed. Measured
# here: a single simultaneous burst claims 80-160 of the 500 (the losers' candidate
# windows are consumed and they return empty, exactly as the real admission loop does),
# so the count is NOT pinned. What IS pinned is the IDENTITY: every id the workers
# printed is `running` in the store and vice versa. A harness printing ids it never
# claimed, or a claim that did not stick, fails here — and `want` being the claim count
# means a burst that claimed nothing fails as an UNGUARDED-ZERO instead of passing.
chk "8 concurrent workers: the store's running set is exactly what the workers printed" \
    "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='running';")" "$claimed"
chk "...and every printed id is one of them (identity, not just cardinality)" \
    "$(cat /tmp/hgout* | sort | comm -23 - <($PSQL -c "SELECT ulid FROM headgate_job WHERE state='running' ORDER BY ulid;") | wc -l | tr -d ' ')|$claimed" \
    "0|$claimed"
chk0 "8 concurrent workers, zero double-claims" "$dup" "0" \
     "...witness: rows were actually claimed (a dead gate double-claims nothing)" "$claimed"
orphan=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE lease_id IS NOT NULL AND state<>'running';" 2>/dev/null)
chk0 "no job holds a lease outside running" "$orphan" "0" \
     "...witness: there are leases to be wrong about" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE lease_id IS NOT NULL;")"

# ----- §13 THE MAINTAINED INFLIGHT COUNTER (round 32b) -----
# The gate's concurrency clause used to aggregate EVERY running row in the fleet on every
# admission (0.09 ms at 200 running, 4.3 ms at 20k, 11.5 ms at 50k). It now reads
# headgate_inflight, +1'd in the same statement as the claim and -1'd in the same
# statement/transaction as every running -> * edge. That trade is only safe if the counter
# is EXACT, so the counter is asserted against the truth after every edge in
# conformance/state_machine.yaml, and the reconciliation is asserted by BREAKING it.
#
# Granularity is (queue, partition_key), matching §5.1's "is this job's PARTITION under
# its ceiling?" — headgate_concurrency_limit is keyed per queue, the ceiling applies per
# partition of that queue. A per-queue counter would be a different policy, so the split
# is asserted too.
# args: partition [queue]
inf(){ $PSQL -c "SELECT COALESCE((SELECT n FROM headgate_inflight WHERE queue='${2:-default}' AND partition_key='$1'), -1);"; }
# the truth the counter is supposed to equal, computed the slow way on purpose
truth(){ $PSQL -c "SELECT count(*) FROM headgate_job WHERE state='running' AND queue='${2:-default}' AND partition_key='$1';"; }
# args: label partition want [queue]
agree(){ chk "$1" "$(inf "$2" "${4:-default}")|$(truth "$2" "${4:-default}")" "$3|$3"; }

reset_pg
$H enqueue count=6 prefix=ia queue=default partition=pa fp=fp sched=1000 retention=86400000 >/dev/null
$H enqueue count=4 prefix=ib queue=default partition=pb fp=fp sched=1000 retention=86400000 >/dev/null
$H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LI quantum=1000 >/dev/null
agree "claim increments per (queue, partition): pa" pa 6
agree "claim increments per (queue, partition): pb" pb 4
# The split is the point: a per-QUEUE counter would read 10 for both.
r=$($PSQL -c "SELECT count(*) FROM headgate_inflight WHERE queue='default';")
chk "the counter is partitioned, not one row per queue" "$r" "2"

# Every running -> * edge in conformance/state_machine.yaml, one at a time, each asserted
# against the recomputed truth so a decrement that fires TWICE fails as loudly as one that
# never fires.
# ----- THE STATE MACHINE IS THE AUTHORITY (round 32i) -----
# conformance/state_machine.yaml is the table, and the register's own precedence rule says
# the table wins. `yaml_and_code_agree_row_for_row` cross-checks it against the TRANSITION
# FUNCTION in both languages — but every backend implements the edges AGAIN, in SQL and in
# Lua, and nothing cross-checked THOSE. The 13 `running -> *` edges below were exercised
# for their INFLIGHT DECREMENT only, and the decrement fires for any edge that leaves
# `running`: a store that archived on `skip` where the table says archived, and CANCELLED
# on `revoke` where it says deleted, moved the counter identically and passed.
# Mutation-tested in round 32i (skip -> cancelled, revoke -> archived, snooze -> retryable):
# every one of them was UNCAUGHT until the destination itself was asserted here.
# `st` prints `deleted` for an absent row, which is what the two DELETE edges land on.
st(){ $PSQL -c "SELECT COALESCE((SELECT state::text FROM headgate_job WHERE ulid='$1'), 'deleted');"; }
$H ack job=ia1 lease=LI fence=1 outcome=success     >/dev/null; agree "running -> completed decrements"   pa 5
chk "state machine: running --success--> completed (retention_ms > 0)" "$(st ia1)" "completed"
$H ack job=ia2 lease=LI fence=1 outcome=retry err=x >/dev/null; agree "running -> retryable decrements"   pa 4
chk "state machine: running --retry--> retryable (attempt + 1 < max_attempts)" "$(st ia2)" "retryable"
$H ack job=ia3 lease=LI fence=1 outcome=skip        >/dev/null; agree "running -> archived (skip) decrements" pa 3
chk "state machine: running --skip--> archived (explicit: do not retry)" "$(st ia3)" "archived"
$H ack job=ia4 lease=LI fence=1 outcome=undecodable >/dev/null; agree "running -> undecodable decrements" pa 2
chk "state machine: running --undecodable--> undecodable (§5.4 no upcast path; never retry)" "$(st ia4)" "undecodable"
$H ack job=ia5 lease=LI fence=1 outcome=revoke      >/dev/null; agree "running -> deleted (revoke) decrements" pa 1
chk "state machine: running --revoke--> deleted (explicit: drop entirely, not archived)" "$(st ia5)" "deleted"
$H ack job=ia6 lease=LI fence=1 outcome=snooze delay=60000 >/dev/null; agree "running -> scheduled (snooze) decrements" pa 0
chk "state machine: running --snooze--> scheduled (not retryable — no attempt consumed)" "$(st ia6)" "scheduled"
$H ack job=ib1 lease=LI fence=1 outcome=rate_limited >/dev/null; agree "running -> available (rate_limited) decrements" pb 3
chk "state machine: running --rate_limited--> available (not retryable — a scheduling outcome)" "$(st ib1)" "available"
# ----- INVARIANT 10 (round 32i) -----
# "Rate-limited is not a failure. `Outcome::RateLimited` re-queues WITHOUT consuming an
# attempt." The line above is the only place in this file that ever acked `rate_limited`,
# and all it looked at was the inflight counter — so a gate that charged an attempt for
# being over-limit (asynq's shape: users fake it with IsFailure + RetryDelayFunc) moved a
# number NOTHING compared. Mutation-tested in round 32i by adding `attempt = j.attempt + 1`
# to the RateLimited ack arm: 356 of 356 assertions stayed green while every throttled job
# burned one of its 25 retries.
# All four fields together, because each one is a different way to get this wrong: the
# STATE (available, not retryable — it is not a backoff), `attempt` (the retry budget),
# `crash_attempt` (quarantine's budget), and the error HISTORY, which over-limit must not
# pollute or the console reports a healthy tenant as failing.
r=$($PSQL -c "SELECT state || '|' || attempt || '|' || crash_attempt || '|' || jsonb_array_length(errors) FROM headgate_job WHERE ulid='ib1';")
chk "invariant 10: rate_limited re-queues consuming NO attempt, NO crash, and writing NO failure" "$r" "available|0|0|0"
# running -> deleted via the OTHER success arm: retention 0 deletes the row outright, so
# the decrement has to happen while it still exists.
# Own queue on purpose: a capacity-1 admit on `default` would re-claim the job the
# rate_limited edge just made available, and every count after it would be off by one.
$H enqueue count=1 prefix=ie queue=eph partition=pe fp=fp sched=1000 retention=0 >/dev/null
$H admit queues=eph capacity=1 lease_ms=30000 worker=w1 lease=LE quantum=1000 >/dev/null
agree "ephemeral claim increments" pe 1 eph
$H ack job=ie1 lease=LE fence=1 outcome=success >/dev/null
agree "running -> deleted (retention 0) decrements" pe 0 eph
# The table's ONE conditional DELETE edge, and the half a `state` column cannot show: the
# row is gone, so the assertion has to be able to say "gone" and mean it. `st` prints
# `deleted` for an absent row, and the sibling assertion above proves the row EXISTED and
# was running a moment earlier, which is what stops this reading as "never enqueued".
chk "state machine: running --success--> deleted when retention_ms == 0 (§9.5 ephemeral)" "$(st ie1)" "deleted"
# running -> archived because attempts ran out (the other arm of `retry`).
$H enqueue count=1 prefix=im queue=maxa partition=pm fp=fp sched=1000 retention=86400000 max_attempts=1 >/dev/null
$H admit queues=maxa capacity=1 lease_ms=30000 worker=w1 lease=LM quantum=1000 >/dev/null
$H ack job=im1 lease=LM fence=1 outcome=retry err=x >/dev/null
r=$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='im1';")
chk "retry past max_attempts archives (the other retry arm)" "$r" "archived"
agree "running -> archived (attempts exhausted) decrements" pm 0 maxa
# running -> retryable and running -> quarantined via the RECLAIMER — the one exit a
# crashed worker cannot take for itself, and therefore the one a counter maintained only
# by the ack arms would leak on every process death.
$H ack job=ib2 lease=LI fence=1 outcome=success >/dev/null   # tidy: pb 3 -> 2
$PSQL -c "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='ib3';" >/dev/null
$H reclaim >/dev/null; agree "running -> retryable (lease lost) decrements" pb 1
$PSQL -c "UPDATE headgate_job SET lease_expires_at_ms=0, crash_attempt=9 WHERE ulid='ib4';" >/dev/null
$H reclaim >/dev/null
r=$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='ib4';")
chk "crash limit quarantines on reclaim" "$r" "quarantined"
agree "running -> quarantined (crash limit) decrements" pb 0
# running -> cancelled, single and bulk. Both are UNFENCED operator yanks of a live lease,
# and both must NOT decrement for the scheduled/available rows they also match — the
# decrement keys off the PRE-update state, which an UPDATE ... RETURNING cannot see.
reset_pg
$H enqueue count=3 prefix=ic queue=default partition=pc fp=fp sched=1000 retention=86400000 >/dev/null
$H enqueue count=2 prefix=iw queue=default partition=pc fp=fp sched=1000 retention=86400000 >/dev/null
$H admit queues=default capacity=3 lease_ms=30000 worker=w1 lease=LC quantum=1000 >/dev/null
agree "3 of 5 in this partition are running" pc 3
$H cancel job=ic1 >/dev/null; agree "running -> cancelled (operator) decrements" pc 2
chk "state machine: running --operator_cancel--> cancelled (an unfenced yank of a live lease)" "$(st ic1)" "cancelled"
$H cancel job=iw1 >/dev/null; agree "cancelling an AVAILABLE job does not decrement" pc 2
chk "state machine: available --operator_cancel--> cancelled (the same edge off the other state)" "$(st iw1)" "cancelled"
# $$-scoped: headgate_operation ids are a primary key and the table is not reset here,
# so a fixed id would make the second run of this suite a silent no-op.
$H bulk id="bk$$" action=cancel queue=default >/dev/null
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='cancelled';")
chk "bulk cancel reaches running and waiting rows alike" "$r" "5"
agree "bulk cancel decrements ONLY the rows that were running" pc 0

# §5.1 THE CEILING ITSELF, on the maintained counter. Without this the whole section
# proves only that a number moves; this proves admission still reads it.
reset_pg
$PSQL -c "INSERT INTO headgate_concurrency_limit VALUES ('cl-default','default',2);" >/dev/null
$H enqueue count=6 prefix=cc queue=default partition=pz fp=fp sched=1000 retention=86400000 >/dev/null
first=$($H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LZ1 quantum=1000 | wc -l | tr -d ' ')
chk "concurrency ceiling caps the first admit at max_concurrent" "$first" "2"
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w2 lease=LZ2 quantum=1000 | wc -l | tr -d ' ')
# The witness is the FIRST admit: four rows are still waiting and the ceiling — not an
# empty queue, not a broken enqueue — is why the second admit returns nothing.
chk0 "...and the ceiling holds on the NEXT admit, from the counter" "$r" "0" \
     "...witness: the first admit really claimed, so there is a ceiling to hold" "$first"
$H ack job=cc1 lease=LZ1 fence=1 outcome=success >/dev/null
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w3 lease=LZ3 quantum=1000 | wc -l | tr -d ' ')
chk "acking one frees exactly one slot" "$r" "1"

# THE SAFETY NET, asserted by BREAKING the counter. Every edge above decrements in the
# same statement as its transition, so drift should be impossible — but "impossible" is
# what a future edge added without a decrement will also look like, and drift HIGH chokes
# a partition against its ceiling with no self-healing path. So: corrupt it by hand, run
# the duty that already sweeps (promote_due), and assert it heals.
$PSQL -c "UPDATE headgate_inflight SET n = 999 WHERE queue='default' AND partition_key='pz';" >/dev/null
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w4 lease=LZ4 quantum=1000 | wc -l | tr -d ' ')
chk0 "a counter drifted HIGH does choke the ceiling (the failure being insured against)" "$r" "0" \
     "...witness: the corrupted counter row is really there to be read" \
     "$($PSQL -c "SELECT n FROM headgate_inflight WHERE queue='default' AND partition_key='pz';")"
$H promote >/dev/null
agree "promote_due's reconciliation heals a counter drifted HIGH" pz 2
$PSQL -c "UPDATE headgate_inflight SET n = 0 WHERE queue='default' AND partition_key='pz';" >/dev/null
$H promote >/dev/null
agree "...and one drifted LOW" pz 2
r=$($H admit queues=default capacity=10 lease_ms=30000 worker=w5 lease=LZ5 quantum=1000 | wc -l | tr -d ' ')
chk0 "the ceiling is enforced again after healing" "$r" "0" \
     "...witness: rows are still waiting under the ceiling (not an empty queue)" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='available' AND partition_key='pz';")"

# ----- §13 ADAPTIVE WIDENING: THE ESCALATION PATH (round 32d) -----
# The gate no longer draws a flat `LIMIT quantum * 4` per partition. It draws about
# capacity / active_partitions rows, plus one, and re-issues at quantum * 4 ONLY when the
# statement proves the narrow window could have changed the admitted set. The bench proves
# it is faster; these assertions prove it did not become a different gate — which is the
# only thing that matters, since a narrow window that silently drops an admissible
# candidate looks exactly like a fast one.
#
# Two kinds of assertion, deliberately. The BEHAVIORAL ones pin the admitted SET through
# the store, so they fail if the escalation ever stops firing. The VERDICT ones drive the
# artifact directly and read the `hg_widen` signal, so they fail if the escalation fires
# on the WRONG condition — a gate that widens every time would pass every behavioral
# assertion in this file while giving back the whole win.
HGSQL=$(cat crates/headgate-postgres/queries/admit.sql)
HGDIRECT=$(cat crates/headgate-postgres/queries/admit_direct.sql)
# Runs ONE narrow pass ($9 = 0) inside a transaction it rolls back, so the fixture is
# untouched and the assertion can be read twice. Prints 't' if the gate would escalate,
# 'f' if it would claim what it drew, and nothing at all if it drew nothing admissible.
# args: queue capacity quantum
verdict(){ $PSQL -c "BEGIN;
PREPARE hgv (text[], int, bigint, bigint, text, text, bigint, int, int) AS
${HGSQL%;}
;
EXECUTE hgv('{$1}', $2, 0, 30000, 'wv', 'LV', $3, 8, 0);
ROLLBACK;" 2>/dev/null | awk -F'|' 'NF>1{print $NF}' | sort -u | paste -sd, -; }
# Runs the compact §13 statement directly and rolls it back. `f` means the shape was
# applicable and produced real claim rows; `t` is its no-write fallback sentinel.
directverdict(){ $PSQL -c "BEGIN;
PREPARE hgd (text[], int, bigint, bigint, text, text, bigint, int, int) AS
${HGDIRECT%;}
;
EXECUTE hgd('{$1}', $2, 0, 30000, 'wd', 'LD', $3, 8, 0);
ROLLBACK;" 2>/dev/null | awk -F'|' 'NF>1{print $NF}' | sort -u | paste -sd, -; }
# The admitted set in the gate's own draw order — an exact set, not a count, because
# "five rows" is also what a gate that admitted the WRONG five returns.
ranset(){ $PSQL -c "SELECT COALESCE(string_agg(ulid, ',' ORDER BY id), '-') FROM headgate_job WHERE state='running';"; }

# A QUARANTINED HEAD DEEPER THAN THE NARROW WINDOW. capacity 5 over one partition draws
# 6 rows; the first 10 are quarantined, so the narrow pass sees a candidate set that is
# entirely blocked and MUST NOT conclude the queue has nothing to offer. This is the
# assertion the whole mechanism turns on: without escalation the gate admits zero here.
reset_pg
$H enqueue count=10 prefix=qa queue=default fp=qhd sched=1000 retention=86400000 >/dev/null
$H enqueue count=10 prefix=qb queue=default fp=qtl sched=1000 retention=86400000 >/dev/null
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint, kind, crash_count, quarantined_at_ms, reason) VALUES ('qhd','w',3,1000,'poison');" >/dev/null
chk "escalation: a fully-blocked narrow window widens rather than reporting empty" "$(verdict default 5 1000)" "t"
$H admit queues=default capacity=5 lease_ms=30000 worker=w1 lease=LQW quantum=1000 >/dev/null
chk "...and the gate still admits the admissible TAIL, exactly" "$(ranset)" "qb1,qb2,qb3,qb4,qb5"
# Invariant 2: a policy-rejected job is never locked. The quarantined head has to still be
# there, available, for another worker and for the inspector.
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='available' AND fingerprint='qhd';")
chk "...and every quarantined head is still available, never locked" "$r" "10"

# ----- INVARIANT 2, THE LOCK ITSELF (round 32i) -----
# The assertion above says "never locked" and cannot see a lock. A `FOR UPDATE` row lock
# lives until END OF TRANSACTION and leaves NO trace afterwards: the row still reads
# `available`, its state never moved, and every count in this file is taken after the
# statement committed. Mutation-tested in round 32i by changing `locked` to draw from
# `candidates` instead of `eligible` (so every quarantined and rate-blocked candidate is
# locked, and only the eligible ones are then claimed): the ENTIRE suite stayed green,
# 335/335. Invariant 2 was, until this block, an unenforced comment.
#
# The lock is only observable from ANOTHER session while the locking transaction is still
# open, so that is what this does: session A runs ONE wide admit pass inside a transaction
# it holds open, and session B asks the gate's own question — `FOR UPDATE SKIP LOCKED` —
# about the two fingerprints separately. `lkhd` is quarantined and must be fully lockable
# by B; `lktl` is admissible and its claimed head must NOT be, which is both the contrast
# and the proof that the probe can see a lock at all.
#
# The handshake is A's OWN EFFECT, not a sleep: B polls until it observes the claimed rows
# locked, so a slow machine waits and a gate that locks nothing times out into a failing
# assertion rather than into a passing one.
reset_pg
$H enqueue count=6 prefix=lk queue=default fp=lkhd sched=1000 retention=86400000 >/dev/null
$H enqueue count=6 prefix=lt queue=default fp=lktl sched=1000 retention=86400000 >/dev/null
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint, kind, crash_count, quarantined_at_ms, reason) VALUES ('lkhd','w',3,1000,'poison');" >/dev/null
# $9 = 1: the WIDE pass, so the draw is quantum * 4 and the candidate set is all 12 rows
# regardless of the adaptive window. capacity 3 => the gate claims lt1,lt2,lt3.
# `count(*) ... FOR UPDATE` is an error in Postgres (no aggregates with row locking), so
# the lock is taken in a subquery and counted outside it.
lockable(){ $PSQL -c "SELECT count(*) FROM (SELECT 1 FROM headgate_job WHERE fingerprint='$1' FOR UPDATE SKIP LOCKED) t;" 2>/dev/null; }
PGAPPNAME=hg-lockprobe $PSQL -c "BEGIN;
PREPARE hgl (text[], int, bigint, bigint, text, text, bigint, int, int) AS
${HGSQL%;}
;
EXECUTE hgl('{default}', 3, 0, 30000, 'wl', 'LL', 1000, 8, 1);
SELECT pg_advisory_lock(9321, 1);
SELECT pg_sleep(30);
ROLLBACK;" >/dev/null 2>&1 &
lkpid=$!
ready=0
for _ in $(seq 1 200); do
  ready=$($PSQL -c "SELECT count(*) FROM pg_locks
                     WHERE locktype='advisory' AND classid=9321 AND objid=1 AND granted;")
  [ "$ready" = "1" ] && break
  sleep 0.1
done
# The advisory lock is acquired AFTER EXECUTE returns, so this probe cannot race ahead
# and make the gate's own SKIP LOCKED skip the rows it was meant to claim.
held=$(lockable lktl)
blocked=$(lockable lkhd)
# Terminated by application_name, not by killing the client: a disconnected psql leaves
# the backend inside pg_sleep holding every one of these locks, and the next reset_pg
# would then block on the TRUNCATE instead of failing an assertion.
$PSQL -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='hg-lockprobe';" >/dev/null 2>&1
wait "$lkpid" 2>/dev/null
chk "invariant 2: the CONTRAST — the rows the gate claimed really are locked mid-statement" "$held" "3"
chk "invariant 2: a policy-rejected candidate is NEVER locked — all 6 quarantined rows stay claimable by another worker" \
    "$blocked" "6"
reset_pg

# THE SAME SHAPE ONE ROW DEEP, which is where a naive `truncate the draw at capacity`
# breaks: rank-4 is admissible precisely because ranks 1-3 are quarantined.
reset_pg
$H enqueue count=3 prefix=ka queue=default fp=khd sched=1000 retention=86400000 >/dev/null
$H enqueue count=7 prefix=kb queue=default fp=ktl sched=1000 retention=86400000 >/dev/null
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint, kind, crash_count, quarantined_at_ms, reason) VALUES ('khd','w',3,1000,'poison');" >/dev/null
$H admit queues=default capacity=3 lease_ms=30000 worker=w1 lease=LQX quantum=1000 >/dev/null
chk "escalation: rank-4 is admissible when ranks 1-3 are quarantined" "$(ranset)" "kb1,kb2,kb3"

# A RATE CLASS SHARED ACROSS PARTITIONS — the case where a narrow draw would admit the
# WRONG TENANTS, not merely fewer of them. rank_class is computed over the FULL candidate
# set (that is deliberate: a blocked candidate still consumes a class slot), so shrinking
# the per-partition draw lets partition B's rows take class slots that partition A's
# earlier rows own. Narrow alone would return ra1..ra4 + rb1,rb2; the verdict catches it
# and the wide pass returns A's first six, which is what the gate has always returned.
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('shared',8,8,8,1000,1000);" >/dev/null
$H enqueue count=10 prefix=ra queue=default partition=A rate=shared fp=fp sched=1000 retention=86400000 >/dev/null
$H enqueue count=10 prefix=rb queue=default partition=B rate=shared fp=fp sched=1000 retention=86400000 >/dev/null
chk "escalation: a shared rate class across partitions widens" "$(verdict default 6 1000)" "t"
$H admit queues=default capacity=6 lease_ms=30000 worker=w1 lease=LRW quantum=1000 >/dev/null
chk "...and the class budget still goes to the EARLIEST six, not the narrow window's" "$(ranset)" "ra1,ra2,ra3,ra4,ra5,ra6"

# THE ONE THAT CAUGHT A REAL BUG, and the reason the truncation test is against
# `quantum * 4` rather than against the fair share `deficit + quantum`. A row beyond the
# fair share is never ADMITTED, so relaxing the test to it looks free — and is not, because
# such a row is still a CANDIDATE and `rank_class` is computed over the full candidate set
# on purpose: a fairness-blocked candidate consumes a rate-class slot exactly as a
# quarantined one does. 3 partitions x 4 jobs, one class holding 4 tokens, quantum 1: the
# gate admits ONLY gA1, because A's ranks 2-4 eat the rest of the class budget even though
# fairness would never have run them. The relaxed narrow gate admitted {gA1, gB1} — it
# handed B a slot A owned. Measured, not reasoned: this assertion is the measurement.
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('rcx',4,4,4,1000,1000);" >/dev/null
for p in A B C; do $H enqueue count=4 prefix=g$p queue=default partition=$p rate=rcx fp=fp sched=1000 retention=86400000 >/dev/null; done
chk "escalation: a fairness-blocked candidate still owns its rate-class slot -> widen" "$(verdict default 6 1)" "t"
$H admit queues=default capacity=6 lease_ms=30000 worker=w1 lease=LFB quantum=1 >/dev/null
chk "...and the class budget is NOT handed to the next partition" "$(ranset)" "gA1"

# THE OTHER HALF OF THE DETECTION, and the half that pays for the mechanism: a partition
# that simply RAN OUT of jobs must NOT trigger a re-draw. Widening on ordinary emptiness
# would be correct and would also hand back the entire win, so it is asserted, not assumed.
reset_pg
for p in A B C; do $H enqueue count=2 prefix=ee$p queue=default partition=$p fp=fp sched=1000 >/dev/null; done
chk "no escalation when a partition is merely exhausted (drew fewer than the window)" "$(verdict default 6 1000)" "f"
# ...and one where the narrow window BINDS but the answer is provably already complete:
# capacity 100 over one partition draws 101 of 400, the final LIMIT binds at 100, and every
# undrawn row sorts after the 100th admitted one. Nothing to re-draw.
reset_pg
$H enqueue count=400 prefix=nw queue=default fp=fp sched=1000 >/dev/null
chk "no escalation when the drawn window already proves the top-capacity set" "$(verdict default 100 1000)" "f"
r=$($H admit queues=default capacity=100 lease_ms=30000 worker=w1 lease=LNW quantum=1000 | wc -l | tr -d ' ')
chk "...and that single narrow pass admits the full capacity" "$r" "100"
# The wide pass can never widen — that is what makes escalation terminate structurally
# rather than on a retry budget. Asserted on the fixture that DOES widen when narrow.
reset_pg
$H enqueue count=10 prefix=za queue=default fp=zhd sched=1000 >/dev/null
$H enqueue count=10 prefix=zb queue=default fp=ztl sched=1000 >/dev/null
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint, kind, crash_count, quarantined_at_ms, reason) VALUES ('zhd','w',3,1000,'poison');" >/dev/null
chk "the narrow pass on this fixture widens" "$(verdict default 5 1000)" "t"
w=$($PSQL -c "BEGIN;
PREPARE hgw (text[], int, bigint, bigint, text, text, bigint, int, int) AS
${HGSQL%;}
;
EXECUTE hgw('{default}', 5, 0, 30000, 'wv', 'LW', 1000, 8, 1);
ROLLBACK;" 2>/dev/null | awk -F'|' 'NF>1{print $NF}' | sort -u | paste -sd, -)
chk "...and the WIDE pass on the same fixture never widens (termination is structural)" "$w" "f"

# ----- §13 THE POLICY-FREE FAST PATH (round 32e) -----
# §13: "if the gate costs more than ~15% throughput it needs a fast path that skips
# evaluation for jobs with no policy attached." When nothing the policy clauses read
# exists, the statement takes a second `eligible` arm that skips the rate-class window,
# all five policy joins, the maintained inflight read, and — when its own exact draw bound
# binds — the whole round-32d escalation chain. It KEEPS the per-partition draw, the
# deficit charge, the inflight counter and the locked-rows state re-check, because those
# are core semantics, not policy.
#
# Two kinds of assertion again, and for the same reason. BEHAVIORAL ones prove the two
# arms admit the same jobs. STRUCTURAL ones prove the fast arm is actually TAKEN (and, as
# importantly, NOT taken when policy exists) — a gate that quietly ran the full path every
# time would pass every behavioral assertion here while delivering none of the win, and a
# gate that took the fast path when a bucket exists would be a correctness bug that only
# shows up on someone's rate limit.
#
# Round 32ak closes the residual direct-lock term for the sole-partition shape. The
# compact statement owns the partition index scan and locks while drawing, so contention
# advances to the next row instead of returning a short batch. This is safe only because
# no policy exists and every row inside the exact fair bound is selected; a second active
# partition or any policy returns a no-write sentinel and the driver runs admit.sql.
reset_pg
$H enqueue count=6 prefix=dl queue=default fp=dlf sched=1000 retention=86400000 >/dev/null
chk "direct fast path: one policy-free active partition is handled without fallback" \
    "$(directverdict default 3 1000)" "f"
PGAPPNAME=hg-direct-lock $PSQL -c "BEGIN;
SELECT id FROM headgate_job WHERE state='available' AND queue='default'
ORDER BY priority DESC, scheduled_at_ms, id LIMIT 3 FOR UPDATE;
SELECT pg_advisory_lock(9321, 2);
SELECT pg_sleep(30);
ROLLBACK;" >/dev/null 2>&1 &
dlpid=$!
for _ in $(seq 1 200); do
  ready=$($PSQL -c "SELECT count(*) FROM pg_locks
                     WHERE locktype='advisory' AND classid=9321 AND objid=2 AND granted;")
  [ "$ready" = "1" ] && break
  sleep 0.1
done
$H admit queues=default capacity=3 lease_ms=30000 worker=wd lease=LD1 quantum=1000 >/dev/null
$PSQL -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='hg-direct-lock';" >/dev/null 2>&1
wait "$dlpid" 2>/dev/null
chk "direct fast path Rust: SKIP LOCKED is work-conserving inside the sole partition" \
    "$(ranset)" "dl4,dl5,dl6"
r=$($PSQL -c "SELECT d.deficit||'|'||i.n||'|'||q.dispatch_count
              FROM headgate_partition_deficit d
              JOIN headgate_inflight i USING (queue, partition_key)
              JOIN headgate_queue_state q USING (queue)
              WHERE d.queue='default' AND d.partition_key='';")
chk "direct fast path Rust: deficit, inflight and queue service are charged atomically" \
    "$r" "997|3|3"

reset_pg
for p in A B; do $H enqueue count=2 prefix=dm$p queue=default partition=$p fp=dmf sched=1000 >/dev/null; done
chk "direct fast path: a second active partition returns the no-write fallback sentinel" \
    "$(directverdict default 3 1000)" "t"
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('unused-direct',9,9,9,1000,1000);" >/dev/null
chk "direct fast path: a visible policy row also returns the no-write fallback sentinel" \
    "$(directverdict default 3 1000)" "t"

# The structural signal is the plan itself. `inflight` is the right probe and the only
# stable one: it is declared AS MATERIALIZED, so it is always a NAMED CTE in the plan
# whatever the planner does with inlining, and it is referenced by the POLICY arm alone —
# so "never executed" means that arm did not run. (`ranked` was the obvious choice and is
# the wrong one: with the escalation chain now sourced from `candidates`, `ranked` has a
# single reference and Postgres inlines it, which erases the node the assertion looked for.)
# Prints `skipped`, `executed`, or `absent` if the CTE is not in the plan at all, so a
# plan-shape change fails LOUDLY instead of quietly reading as "executed". (Round 32h:
# these were `1`/`0`. A bare `0` is the shape three separate vacuity bugs wore in this
# sweep, and `chk` now refuses one — but here the signal was never actually vacuous, so
# it is spelled out rather than wrapped in a witness that would prove nothing extra.)
# args: cte queue capacity quantum
explain_admit(){ $PSQL -c "BEGIN;
PREPARE hgf (text[], int, bigint, bigint, text, text, bigint, int, int) AS
${HGSQL%;}
;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE hgf('{$1}', $2, 0, 30000, 'wf', 'LF', $3, 8, 0);
ROLLBACK;" 2>/dev/null; }
cteskip(){ local line
  line=$(explain_admit "$2" "$3" "$4" | awk -v c="CTE $1\$" '$0 ~ c {getline; print}')
  if [ -z "$line" ]; then echo absent
  elif echo "$line" | grep -q 'never executed'; then echo skipped
  else echo executed; fi; }
# How many rows the per-partition draw actually fetched. This is the fast path's whole
# economic argument — it draws its EXACT bound instead of the round-32d adaptive window —
# so it is asserted as a number, not inferred from a stopwatch.
candrows(){ explain_admit "$1" "$2" "$3" \
  | awk '/CTE candidates$/ {getline; if (match($0, /rows=[0-9]+/)) print substr($0, RSTART+5, RLENGTH-5)}'; }

reset_pg
for p in A B C; do $H enqueue count=6 prefix=fp$p queue=default partition=$p fp=fpf sched=1000 >/dev/null; done
chk "fast path: with no policy anywhere, the policy arm's inflight read is not executed" "$(cteskip inflight default 4 2)" "skipped"
# capacity 4 over 3 partitions, quantum 2, deficit 0: the fast bound is
# LEAST(quantum*4, deficit+quantum, capacity) = 2 per partition = 6 rows; round 32d's
# adaptive window is ceil(4/3)+1 = 3 per partition = 9. Fewer rows read for the same answer.
chk "fast path: the draw is the EXACT fair bound (2/partition), not the adaptive window" "$(candrows default 4 2)" "6"
$H admit queues=default capacity=4 lease_ms=30000 worker=w1 lease=LF1 quantum=2 >/dev/null
chk "fast path: the per-partition draw survives — 2/2/- at quantum 2, capacity 4" "$(ranset)" "fpA1,fpA2,fpB1,fpB2"
r=$($PSQL -c "SELECT COALESCE(string_agg(partition_key||':'||deficit, ',' ORDER BY partition_key),'-') FROM headgate_partition_deficit;")
chk "fast path: ...and the deficit is still charged, including to the partition that yielded" "$r" "A:0,B:0,C:2"

# THE EQUIVALENCE, asserted directly rather than inferred: the same fixture admitted by
# the fast arm and by the full arm must return the same jobs. The full arm is forced by a
# rate bucket for a class NO JOB USES — it constrains nothing, so any difference in the
# answer is the fast path's, not the bucket's.
reset_pg
for p in A B C; do $H enqueue count=6 prefix=eq$p queue=default partition=$p fp=eqf sched=1000 >/dev/null; done
$H admit queues=default capacity=5 lease_ms=30000 worker=w1 lease=LF2 quantum=2 >/dev/null
free_set=$(ranset)
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('unused-class',9,9,9,1000,1000);" >/dev/null
for p in A B C; do $H enqueue count=6 prefix=eq$p queue=default partition=$p fp=eqf sched=1000 >/dev/null; done
chk "fast path: an irrelevant rate bucket DOES force the full path (the probe is sound)" "$(cteskip inflight default 5 2)" "executed"
$H admit queues=default capacity=5 lease_ms=30000 worker=w1 lease=LF3 quantum=2 >/dev/null
chk "fast path: ...and both arms admit exactly the same jobs" "$(ranset)" "$free_set"

# THE OTHER SHAPE, where the fast arm's exact bound is WIDER than round 32d's adaptive
# window and the escalation chain therefore has to come back on. 5 partitions x 12 jobs,
# capacity 12, quantum 8: the exact bound is 8/partition but the adaptive window is
# ceil(12/5)+1 = 4, so the draw is 4 (20 rows) and the gate must be able to widen. Skipping
# the escalation here would silently under-admit, which is exactly the bug the mechanism
# has to avoid — so the answer is diffed against the full arm again.
reset_pg
for p in A B C D E; do $H enqueue count=12 prefix=es$p queue=default partition=$p fp=esf sched=1000 >/dev/null; done
chk "fast path: when the adaptive window binds below the exact bound, the draw follows it" "$(candrows default 12 8)" "20"
$H admit queues=default capacity=12 lease_ms=30000 worker=w1 lease=LF4 quantum=8 >/dev/null
esc_set=$(ranset)
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('unused-class',99,99,99,1000,1000);" >/dev/null
for p in A B C D E; do $H enqueue count=12 prefix=es$p queue=default partition=$p fp=esf sched=1000 >/dev/null; done
$H admit queues=default capacity=12 lease_ms=30000 worker=w1 lease=LF5 quantum=8 >/dev/null
chk "fast path: ...and the escalating fast arm still admits what the full arm admits" "$esc_set" "$(ranset)"

# EACH PROBE, ONE AT A TIME. Err toward the full path: a false "fast" is a correctness
# bug, a false "slow" is only slow. So every one of the three tables disables it on its
# own, even when the row present provably could not have blocked anything.
reset_pg
$H enqueue count=6 prefix=pb queue=default fp=pbf sched=1000 >/dev/null
chk "fast path: taken when all three policy tables are empty" "$(cteskip inflight default 3 2)" "skipped"
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint,kind,crash_count,quarantined_at_ms,reason) VALUES ('some-other-fp','w',3,1000,'x');" >/dev/null
chk "fast path: a quarantine row for an UNRELATED fingerprint still forces the full path" "$(cteskip inflight default 3 2)" "executed"
$PSQL -c "DELETE FROM headgate_quarantine;" >/dev/null
$PSQL -c "INSERT INTO headgate_concurrency_limit VALUES ('cl-other','some-other-queue',1);" >/dev/null
chk "fast path: a ceiling on ANOTHER queue does NOT force it — the probe is queue-scoped" "$(cteskip inflight default 3 2)" "skipped"
$PSQL -c "INSERT INTO headgate_concurrency_limit VALUES ('cl-this','default',1);" >/dev/null
chk "fast path: ...and a ceiling on THIS queue does force it" "$(cteskip inflight default 3 2)" "executed"

# POLICY APPEARING BETWEEN ADMITS — the fast path's dangerous case. The first admit runs
# policy-free and writes deficit and inflight; a quarantine then lands; the second admit
# must evaluate the full path AGAINST THE STATE THE FAST PATH LEFT. Detection is inside
# the statement, so it shares the statement's snapshot: there is no window in which a
# committed policy row is missed.
reset_pg
$H enqueue count=3 prefix=ma queue=default fp=mhd sched=1000 >/dev/null
$H enqueue count=3 prefix=mb queue=default fp=mtl sched=1000 >/dev/null
$H admit queues=default capacity=2 lease_ms=30000 worker=w1 lease=LM1 quantum=1000 >/dev/null
chk "mid-sweep: the first admit runs policy-free and takes the head" "$(ranset)" "ma1,ma2"
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint,kind,crash_count,quarantined_at_ms,reason) VALUES ('mhd','w',3,1000,'poison');" >/dev/null
chk "mid-sweep: ...the new quarantine row is seen on the very next call" "$(cteskip inflight default 2 1000)" "executed"
$H admit queues=default capacity=2 lease_ms=30000 worker=w2 lease=LM2 quantum=1000 >/dev/null
chk "mid-sweep: ...and the second admit skips ma3 for the admissible tail" "$(ranset)" "ma1,ma2,mb1,mb2"
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='ma3' AND state='available';")
chk "mid-sweep: ...leaving the quarantined row available, never locked (invariant 2)" "$r" "1"

echo "== Postgres lifecycle (through the Rust store) =="
reset_pg
$H enqueue count=1 prefix=e queue=default sched=1000 retention=0        >/dev/null  # ephemeral
$H enqueue count=2 prefix=k queue=default sched=1000 retention=86400000 >/dev/null  # kept
$H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LA quantum=1000 >/dev/null
$H ack job=e1 lease=LA fence=1 outcome=success >/dev/null
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='e1';")
# "the row is gone" is also true of a row that was never written. The witness is the
# RETAINED sibling from the same enqueue+admit: if the fixture never landed, k1 is
# missing too and this fails before the zero can pass.
chk0 "ephemeral job (retention 0) is deleted on success" "$r" "0" \
     "...witness: the retained sibling from the same batch IS there" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid IN ('k1','k2');")"
$H ack job=k1 lease=LA fence=1 outcome=success >/dev/null
r=$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='k1';")
chk "retained job completes on success" "$r" "completed"
# §4.6 retention sweep: a 1ms retention lapses, the 24h one survives.
$H enqueue count=1 prefix=rt queue=default sched=1000 retention=1 >/dev/null
$H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LT quantum=1000 >/dev/null
$H ack job=rt1 lease=LT fence=1 outcome=success >/dev/null
sleep 0.05
$H evict >/dev/null
r="$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='rt1';")|$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='k1';")"
chk "retention sweep evicts lapsed, keeps retained" "$r" "0|1"
$H ack job=k2 lease=LA fence=1 outcome=retry err=boom logs=opened-conn,got-500 >/dev/null
r=$($PSQL -c "SELECT errors->-1->'logs'->>1 FROM headgate_job WHERE ulid='k2';")
chk "per-attempt logs land inside the attempt's entry" "$r" "got-500"
r=$($PSQL -c "SELECT state||'|'||attempt||'|'||crash_attempt FROM headgate_job WHERE ulid='k2';")
chk "returned error retries: attempt=1, crash_attempt=0" "$r" "retryable|1|0"
r=$($H ack job=k2 lease=LA fence=1 outcome=success 2>&1; echo "rc=$?")
chk "ack after the lease is gone is rejected, never a no-op" "${r##*rc=}" "1"

reset_pg
$H enqueue count=2 prefix=r queue=default sched=1000 retention=86400000 >/dev/null
$H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LR quantum=1000 >/dev/null
$PSQL -c "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='r1';" >/dev/null  # dead worker
rec=$($H reclaim | cut -d'|' -f1)
chk "expired lease is reclaimed" "$rec" "r1"
r=$($PSQL -c "SELECT state||'|'||attempt||'|'||crash_attempt FROM headgate_job WHERE ulid='r1';")
chk "reclaim is LeaseLost, not Retry: attempt=0, crash_attempt=1" "$r" "retryable|0|1"
lost=$($H renew lease_ms=30000 refs=r1:LR:1,r2:LR:1 | tr -d ' ')
chk "renew returns the lost lease and extends the held one" "$lost" "r1"

# ----- §5.2 A CRASH-SUSPECT YIELDS ITS QUEUE POSITION (round 32) -----
# SQS moves a message received three times without deletion to the back of the queue, so
# a suspect stops head-of-line-blocking everything behind it BEFORE it reaches a terminal
# state. The register carried that as a design gap; it was already the behavior. Reclaim
# re-stamps `scheduled_at_ms` to the STORE clock plus the crash backoff, and that column
# is the gate's second sort key — so the suspect goes behind every same-priority sibling
# enqueued while it was running. Asserted, not assumed: without this, three jobs behind a
# poison pill would re-block on it once per crash cycle until the quarantine limit trips.
# Capacity is bounded to 2 on purpose — see idset().
reset_pg
$H enqueue count=1 prefix=hola queue=default partition=hol fp=fp sched=1000 retention=86400000 >/dev/null
$H enqueue count=1 prefix=holb queue=default partition=hol fp=fp sched=1001 retention=86400000 >/dev/null
$H enqueue count=1 prefix=holc queue=default partition=hol fp=fp sched=1002 retention=86400000 >/dev/null
r=$($H admit queues=default capacity=1 lease_ms=30000 worker=w1 lease=LH1 quantum=1 | cut -d'|' -f1)
chk "head-of-line: the draw takes the partition's oldest job first" "$r" "hola1"
$PSQL -c "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='hola1';" >/dev/null  # crash
$H reclaim >/dev/null
r=$($PSQL -c "SELECT (SELECT scheduled_at_ms FROM headgate_job WHERE ulid='hola1')
             > (SELECT max(scheduled_at_ms) FROM headgate_job WHERE ulid IN ('holb1','holc1'));")
chk "crash-attributed reclaim re-stamps the suspect BEHIND its siblings" "$r" "t"
sleep 1.2   # past the crash backoff: all three are now due, so this is ORDER, not delay
$H promote >/dev/null
r=$($H admit queues=default capacity=2 lease_ms=30000 worker=w1 lease=LH2 quantum=10 | idset)
chk "the next admit yields B and C, never the suspect" "$r" "holb1,holc1"
r=$($H admit queues=default capacity=2 lease_ms=30000 worker=w1 lease=LH3 quantum=10 | idset)
chk "...and the suspect follows them; it yielded position, it was not lost" "$r" "hola1"

reset_pg
$H enqueue count=1 prefix=bomb fp=fp-BOMB sched=1000 retention=86400000 >/dev/null
for i in 1 2 3; do
  $PSQL -c "UPDATE headgate_job SET scheduled_at_ms=0 WHERE ulid='bomb1';" >/dev/null
  $H promote >/dev/null
  $H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LB$i quantum=1000 >/dev/null
  $PSQL -c "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='bomb1' AND state='running';" >/dev/null
  $H reclaim >/dev/null
done
r=$($PSQL -c "SELECT state||'|'||crash_attempt FROM headgate_job WHERE ulid='bomb1';")
chk "third crash quarantines the fingerprint" "$r" "quarantined|3"
r=$($H enqueue count=1 prefix=again fp=fp-BOMB 2>&1 | grep -c 'quarantined')
chk "enqueue of a quarantined fingerprint is rejected" "$r" "1"
# §5.2 the sweeper: a waiting SIBLING of the quarantined fingerprint parks VISIBLY,
# instead of sitting gate-excluded forever. (Inserted raw — enqueue would reject it.)
$PSQL -c "INSERT INTO headgate_job (ulid,kind,payload,queue,fingerprint,enqueued_at_ms,scheduled_at_ms)
          VALUES ('sib1','w','\x00','default','fp-BOMB',1000,1000);" >/dev/null
r=$($H sweep-quarantine)
chk "quarantine sweeper parks waiting siblings visibly" "$r" "1"
r=$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='sib1';")
chk "...in the terminal quarantined state" "$r" "quarantined"

reset_pg
# A rolled-back enqueue and an enqueue that silently wrote NOTHING leave the same empty
# table — which is the one failure this test exists to rule out. So the COMMIT arm runs
# FIRST and is the witness: it proves `tx` can write at all before the rollback arm
# claims it did not.
$H tx mode=commit id=t2 >/dev/null
committed=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='t2';")
chk "transactional enqueue commits with the caller" "$committed" "1"
$H tx mode=rollback id=t1 >/dev/null
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='t1';")
chk0 "transactional enqueue rolls back with the caller" "$r" "0" \
     "...witness: the same path COMMITS when told to (so absence means rollback)" "$committed"
$H enqueue count=1 prefix=uq1- unique=K1 sched=1000 >/dev/null
r=$($H enqueue count=1 prefix=uq2- unique=K1 sched=1000 2>&1)
chk "duplicate unique key returns the existing id" "$r" "ERR duplicate unique key; existing job uq1-1"

# §4.4 LIFECYCLE mode: released by terminal state, not by the clock.
$H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LU quantum=1000 >/dev/null
$H ack job=uq1-1 lease=LU fence=1 outcome=success >/dev/null
r=$($H enqueue count=1 prefix=uq3- unique=K1 sched=1000 2>&1)
chk "lifecycle unique key releases on terminal state" "$r" "1"

# §4.4 THROTTLE mode: released by the clock, held across completion.
$H enqueue count=1 prefix=th1- unique=K2 window=60000 sched=1000 retention=86400000 >/dev/null
r=$($H enqueue count=1 prefix=th2- unique=K2 window=60000 sched=1000 2>&1)
chk "throttle unique key blocks within the window" "$r" "ERR duplicate unique key; existing job th1-1"
$PSQL -c "UPDATE headgate_job SET unique_expires_at_ms=1 WHERE ulid='th1-1';" >/dev/null  # window over
r=$($H enqueue count=1 prefix=th3- unique=K2 window=60000 sched=1000 2>&1)
chk "throttle unique key releases when the window ends" "$r" "1"

# ================= ROUND 32L: THE MUTATION SWEEP'S POSTGRES HALF =================
# Round 32j built the evidence linter and stated its own limit: "the linter checks that
# evidence exists and runs, not that it is SUFFICIENT." Round 32l applied round 32i's
# technique to the ~30 EVIDENCE.md blocks whose NOTE admitted the ✅ was broader than what
# ran. ELEVEN mutations were UNCAUGHT by all 462 assertions. What follows is the Postgres
# half of closing them; the Redis half is in the Redis store section, and the two that
# cannot be reached from a store harness (`once`'s post-effect fence, `extend_lease`'s
# lost-lease report, `skip` vs `backfill`, Go's read-only 403) are integration tests.
#
# Every assertion below carries the CONTROL that keeps it from passing on an absent
# fixture, because the five unique assertions immediately above are exactly what a
# sufficient-looking-but-blind assertion set looks like: all five ack SUCCESS and
# re-enqueue at once, so neither "held across a retry" nor "survives completion" is
# reachable from any of them.
reset_pg

# --- Unique/dedup, hole 1: a LIFECYCLE key must survive a RETRY. -------------------
# MUTATION: drop the lifecycle key when the job goes retryable (PgStore's Retry arm sets
# unique_key = NULL; ack.lua calls release_unique() in the retryable branch). 462/462
# stayed green. In production that is a second copy of a job that is still in flight.
$H enqueue count=1 prefix=uqr1- queue=uqholdq unique=KRET sched=1000 max_attempts=9 retention=86400000 >/dev/null
f=$($H admit queues=uqholdq capacity=1 lease_ms=30000 worker=w1 lease=LRET quantum=1000 | cut -d'|' -f3)
$H ack job=uqr1-1 lease=LRET fence="$f" outcome=retry err=boom >/dev/null
chk "32l unique: the holder really is RETRYABLE, not terminal (the fixture landed)" \
    "$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='uqr1-1';")" "retryable"
r=$($H enqueue count=1 prefix=uqr2- queue=uqholdq unique=KRET sched=1000 2>&1)
chk "32l unique: a LIFECYCLE key is STILL HELD while its holder is retryable — a retry is not a release" \
    "$r" "ERR duplicate unique key; existing job uqr1-1"

# --- Unique/dedup, hole 2: a THROTTLE window must survive COMPLETION. --------------
# MUTATION: release the throttle key at success on both backends. 462/462 stayed green,
# because every throttle assertion above ends the window by hand instead of ending the
# JOB. §4.4's whole distinction — "released by the clock REGARDLESS of the job's fate" —
# was therefore unfalsifiable: throttle mode could silently collapse into lifecycle mode.
$H enqueue count=1 prefix=tsv1- queue=tsvq unique=KTSV window=600000 sched=1000 retention=86400000 >/dev/null
f=$($H admit queues=tsvq capacity=1 lease_ms=30000 worker=w1 lease=LTSV quantum=1000 | cut -d'|' -f3)
$H ack job=tsv1-1 lease=LTSV fence="$f" outcome=success >/dev/null
chk "32l unique: the throttle holder really COMPLETED (the fixture landed)" \
    "$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='tsv1-1';")" "completed"
r=$($H enqueue count=1 prefix=tsv2- queue=tsvq unique=KTSV window=600000 sched=1000 2>&1)
chk "32l unique: a THROTTLE window SURVIVES completion — only the CLOCK releases it, never the job's fate" \
    "$r" "ERR duplicate unique key; existing job tsv1-1"

# --- Fencing token: the fence must be a TERM, not decoration. ----------------------
# MUTATION: remove `j.fence = $3` from the ack identity clause (and `h[3] ~= fence` from
# ack.lua). 462/462 + 96/96 stayed green, and a probe confirmed an ack carrying fence 100
# against a real fence of 1 COMPLETED THE JOB. Every proof the row cited also changes the
# LEASE ID, so lease_id alone was always the deciding term and the fence was never tested.
# These three isolate it: same job, same lease id, one stale fence, plus the control.
$H enqueue count=1 prefix=fen1- queue=fenq sched=1000 retention=86400000 >/dev/null
f=$($H admit queues=fenq capacity=1 lease_ms=30000 worker=w1 lease=LFEN quantum=1000 | cut -d'|' -f3)
r=$($H ack job=fen1-1 lease=LFEN fence=$((f + 1)) outcome=success 2>&1)
chk "32l fence: an ack with the RIGHT lease id but a STALE fence is REJECTED — the fence is a term, not decoration" \
    "$r" "ERR lease no longer held for job fen1-1; stop work immediately"
chk "32l fence: ...and the job is untouched, still running under its real holder" \
    "$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='fen1-1';")" "running"
r=$($H ack job=fen1-1 lease=LFEN fence="$f" outcome=success 2>&1)
chk "32l fence: ...control: the SAME ack with the REAL fence succeeds, so it was the fence that refused" \
    "$r" "ok"

# --- At-least-once: a lease that expires ON ITS OWN. ------------------------------
# The row's own NOTE said it: "every proof forces lease_expires_at_ms = 0 by hand". So the
# MUTATION `WHERE ... lease_expires_at_ms <= 0` — which strands every REAL crashed worker
# forever while still reclaiming every hand-zeroed fixture — was UNCAUGHT on Postgres by
# all 462 assertions and all 96 scenarios (Redis caught it, via its inspect tests). This
# claims a SHORT lease and lets the clock do the work; nothing writes a zero anywhere.
$H enqueue count=1 prefix=alo1- queue=aloq fp=alo-fp sched=1000 retention=86400000 >/dev/null
$H admit queues=aloq capacity=1 lease_ms=300 worker=w1 lease=LALO quantum=1000 >/dev/null
exp=$($PSQL -c "SELECT lease_expires_at_ms FROM headgate_job WHERE ulid='alo1-1';")
chk "32l at-least-once: the claim stamped a REAL future lease — no fixture zeroed it" \
    "$(if [ "${exp:-0}" -gt 1000000000000 ]; then echo real-timestamp; else echo "suspect:${exp:-none}"; fi)" \
    "real-timestamp"
sleep 0.7
chk "32l at-least-once: a lease that expired ON ITS OWN is reclaimed — the sweep reads the clock, not a zero" \
    "$($H reclaim limit=10 | grep -c '^alo1-1|')" "1"
chk "32l at-least-once: ...and the survivor is retryable with the CRASH counted, never lost and never a retry" \
    "$($PSQL -c "SELECT state||'|'||attempt||'|'||crash_attempt FROM headgate_job WHERE ulid='alo1-1';")" \
    "retryable|0|1"

# --- Retries + backoff: the BACKOFF half, which the row admitted was unproven. -----
# MUTATION: replace `LEAST(cap, base * 2^attempt)` with `base` — no growth, no ceiling.
# 462/462 stayed green: every live-store test sets retry_base_ms = 1 or acks once.
# Postgres adds up-to-one-base jitter, so CONSECUTIVE bands overlap by construction and
# only NON-adjacent ones are decisive; that is why 1x, 8x and the clamp are asserted here
# and the exact per-attempt values are asserted on Redis, which has no jitter.
# `attempt` is seeded directly — it is the INPUT to the formula, and seeding it is not the
# hand-forced-lease shape this round is closing (nothing here fakes a clock).
$H enqueue count=1 prefix=bk0- queue=bkq sched=1000 max_attempts=99 retention=86400000 >/dev/null
$H enqueue count=1 prefix=bk3- queue=bkq sched=1000 max_attempts=99 retention=86400000 >/dev/null
$H enqueue count=1 prefix=bkc- queue=bkq sched=1000 max_attempts=99 retention=86400000 >/dev/null
seed "32l backoff fixtures: three jobs standing at attempt 0, 3 and 20" \
     "UPDATE headgate_job SET attempt=3 WHERE ulid='bk3-1';
      UPDATE headgate_job SET attempt=20 WHERE ulid='bkc-1';"
# Read the delay immediately after each ack, so the only slack is one psql round trip.
pgdelay(){ $PSQL -c "SELECT scheduled_at_ms - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint FROM headgate_job WHERE ulid='$1';"; }
band(){ if [ "${1:-0}" -ge "$2" ] && [ "${1:-0}" -lt "$3" ]; then echo in-band; else echo "delay=${1:-none}"; fi; }
for row in $($H admit queues=bkq capacity=10 lease_ms=60000 worker=w1 lease=LBK quantum=1000); do
  bid=$(echo "$row" | cut -d'|' -f1); blz=$(echo "$row" | cut -d'|' -f2); bfz=$(echo "$row" | cut -d'|' -f3)
  $H ack job="$bid" lease="$blz" fence="$bfz" outcome=retry err=boom >/dev/null
  case "$bid" in bk0-1) d0=$(pgdelay "$bid");; bk3-1) d3=$(pgdelay "$bid");; bkc-1) dc=$(pgdelay "$bid");; esac
done
chk "32l backoff: attempt 0 waits ONE base period (1000ms + up to 1000ms jitter)" \
    "$(band "${d0:-}" 900 2000)" "in-band"
chk "32l backoff: attempt 3 waits EIGHT — the curve is base * 2^attempt, not linear and not constant" \
    "$(band "${d3:-}" 7900 9000)" "in-band"
chk "32l backoff: attempt 20 is CLAMPED at retry_cap_ms (3600000), never 2^20 base periods" \
    "$(band "${dc:-}" 3599900 3601000)" "in-band"

# --- Retention/eviction: `quarantined` is exempt BY DESIGN. -----------------------
# The row's NOTE: "the `quarantined` is exempt by design claim is a source comment, not an
# assertion." MUTATION: add 'quarantined' to the sweep's state list. 462/462 stayed green
# — the sweep would silently delete the evidence an operator is meant to come back to.
$H enqueue count=1 prefix=rq1- queue=retq fp=rq-fp sched=1000 retention=1 >/dev/null
$H enqueue count=1 prefix=rc1- queue=retq fp=rc-fp sched=1000 retention=1 >/dev/null
seed "32l retention fixtures: one quarantined and one completed, BOTH with a lapsed retention" \
     "UPDATE headgate_job SET state='quarantined', finalized_at_ms=1 WHERE ulid='rq1-1';
      UPDATE headgate_job SET state='completed', finalized_at_ms=1 WHERE ulid='rc1-1';"
chk "32l retention: the sweep evicted the equally-lapsed COMPLETED sibling, so it really ran" \
    "$($H evict limit=100)" "1"
chk "32l retention: ...and the lapsed QUARANTINED row SURVIVES — it parks visibly until an operator acts" \
    "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='rq1-1';")" "1"

# --- Runtime policy writes: quarantine release, for EFFECT, on Postgres. ----------
# MUTATION: keep deleting the registry row but leave every job parked. 462/462 stayed
# green: Redis asserts this for effect (round 16) and Postgres asserted it NOWHERE — there
# was no `quarantine-release` verb on the PG harness at all until round 32l added one.
$H enqueue count=2 prefix=qz- queue=qzq fp=qz-fp sched=1000 retention=86400000 >/dev/null
seed "32l quarantine fixtures: two siblings parked under one quarantined fingerprint" \
     "UPDATE headgate_job SET state='quarantined', finalized_at_ms=1 WHERE queue='qzq';
      INSERT INTO headgate_quarantine (fingerprint, kind, crash_count, quarantined_at_ms, reason)
      VALUES ('qz-fp', 'w', 3, 1, 'crash limit reached');"
chk0 "32l quarantine: the gate admits NOTHING while the fingerprint is parked" \
     "$($H admit queues=qzq capacity=10 lease_ms=30000 worker=w1 lease=LQZ0 quantum=1000 | wc -l | tr -d ' ')" "0" \
     "32l quarantine: ...witness: the two siblings really are in the store, parked" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE queue='qzq' AND state='quarantined';")"
chk "32l quarantine: release reports the number of jobs it actually freed, not just the registry row" \
    "$($H quarantine-release fp=qz-fp)" "2"
chk "32l quarantine: ...and the freed siblings are ADMISSIBLE again — a release that only deletes the registry row is a lie" \
    "$($H admit queues=qzq capacity=10 lease_ms=30000 worker=w1 lease=LQZ1 quantum=1000 | wc -l | tr -d ' ')" "2"

# --- Pause / resume: RESUME, asserted for effect. ---------------------------------
# The row's NOTE: "only PAUSE is asserted for effect; the resume calls elsewhere are
# unasserted setup." MUTATION: make `set_queue_paused(q, false)` a no-op in all four store
# implementations. It WAS caught — but only as collateral damage three sections later (a
# queue left paused starved an unrelated unique-key fixture), which is a red that tells an
# operator nothing. These two make resume its own assertion, with pause as the control.
$H enqueue count=1 prefix=pz1- queue=pzq sched=1000 retention=86400000 >/dev/null
$H pause queue=pzq >/dev/null
chk "32l pause: a paused queue blocks the job at the gate (the control)" \
    "$($H explain job=pz1-1)" "admissible=false blocked_by=queue_paused"
$H pause queue=pzq paused=false >/dev/null
chk "32l resume: ...and RESUME really un-pauses it — the same job, the same gate, now admissible" \
    "$($H explain job=pz1-1)" "admissible=true blocked_by=none"
chk "32l resume: ...and the gate actually yields it, so resume reaches the CLAIM and not only the explain" \
    "$($H admit queues=pzq capacity=10 lease_ms=30000 worker=w1 lease=LPZ quantum=1000 | wc -l | tr -d ' ')" "1"
# ================= END ROUND 32L, POSTGRES HALF =================

# ----- INVARIANT 4, THE SUB-SECOND DURATION (round 32i) -----
# "Every duration is milliseconds on the wire, validated at the boundary. A duration that
# rounds to zero is an error." Every window this file used was 60000, and every timeout
# was a round number of seconds — so a boundary that floored durations to whole seconds
# changed NOTHING any assertion could see. That is asynq's bug exactly: `int(ttl.Seconds())`
# turned a 500ms unique TTL into 0, and 0 is not "no window", it is LIFECYCLE mode — a
# permanent lock. Mutation-tested in round 32i by flooring unique_window_ms to seconds in
# both Postgres drivers: 337 of 337 assertions stayed green.
#
# The check is EXACT and timing-free rather than a stopwatch: `enqueue` writes
# enqueued_at_ms and unique_expires_at_ms from the SAME store clock read, so their
# difference IS the window the boundary accepted, to the millisecond. `lifecycle` is
# printed for a NULL expiry, which is precisely what a floored 500 collapses to.
uwin(){ $PSQL -c "SELECT COALESCE((unique_expires_at_ms - enqueued_at_ms)::text, 'lifecycle') FROM headgate_job WHERE ulid='$1';"; }
$H enqueue count=1 prefix=sw1- unique=KSUB1 window=500 sched=1000 retention=86400000 >/dev/null
chk "invariant 4: a 500ms unique window survives the boundary AS 500ms, never floored into a lifecycle lock" \
    "$(uwin sw1-1)" "500"
# Not only the round-to-zero: any second-granularity boundary loses this one too, and a
# 1500ms window that silently becomes 1000ms is the same class of bug one order up.
$H enqueue count=1 prefix=sw2- unique=KSUB2 window=1500 sched=1000 retention=86400000 >/dev/null
chk "invariant 4: ...and 1500ms is not truncated to 1000ms either (the rounding, not just the zero)" \
    "$(uwin sw2-1)" "1500"
# The other half of the invariant: VALIDATED at the boundary. A negative duration is an
# error there, never clamped to 0 (which would be a silent permanent lock).
r=$($H enqueue count=1 prefix=sw3- unique=KSUB3 window=-1 sched=1000 2>&1)
chk "invariant 4: a negative duration is REJECTED at the boundary, never clamped into lifecycle" \
    "$r" "ERR unique_window_ms must be >= 0"

# ----- §5.9 kind format + §4.4b strict caller-supplied id (round 32) -----
# The kind rule is enforced at the STORE boundary, not in the runtime: the control API and
# these harnesses call Store::enqueue directly and never come through the runtime, so a
# runtime-level rule would leave the API unguarded.
reset_pg
r=$($H enqueue count=1 prefix=bk queue=default kind='bad kind' payload='{}' fp=auto sched=1000 2>&1)
chk "PG §5.9: the store rejects a malformed kind" "$r" "ERR $KINDMSG"
r=$($H enqueue count=1 prefix=kw queue=default kind=w payload='{}' fp=auto sched=1000 2>&1)
chk "PG §5.9: a ONE-character kind stays legal (River requires two)" "$r" "1"

# §4.4b. OLD behavior, all four backends: ANY repeated id was a bare "duplicate job id"
# that the API served as a 400 — identical whether the caller was retrying the same
# enqueue or clobbering a different job. The contract is now split by CONTENT.
reset_pg
$H enqueue count=1 prefix=idc queue=default payload='{"n":1}' fp=auto sched=1000 retention=86400000 >/dev/null
r=$($H enqueue count=1 prefix=idc queue=default payload='{"n":1}' fp=auto sched=1000 retention=86400000 2>&1)
chk "PG §4.4b: re-enqueue of an identical id is idempotent success" "$r" "1"
r=$($PSQL -c "SELECT count(*) FROM headgate_job;")
chk "PG §4.4b: ...and the job is NOT duplicated" "$r" "1"
# The strongest form of "not re-written": the §5.5 arrival counter did not move either.
r=$($PSQL -c "SELECT COALESCE(sum(arrived),0) FROM headgate_queue_counter WHERE queue='default';")
chk "PG §4.4b: ...and no counter moved (no silent re-write)" "$r" "1"
r=$($H enqueue count=1 prefix=idc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "PG §4.4b: same id, DIFFERENT content is a typed conflict" "$r" "ERR id conflict: job idc1"
r=$($H enqueue count=2 prefix=idc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "PG §4.4b: a conflict rejects the WHOLE batch, naming the offender" "$r" "ERR id conflict: job idc1"
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='idc2';")
chk0 "PG §4.4b: ...and the clean sibling in that batch is not written" "$r" "0" \
     "...witness: the offending row idc1 is still there (the store is not simply empty)" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='idc1';")"
# A terminal row still owns its id — reuse follows retention eviction (§4.6), never the
# transition into a terminal state.
$H admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LIDC quantum=1000 >/dev/null
$H ack job=idc1 lease=LIDC fence=1 outcome=success >/dev/null
r=$($H enqueue count=1 prefix=idc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "PG §4.4b: a COMPLETED row still owns its id (reuse follows eviction)" "$r" "ERR id conflict: job idc1"

# §5.8 singleton duties: a lease from the store, no separate election.
$PSQL -c "TRUNCATE headgate_duty;" >/dev/null
won=$($H duty name=reclaimer holder=w1 lease_ms=60000)
chk "duty lease: first claimer wins" "$won" "true"
r=$($H duty name=reclaimer holder=w2 lease_ms=60000)
# A duty call that ALWAYS answers false — a broken lease, an unreachable table — refuses
# the second claimer just as convincingly. The first claim is the witness.
chk0 "duty lease: second claimer is refused" "$r" "false" \
     "...witness: the first claim succeeded, so `false` means CONTENTION" "$won"
$H duty-release name=reclaimer holder=w1 >/dev/null
r=$($H duty name=reclaimer holder=w2 lease_ms=60000)
chk "duty lease: released duty is claimable immediately" "$r" "true"
# Hygiene: leave no duty held, or the NEXT run's worker cannot claim it for 60s.
$H duty-release name=reclaimer holder=w2 >/dev/null

# ============================ ROUND 32K ============================
# Four capabilities that were IMPLEMENTED and UNEXERCISED. Round 32j's evidence linter
# recorded each of them as `none:` or as a NOTE saying the ✅ was broader than what ran;
# every assertion below reaches code that no test in either language had ever executed.
# ===================================================================

# ----- §4.7 DELAY / SCHEDULED: the WITHHOLDING half (round 32k) -----
# The register's weakest ✅ that still had evidence. Every citation proved a future
# `scheduled_at_ms` is STORED, RESCHEDULED TO or SNOOZED TO; NOTHING asserted that a
# not-yet-due job is kept AWAY from the gate, because every admission fixture in this
# corpus enqueues at `sched=1000` — already due since 1970. A gate that ignored
# `scheduled_at_ms` entirely would have passed all of them.
reset_pg
$H enqueue count=1 prefix=dlyA queue=dly fp=fpdlya sched=1000 retention=86400000 >/dev/null
$H enqueue count=1 prefix=dlyB queue=dly fp=fpdlyb sched=99999999999999 retention=86400000 >/dev/null
r=$($H admit queues=dly capacity=10 lease_ms=30000 worker=wd lease=LDLY quantum=1000 | idset)
chk "§4.7 delay: the gate draws the DUE job and WITHHOLDS the not-yet-due sibling" "$r" "dlyA1"
r=$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='dlyB1';")
chk "§4.7 delay: ...and it is withheld in \`scheduled\`, not merely unlucky in the draw" "$r" "scheduled"
$PSQL -c "UPDATE headgate_job SET scheduled_at_ms=1 WHERE ulid='dlyB1';" >/dev/null
r=$($H promote)
chk "§4.7 delay: promotion releases it once its time has come" "$r" "1"
r=$($H admit queues=dly capacity=10 lease_ms=30000 worker=wd lease=LDLY2 quantum=1000 | idset)
chk "§4.7 delay: ...and the SAME gate, same call, now yields it" "$r" "dlyB1"

# ----- §4 PER-TASK TIMEOUT + ABSOLUTE DEADLINE (round 32k) -----
# Implemented in BOTH worker loops since Phase 3, and until this round no test in either
# language ever constructed an envelope with a non-zero `timeout_ms`/`deadline_ms` — the
# harnesses had no flag for them, so the shell suite could not reach the code either.
# `timeout=` / `deadline=` on enqueue and `sleep=` on drain are what make it reachable.
reset_pg
$H enqueue count=1 prefix=tmo queue=tmoq fp=fptmo sched=1000 retention=86400000 timeout=50 >/dev/null
r=$($H drain queues=tmoq count=1 sleep=400)
chk "§4 timeout: the runtime really drew and ran the job" "$r" "1"
r=$($PSQL -c "SELECT state||'/'||attempt||'/'||crash_attempt FROM headgate_job WHERE ulid='tmo1';")
chk "§4 timeout: an attempt that outruns timeout_ms is a RETRY that CONSUMES an attempt (never a crash)" \
    "$r" "retryable/1/0"
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='tmo1' AND errors::text LIKE '%attempt timed out after 50ms%';")
chk "§4 timeout: ...and the error names the timeout and its value, not a generic failure" "$r" "1"
# The control: the SAME handler, the SAME sleep, no timeout on the envelope. Without this
# the assertions above pass for a runtime that fails every long handler.
$H enqueue count=1 prefix=tmok queue=tmokq fp=fptmok sched=1000 retention=86400000 >/dev/null
$H drain queues=tmokq count=1 sleep=400 >/dev/null
r=$($PSQL -c "SELECT state||'/'||attempt FROM headgate_job WHERE ulid='tmok1';")
chk "§4 timeout: ...control: the same 400ms handler with NO timeout completes" "$r" "completed/0"

reset_pg
$H enqueue count=1 prefix=dln queue=dlnq fp=fpdln sched=1000 retention=86400000 deadline=1000 >/dev/null
r=$($H drain queues=dlnq count=1)
chk "§4 deadline: the runtime really drew the job" "$r" "1"
r=$($PSQL -c "SELECT state||'/'||attempt FROM headgate_job WHERE ulid='dln1';")
chk "§4 deadline: an exceeded absolute deadline ARCHIVES and spends NO attempt (skip, not retry)" \
    "$r" "archived/0"
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='dln1' AND errors::text LIKE '%deadline exceeded%';")
chk "§4 deadline: ...and says why" "$r" "1"
$H enqueue count=1 prefix=dlnok queue=dlnokq fp=fpdlnok sched=1000 retention=86400000 deadline=99999999999999 >/dev/null
$H drain queues=dlnokq count=1 >/dev/null
r=$($PSQL -c "SELECT state FROM headgate_job WHERE ulid='dlnok1';")
chk "§4 deadline: ...control: a deadline still in the future runs normally" "$r" "completed"

# ----- §5.7 CURSOR ITERATION (round 32k) -----
# `JobCtx::step_cursor` / `set_cursor` had ZERO call sites repo-wide outside their own
# definitions and doc comments: no test, example, harness or scenario had ever written a
# cursor, `cp_cursor` appeared in the cross-language diff only as
# `coalesce(cp_cursor,''::bytea)` — i.e. always empty — and `checkpoint.lua`'s cursor
# branch was unreachable. Three properties, all of them the point of the feature:
# the loop persists its position, an INTERRUPTED loop RESUMES from that position rather
# than restarting, and every cursor write re-verifies the fence.
reset_pg
$H enqueue count=1 prefix=cur queue=curq fp=fpcur sched=1000 retention=86400000 payload='{}' >/dev/null
r=$($H cursor queues=curq pages=6 stop=3)
chk "§5.7 cursor: an interrupted resumable loop stops where it was interrupted" \
    "$r" "resumed_from=0|processed=1,2,3|outcome=rate_limited"
r=$($PSQL -c "SELECT encode(cp_cursor,'escape')||' '||(checkpoint->>'cursor_step') FROM headgate_job WHERE ulid='cur1';")
chk "§5.7 cursor: ...and its position is DURABLE in cp_cursor, tagged with the owning step" \
    "$r" '{"page":3} scan'
r=$($PSQL -c "SELECT state||'/'||attempt FROM headgate_job WHERE ulid='cur1';")
chk "§5.7 cursor: ...a released loop consumes no attempt (the cursor is progress, not failure)" \
    "$r" "available/0"
r=$($H cursor queues=curq pages=6)
chk "§5.7 cursor: the resume starts AT the cursor and never re-runs a completed page" \
    "$r" "resumed_from=3|processed=4,5,6|outcome=success"
r=$($PSQL -c "SELECT state||'/'||coalesce(encode(cp_cursor,'escape'),'-') FROM headgate_job WHERE ulid='cur1';")
chk "§5.7 cursor: ...and a finished cursor step clears the cursor behind it" "$r" "completed/-"

# The fence half, which is invariant 13's second sentence ("every step boundary
# re-verifies the fence") on the cursor path. An operator cancels the job mid-loop, which
# clears the lease; the NEXT set_cursor must be refused and the handler must stop THERE.
reset_pg
$H enqueue count=1 prefix=stl queue=curq fp=fpstl sched=1000 retention=86400000 payload='{}' >/dev/null
r=$($H cursor queues=curq pages=6 steal=2)
chk "§5.7 cursor: a cursor write is FENCE-VERIFIED — a stolen job stops at the boundary, not at page 6" \
    "$r" "resumed_from=0|processed=1,2|outcome=lease_lost"
r=$($PSQL -c "SELECT state||'/'||encode(cp_cursor,'escape') FROM headgate_job WHERE ulid='stl1';")
chk "§5.7 cursor: ...and the runtime does NOT ack a job that is no longer its own" \
    "$r" 'cancelled/{"page":2}'

# ----- §5.5 BACKLOG DERIVATIVES (round 32k) -----
# arrival_rate / drain_rate / time_to_drain_ms are computed in all four adapters and were
# asserted by NOTHING: the store-level tests read only `queue`, `paused` and `by_state`,
# and the one place GET /queues is compared — the Rust-vs-Go byte diff — EMPTIES
# `headgate_queue_counter` immediately beforehand so the rates are time-stable at 0, i.e.
# the diff was deliberately blind to the derivatives it transported. Asserted here against
# a known fixture, so the arithmetic itself is the assertion.
reset_pg
$H enqueue count=10 prefix=bd queue=bdq fp=fpbd sched=1000 retention=86400000 >/dev/null
# The enqueue moved `arrived` itself; overwrite the counter with a FIXED fixture, after.
seed "§5.5 backlog fixture: 120 arrivals and 180 completions in the current minute" \
     "DELETE FROM headgate_queue_counter WHERE queue='bdq';
      INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived, completed)
      VALUES ('bdq', (extract(epoch from clock_timestamp())*1000)::bigint/60000*60000, 120, 180);"
r=$($H qstats queue=bdq)
chk "§5.5 derivatives: 120 arrivals and 180 drains over the 60s window are 2.0 and 3.0 jobs/sec" \
    "$r" "bdq|2.000|3.000|10000"
# ...and the time-to-drain is the backlog over the NET drain rate, not over the drain rate:
# 10 waiting / (3.0 - 2.0) per sec = 10s. Changing only the backlog moves only that term.
$H enqueue count=5 prefix=bd2 queue=bdq fp=fpbd2 sched=1000 retention=86400000 >/dev/null
seed "§5.5 backlog fixture: same rates, 15 jobs waiting instead of 10" \
     "DELETE FROM headgate_queue_counter WHERE queue='bdq';
      INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived, completed)
      VALUES ('bdq', (extract(epoch from clock_timestamp())*1000)::bigint/60000*60000, 120, 180);"
r=$($H qstats queue=bdq)
chk "§5.5 derivatives: time-to-drain is backlog / (drain - arrival), so 15 jobs take 15s" \
    "$r" "bdq|2.000|3.000|15000"
# The alert condition (§5.5): arrival >= drain means the backlog never drains, and the
# answer is ABSENT rather than infinite or negative.
seed "§5.5 backlog fixture: arrivals now OUTPACE drains" \
     "UPDATE headgate_queue_counter SET arrived = 300 WHERE queue='bdq';"
r=$($H qstats queue=bdq)
chk "§5.5 derivatives: arrival >= drain has NO time-to-drain — that absence IS the alert" \
    "$r" "bdq|5.000|3.000|-"

# §5.5 AGE OF OLDEST. The timestamp is planted relative to STORE time and the adapter
# returns an AGE, not the raw timestamp. A generous band absorbs the harness round-trip
# while still failing a caller-clock value, seconds-vs-ms value, newest-row lookup, or 0.
reset_pg
old_at=$($PSQL -c "SELECT (extract(epoch from clock_timestamp())*1000)::bigint - 5000;")
$H enqueue count=1 prefix=age queue=ageq fp=fpage sched="$old_at" retention=86400000 >/dev/null
age=$($H qstats queue=ageq age=1 | awk -F'|' '{print $5}')
age_band="out-of-band:$age"
case "$age" in
  ''|*[!0-9]*) ;;
  *) if [ "$age" -ge 4000 ] && [ "$age" -le 15000 ]; then age_band=in-band; fi ;;
esac
chk "§5.5 age-of-oldest: Postgres returns the store-clock AGE of the oldest available job" \
    "$age_band" "in-band"
seed "§5.5 age-of-oldest empty queue exists without an available job" \
     "INSERT INTO headgate_queue_state (queue, paused, weight) VALUES ('age-empty', false, 1);"
r=$($H qstats queue=age-empty age=1)
chk "§5.5 age-of-oldest: an empty queue reports no age, never zero-age evidence" \
    "$r" "age-empty|0.000|0.000|-|-"

# §5.5 QUIET GROUPS. One partition owns 9 in-flight jobs while its peers own 1 and 2;
# the pure threshold test pins WHY that is noisy, and this live fixture proves all four
# metrics remove it. The noisy partition also holds the oldest jobs, so age is the
# discriminating signal rather than a decorative copy of the queue-wide value.
reset_pg
metric_now=$($PSQL -c "SELECT (extract(epoch from clock_timestamp())*1000)::bigint;")
$H enqueue count=5 prefix=qgn queue=qgq partition=noisy fp=qgnf sched=$((metric_now-20000)) retention=86400000 >/dev/null
$H enqueue count=2 prefix=qga queue=qgq partition=quiet-a fp=qgaf sched=$((metric_now-5000)) retention=86400000 >/dev/null
$H enqueue count=2 prefix=qgb queue=qgq partition=quiet-b fp=qgbf sched=$((metric_now-4000)) retention=86400000 >/dev/null
seed "§5.5 quiet-group fixture: one in-flight-skewed tenant and fixed per-partition rates" \
     "INSERT INTO headgate_inflight(queue, partition_key, n) VALUES
        ('qgq','noisy',9),('qgq','quiet-a',1),('qgq','quiet-b',2)
      ON CONFLICT (queue, partition_key) DO UPDATE SET n = EXCLUDED.n;
      UPDATE headgate_partition_counter
      SET arrived = CASE partition_key WHEN 'noisy' THEN 600 ELSE 60 END,
          completed = CASE partition_key WHEN 'noisy' THEN 0 ELSE 120 END
      WHERE queue = 'qgq';"
r=$($H qstats queue=qgq quiet=1)
quiet_contract=$(echo "$r" | awk -F'|' '{print $6"|"$7"|"$8"|"$10"|"$11}')
chk "§5.5 quiet groups: rates and time-to-drain exclude the noisy partition, visibly" \
    "$quiet_contract" "2.000|4.000|2000|1|false"
overall_age=$(echo "$r" | awk -F'|' '{print $5}'); quiet_age=$(echo "$r" | awk -F'|' '{print $9}')
quiet_age_contract="bad:$overall_age/$quiet_age"
case "$overall_age:$quiet_age" in
  *[!0-9:]*|:*) ;;
  *) if [ "$quiet_age" -ge 3000 ] && [ $((overall_age - quiet_age)) -ge 9000 ]; then
       quiet_age_contract=filtered-oldest
     fi ;;
esac
chk "§5.5 quiet groups: oldest age ignores the noisy tenant's much older jobs" \
    "$quiet_age_contract" "filtered-oldest"
seed "§5.5 quiet-group control: balanced in-flight work makes every partition quiet" \
     "UPDATE headgate_inflight SET n = 3 WHERE queue = 'qgq';"
r=$($H qstats queue=qgq quiet=1)
balanced=$(echo "$r" | awk -F'|' '{print $6"|"$7"|"$8"|"$10}')
chk "§5.5 quiet groups: balanced busy tenants are not silently filtered" \
    "$balanced" "12.000|4.000|-|0"

echo "== Redis =="
$RED flushall >/dev/null
for i in $(seq 1 20); do $RED hset hg:job:j$i fingerprint fp rate_class stripe partition_key p state available >/dev/null; $RED zadd hg:pending:default:p $i j$i >/dev/null; done
$RED sadd hg:parts:default p >/dev/null
$RED hset hg:rate:stripe tokens 5 burst 5 limit 5 window 1000 refilled 1000 >/dev/null
n=$($RED --eval crates/headgate-redis/lua/admit.lua hg , "default" 100 1000 30000 w1 L1 1000 | grep -c .)
chk "fleet rate limit caps at bucket size" "$n" "5"
chk "lease written for every claim" "$($RED zcard hg:lease)" "5"

$RED flushall >/dev/null
for i in $(seq 1 5000); do echo "hset hg:job:n$i fingerprint fp partition_key noisy state available"; echo "zadd hg:pending:default:noisy $i n$i"; done | $RED --pipe >/dev/null 2>&1
for i in 1 2 3; do $RED hset hg:job:a$i fingerprint fp partition_key A state available >/dev/null; $RED zadd hg:pending:default:A $i a$i >/dev/null
                  $RED hset hg:job:b$i fingerprint fp partition_key B state available >/dev/null; $RED zadd hg:pending:default:B $i b$i >/dev/null; done
$RED sadd hg:parts:default noisy A B >/dev/null
np=$($RED --eval crates/headgate-redis/lua/admit.lua hg , "default" 9 1000 30000 w1 L1 3 | sed 's/[0-9]*$//' | sort -u | grep -c .)
chk "fairness spans partitions under a 5000-job flood" "$np" "3"

$RED flushall >/dev/null
for i in 1 2 3 4 5; do $RED hset hg:job:bomb$i fingerprint fp-BOMB partition_key p state available >/dev/null; $RED zadd hg:pending:default:p $i bomb$i >/dev/null
                       $RED hset hg:job:ok$i fingerprint fp-OK partition_key p state available >/dev/null; $RED zadd hg:pending:default:p $((10+i)) ok$i >/dev/null; done
$RED sadd hg:parts:default p >/dev/null; $RED sadd hg:quarantine fp-BOMB >/dev/null
# The file's own preflight comment names this exact shape: `grep -c bomb` on NOTHING
# returns 0, which is precisely the value this assertion wants. A dead server was ruled
# out by the ping; a broken admit.lua was not. So the whole admitted set is captured and
# the CLEAN half is asserted first — five `ok` rows must come back before "no bombs"
# means anything.
qadmitted=$($RED --eval crates/headgate-redis/lua/admit.lua hg , "default" 100 1000 30000 w1 L1 1000)
oks=$(echo "$qadmitted" | grep -c ok)
bombs=$(echo "$qadmitted" | grep -c bomb)
chk "the gate admits the five NON-quarantined siblings" "$oks" "5"
chk0 "quarantined fingerprint never admitted" "$bombs" "0" \
     "...witness: the gate returned rows at all (grep -c on nothing is also 0)" "$oks"

# ----- round 32f: THE HOISTED QUARANTINE PROBE. admit.lua used to run one SISMEMBER per
# CANDIDATE; it now runs one O(1) SCARD (lazily, on the first candidate) and skips the
# per-candidate probe only while the set is EMPTY, which is sound because the set is
# written by reclaim.lua and admin.lua and never by this script, and the script is the
# atomic unit — so an empty set makes every SISMEMBER false by construction.
# The guard has exactly two branches and BOTH are pinned against the same fixture, because
# "admission is unchanged" is two statements: the empty-set branch must admit everything
# the probe would have, and the non-empty branch must still reject what the probe rejected.
qseed(){ $RED flushall >/dev/null
  for p in A B C; do for i in 1 2 3 4; do
    $RED hset hg:job:q$p$i fingerprint qfp-$p$i partition_key $p state available >/dev/null
    $RED zadd hg:pending:default:$p $i q$p$i >/dev/null
  done; done
  $RED sadd hg:parts:default A B C >/dev/null; }
qadmit(){ $RED --eval crates/headgate-redis/lua/admit.lua hg , "default" 6 1000 30000 w1 LQ 2 | sort | tr '\n' ','; }
qseed;                                              empty_set=$(qadmit)
chk "hoist: EMPTY quarantine admits the full per-partition fair share" "$empty_set" "qA1,qA2,qB1,qB2,qC1,qC2,"
# The same fixture with a NON-EMPTY set holding fingerprints NO candidate carries. The
# guard is now false, so every candidate is probed individually — and the answer must not
# move. A guard that leaked into this branch would silently reject everything.
qseed; $RED sadd hg:quarantine qfp-ZZ1 qfp-ZZ2 qfp-ZZ3 >/dev/null
chk "hoist: a NON-EMPTY set of unrelated fingerprints admits exactly the same jobs" "$(qadmit)" "$empty_set"
# ...and the non-empty branch still REJECTS. Head of every partition quarantined: the draw
# is unchanged (2/partition) so each partition offers {1,2} and only {2} may run. A guard
# that short-circuited a non-empty set would admit all six here.
qseed; $RED sadd hg:quarantine qfp-A1 qfp-B1 qfp-C1 >/dev/null
chk "hoist: ...and a NON-EMPTY set still excludes exactly the quarantined heads" "$(qadmit)" "qA2,qB2,qC2,"

echo "== Redis store (through the Rust store) =="
cargo build -q -p headgate-redis --bin hg-redis-harness || { echo "FATAL: redis harness build failed"; exit 2; }
export HG_REDIS="redis://127.0.0.1:$RP" HG_REDIS_PREFIX=hg
HR=target/debug/hg-redis-harness
force_expire(){ $RED hset hg:job:$1 lease_expires_at_ms 0 >/dev/null; $RED zadd hg:lease XX CH 0 $1 >/dev/null; }

$RED flushall >/dev/null
$RED hset hg:rate:stripe tokens 5 burst 5 limit 5 window 1000 refilled 1000 >/dev/null
$HR enqueue count=20 prefix=u queue=default rate=stripe fp=fp sched=1000 >/dev/null
r=$($HR admit queues=default capacity=100 lease_ms=30000 worker=w1 lease=L1 quantum=1000 | wc -l)
chk "fleet rate limit caps at bucket size" "$r" "5"

# TRAP 0 ON REDIS — the same two halves as the Postgres section above, against
# `redis.call('TIME')` instead of `clock_timestamp()`. admit.lua still takes an ARGV[3]
# slot that nothing reads, so the mutation this catches is "read it again".
$RED flushall >/dev/null
$HR enqueue count=1 prefix=rt0 queue=default fp=fp sched=1000 >/dev/null
$HR admit queues=default capacity=1 lease_ms=30000 worker=w1 lease=LRT0 quantum=1000 >/dev/null
# The store's own clock, read the way the script reads it: TIME is [seconds, microseconds].
now=$($RED time | { read sec; read usec; echo $((sec * 1000 + usec / 1000)); })
exp=$($RED hget hg:job:rt01 lease_expires_at_ms)
chk "Redis trap 0: lease_expires_at_ms is stamped from STORE time, never the calling worker's clock" \
    "$(if [ -n "$exp" ] && [ $(( exp - 30000 - now > 0 ? exp - 30000 - now : now - exp + 30000 )) -lt 5000 ]; then echo store-clock; else echo "skewed:$exp vs $now"; fi)" \
    "store-clock"
# The refill half: a bucket emptied at STORE now refills ~nothing over one spawn, and a
# whole bucket over 60 seconds of worker skew.
$RED flushall >/dev/null
now=$($RED time | { read sec; read usec; echo $((sec * 1000 + usec / 1000)); })
$RED hset hg:rate:rt0rc tokens 0 burst 5 limit 5 window 10000 refilled "$now" >/dev/null
$HR enqueue count=10 prefix=rt0r queue=default rate=rt0rc fp=fp sched=1000 >/dev/null
r=$($HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LRT0R quantum=1000 | wc -l | tr -d ' ')
chk0 "Redis trap 0: a bucket emptied at STORE now refills ~nothing — a 60s-fast worker would admit a whole second bucket" \
     "$r" "0" \
     "Redis trap 0: ...witness: ten jobs are waiting on that class, so the empty bucket is why none ran" \
     "$($RED zcard hg:pending:default:)"

# ----- PRIORITY ON REDIS: SAME CONTRACT, BOUNDED DRAW -----
# The pending zset remains scheduled-time indexed so due selection is a bounded score
# query. The Lua gate then sorts that per-partition draw by priority DESC, scheduled time,
# id before queue weighting. Priority therefore orders jobs WITHIN a queue and can never
# override queue weight (§11.3), matching both SQL gates.
$RED flushall >/dev/null
$HR enqueue count=1 prefix=pa queue=default partition=P fp=fp sched=1000 priority=0 retention=86400000 >/dev/null
$HR enqueue count=1 prefix=pb queue=default partition=P fp=fp sched=1001 priority=9 retention=86400000 >/dev/null
$HR enqueue count=1 prefix=pc queue=default partition=P fp=fp sched=1002 priority=5 retention=86400000 >/dev/null
ord=""
for i in 1 2 3; do
  ord="$ord$($HR admit queues=default capacity=1 lease_ms=30000 worker=wp lease=LRP$i quantum=1000 | cut -d'|' -f1) "
done
chk "Redis priority: the gate applies priority DESC within the queue, matching both SQL gates" \
    "$(trim "$ord")" "pb1 pc1 pa1"
# The MECHANISM, not just its effect: the pending zset remains a due-time index; priority
# is applied to the bounded candidate draw in admit.lua rather than encoded into a score.
$RED flushall >/dev/null
$HR enqueue count=1 prefix=pa queue=default partition=P fp=fp sched=1000 priority=0 retention=86400000 >/dev/null
$HR enqueue count=1 prefix=pb queue=default partition=P fp=fp sched=1001 priority=9 retention=86400000 >/dev/null
$HR enqueue count=1 prefix=pc queue=default partition=P fp=fp sched=1002 priority=5 retention=86400000 >/dev/null
chk "Redis priority: the pending zset remains scheduled_at_ms-indexed for bounded due draws" \
    "$($RED zscore hg:pending:default:P pa1),$($RED zscore hg:pending:default:P pb1),$($RED zscore hg:pending:default:P pc1)" \
    "1000,1001,1002"
# ...and the value is stored independently from that score, which is what admit.lua sorts.
chk "Redis priority: ...and the independently stored value is the ordering input" \
    "$($RED hget hg:job:pb1 priority)" "9"

$RED flushall >/dev/null
for batch in 0 1 2 3 4; do
  $HR enqueue count=1000 prefix=n$batch- queue=default partition=noisy fp=fp sched=1000 >/dev/null
done
$HR enqueue count=3    prefix=a queue=default partition=A     fp=fp sched=1000 >/dev/null
$HR enqueue count=3    prefix=b queue=default partition=B     fp=fp sched=1000 >/dev/null
r=$($HR admit queues=default capacity=9 lease_ms=30000 worker=w1 lease=L1 quantum=3 | cut -d'|' -f4 | sort -u | wc -l)
chk "fairness spans partitions under a 5000-job flood" "$r" "3"

# Round 32: the Lua gate is the semantic the other two adopted, so assert it here too —
# and assert what round 32 actually FIXED on this side. `bucket_avail` always returned
# math.huge for a missing bucket, but the spend loop then HINCRBY'd one INTO EXISTENCE
# holding nothing but tokens=-N, and the NEXT admit died reading its absent `refilled`
# field. Fail-open only works if it leaves no wreckage, so the second admit is the test.
$RED flushall >/dev/null
$HR enqueue count=20 prefix=uc queue=default rate=nosuchclass fp=fp sched=1000 retention=86400000 >/dev/null
r=$($HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LFO1 quantum=1000 | wc -l)
chk "Redis: unconfigured rate class is UNLIMITED (fail open)" "$r" "10"
chk0 "...and mints no bucket hash for it" "$($RED exists hg:rate:nosuchclass)" "0" \
     "...witness: the fail-open admit really admitted" "$r"
r=$($HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LFO2 quantum=1000 | wc -l)
chk "...and a SECOND admit still works (no half-built bucket left behind)" "$r" "10"

$RED flushall >/dev/null
$RED hset hg:rate:killed tokens 0 burst 5 limit 0 window 1000 refilled 1000 >/dev/null
$HR enqueue count=10 prefix=ks queue=default rate=killed fp=fp sched=1000 retention=86400000 >/dev/null
r=$($HR admit queues=default capacity=100 lease_ms=30000 worker=w1 lease=LKS quantum=1000 | wc -l)
chk0 "Redis: invariant-16 kill switch still admits nothing" "$r" "0" \
     "...witness: ten jobs are actually waiting on the killed class" \
     "$($RED zcard hg:pending:default:)"

# §5.3 prefetch semantics, same contract, same numbers as the SQL gates.
$RED flushall >/dev/null
for p in A B C; do $HR enqueue count=4 prefix=pf$p queue=default partition=$p fp=fp sched=1000 retention=86400000 >/dev/null; done
r=$($HR admit queues=default capacity=6 lease_ms=30000 worker=w1 lease=LP1 quantum=2 | split)
chk "Redis prefetch: capacity 6, quantum 2, 3 partitions -> 2 per partition" "$r" "2,2,2"
$RED flushall >/dev/null
for p in A B C; do $HR enqueue count=4 prefix=pf$p queue=default partition=$p fp=fp sched=1000 retention=86400000 >/dev/null; done
r=$($HR admit queues=default capacity=6 lease_ms=30000 worker=w1 lease=LP2 quantum=1000 | split)
chk "Redis prefetch: a non-binding quantum lets one partition fill the batch" "$r" "4,2"

# INVARIANT 11 ON REDIS. admit.lua's `deficit(queue, part)` returns `stored + quantum` and
# is used BOTH as the ZRANGEBYSCORE draw bound and as the per-partition admission cap, so
# the same "credit charged but never redeemable" mutation lives in one function there.
$RED flushall >/dev/null
$HR enqueue count=6 prefix=rwc queue=default partition=A fp=fp sched=1000 >/dev/null
r=$($HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LRW0 quantum=1 | wc -l | tr -d ' ')
chk "Redis invariant 11: with NO accrued credit one quantum is all a partition draws (the contrast)" "$r" "1"
$RED flushall >/dev/null
$HR enqueue count=6 prefix=rwc queue=default partition=A fp=fp sched=1000 >/dev/null
$RED hset hg:deficit:default A 3 >/dev/null
r=$($HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LRW1 quantum=1 | wc -l | tr -d ' ')
chk "Redis invariant 11: 3 rounds of accrued credit are SPENT on the next admit, never idled" "$r" "4"

$RED flushall >/dev/null
$HR enqueue count=1 prefix=e queue=default sched=1000 retention=0        >/dev/null
$HR enqueue count=2 prefix=k queue=default sched=1000 retention=86400000 >/dev/null
$HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LA quantum=1000 >/dev/null
$HR ack job=e1 lease=LA fence=1 outcome=success >/dev/null
chk0 "ephemeral job (retention 0) is deleted on success" "$($RED exists hg:job:e1)" "0" \
     "...witness: the retained siblings from the same batch ARE there" \
     "$(( $($RED exists hg:job:k1) + $($RED exists hg:job:k2) ))"
$HR ack job=k1 lease=LA fence=1 outcome=success >/dev/null
chk "retained job completes on success" "$($RED hget hg:job:k1 state)" "completed"
# §4.6 retention sweep, same contract as the SQL backend (ret zset scored by due time).
$HR enqueue count=1 prefix=rt queue=default sched=1000 retention=1 >/dev/null
$HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LT quantum=1000 >/dev/null
$HR ack job=rt1 lease=LT fence=1 outcome=success >/dev/null
sleep 0.05
$HR evict >/dev/null
r="$($RED exists hg:job:rt1)|$($RED exists hg:job:k1)"
chk "retention sweep evicts lapsed, keeps retained" "$r" "0|1"
$HR ack job=k2 lease=LA fence=1 outcome=retry err=boom logs=opened-conn,got-500 >/dev/null
r=$($RED hget hg:job:k2 errors | grep -c 'got-500')
chk "per-attempt logs land inside the attempt's entry" "$r" "1"
r="$($RED hget hg:job:k2 state)|$($RED hget hg:job:k2 attempt)|$($RED hget hg:job:k2 crash_attempt)"
chk "returned error retries: attempt=1, crash_attempt=0" "$r" "retryable|1|0"
# INVARIANT 10 ON REDIS. `rate_limited` was acked exactly ONCE in this whole file, on the
# Postgres side, and never here at all — so the Lua ack arm's counter behaviour had no
# coverage on either language. Same four fields as the SQL assertion, so the two gates are
# held to one contract rather than two.
$HR enqueue count=1 prefix=rl queue=default sched=1000 retention=86400000 >/dev/null
$HR admit queues=default capacity=1 lease_ms=30000 worker=w1 lease=LRL quantum=1000 >/dev/null
chk "invariant 10: ...the job really was claimed first (a rate_limited ack of nothing is not an assertion)" \
    "$($RED hget hg:job:rl1 state)" "running"
$HR ack job=rl1 lease=LRL fence=1 outcome=rate_limited >/dev/null
r="$($RED hget hg:job:rl1 state)|$($RED hget hg:job:rl1 attempt)|$($RED hget hg:job:rl1 crash_attempt)|$($RED hget hg:job:rl1 errors)"
chk "Redis invariant 10: rate_limited re-queues consuming NO attempt, NO crash, and writing NO failure" \
    "$r" "available|0|0|[]"
r=$($HR ack job=k2 lease=LA fence=1 outcome=success 2>&1; echo "rc=$?")
chk "ack after the lease is gone is rejected, never a no-op" "${r##*rc=}" "1"

$RED flushall >/dev/null
$HR enqueue count=2 prefix=r queue=default sched=1000 retention=86400000 >/dev/null
$HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LR quantum=1000 >/dev/null
force_expire r1
rec=$($HR reclaim | cut -d'|' -f1)
chk "expired lease is reclaimed" "$rec" "r1"
r="$($RED hget hg:job:r1 state)|$($RED hget hg:job:r1 attempt)|$($RED hget hg:job:r1 crash_attempt)"
chk "reclaim is LeaseLost, not Retry: attempt=0, crash_attempt=1" "$r" "retryable|0|1"
lost=$($HR renew lease_ms=30000 refs=r1:LR:1,r2:LR:1)
chk "renew returns the lost lease and extends the held one" "$lost" "r1"

# §5.2 a crash-suspect yields its queue position — the SAME contract on Redis, reached a
# different way: reclaim.lua re-scores the `pending:{queue}:{part}` zset (the gate's whole
# ordering key here) with `now + backoff` off `redis.call('TIME')`, and re-SADDs the
# partition so a reclaimed job can never leave its partition unlisted.
$RED flushall >/dev/null
$HR enqueue count=1 prefix=hola queue=default partition=hol fp=fp sched=1000 retention=86400000 >/dev/null
$HR enqueue count=1 prefix=holb queue=default partition=hol fp=fp sched=1001 retention=86400000 >/dev/null
$HR enqueue count=1 prefix=holc queue=default partition=hol fp=fp sched=1002 retention=86400000 >/dev/null
r=$($HR admit queues=default capacity=1 lease_ms=30000 worker=w1 lease=LH1 quantum=1 | cut -d'|' -f1)
chk "Redis head-of-line: the draw takes the partition's oldest job first" "$r" "hola1"
force_expire hola1
$HR reclaim >/dev/null
r=$(awk -v a="$($RED zscore hg:pending:default:hol hola1)" \
        -v c="$($RED zscore hg:pending:default:hol holc1)" 'BEGIN{print (a>c)?"t":"f"}')
chk "Redis: reclaim re-scores the suspect BEHIND its siblings in the pending zset" "$r" "t"
chk "...and the partition stays listed (a reclaim must never orphan one)" "$($RED sismember hg:parts:default hol)" "1"
sleep 1.2
$HR promote >/dev/null
r=$($HR admit queues=default capacity=2 lease_ms=30000 worker=w1 lease=LH2 quantum=10 | idset)
chk "Redis: the next admit yields B and C, never the suspect" "$r" "holb1,holc1"
r=$($HR admit queues=default capacity=2 lease_ms=30000 worker=w1 lease=LH3 quantum=10 | idset)
chk "Redis: ...and the suspect follows them, not lost" "$r" "hola1"

$RED flushall >/dev/null
$HR enqueue count=1 prefix=bomb fp=fp-BOMB sched=1000 retention=86400000 >/dev/null
for i in 1 2 3; do
  $RED hset hg:job:bomb1 scheduled_at_ms 0 >/dev/null
  $RED zadd hg:pending:default: 0 bomb1 >/dev/null
  $HR promote >/dev/null
  $HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LB$i quantum=1000 >/dev/null
  force_expire bomb1
  $HR reclaim >/dev/null
done
r="$($RED hget hg:job:bomb1 state)|$($RED hget hg:job:bomb1 crash_attempt)"
chk "third crash quarantines the fingerprint" "$r" "quarantined|3"
r=$($HR enqueue count=1 prefix=again fp=fp-BOMB 2>&1 | grep -c 'quarantined')
chk "enqueue of a quarantined fingerprint is rejected" "$r" "1"

# ----- the §10 Inspect surface over Redis (round 16) -----
r=$($HR counts queue=default | grep quarantined)
chk "inspect counts are exact zset cardinalities" "$r" "quarantined=1"
$HR enqueue count=1 prefix=insp queue=default sched=1000 retention=86400000 >/dev/null
r=$($HR explain job=insp1)
chk "explain: waiting job is admissible" "$r" "admissible=true blocked_by=none"
$HR pause queue=default >/dev/null
r=$($HR explain job=insp1)
chk "explain: paused queue blocks, no self-clearing ETA" "$r" "admissible=false blocked_by=queue_paused"
$HR pause queue=default paused=false >/dev/null
r=$($HR quarantine-release fp=fp-BOMB)
chk "quarantine release frees the parked job" "$r" "1"
r=$($HR counts queue=default | grep available)
chk "released + waiting jobs both count as available" "$r" "available=2"

$HR enqueue count=1 prefix=uq1- unique=K1 sched=1000 >/dev/null
r=$($HR enqueue count=1 prefix=uq2- unique=K1 sched=1000 2>&1)
chk "duplicate unique key returns the existing id" "$r" "ERR duplicate unique key; existing job uq1-1"
$HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LU quantum=1000 >/dev/null
$HR ack job=uq1-1 lease=LU fence=1 outcome=success >/dev/null
r=$($HR enqueue count=1 prefix=uq3- unique=K1 sched=1000 2>&1)
chk "lifecycle unique key releases on terminal state" "$r" "1"
$HR enqueue count=1 prefix=th1- unique=K2 window=60000 sched=1000 retention=86400000 >/dev/null
r=$($HR enqueue count=1 prefix=th2- unique=K2 window=60000 sched=1000 2>&1)
chk "throttle unique key blocks within the window" "$r" "ERR duplicate unique key; existing job th1-1"
# Effective uniqueness keys are deliberately binary and versioned. Keep the test at
# the public raw-key suffix while locating the one matching internal key inside Redis;
# a shell variable cannot carry the NUL bytes in the scoped namespace.
r_unique_count(){ $RED eval "return #redis.call('KEYS', ARGV[1])" 0 "hg:$1:*$2"; }
r_unique_pttl(){ $RED eval "local k=redis.call('KEYS', ARGV[1]); if #k ~= 1 then return -100-#k end; return redis.call('PTTL', k[1])" 0 "hg:$1:*$2"; }
r_unique_pexpire(){ $RED eval "local k=redis.call('KEYS', ARGV[1]); if #k ~= 1 then return 0 end; return redis.call('PEXPIRE', k[1], ARGV[2])" 0 "hg:$1:*$2" "$3"; }
r_unique_delete(){ $RED eval "local k=redis.call('KEYS', ARGV[1]); if #k == 0 then return 0 end; return redis.call('DEL', unpack(k))" 0 "hg:$1:*$2"; }
r_unique_pexpire uniquet K2 1 >/dev/null; sleep 0.05   # window over (store-clock TTL)
r=$($HR enqueue count=1 prefix=th3- unique=K2 window=60000 sched=1000 2>&1)
chk "throttle unique key releases when the window ends" "$r" "1"

# ================== ROUND 32L: THE MUTATION SWEEP'S REDIS HALF ==================
# The mirror of the Postgres block in the lifecycle section — same holes, same mutations,
# and in one case a STRONGER assertion: `ack.lua` computes the retry backoff with no
# jitter at all, so where Postgres can only pin non-adjacent bands, Redis pins the exact
# millisecond. Dedicated queues and key names throughout, so nothing here perturbs the
# `default` fixtures the sections above and below share.

# --- Unique/dedup, hole 1: a LIFECYCLE key must survive a RETRY. -------------------
$HR enqueue count=1 prefix=ruqr1- queue=ruqholdq unique=RKRET sched=1000 max_attempts=9 retention=86400000 >/dev/null
f=$($HR admit queues=ruqholdq capacity=1 lease_ms=30000 worker=w1 lease=RLRET quantum=1000 | cut -d'|' -f3)
$HR ack job=ruqr1-1 lease=RLRET fence="$f" outcome=retry err=boom >/dev/null
chk "Redis 32l unique: the holder really is RETRYABLE, not terminal (the fixture landed)" \
    "$($RED hget hg:job:ruqr1-1 state)" "retryable"
r=$($HR enqueue count=1 prefix=ruqr2- queue=ruqholdq unique=RKRET sched=1000 2>&1)
chk "Redis 32l unique: a LIFECYCLE key is STILL HELD while its holder is retryable — a retry is not a release" \
    "$r" "ERR duplicate unique key; existing job ruqr1-1"

# --- Unique/dedup, hole 2: a THROTTLE window must survive COMPLETION. --------------
$HR enqueue count=1 prefix=rtsv1- queue=rtsvq unique=RKTSV window=600000 sched=1000 retention=86400000 >/dev/null
f=$($HR admit queues=rtsvq capacity=1 lease_ms=30000 worker=w1 lease=RLTSV quantum=1000 | cut -d'|' -f3)
$HR ack job=rtsv1-1 lease=RLTSV fence="$f" outcome=success >/dev/null
chk "Redis 32l unique: the throttle holder really COMPLETED (the fixture landed)" \
    "$($RED hget hg:job:rtsv1-1 state)" "completed"
r=$($HR enqueue count=1 prefix=rtsv2- queue=rtsvq unique=RKTSV window=600000 sched=1000 2>&1)
chk "Redis 32l unique: a THROTTLE window SURVIVES completion — only the CLOCK releases it, never the job's fate" \
    "$r" "ERR duplicate unique key; existing job rtsv1-1"

# --- Fencing token: the fence must be a TERM, not decoration. ----------------------
$HR enqueue count=1 prefix=rfen1- queue=rfenq sched=1000 retention=86400000 >/dev/null
f=$($HR admit queues=rfenq capacity=1 lease_ms=30000 worker=w1 lease=RLFEN quantum=1000 | cut -d'|' -f3)
r=$($HR ack job=rfen1-1 lease=RLFEN fence=$((f + 1)) outcome=success 2>&1)
chk "Redis 32l fence: an ack with the RIGHT lease id but a STALE fence is REJECTED — the fence is a term, not decoration" \
    "$r" "ERR lease no longer held for job rfen1-1; stop work immediately"
chk "Redis 32l fence: ...and the job is untouched, still running under its real holder" \
    "$($RED hget hg:job:rfen1-1 state)" "running"
r=$($HR ack job=rfen1-1 lease=RLFEN fence="$f" outcome=success 2>&1)
chk "Redis 32l fence: ...control: the SAME ack with the REAL fence succeeds, so it was the fence that refused" \
    "$r" "ok"

# --- Retries + backoff: EXACT, because ack.lua adds no jitter. ---------------------
# `attempt` is seeded on the hash before the ack — it is the INPUT to the formula. The
# expected values are the formula's own: min(cap, base * 2^attempt) with base=1000 and
# cap=3600000, i.e. 1000, 8000 and the clamp. A constant-base mutation leaves all three at
# 1000; an uncapped one puts attempt 20 at 1048576000. Both die here.
rdelay(){ local sched now
  sched=$($RED hget hg:job:"$1" scheduled_at_ms)
  now=$($RED time | { read -r sec; read -r usec; echo $((sec * 1000 + usec / 1000)); })
  echo $((sched - now)); }
rband(){ if [ "${1:-0}" -ge "$2" ] && [ "${1:-0}" -le "$3" ]; then echo in-band; else echo "delay=${1:-none}"; fi; }
for a in 0 3 20; do
  $HR enqueue count=1 prefix=rbk$a- queue=rbkq sched=1000 max_attempts=99 retention=86400000 >/dev/null
  $RED hset hg:job:rbk$a-1 attempt "$a" >/dev/null
  f=$($HR admit queues=rbkq capacity=1 lease_ms=60000 worker=w1 lease=RLBK$a quantum=1000 | cut -d'|' -f3)
  $HR ack job=rbk$a-1 lease=RLBK$a fence="$f" outcome=retry err=boom >/dev/null
done
chk "Redis 32l backoff: attempt 0 retries at EXACTLY one base period (1000ms, no jitter on this gate)" \
    "$(rband "$(rdelay rbk0-1)" 800 1000)" "in-band"
chk "Redis 32l backoff: attempt 3 retries at EXACTLY 8x — base * 2^attempt, not linear and not constant" \
    "$(rband "$(rdelay rbk3-1)" 7800 8000)" "in-band"
chk "Redis 32l backoff: attempt 20 is CLAMPED at retry_cap_ms (3600000), never 2^20 base periods" \
    "$(rband "$(rdelay rbk20-1)" 3599800 3600000)" "in-band"

# --- Pause / resume: RESUME, asserted for effect. ---------------------------------
# The row's ✅ rested on `explain: paused queue blocks`; every RESUME in this file was
# unasserted setup, and a `set_queue_paused(q, false)` no-op was caught only as collateral
# damage three sections later. Here resume is its own assertion, pause is the control, and
# the claim path is checked as well as the explain.
$HR enqueue count=1 prefix=rpz1- queue=rpzq sched=1000 retention=86400000 >/dev/null
$HR pause queue=rpzq >/dev/null
chk "Redis 32l pause: a paused queue blocks the job at the gate (the control)" \
    "$($HR explain job=rpz1-1)" "admissible=false blocked_by=queue_paused"
$HR pause queue=rpzq paused=false >/dev/null
chk "Redis 32l resume: ...and RESUME really un-pauses it — the same job, the same gate, now admissible" \
    "$($HR explain job=rpz1-1)" "admissible=true blocked_by=none"
chk "Redis 32l resume: ...and the gate actually yields it, so resume reaches the CLAIM and not only the explain" \
    "$($HR admit queues=rpzq capacity=10 lease_ms=30000 worker=w1 lease=RLPZ quantum=1000 | wc -l | tr -d ' ')" "1"
# ================== END ROUND 32L, REDIS HALF ==================

# INVARIANT 4 ON REDIS — the same sub-second window, read off the store's own TTL.
# THROTTLE writes {p}:uniquet:{key} with PX = window; LIFECYCLE writes {p}:unique:{key}
# with no expiry at all. A window floored to whole seconds therefore does not merely
# shorten the lock, it moves the key to the OTHER namespace and makes it permanent, which
# is exactly asynq's stranded-forever shape. PTTL answers both questions at once: -2 is
# "no such key" (floored to lifecycle) and a value above the window is a rounded-UP one.
r_unique_delete uniquet KSUB1 >/dev/null
r_unique_delete unique KSUB1 >/dev/null
$HR enqueue count=1 prefix=rsw1- unique=KSUB1 window=500 sched=1000 retention=86400000 >/dev/null
t=$(r_unique_pttl uniquet KSUB1)
# 400ms of slack for one redis-cli round trip; the discriminating values (-2 for absent,
# ~1000 for rounded up) are both far outside it.
chk "Redis invariant 4: a 500ms unique window is a sub-second THROTTLE TTL, never floored into a lifecycle key" \
    "$(if [ "${t:-0}" -gt 100 ] && [ "${t:-0}" -le 500 ]; then echo "throttle 100<pttl<=500"; else echo "pttl=$t"; fi)" \
    "throttle 100<pttl<=500"
chk0 "Redis invariant 4: ...and it wrote NO permanent lifecycle key for the same name" \
     "$(r_unique_count unique KSUB1)" "0" \
     "Redis invariant 4: ...witness: the throttle key really is there to have been mis-filed" \
     "$(r_unique_count uniquet KSUB1)"
r=$($HR enqueue count=1 prefix=rsw3- unique=KSUB3 window=-1 sched=1000 2>&1)
chk "Redis invariant 4: a negative duration is REJECTED at the boundary, never clamped into lifecycle" \
    "$r" "ERR unique_window_ms must be >= 0"

# ----- §5.9 kind format + §4.4b strict caller-supplied id: the SAME contract on Redis.
# Here the classification is a pass inside enqueue.lua, where the script IS the
# transaction — the batch is atomic with no pre-check race window at all.
red_arrived(){ local t=0 k; for k in $($RED keys "hg:hist:$1:*"); do t=$((t + $($RED hget "$k" arrived))); done; echo $t; }
$RED flushall >/dev/null
r=$($HR enqueue count=1 prefix=bk queue=default kind='bad kind' payload='{}' fp=auto sched=1000 2>&1)
chk "Redis §5.9: the store rejects a malformed kind" "$r" "ERR $KINDMSG"
$HR enqueue count=1 prefix=idc queue=default payload='{"n":1}' fp=auto sched=1000 retention=86400000 >/dev/null
r=$($HR enqueue count=1 prefix=idc queue=default payload='{"n":1}' fp=auto sched=1000 retention=86400000 2>&1)
chk "Redis §4.4b: re-enqueue of an identical id is idempotent success" "$r" "1"
r=$($HR counts queue=default | grep available)
chk "Redis §4.4b: ...and the job is NOT duplicated" "$r" "available=1"
chk "Redis §4.4b: ...and no counter moved (no silent re-write)" "$(red_arrived default)" "1"
r=$($HR enqueue count=1 prefix=idc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "Redis §4.4b: same id, DIFFERENT content is a typed conflict" "$r" "ERR id conflict: job idc1"
r=$($HR enqueue count=2 prefix=idc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "Redis §4.4b: a conflict rejects the WHOLE batch, naming the offender" "$r" "ERR id conflict: job idc1"
chk0 "Redis §4.4b: ...and the clean sibling in that batch is not written" "$($RED exists hg:job:idc2)" "0" \
     "...witness: the offending row idc1 is still there (the keyspace is not simply empty)" \
     "$($RED exists hg:job:idc1)"
$HR admit queues=default capacity=10 lease_ms=30000 worker=w1 lease=LIDC quantum=1000 >/dev/null
$HR ack job=idc1 lease=LIDC fence=1 outcome=success >/dev/null
r=$($HR enqueue count=1 prefix=idc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "Redis §4.4b: a COMPLETED row still owns its id (reuse follows eviction)" "$r" "ERR id conflict: job idc1"
$RED flushall >/dev/null

$RED del hg:duty:reclaimer >/dev/null
won=$($HR duty name=reclaimer holder=w1 lease_ms=60000)
chk "duty lease: first claimer wins" "$won" "true"
r=$($HR duty name=reclaimer holder=w2 lease_ms=60000)
chk0 "duty lease: second claimer is refused" "$r" "false" \
     "...witness: the first claim succeeded, so `false` means CONTENTION" "$won"
$HR duty-release name=reclaimer holder=w1 >/dev/null
r=$($HR duty name=reclaimer holder=w2 lease_ms=60000)
chk "duty lease: released duty is claimable immediately" "$r" "true"
$HR duty-release name=reclaimer holder=w2 >/dev/null  # hygiene: leave no duty held

# ============================ ROUND 32K, ON REDIS ============================
# The same four unexercised capabilities, against the Lua gate. Redis is not a formality
# here: `checkpoint.lua`'s cursor branch (`HSET jk cp_cursor`) had never been reached by
# ANYTHING in either language, and the Redis derivatives come from the `hist:` hashes
# rather than from a SQL counter table, so they are a different computation with the same
# contract.
# =============================================================================

# §4.7 the WITHHOLDING half of delay/scheduled, on the Lua gate.
$RED flushall >/dev/null
$HR enqueue count=1 prefix=dlyA queue=dly fp=fpdlya sched=1000 retention=86400000 >/dev/null
$HR enqueue count=1 prefix=dlyB queue=dly fp=fpdlyb sched=99999999999999 retention=86400000 >/dev/null
r=$($HR admit queues=dly capacity=10 lease_ms=30000 worker=wd lease=LDLY quantum=1000 | idset)
chk "Redis §4.7 delay: the gate draws the DUE job and WITHHOLDS the not-yet-due sibling" "$r" "dlyA1"
r=$($RED hget hg:job:dlyB1 state)
chk "Redis §4.7 delay: ...and it is withheld in \`scheduled\`, not merely unlucky in the draw" "$r" "scheduled"
# Make it due the way the store would: the hash field, the `sched` sweep zset AND the
# per-partition `pending` zset whose SCORE is what admit.lua's ZRANGEBYSCORE reads.
$RED hset hg:job:dlyB1 scheduled_at_ms 1 >/dev/null
$RED zadd hg:sched XX CH 1 dlyB1 >/dev/null
$RED zadd hg:pending:dly: XX CH 1 dlyB1 >/dev/null
r=$($HR promote)
chk "Redis §4.7 delay: promotion releases it once its time has come" "$r" "1"
r=$($HR admit queues=dly capacity=10 lease_ms=30000 worker=wd lease=LDLY2 quantum=1000 | idset)
chk "Redis §4.7 delay: ...and the SAME gate, same call, now yields it" "$r" "dlyB1"

# §4 per-attempt timeout and absolute deadline, through the Redis store port.
$RED flushall >/dev/null
$HR enqueue count=1 prefix=tmo queue=tmoq fp=fptmo sched=1000 retention=86400000 timeout=50 >/dev/null
r=$($HR drain queues=tmoq count=1 sleep=400)
chk "Redis §4 timeout: the runtime really drew and ran the job" "$r" "1"
r=$($RED hget hg:job:tmo1 state)/$($RED hget hg:job:tmo1 attempt)/$($RED hget hg:job:tmo1 crash_attempt)
chk "Redis §4 timeout: an attempt that outruns timeout_ms is a RETRY that CONSUMES an attempt" \
    "$r" "retryable/1/0"
r=$($RED hget hg:job:tmo1 errors | grep -c 'attempt timed out after 50ms')
chk "Redis §4 timeout: ...and the error names the timeout and its value" "$r" "1"
$HR enqueue count=1 prefix=tmok queue=tmokq fp=fptmok sched=1000 retention=86400000 >/dev/null
$HR drain queues=tmokq count=1 sleep=400 >/dev/null
r=$($RED hget hg:job:tmok1 state)/$($RED hget hg:job:tmok1 attempt)
chk "Redis §4 timeout: ...control: the same 400ms handler with NO timeout completes" "$r" "completed/0"

$RED flushall >/dev/null
$HR enqueue count=1 prefix=dln queue=dlnq fp=fpdln sched=1000 retention=86400000 deadline=1000 >/dev/null
r=$($HR drain queues=dlnq count=1)
chk "Redis §4 deadline: the runtime really drew the job" "$r" "1"
r=$($RED hget hg:job:dln1 state)/$($RED hget hg:job:dln1 attempt)
chk "Redis §4 deadline: an exceeded absolute deadline ARCHIVES and spends NO attempt" "$r" "archived/0"
$HR enqueue count=1 prefix=dlnok queue=dlnokq fp=fpdlnok sched=1000 retention=86400000 deadline=99999999999999 >/dev/null
$HR drain queues=dlnokq count=1 >/dev/null
r=$($RED hget hg:job:dlnok1 state)
chk "Redis §4 deadline: ...control: a deadline still in the future runs normally" "$r" "completed"

# §5.7 cursor iteration over Redis — the first thing ever to reach checkpoint.lua's
# cursor branch, in either language.
$RED flushall >/dev/null
$HR enqueue count=1 prefix=cur queue=curq fp=fpcur sched=1000 retention=86400000 payload='{}' >/dev/null
r=$($HR cursor queues=curq pages=6 stop=3)
chk "Redis §5.7 cursor: an interrupted resumable loop stops where it was interrupted" \
    "$r" "resumed_from=0|processed=1,2,3|outcome=rate_limited"
r=$($RED hget hg:job:cur1 cp_cursor)
chk "Redis §5.7 cursor: ...and checkpoint.lua's cursor branch really wrote it" "$r" '{"page":3}'
r=$($HR cursor queues=curq pages=6)
chk "Redis §5.7 cursor: the resume starts AT the cursor and never re-runs a completed page" \
    "$r" "resumed_from=3|processed=4,5,6|outcome=success"
r=$($RED hget hg:job:cur1 state)/$($RED hexists hg:job:cur1 cp_cursor)
chk "Redis §5.7 cursor: ...and a finished cursor step HDELs the cursor behind it" "$r" "completed/0"

$RED flushall >/dev/null
$HR enqueue count=1 prefix=stl queue=curq fp=fpstl sched=1000 retention=86400000 payload='{}' >/dev/null
r=$($HR cursor queues=curq pages=6 steal=2)
chk "Redis §5.7 cursor: a cursor write is FENCE-VERIFIED — a stolen job stops at the boundary" \
    "$r" "resumed_from=0|processed=1,2|outcome=lease_lost"
r=$($RED hget hg:job:stl1 state)/$($RED hget hg:job:stl1 cp_cursor)
chk "Redis §5.7 cursor: ...and the runtime does NOT ack a job that is no longer its own" \
    "$r" 'cancelled/{"page":2}'

# §5.5 backlog derivatives over Redis: the SAME contract from a DIFFERENT computation —
# the `hist:` hashes the history() surface reads, not a SQL counter table.
$RED flushall >/dev/null
$HR enqueue count=10 prefix=bd queue=bdq fp=fpbd sched=1000 retention=86400000 >/dev/null
bucket=$($RED time | { read sec; read usec; echo $(( (sec * 1000 + usec / 1000) / 60000 * 60000 )); })
$RED del "hg:hist:bdq:$bucket" >/dev/null
$RED hset "hg:hist:bdq:$bucket" arrived 120 completed 180 >/dev/null
r=$($HR qstats queue=bdq)
chk "Redis §5.5 derivatives: 120 arrivals and 180 drains over the 60s window are 2.0 and 3.0 jobs/sec" \
    "$r" "bdq|2.000|3.000|10000"
$HR enqueue count=5 prefix=bd2 queue=bdq fp=fpbd2 sched=1000 retention=86400000 >/dev/null
$RED del "hg:hist:bdq:$bucket" >/dev/null
$RED hset "hg:hist:bdq:$bucket" arrived 120 completed 180 >/dev/null
r=$($HR qstats queue=bdq)
chk "Redis §5.5 derivatives: time-to-drain is backlog / (drain - arrival), so 15 jobs take 15s" \
    "$r" "bdq|2.000|3.000|15000"
$RED hset "hg:hist:bdq:$bucket" arrived 300 >/dev/null
r=$($HR qstats queue=bdq)
chk "Redis §5.5 derivatives: arrival >= drain has NO time-to-drain — that absence IS the alert" \
    "$r" "bdq|5.000|3.000|-"

# The Redis implementation reads the HEAD score of idx:{queue}:available and subtracts
# redis.call(TIME)'s clock, so it has the same SLO-shaped contract without a key scan.
$RED flushall >/dev/null
redis_sec=$($RED time | sed -n '1p')
old_at=$((redis_sec * 1000 - 5000))
$HR enqueue count=1 prefix=age queue=ageq fp=fpage sched="$old_at" retention=86400000 >/dev/null
age=$($HR qstats queue=ageq age=1 | awk -F'|' '{print $5}')
age_band="out-of-band:$age"
case "$age" in
  ''|*[!0-9]*) ;;
  *) if [ "$age" -ge 4000 ] && [ "$age" -le 15000 ]; then age_band=in-band; fi ;;
esac
chk "Redis §5.5 age-of-oldest: the available zset head becomes a store-clock age" \
    "$age_band" "in-band"
$RED sadd hg:queues age-empty >/dev/null
r=$($HR qstats queue=age-empty age=1)
chk "Redis §5.5 age-of-oldest: an empty queue reports no age, never zero-age evidence" \
    "$r" "age-empty|0.000|0.000|-|-"

$RED flushall >/dev/null
metric_now=$($RED time | { read sec; read usec; echo $((sec * 1000 + usec / 1000)); })
$HR enqueue count=5 prefix=qgn queue=qgq partition=noisy fp=qgnf sched=$((metric_now-20000)) retention=86400000 >/dev/null
$HR enqueue count=2 prefix=qga queue=qgq partition=quiet-a fp=qgaf sched=$((metric_now-5000)) retention=86400000 >/dev/null
$HR enqueue count=2 prefix=qgb queue=qgq partition=quiet-b fp=qgbf sched=$((metric_now-4000)) retention=86400000 >/dev/null
# Make the quiet-side backlog internally consistent with the 1/2 in-flight fixture:
# one quiet-a and both quiet-b jobs leave pending and enter running. The noisy side is
# excluded, so its synthetic count can be larger than this deliberately small fixture.
for spec in qga1:quiet-a qgb1:quiet-b qgb2:quiet-b; do
  id=${spec%%:*}; part=${spec#*:}
  $RED zrem "hg:pending:qgq:$part" "$id" >/dev/null
  $RED zrem "hg:avail:qgq:$part" "$id" >/dev/null
  $RED zrem hg:idx:qgq:available "$id" >/dev/null
  $RED zadd hg:idx:qgq:running "$metric_now" "$id" >/dev/null
  $RED hset "hg:job:$id" state running >/dev/null
done
$RED hset hg:inflight:qgq noisy 9 quiet-a 1 quiet-b 2 >/dev/null
bucket=$((metric_now / 60000 * 60000))
$RED hset "hg:histp:qgq:noisy:$bucket" arrived 600 completed 0 >/dev/null
$RED hset "hg:histp:qgq:quiet-a:$bucket" arrived 60 completed 120 >/dev/null
$RED hset "hg:histp:qgq:quiet-b:$bucket" arrived 60 completed 120 >/dev/null
r=$($HR qstats queue=qgq quiet=1)
quiet_contract=$(echo "$r" | awk -F'|' '{print $6"|"$7"|"$8"|"$10"|"$11}')
chk "Redis §5.5 quiet groups: rates and time-to-drain exclude the noisy partition" \
    "$quiet_contract" "2.000|4.000|2000|1|false"
overall_age=$(echo "$r" | awk -F'|' '{print $5}'); quiet_age=$(echo "$r" | awk -F'|' '{print $9}')
quiet_age_contract="bad:$overall_age/$quiet_age"
case "$overall_age:$quiet_age" in
  *[!0-9:]*|:*) ;;
  *) if [ "$quiet_age" -ge 3000 ] && [ $((overall_age - quiet_age)) -ge 9000 ]; then
       quiet_age_contract=filtered-oldest
     fi ;;
esac
chk "Redis §5.5 quiet groups: per-partition available heads keep noisy depth from hiding quiet age" \
    "$quiet_age_contract" "filtered-oldest"
$RED hset hg:inflight:qgq noisy 3 quiet-a 3 quiet-b 3 >/dev/null
r=$($HR qstats queue=qgq quiet=1)
balanced=$(echo "$r" | awk -F'|' '{print $6"|"$7"|"$8"|"$10}')
chk "Redis §5.5 quiet groups: balanced busy tenants are not silently filtered" \
    "$balanced" "12.000|4.000|-|0"
$RED flushall >/dev/null

echo "== MySQL store (through the Rust store) =="
export HG_MYSQL="${HG_MYSQL:-mysql://root:hg@127.0.0.1:3307/hg}"
cargo build -q -p headgate-mysql --bin hg-mysql-harness || { echo "FATAL: mysql harness build failed"; exit 2; }
HM=target/debug/hg-mysql-harness
# Build both adapters before probing. A server can recover between optional MySQL
# sections; leaving GM inside the first successful probe made a later recovery enter the
# six-cell contracts with an unbound command path.
(cd go && go build -o ../target/debug/hg-go-mysql-harness ./driver/headgatemysql/cmd/hg-go-mysql-harness) \
  || { echo "FATAL: go mysql harness build failed"; exit 2; }
GM=target/debug/hg-go-mysql-harness
# Watchdog probe (perl alarm — macOS has no `timeout`): a WEDGED container accepts TCP
# via the docker proxy but never answers the protocol, and a hung probe is worse than a
# skipped section. Learned live: the container froze mid-suite and took the probe with it.
# A FUNCTION because round 32c added a SECOND MySQL section (the §10.1 API parity pair)
# far below this one, and two copies of a soft-skip gate is two chances to get it wrong.
mysql_up(){ perl -e 'alarm 5; exec @ARGV' -- "$HM" promote >/dev/null 2>&1; }
if mysql_up; then
  # Round 32j: the transcript's mysql_live marker is written from the PROBE, not from the
  # environment variable — HG_MYSQL is defaulted a few lines above and is therefore always
  # "set", which is exactly the shape of claim this repo keeps finding to be empty.
  printf '#\tmysql_live\tyes\n' >> "$TRANSCRIPT"
  # Fixture reset, the section's reset_pg equivalent: stray crash-loop leftovers from
  # aborted runs can QUARANTINE the shared 'fp' fingerprint, silently rejecting every
  # later enqueue in this section (found live: six assertions went vacuous at once).
  $HM sql stmt="DELETE FROM headgate_quarantine" >/dev/null
  $HM sql stmt="DELETE FROM headgate_job WHERE queue LIKE 'mys%'" >/dev/null
  # Per-run queue/id names ($$) isolate every assertion from prior container state.
  q=mys-$$
  $HM enqueue count=20 prefix=mu-$$- queue=$q rate=mys-stripe-$$ fp=fp sched=1000 retention=86400000 >/dev/null
  r=$($HM admit queues=$q capacity=100 lease_ms=600000 worker=w1 lease=ML1 quantum=1000 | grep -c '|' | tr -d ' ')
  # Round 32, maintainer decision "fail open": an UNCONFIGURED rate class is UNLIMITED on
  # every backend. This assertion used to demand 0 (the old COALESCE(b.avail, 0) SQL-gate
  # semantic) and was the recorded cross-gate divergence; it now demands 20, the Lua
  # gate's answer, on all three. A limit nobody has written is not a limit.
  chk "MySQL: unconfigured rate class is UNLIMITED (fail open, all gates)" "$r" "20"
  q2=mys2-$$
  $HM enqueue count=500 prefix=mn-$$- queue=$q2 partition=noisy fp=fp sched=1000 retention=86400000 >/dev/null
  $HM enqueue count=3 prefix=ma-$$- queue=$q2 partition=A fp=fp sched=1000 retention=86400000 >/dev/null
  $HM enqueue count=3 prefix=mb-$$- queue=$q2 partition=B fp=fp sched=1000 retention=86400000 >/dev/null
  r=$($HM admit queues=$q2 capacity=9 lease_ms=600000 worker=w1 lease=ML2 quantum=3 | cut -d'|' -f4 | sort -u | wc -l | tr -d ' ')
  chk "MySQL: fairness spans partitions under a 500-job flood" "$r" "3"
  # Invariant 16's kill switch must survive fail-open here too: a CONFIGURED class with
  # limit 0 and an empty bucket still admits nothing. Fail-open reads "no ROW", not
  # "no budget", and this is the assertion that keeps the two apart.
  qk=mysk-$$
  $HM sql stmt="INSERT INTO headgate_rate_bucket VALUES ('mys-killed-$$',0,5,0,1000,1000)" >/dev/null
  $HM enqueue count=10 prefix=mks-$$- queue=$qk rate=mys-killed-$$ fp=fp sched=1000 retention=86400000 >/dev/null
  r=$($HM admit queues=$qk capacity=100 lease_ms=600000 worker=w1 lease=MLK quantum=1000 | grep -c '|' | tr -d ' ')
  chk0 "MySQL: invariant-16 kill switch still admits nothing" "$r" "0" \
       "...witness: ten jobs are actually waiting on the killed class" \
       "$($HM dump queue=$qk | grep -c .)"
  # §5.3 prefetch semantics, same contract and same numbers as the other two gates.
  qp=mysp-$$
  for p in A B C; do $HM enqueue count=4 prefix=mpf$p-$$- queue=$qp partition=$p fp=fp sched=1000 retention=86400000 >/dev/null; done
  r=$($HM admit queues=$qp capacity=6 lease_ms=600000 worker=w1 lease=MLP1 quantum=2 | split)
  chk "MySQL prefetch: capacity 6, quantum 2, 3 partitions -> 2 per partition" "$r" "2,2,2"
  qp2=mysp2-$$
  for p in A B C; do $HM enqueue count=4 prefix=mpg$p-$$- queue=$qp2 partition=$p fp=fp sched=1000 retention=86400000 >/dev/null; done
  r=$($HM admit queues=$qp2 capacity=6 lease_ms=600000 worker=w1 lease=MLP2 quantum=1000 | split)
  chk "MySQL prefetch: a non-binding quantum lets one partition fill the batch" "$r" "4,2"
  # ----- PRIORITY ON MYSQL: THE SAME SQL-GATE ORDERING (round 32j) -----
  # `eligible.sql` orders by `priority DESC, scheduled_at_ms, id` exactly as `admit.sql`
  # does, and `headgate_job_avail_partition` carries `priority DESC` as its third column,
  # so this backend belongs on the SQL side of the divergence the Redis block pins.
  # Same fixture, same expected draw, capacity=1 three times for the same reason (the
  # claim read-back is ORDER BY id here too).
  # WRITTEN, NOT VERIFIED: no MySQL server has ever parsed this section — see
  # conformance/MYSQL_VERIFICATION.md. It compiles and it soft-skips.
  qpri=myspri-$$
  $HM enqueue count=1 prefix=mpa-$$- queue=$qpri partition=P fp=fp sched=1000 priority=0 retention=86400000 >/dev/null
  $HM enqueue count=1 prefix=mpb-$$- queue=$qpri partition=P fp=fp sched=1001 priority=9 retention=86400000 >/dev/null
  $HM enqueue count=1 prefix=mpc-$$- queue=$qpri partition=P fp=fp sched=1002 priority=5 retention=86400000 >/dev/null
  ord=""
  for i in 1 2 3; do
    ord="$ord$($HM admit queues=$qpri capacity=1 lease_ms=600000 worker=wp lease=MLPR$i quantum=1000 | cut -d'|' -f1) "
  done
  chk "MySQL priority: the SQL gate draws priority DESC first, ahead of scheduled_at_ms" \
      "$(trim "$ord")" "mpb-$$-1 mpc-$$-1 mpa-$$-1"
  chk "MySQL priority: ...and the stored column carries the non-default values (0/9/5 by id)" \
      "$($HM dump queue=$qpri | cut -d'|' -f6 | tr '\n' ',' | sed 's/,$//')" "0,9,5"

  q3=mys3-$$
  $HM enqueue count=2 prefix=mk-$$- queue=$q3 sched=1000 retention=86400000 >/dev/null
  $HM admit queues=$q3 capacity=10 lease_ms=600000 worker=w1 lease=MLA quantum=1000 >/dev/null
  $HM ack job=mk-$$-1 lease=MLA fence=1 outcome=success >/dev/null
  chk "MySQL: retained job completes on success" "$($HM state job=mk-$$-1)" "completed"
  $HM ack job=mk-$$-2 lease=MLA fence=1 outcome=retry err=boom logs=opened-conn,got-500 >/dev/null
  chk "MySQL: returned error retries" "$($HM state job=mk-$$-2)" "retryable"
  r=$($HM ack job=mk-$$-2 lease=MLA fence=1 outcome=success 2>&1; echo "rc=$?")
  chk "MySQL: ack after the lease is gone is rejected, never a no-op" "${r##*rc=}" "1"

  # §5.2 a crash-suspect yields its queue position — same contract, third gate. MySQL has
  # no cheap read-only hook for the ordering key here (`sql` is exec_drop and `dump` omits
  # store-clock columns on purpose), so the assertion is purely what the gate ADMITS.
  qh=mysh-$$
  $HM enqueue count=1 prefix=mha-$$- queue=$qh partition=hol fp=fp sched=1000 retention=86400000 >/dev/null
  $HM enqueue count=1 prefix=mhb-$$- queue=$qh partition=hol fp=fp sched=1001 retention=86400000 >/dev/null
  $HM enqueue count=1 prefix=mhc-$$- queue=$qh partition=hol fp=fp sched=1002 retention=86400000 >/dev/null
  r=$($HM admit queues=$qh capacity=1 lease_ms=600000 worker=w1 lease=MH1 quantum=1 | cut -d'|' -f1)
  chk "MySQL head-of-line: the draw takes the partition's oldest job first" "$r" "mha-$$-1"
  $HM sql stmt="UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='mha-$$-1'" >/dev/null
  $HM reclaim >/dev/null
  sleep 1.3
  $HM promote >/dev/null
  r=$($HM admit queues=$qh capacity=2 lease_ms=600000 worker=w1 lease=MH2 quantum=10 | idset)
  chk "MySQL: the next admit yields B and C, never the suspect" "$r" "mhb-$$-1,mhc-$$-1"
  r=$($HM admit queues=$qh capacity=2 lease_ms=600000 worker=w1 lease=MH3 quantum=10 | idset)
  chk "MySQL: ...and the suspect follows them, not lost" "$r" "mha-$$-1"
  # cross-store execution: the REAL runtime drains MySQL through the same drain path
  q4=mys4-$$
  $HM enqueue count=3 prefix=md-$$- queue=$q4 payload='{}' sched=1000 retention=86400000 >/dev/null
  r=$($HM drain queues=$q4 count=10)
  chk "MySQL: the Rust runtime executes (drain)" "$r" "3"
  chk "MySQL: ...and completed" "$($HM state job=md-$$-1)" "completed"

  # ----- §5.9 kind format + §4.4b strict caller-supplied id: the SAME contract on MySQL,
  # where the pre-check runs inside the explicit enqueue transaction added in batch 1.
  qi=mysi-$$
  r=$($HM enqueue count=1 prefix=mbk-$$- queue=$qi kind='bad kind' payload='{}' fp=auto sched=1000 2>&1)
  chk "MySQL §5.9: the store rejects a malformed kind" "$r" "ERR $KINDMSG"
  $HM enqueue count=1 prefix=mid-$$- queue=$qi payload='{"n":1}' fp=auto sched=1000 retention=86400000 >/dev/null
  r=$($HM enqueue count=1 prefix=mid-$$- queue=$qi payload='{"n":1}' fp=auto sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4b: re-enqueue of an identical id is idempotent success" "$r" "1"
  chk "MySQL §4.4b: ...and the job is NOT duplicated" "$($HM dump queue=$qi | grep -c .)" "1"
  r=$($HM enqueue count=1 prefix=mid-$$- queue=$qi payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4b: same id, DIFFERENT content is a typed conflict" "$r" "ERR id conflict: job mid-$$-1"
  r=$($HM enqueue count=2 prefix=mid-$$- queue=$qi payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4b: a conflict rejects the WHOLE batch, naming the offender" "$r" "ERR id conflict: job mid-$$-1"
  chk "MySQL §4.4b: ...and the clean sibling in that batch is not written" "$($HM dump queue=$qi | grep -c .)" "1"
  $HM admit queues=$qi capacity=10 lease_ms=600000 worker=w1 lease=MIDC quantum=1000 >/dev/null
  $HM ack job=mid-$$-1 lease=MIDC fence=1 outcome=success >/dev/null
  r=$($HM enqueue count=1 prefix=mid-$$- queue=$qi payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4b: a COMPLETED row still owns its id (reuse follows eviction)" "$r" "ERR id conflict: job mid-$$-1"

  # ----- §4.4 UNIQUENESS ON §6'S GENERATED COLUMNS (round 32c) -----
  # MySQL has no partial indexes, so both modes ride a generated column that is NULL
  # when the key is not held (`unique_active` / `unique_throttle`) plus a unique index
  # over it — MySQL treats NULLs as distinct, and that IS the mechanism. The corners
  # below are the ones the Postgres section covers and this backend never got; the
  # per-state matrix and the generated columns themselves are asserted in
  # crates/headgate-mysql/tests/unique.rs.
  qu=mysu-$$
  $HM enqueue count=1 prefix=muq1-$$- queue=$qu unique=MK1-$$ sched=1000 retention=86400000 >/dev/null
  r=$($HM enqueue count=1 prefix=muq2-$$- queue=$qu unique=MK1-$$ sched=1000 2>&1)
  chk "MySQL §4.4 LIFECYCLE: a duplicate unique key returns the existing id" "$r" "ERR duplicate unique key; existing job muq1-$$-1"
  # `unique_active` names FOUR live states, not one. A generated column listing the
  # wrong set passes a single-state test and leaks keys forever, so RUNNING is asserted.
  $HM admit queues=$qu capacity=10 lease_ms=600000 worker=w1 lease=MU quantum=1000 >/dev/null
  r=$($HM enqueue count=1 prefix=muq3-$$- queue=$qu unique=MK1-$$ sched=1000 2>&1)
  chk "MySQL §4.4 LIFECYCLE: ...still held while the holder is RUNNING" "$r" "ERR duplicate unique key; existing job muq1-$$-1"
  # A TERMINAL state releases it — reached here through `skip` (archived), so the
  # release is not secretly a property of the success path alone.
  $HM ack job=muq1-$$-1 lease=MU fence=1 outcome=skip >/dev/null
  chk "MySQL §4.4 LIFECYCLE: ...the holder reaches a terminal state" "$($HM state job=muq1-$$-1)" "archived"
  r=$($HM enqueue count=1 prefix=muq4-$$- queue=$qu unique=MK1-$$ sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4 LIFECYCLE: ...and the generated column releases the key there" "$r" "1"
  # The RETRYABLE arm, separately: a failed attempt must NOT release a lifecycle key.
  qv=mysv-$$
  $HM enqueue count=1 prefix=mvq1-$$- queue=$qv unique=MK2-$$ sched=1000 retention=86400000 >/dev/null
  $HM admit queues=$qv capacity=10 lease_ms=600000 worker=w1 lease=MV quantum=1000 >/dev/null
  $HM ack job=mvq1-$$-1 lease=MV fence=1 outcome=retry err=boom >/dev/null
  chk "MySQL §4.4 LIFECYCLE: a failed attempt lands RETRYABLE" "$($HM state job=mvq1-$$-1)" "retryable"
  r=$($HM enqueue count=1 prefix=mvq2-$$- queue=$qv unique=MK2-$$ sched=1000 2>&1)
  chk "MySQL §4.4 LIFECYCLE: ...and RETRYABLE still holds the key (that is the point)" "$r" "ERR duplicate unique key; existing job mvq1-$$-1"

  # THROTTLE rides `unique_throttle`, which reads unique_expires_at_ms and NEVER state —
  # so it is released by the CLOCK, lazily, by the conflicting enqueue itself.
  qt=myst-$$
  $HM enqueue count=1 prefix=mth1-$$- queue=$qt unique=MT1-$$ window=60000 sched=1000 retention=86400000 >/dev/null
  r=$($HM enqueue count=1 prefix=mth2-$$- queue=$qt unique=MT1-$$ window=60000 sched=1000 2>&1)
  chk "MySQL §4.4 THROTTLE: blocks within the window" "$r" "ERR duplicate unique key; existing job mth1-$$-1"
  $HM admit queues=$qt capacity=10 lease_ms=600000 worker=w1 lease=MT quantum=1000 >/dev/null
  $HM ack job=mth1-$$-1 lease=MT fence=1 outcome=success >/dev/null
  chk "MySQL §4.4 THROTTLE: the holder completes" "$($HM state job=mth1-$$-1)" "completed"
  r=$($HM enqueue count=1 prefix=mth3-$$- queue=$qt unique=MT1-$$ window=60000 sched=1000 2>&1)
  chk "MySQL §4.4 THROTTLE: ...and the key SURVIVES completion (unlike LIFECYCLE)" "$r" "ERR duplicate unique key; existing job mth1-$$-1"
  $HM sql stmt="UPDATE headgate_job SET unique_expires_at_ms=1 WHERE ulid='mth1-$$-1'" >/dev/null
  r=$($HM enqueue count=1 prefix=mth4-$$- queue=$qt unique=MT1-$$ window=60000 sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4 THROTTLE: an expired window is released LAZILY by the next enqueue" "$r" "1"

  # THE OPEN CORNER, recorded rather than fixed. The register's "Unique / dedup" row
  # names it on Postgres: throttle + retention_ms = 0 deletes the row — and the key
  # lives IN the row's generated column — so the window dies with it, early. MySQL
  # reaches the same place through a different index mechanism. If this assertion ever
  # flips, the SEMANTIC changed and the register row must change with it.
  qz=mysz-$$
  $HM enqueue count=1 prefix=mtz1-$$- queue=$qz unique=MTZ-$$ window=600000 sched=1000 retention=0 >/dev/null
  r=$($HM enqueue count=1 prefix=mtz2-$$- queue=$qz unique=MTZ-$$ window=600000 sched=1000 2>&1)
  chk "MySQL §4.4 THROTTLE+retention0: the 10-minute window holds while the row exists" "$r" "ERR duplicate unique key; existing job mtz1-$$-1"
  $HM admit queues=$qz capacity=10 lease_ms=600000 worker=w1 lease=MTZ quantum=1000 >/dev/null
  # `state` prints nothing for a row that does not exist — and nothing for a row that was
  # never written, and nothing if the harness errored. The state BEFORE the ack is the
  # witness: the holder has to have been running for its disappearance to mean anything.
  mtz_before=$($HM state job=mtz1-$$-1)
  chk "MySQL: ...the ephemeral holder is RUNNING before the ack" "$mtz_before" "running"
  $HM ack job=mtz1-$$-1 lease=MTZ fence=1 outcome=success >/dev/null
  chk0 "MySQL: ...the ephemeral holder is DELETED at ack (§9.5, retention 0)" "$($HM state job=mtz1-$$-1)" "" \
       "...witness: the holder existed and was running before the ack" "$mtz_before"
  r=$($HM enqueue count=1 prefix=mtz3-$$- queue=$qz unique=MTZ-$$ window=600000 sched=1000 retention=86400000 2>&1)
  chk "MySQL: ...so the window dies WITH the row, ~10 minutes early (OPEN CORNER)" "$r" "1"

  # §4.4 vs §4.4b: two contracts sharing one enqueue, and they must not blur. Duplicate
  # is opt-in, RELEASABLE uniqueness over a caller-chosen key and it names the WINNER;
  # IdConflict is the never-released primary key and names the id you asked for. The
  # classification ORDER is fixed (id first), which is what keeps the API's 409
  # reachable for a caller who also uses a unique key.
  qc=mysc-$$
  $HM enqueue count=1 prefix=muc1-$$- queue=$qc unique=MUC-$$ payload='{"n":1}' fp=auto sched=1000 retention=86400000 >/dev/null
  r=$($HM enqueue count=1 prefix=muc2-$$- queue=$qc unique=MUC-$$ payload='{"n":1}' fp=auto sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4 vs §4.4b: a DIFFERENT id with the same unique key is Duplicate" "$r" "ERR duplicate unique key; existing job muc1-$$-1"
  r=$($HM enqueue count=1 prefix=muc1-$$- queue=$qc unique=MUC-$$ payload='{"n":1}' fp=auto sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4b: ...the SAME id with the same content stays idempotent success" "$r" "1"
  r=$($HM enqueue count=1 prefix=muc1-$$- queue=$qc unique=MUC-$$ payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
  chk "MySQL §4.4b: ...and the same id with DIFFERENT content is the id conflict (classified FIRST)" "$r" "ERR id conflict: job muc1-$$-1"
  chk "MySQL §4.4: no refusal left a row behind" "$($HM dump queue=$qc | grep -c .)" "1"

  # ----- Cross-language (Go + Rust on one MySQL) -----
  export HG_MYSQL
  q5=mys5-$$
  $GM enqueue count=6 prefix=xm-$$- queue=$q5 payload='{}' fp=auto sched=1000 retention=86400000 >/dev/null
  admitted=$($HM admit queues=$q5 capacity=3 lease_ms=600000 worker=wR lease=MXL quantum=1000)
  r=$(echo "$admitted" | grep -c '|' | tr -d ' ')
  chk "MySQL: Go enqueues; Rust admits" "$r" "3"
  first=$(echo "$admitted" | head -1 | cut -d'|' -f1)
  r=$($GM ack "job=$first" lease=MXL fence=1 outcome=success)
  chk "MySQL: Go acks a lease Rust issued" "$r" "ok"
  r=$($GM drain queues=$q5 count=10)
  chk "MySQL: Rust-admitted leftovers + Go runtime drain the rest" "$r" "3"
  # NOTE: the 2 still-leased Rust claims stay running; the Go drain takes the 3 waiting.
  q6=mys6-$$
  $GM enqueue count=2 prefix=xg-$$- queue=$q6 payload='{}' fp=auto sched=1000 retention=86400000 >/dev/null
  r=$($HM drain queues=$q6 count=10)
  chk "MySQL: Go enqueues; the Rust runtime executes" "$r" "2"
  # §3.2 table diff: the same scenario driven once per language must leave identical
  # deterministic rows (one dumper — the Rust harness — reads both).
  mysql_scenario(){ # $1 = harness, $2 = queue
    $1 enqueue count=4 prefix=xs-$2- queue=$2 payload='{}' fp=auto sched=1000 retention=86400000 >/dev/null
    $1 enqueue count=1 prefix=xu-$2- unique=XK-$2 queue=$2 sched=1000 retention=86400000 >/dev/null
    $1 admit queues=$2 capacity=2 lease_ms=600000 worker=wX lease=LX quantum=1000 >/dev/null
    $1 ack job=xs-$2-1 lease=LX fence=1 outcome=success >/dev/null
    $1 ack job=xs-$2-2 lease=LX fence=1 outcome=retry err=boom >/dev/null
    $HM dump queue=$2 | sed "s/$2/QUEUE/g"
  }
  mysql_scenario "$HM" mysr-$$ > /tmp/hgx-mtable-rust.txt
  mysql_scenario "$GM" mysg-$$ > /tmp/hgx-mtable-go.txt
  lines=$(wc -l < /tmp/hgx-mtable-rust.txt)
  chk "MySQL table snapshot is non-trivial (no vacuous pass)" "$((lines >= 5))" "1"
  if cmp -s /tmp/hgx-mtable-rust.txt /tmp/hgx-mtable-go.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-mtable-rust.txt /tmp/hgx-mtable-go.txt | head -10; fi
  chk "MySQL table diff: Go-driven and Rust-driven stores match byte-for-byte" "$d" "identical"

  # ----- §10 THE GO MySQL INSPECT SURFACE (round 32c), cross-language -----
  # Until this round go/driver/headgatemysql declined InspectStore, so the §10 console,
  # the §11.2 control channel and the scheduler/operations/quarantine/retention duties
  # were compiled-but-dormant over MySQL. These assert the Go surface ANSWERS, and that
  # it answers the same as the Rust surface reading the same rows — one store, two
  # languages, the discipline the Redis section already applies to its Lua twin.
  q7=mys7-$$
  $HM enqueue count=3 prefix=mi-$$- queue=$q7 payload='{}' fp=auto sched=1000 retention=86400000 >/dev/null
  chk "MySQL: Go Inspect counts agree with Rust's over one store" \
      "$($GM counts queue=$q7 | grep available)" "$($HM counts queue=$q7 | grep available)"
  chk "MySQL: Go's explain reports the waiting job admissible" \
      "$($GM explain job=mi-$$-1)" "admissible=true blocked_by=none"
  $GM pause queue=$q7 >/dev/null
  chk "MySQL: Go pauses the queue and RUST's explain sees it (one store, two languages)" \
      "$($HM explain job=mi-$$-1)" "admissible=false blocked_by=queue_paused"
  $GM pause queue=$q7 paused=false >/dev/null
  chk "MySQL: ...and both agree again once Go resumes it" \
      "$($GM explain job=mi-$$-1)" "admissible=true blocked_by=none"
  # §5.2 the sweeper through the GO surface: a waiting SIBLING of a quarantined
  # fingerprint parks VISIBLY instead of sitting gate-excluded forever. The fingerprint
  # is $$-scoped because quarantine_sweep is GLOBAL.
  $HM sql stmt="INSERT INTO headgate_quarantine (fingerprint,kind,crash_count,quarantined_at_ms,reason) VALUES ('mfp-$$','w',3,1,'test')" >/dev/null
  $HM sql stmt="UPDATE headgate_job SET fingerprint='mfp-$$' WHERE ulid='mi-$$-2'" >/dev/null
  $GM sweep-quarantine >/dev/null
  chk "MySQL: Go's quarantine sweeper parks the waiting sibling visibly" "$($GM state job=mi-$$-2)" "quarantined"
  chk "MySQL: ...Go's operator release frees it" "$($GM quarantine-release fp=mfp-$$)" "1"
  chk "MySQL: ...and Rust reads it back available" "$($HM state job=mi-$$-2)" "available"
  $HM sql stmt="DELETE FROM headgate_quarantine WHERE fingerprint='mfp-$$'" >/dev/null

  # §5.5 AGE OF OLDEST, now against a LIVE MySQL rather than a compiled query. The
  # timestamp comes from MySQL's clock through the harness; both adapters must return an
  # age in milliseconds, and an explicitly configured empty queue must return ABSENT.
  mage=mysage-$$
  mage_empty=mysage-empty-$$
  metric_now=$($HM scalar-i64 stmt="SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)")
  $HM enqueue count=1 prefix=mage-$$- queue=$mage fp=magefp-$$ sched=$((metric_now-5000)) retention=86400000 >/dev/null
  $HM sql stmt="INSERT INTO headgate_queue_state(queue,paused) VALUES ('$mage_empty',FALSE)" >/dev/null
  rust_age=$($HM qstats queue=$mage age=1 | awk -F'|' '{print $5}')
  go_age=$($GM qstats queue=$mage age=1 | awk -F'|' '{print $5}')
  rust_age_band="out-of-band:$rust_age"
  go_age_band="out-of-band:$go_age"
  case "$rust_age" in
    ''|*[!0-9]*) ;;
    *) if [ "$rust_age" -ge 4000 ] && [ "$rust_age" -le 30000 ]; then rust_age_band=in-band; fi ;;
  esac
  case "$go_age" in
    ''|*[!0-9]*) ;;
    *) if [ "$go_age" -ge 4000 ] && [ "$go_age" -le 30000 ]; then go_age_band=in-band; fi ;;
  esac
  chk "MySQL §5.5 age-of-oldest: Rust returns the store-clock AGE in milliseconds" "$rust_age_band" "in-band"
  chk "MySQL §5.5 age-of-oldest: Go independently returns the same bounded contract" "$go_age_band" "in-band"
  empty_age="$mage_empty|0.000|0.000|-|-"
  chk "MySQL §5.5 age-of-oldest: Rust reports no age for an empty configured queue" \
      "$($HM qstats queue=$mage_empty age=1)" "$empty_age"
  chk "MySQL §5.5 age-of-oldest: Go reports the identical empty-queue contract" \
      "$($GM qstats queue=$mage_empty age=1)" "$empty_age"

  # §5.5 QUIET GROUPS. Synthetic in-flight skew is deliberate: the noisy neighbour owns
  # 9 slots against peers at 1 and 2, while per-partition counters make the filtered
  # rates exact. Its jobs are also much older, so copying queue-wide age fails loudly.
  mqg=mysqg-$$
  metric_now=$($HM scalar-i64 stmt="SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)")
  $HM enqueue count=5 prefix=mqgn-$$- queue=$mqg partition=noisy fp=mqgnf-$$ sched=$((metric_now-20000)) retention=86400000 >/dev/null
  $HM enqueue count=2 prefix=mqga-$$- queue=$mqg partition=quiet-a fp=mqgaf-$$ sched=$((metric_now-5000)) retention=86400000 >/dev/null
  $HM enqueue count=2 prefix=mqgb-$$- queue=$mqg partition=quiet-b fp=mqgbf-$$ sched=$((metric_now-4000)) retention=86400000 >/dev/null
  $HM sql stmt="INSERT INTO headgate_inflight(queue,partition_key,n) VALUES ('$mqg','noisy',9),('$mqg','quiet-a',1),('$mqg','quiet-b',2) AS new ON DUPLICATE KEY UPDATE n=new.n" >/dev/null
  $HM sql stmt="UPDATE headgate_partition_counter SET arrived=CASE partition_key WHEN 'noisy' THEN 600 ELSE 60 END, completed=CASE partition_key WHEN 'noisy' THEN 0 ELSE 120 END WHERE queue='$mqg'" >/dev/null
  rust_q=$($HM qstats queue=$mqg quiet=1)
  go_q=$($GM qstats queue=$mqg quiet=1)
  rust_quiet=$(echo "$rust_q" | awk -F'|' '{print $6"|"$7"|"$8"|"$10"|"$11}')
  go_quiet=$(echo "$go_q" | awk -F'|' '{print $6"|"$7"|"$8"|"$10"|"$11}')
  chk "MySQL §5.5 quiet groups: Rust excludes the noisy tenant from rates and drain time" \
      "$rust_quiet" "2.000|4.000|2000|1|false"
  chk "MySQL §5.5 quiet groups: Go independently computes the identical filtered metrics" \
      "$go_quiet" "2.000|4.000|2000|1|false"
  quiet_age_verdict(){
    local line="$1" overall quiet
    overall=$(echo "$line" | awk -F'|' '{print $5}')
    quiet=$(echo "$line" | awk -F'|' '{print $9}')
    case "$overall:$quiet" in
      *[!0-9:]*|:*) echo "bad:$overall/$quiet" ;;
      *) if [ "$quiet" -ge 3000 ] && [ $((overall - quiet)) -ge 9000 ]; then
           echo filtered-oldest
         else
           echo "bad:$overall/$quiet"
         fi ;;
    esac
  }
  chk "MySQL §5.5 quiet groups: Rust's oldest age ignores the noisy tenant's older jobs" \
      "$(quiet_age_verdict "$rust_q")" "filtered-oldest"
  chk "MySQL §5.5 quiet groups: Go's oldest age ignores the same noisy tenant" \
      "$(quiet_age_verdict "$go_q")" "filtered-oldest"
  $HM sql stmt="UPDATE headgate_inflight SET n=3 WHERE queue='$mqg'" >/dev/null
  rust_balanced=$($HM qstats queue=$mqg quiet=1 | awk -F'|' '{print $6"|"$7"|"$8"|"$10}')
  go_balanced=$($GM qstats queue=$mqg quiet=1 | awk -F'|' '{print $6"|"$7"|"$8"|"$10}')
  chk "MySQL §5.5 quiet groups: balanced tenants are not silently filtered (Rust)" \
      "$rust_balanced" "12.000|4.000|-|0"
  chk "MySQL §5.5 quiet groups: balanced tenants are not silently filtered (Go)" \
      "$go_balanced" "12.000|4.000|-|0"
else
  skipped "MySQL store section (whole)" "no MySQL at $HG_MYSQL"
  echo "   start one with: docker run -d --name headgate-mysql -p 127.0.0.1:3307:3306 -e MYSQL_ROOT_PASSWORD=hg -e MYSQL_DATABASE=hg mysql:8.4"
  echo "   and apply crates/headgate-mysql/migrations/0001_init.sql"
fi

echo "== Cross-language (Go + Rust on one Redis) =="
(cd go && go build -o ../target/debug/hg-go-redis-harness ./driver/headgateredis/cmd/hg-go-redis-harness) \
  || { echo "FATAL: go redis harness build failed"; exit 2; }
GR=target/debug/hg-go-redis-harness

# Go enqueues; Rust admits under the shared fleet limit — one keyspace, one gate.
$RED flushall >/dev/null
$RED hset hg:rate:stripe tokens 5 burst 5 limit 5 window 1000 refilled 1000 >/dev/null
$GR enqueue count=20 prefix=xg queue=default rate=stripe fp=fp sched=1000 retention=86400000 >/dev/null
admitted=$($HR admit queues=default capacity=100 lease_ms=600000 worker=wR lease=LXR quantum=1000)
r=$(echo "$admitted" | grep -c '|')
chk "Go enqueues; Rust admits under the shared fleet limit (Redis)" "$r" "5"
first=$(echo "$admitted" | head -1 | cut -d'|' -f1)
r=$($GR ack "job=$first" lease=LXR fence=1 outcome=success)
chk "Go acks a lease Rust issued (Redis)" "$r" "ok"

# Execution both directions over the SAME Lua artifacts.
$RED flushall >/dev/null
$HR enqueue count=3 prefix=rxg queue=default payload='{}' sched=1000 retention=86400000 >/dev/null
r=$($GR drain queues=default count=10)
chk "Rust enqueues; the Go runtime executes (Redis)" "$r" "3"
$GR enqueue count=3 prefix=gxr queue=default payload='{}' sched=1000 retention=86400000 >/dev/null
r=$($HR drain queues=default count=10)
chk "Go enqueues; the Rust runtime executes (Redis)" "$r" "3"
r=$($RED hget hg:job:rxg1 state)+$($RED hget hg:job:gxr1 state)
chk "...and both directions completed" "$r" "completed+completed"

# §5.7 THE CURSOR, ACROSS LANGUAGES, OVER THE SAME LUA. Same contract as the Postgres
# crossing below: Rust's `set_cursor` writes RAW BYTES, Go's generic `SetCursor[C]`
# marshals — so the bytes ARE the interface, and `checkpoint.lua` is the one artifact both
# languages run. Its cursor branch had never been reached by anything before round 32k.
$RED flushall >/dev/null
$HR enqueue count=1 prefix=xrc queue=xrcq fp=fpxrc sched=1000 retention=86400000 payload='{}' >/dev/null
$HR cursor queues=xrcq pages=6 stop=3 >/dev/null
r=$($RED hget hg:job:xrc1 cp_cursor)
chk "xlang §5.7 (Redis): Rust's raw-byte cursor lands in the job hash as Go's JSON shape" "$r" '{"page":3}'
r=$($GR cursor queues=xrcq pages=6)
chk "xlang §5.7 (Redis): Go's GENERIC StepCursor resumes from it, at page 4 and not page 1" \
    "$r" "resumed_from=3|processed=4,5,6|outcome=success"
$RED flushall >/dev/null
$GR enqueue count=1 prefix=xgc queue=xrcq fp=fpxgc sched=1000 retention=86400000 payload='{}' >/dev/null
$GR cursor queues=xrcq pages=6 stop=2 >/dev/null
r=$($HR cursor queues=xrcq pages=6)
chk "xlang §5.7 (Redis): ...and RUST resumes from a cursor Go marshalled" \
    "$r" "resumed_from=2|processed=3,4,5,6|outcome=success"

# §8.4 TRACE CONTEXT over Redis (round 32). Worth its own assertion because this is the
# backend where the headers ride enqueue.lua's TRAILING argument block — the shape chosen
# so the script's three existing passes keep their `2 + i * F + k` index math untouched.
# If that block were mis-indexed, the value would come back shifted or empty here and
# nowhere else. admit.lua needed no change at all: the store reads the job hash after the
# atomic claim, so the claim path never saw a new field.
$RED flushall >/dev/null
TPR='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
$GR enqueue count=1 prefix=rtp queue=default fp=fp sched=1000 retention=86400000 \
    tp="$TPR" ts='vendor=opaque' >/dev/null
r=$($HR admit_trace queues=default capacity=1 lease_ms=600000 worker=wR lease=LTP quantum=100)
chk "xlang §8.4 (Redis): Go enqueues traceparent -> Rust reads back the IDENTICAL value" \
    "$r" "rtp1|$TPR|$TPR|vendor=opaque"
$RED flushall >/dev/null
# A header-less enqueue must write NO `headers` field — the trailing block is additive
# in the byte sense too, or the Go/Rust keyspace diff below would start failing.
$GR enqueue count=1 prefix=rnh queue=default fp=fp sched=1000 retention=86400000 >/dev/null
r=$($RED hexists hg:job:rnh1 headers)
# HEXISTS on a job that does not exist is also 0. The witness is the job hash itself:
# an enqueue that silently failed cannot be allowed to prove "no headers were written".
chk0 "xlang §8.4 (Redis): a header-less job writes no headers field at all" "$r" "0" \
     "...witness: the header-less job WAS enqueued (hexists on a missing key is 0 too)" \
     "$($RED exists hg:job:rnh1)"
$RED flushall >/dev/null

# §3.2 keyspace diff on REDIS: one scenario driven per language, normalized dump
# byte-identical. Deterministic fields only (no store-clock scores/timestamps).
redis_scenario(){ # $1 = harness
  $RED flushall >/dev/null
  $RED hset hg:rate:stripe tokens 3 burst 3 limit 3 window 100000 refilled 1000 >/dev/null
  $1 enqueue count=6 prefix=xs queue=default payload='{}' fp=auto sched=1000 retention=86400000 >/dev/null
  $1 enqueue count=1 prefix=xu unique=XK queue=default sched=1000 retention=86400000 >/dev/null
  $1 enqueue count=1 prefix=xt unique=XT window=60000 queue=default sched=1000 retention=86400000 >/dev/null
  $1 enqueue count=2 prefix=xr queue=default rate=stripe fp=fp sched=1000 retention=86400000 >/dev/null
  $1 admit queues=default capacity=4 lease_ms=600000 worker=wX lease=LX quantum=1000 >/dev/null
  $1 ack job=xs1 lease=LX fence=1 outcome=success >/dev/null
  $1 ack job=xs2 lease=LX fence=1 outcome=retry err=boom >/dev/null
  $1 ack job=xr1 lease=LX fence=1 outcome=skip err=nope >/dev/null
  for id in $($RED keys 'hg:job:*' | sed 's/^hg:job://' | sort); do
    echo "job $id $($RED hmget hg:job:$id kind queue partition_key rate_class fingerprint priority attempt crash_attempt max_attempts state retention_ms unique_window_ms fence | paste -sd'|' -)"
  done
  echo "quarantine [$($RED smembers hg:quarantine | sort | paste -sd, -)]"
  for k in $($RED keys 'hg:unique:*' | sort); do echo "unique $k -> $($RED get $k)"; done
  for k in $($RED keys 'hg:uniquet:*' | sort); do echo "throttle $k -> $($RED get $k)"; done
  for st in available scheduled retryable running completed archived; do
    echo "idx default:$st [$($RED zrange hg:idx:default:$st 0 -1 | sort | paste -sd, -)]"
  done
  echo "parts [$($RED smembers hg:parts:default | sort | paste -sd, -)]"
  echo "rate stripe tokens=$($RED hget hg:rate:stripe tokens)"
}
redis_scenario "$GR" > /tmp/hgx-rkeyspace-go.txt
redis_scenario "$HR" > /tmp/hgx-rkeyspace-rust.txt
lines=$(wc -l < /tmp/hgx-rkeyspace-go.txt)
chk "redis keyspace snapshot is non-trivial (no vacuous pass)" "$((lines >= 18))" "1"
if cmp -s /tmp/hgx-rkeyspace-go.txt /tmp/hgx-rkeyspace-rust.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-rkeyspace-go.txt /tmp/hgx-rkeyspace-rust.txt | head -10; fi
chk "redis keyspace diff: Go-driven and Rust-driven stores match byte-for-byte" "$d" "identical"
$RED flushall >/dev/null

echo "== Cross-language (Go + Rust on one Postgres) =="
command -v go >/dev/null || { echo "FATAL: go not found; cross-language section needs it"; exit 2; }
(cd go && go build -o ../target/debug/hg-go-harness ./driver/headgatepgx/cmd/hg-go-harness) \
  || { echo "FATAL: go harness build failed"; exit 2; }
G=target/debug/hg-go-harness

# The checked-in direct statement is decoded positionally by Go too. Contention makes
# its execution observable: the compact scan skips three held heads and claims the next
# three; the old draw-then-ID-lock path returned an empty batch on this fixture.
reset_pg
$G enqueue count=6 prefix=dg queue=default fp=dgf sched=1000 retention=86400000 >/dev/null
PGAPPNAME=hg-go-direct-lock $PSQL -c "BEGIN;
SELECT id FROM headgate_job WHERE state='available' AND queue='default'
ORDER BY priority DESC, scheduled_at_ms, id LIMIT 3 FOR UPDATE;
SELECT pg_advisory_lock(9321, 3);
SELECT pg_sleep(30);
ROLLBACK;" >/dev/null 2>&1 &
dgpid=$!
for _ in $(seq 1 200); do
  ready=$($PSQL -c "SELECT count(*) FROM pg_locks
                     WHERE locktype='advisory' AND classid=9321 AND objid=3 AND granted;")
  [ "$ready" = "1" ] && break
  sleep 0.1
done
$G admit queues=default capacity=3 lease_ms=30000 worker=gd lease=GD1 quantum=1000 >/dev/null
$PSQL -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='hg-go-direct-lock';" >/dev/null 2>&1
wait "$dgpid" 2>/dev/null
chk "direct fast path Go: positional decoding reaches the same work-conserving tail" \
    "$(ranset)" "dg4,dg5,dg6"

# ===================== ROUND 32m: CLOSE THE FIVE-ROW EVIDENCE DEBT =====================
# Age-of-oldest and quiet-group metrics run above. These three contracts exercise the
# protected admission artifacts themselves, through every backend and both language
# adapters. The fixtures are deliberately adversarial: runtime weight changes, priority
# pressure from the low-weight queue, over-estimate refunds, under-estimate debits, every
# saturation terminal, and a stale displaced fence.

weighted_cells=0
weighted_queue_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3"
  local fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local heavy="wqh-$suffix" light="wql-$suffix"
  local hp="wha-$suffix-" lp="wla-$suffix-" out ids hcount lcount
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
  esac
  $tool queue-weight queue="$heavy" weight=3 >/dev/null
  $tool queue-weight queue="$light" weight=1 >/dev/null
  # The low-weight queue gets enormous job priority. If priority were allowed to cross
  # the queue boundary it would take all eight; weight must still choose 6:2.
  $tool enqueue count=8 prefix="$hp" queue="$heavy" partition=p priority=0 sched=1000 retention=86400000 >/dev/null
  $tool enqueue count=8 prefix="$lp" queue="$light" partition=p priority=99 sched=1000 retention=86400000 >/dev/null
  out=$($tool admit queues="$heavy,$light" capacity=8 lease_ms=600000 worker=w lease="WQ1-$suffix" quantum=1000)
  ids=$(echo "$out" | cut -d'|' -f1)
  hcount=$(echo "$ids" | grep -c "^$hp" || true)
  lcount=$(echo "$ids" | grep -c "^$lp" || true)
  chk "Weighted queues $label: 3:1 yields 6:2 even when the light queue has higher job priority" \
      "$hcount|$lcount" "6|2"

  # Rescale persisted service when policy changes. Both queues sat at virtual position 2
  # after 6:2; changing 3 -> 1 must preserve that position and make the next four 2:2.
  $tool queue-weight queue="$heavy" weight=1 >/dev/null
  out=$($tool admit queues="$heavy,$light" capacity=4 lease_ms=600000 worker=w lease="WQ2-$suffix" quantum=1000)
  ids=$(echo "$out" | cut -d'|' -f1)
  hcount=$(echo "$ids" | grep -c "^$hp" || true)
  lcount=$(echo "$ids" | grep -c "^$lp" || true)
  chk "Weighted queues $label: a runtime 3-to-1 change rescales history and immediately yields 2:2" \
      "$hcount|$lcount" "2|2"

  # The heavy queue is now empty and the light queue has four rows. Weighted fairness is
  # work-conserving: it fills the whole request instead of idling three slots.
  out=$($tool admit queues="$heavy,$light" capacity=4 lease_ms=600000 worker=w lease="WQ3-$suffix" quantum=1000)
  ids=$(echo "$out" | cut -d'|' -f1)
  hcount=$(echo "$ids" | grep -c "^$hp" || true)
  lcount=$(echo "$ids" | grep -c "^$lp" || true)
  chk "Weighted queues $label: an alone busy queue consumes all remaining capacity (work conserving)" \
      "$(echo "$ids" | grep -c . || true)|$hcount|$lcount" "4|0|4"
  [ "$fail" = "$fail_before" ] && weighted_cells=$((weighted_cells+1))
}

echo "== Weighted queue contract (all stores, both languages) =="
weighted_queue_contract pg "$H" "Postgres/Rust"
weighted_queue_contract pg "$G" "Postgres/Go"
weighted_queue_contract redis "$HR" "Redis/Rust"
weighted_queue_contract redis "$GR" "Redis/Go"
if mysql_up; then
  weighted_queue_contract mysql "$HM" "MySQL/Rust"
  weighted_queue_contract mysql "$GM" "MySQL/Go"
  chk "Weighted queues: all six backend/language cells completed the adversarial contract" \
      "$weighted_cells" "6"
else
  skipped "Weighted queues MySQL/Rust + MySQL/Go" "no live MySQL"
fi

cost_tokens(){ # backend rate-class
  case "$1" in
    pg) $PSQL -c "SELECT tokens FROM headgate_rate_bucket WHERE name='$2';" ;;
    redis) $RED hget "hg:rate:$2" tokens ;;
    mysql) $HM scalar-i64 stmt="SELECT tokens FROM headgate_rate_bucket WHERE name='$2'" ;;
  esac
}

cost_charge_witness(){ # backend base-id
  local backend="$1" base="$2" n=0
  case "$backend" in
    pg)
      $PSQL -c "SELECT count(*) FROM headgate_job WHERE
        (ulid='${base}a1' AND rate_charge=3) OR
        (ulid='${base}b1' AND rate_charge=2) OR
        (ulid='${base}c1' AND rate_charge=0);"
      ;;
    redis)
      [ "$($RED hget "hg:job:${base}a1" rate_charge)" = 3 ] && n=$((n+1))
      [ "$($RED hget "hg:job:${base}b1" rate_charge)" = 2 ] && n=$((n+1))
      [ "$($RED hget "hg:job:${base}c1" rate_charge)" = 0 ] && n=$((n+1))
      echo "$n"
      ;;
    mysql)
      $HM scalar-i64 stmt="SELECT count(*) FROM headgate_job WHERE
        (ulid='${base}a1' AND rate_charge=3) OR
        (ulid='${base}b1' AND rate_charge=2) OR
        (ulid='${base}c1' AND rate_charge=0)"
      ;;
  esac
}

cost_weight_cells=0
cost_weight_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3"
  local fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local queue="cwq-$suffix" rc="cwrc-$suffix" base="cw-$suffix-"
  local ids norm refund third debit stale stale_verdict
  case "$backend" in
    pg)
      reset_pg
      $PSQL -c "INSERT INTO headgate_rate_bucket
        (name,tokens,burst,limit_per_window,window_ms,refilled_at_ms)
        VALUES ('$rc',5,5,0,1000,(extract(epoch from clock_timestamp())*1000)::bigint);" >/dev/null
      ;;
    redis)
      $RED flushall >/dev/null
      $RED hset "hg:rate:$rc" tokens 5 burst 5 limit 0 window 1000 refilled 0 >/dev/null
      ;;
    mysql)
      $HM sql stmt="INSERT INTO headgate_rate_bucket
        (name,tokens,burst,limit_per_window,window_ms,refilled_at_ms)
        VALUES ('$rc',5,5,0,1000,CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED))" >/dev/null
      ;;
  esac
  $tool enqueue count=1 prefix="${base}a" queue="$queue" partition=p rate="$rc" weight=3 sched=1000 retention=86400000 >/dev/null
  $tool enqueue count=1 prefix="${base}b" queue="$queue" partition=p rate="$rc" weight=2 sched=1000 retention=86400000 >/dev/null
  $tool enqueue count=1 prefix="${base}c" queue="$queue" partition=p rate="$rc" weight=1 sched=1000 retention=86400000 >/dev/null
  ids=$($tool admit queues="$queue" capacity=3 lease_ms=600000 worker=w lease="CW1-$suffix" quantum=1000 | cut -d'|' -f1)
  norm=$(echo "$ids" | sed "s/^$base//" | sort | paste -sd, -)
  chk "Cost-weighted limits $label: estimates 3+2 consume a five-token bucket and cost 1 waits" \
      "$norm|$(cost_tokens "$backend" "$rc")|$(cost_charge_witness "$backend" "$base")" \
      "a1,b1|0|3"

  $tool ack job="${base}a1" lease="CW1-$suffix" fence=1 outcome=success actual=1 >/dev/null
  refund=$(cost_tokens "$backend" "$rc")
  chk "Cost-weighted limits $label: actual cost 1 refunds two fenced tokens from estimate 3" \
      "$refund" "2"

  # With two tokens left, a weight-3 job is blocked even though only one job is ahead.
  # An explainer that still counts rows (`avail <= ahead`) reports this as admissible.
  $tool enqueue count=1 prefix="${base}d" queue="$queue" partition=p rate="$rc" weight=3 sched=1000 retention=86400000 >/dev/null
  chk "Cost-weighted limits $label: explain includes the job's own estimate, not just rows ahead" \
      "$($tool explain job="${base}d1")" "admissible=false blocked_by=rate_class"

  third=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=w lease="CW2-$suffix" quantum=1000 | cut -d'|' -f1 | sed "s/^$base//")
  chk "Cost-weighted limits $label: the refund releases the waiting cost-1 job and spends one" \
      "$third|$(cost_tokens "$backend" "$rc")" "c1|1"

  $tool ack job="${base}b1" lease="CW1-$suffix" fence=1 outcome=success actual=4 >/dev/null
  debit=$(cost_tokens "$backend" "$rc")
  chk "Cost-weighted limits $label: actual cost 4 debits two more than estimate 2" \
      "$debit" "-1"

  stale=$($tool ack job="${base}a1" lease="CW1-$suffix" fence=1 outcome=success actual=0 2>&1 || true)
  stale_verdict=accepted
  case "$stale" in ERR*) stale_verdict=rejected ;; esac
  chk "Cost-weighted limits $label: a stale fence cannot refund the bucket a second time" \
      "$stale_verdict|$(cost_tokens "$backend" "$rc")" "rejected|-1"
  [ "$fail" = "$fail_before" ] && cost_weight_cells=$((cost_weight_cells+1))
}

echo "== Cost-weighted limit contract (all stores, both languages) =="
cost_weight_contract pg "$H" "Postgres/Rust"
cost_weight_contract pg "$G" "Postgres/Go"
cost_weight_contract redis "$HR" "Redis/Rust"
cost_weight_contract redis "$GR" "Redis/Go"
if mysql_up; then
  cost_weight_contract mysql "$HM" "MySQL/Rust"
  cost_weight_contract mysql "$GM" "MySQL/Go"
  chk "Cost-weighted limits: all six backend/language cells completed the reconciliation contract" \
      "$cost_weight_cells" "6"
else
  skipped "Cost-weighted limits MySQL/Rust + MySQL/Go" "no live MySQL"
fi

# Round 32am / explicit gap 27: Oban-style replacement is a request-only allowlist,
# never a wholesale overwrite. The duplicate result remains typed and names the winner;
# these live cells prove the mutation commits despite that result, immutable routing is
# retained, scheduled indexes move with scheduled_at, and a running holder is untouched.
replace_snapshot(){ # backend id
  case "$1" in
    pg) $PSQL -c "SELECT convert_from(payload,'UTF8')||'|'||priority||'|'||max_attempts||'|'||queue FROM headgate_job WHERE ulid='$2';" ;;
    redis) echo "$($RED hget "hg:job:$2" payload)|$($RED hget "hg:job:$2" priority)|$($RED hget "hg:job:$2" max_attempts)|$($RED hget "hg:job:$2" queue)" ;;
    mysql) $HM scalar-string stmt="SELECT CONCAT(CAST(payload AS CHAR),'|',priority,'|',max_attempts,'|',queue) FROM headgate_job WHERE ulid='$2'" ;;
  esac
}
replace_sched_snapshot(){ # backend id
  case "$1" in
    pg) $PSQL -c "SELECT scheduled_at_ms||'|'||state::text FROM headgate_job WHERE ulid='$2';" ;;
    redis) echo "$($RED hget "hg:job:$2" scheduled_at_ms)|$($RED hget "hg:job:$2" state)" ;;
    mysql) $HM scalar-string stmt="SELECT CONCAT(scheduled_at_ms,'|',CAST(state AS CHAR)) FROM headgate_job WHERE ulid='$2'" ;;
  esac
}
replace_cells=0
replace_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3" suffix="ur-${1}-${3//[^A-Za-z0-9]/}-$$"
  local q="urq-$suffix" key="urk-$suffix" id="uro-$suffix-1" before_fail=$fail
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
    mysql) $HM sql stmt="DELETE FROM headgate_job WHERE ulid LIKE 'ur%-$suffix-%' OR queue='$q'" >/dev/null ;;
  esac
  $tool enqueue count=1 prefix="uro-$suffix-" queue="$q" unique="$key" payload=old priority=1 max_attempts=25 sched=1000 retention=86400000 >/dev/null
  $tool enqueue count=1 prefix="urn-$suffix-" queue=immutable-route unique="$key" payload=new priority=9 max_attempts=7 replace=13 sched=1000 retention=86400000 >/dev/null 2>&1 || true
  chk "Unique replace $label: payload, priority and max-attempt allowlist commits while routing stays immutable" \
      "$(replace_snapshot "$backend" "$id")" "new|9|7|$q"

  $tool admit queues="$q" capacity=1 lease_ms=600000 worker=w lease="URL-$suffix" quantum=1000 >/dev/null
  $tool enqueue count=1 prefix="urr-$suffix-" queue="$q" unique="$key" payload=blocked priority=20 replace=4 sched=1000 retention=86400000 >/dev/null 2>&1 || true
  chk "Unique replace $label: a running holder is never modified" \
      "$(replace_snapshot "$backend" "$id")" "new|9|7|$q"

  local skey="ursk-$suffix" sid="urso-$suffix-1"
  $tool enqueue count=1 prefix="urso-$suffix-" queue="$q" unique="$skey" payload=sched sched=9999999999999 retention=86400000 >/dev/null
  $tool enqueue count=1 prefix="ursn-$suffix-" queue="$q" unique="$skey" payload=ignored sched=8888888888888 replace=2 retention=86400000 >/dev/null 2>&1 || true
  chk "Unique replace $label: scheduled_at replacement preserves scheduled state and its store index" \
      "$(replace_sched_snapshot "$backend" "$sid")" "8888888888888|scheduled"
  [ "$fail" = "$before_fail" ] && replace_cells=$((replace_cells+1))
}

echo "== Unique-conflict replacement contract (all stores, both languages) =="
replace_contract pg "$H" "Postgres/Rust"
replace_contract pg "$G" "Postgres/Go"
replace_contract redis "$HR" "Redis/Rust"
replace_contract redis "$GR" "Redis/Go"
if mysql_up; then
  replace_contract mysql "$HM" "MySQL/Rust"
  replace_contract mysql "$GM" "MySQL/Go"
  chk "Unique replace: all six backend/language cells completed the guarded contract" "$replace_cells" "6"
else
  skipped "Unique replace MySQL/Rust + MySQL/Go" "no live MySQL"
fi

sat_state(){
  case "$1" in
    pg) $PSQL -c "SELECT state::text FROM headgate_job WHERE ulid='$2';" ;;
    redis) $RED hget "hg:job:$2" state ;;
    mysql) $HM state job="$2" ;;
  esac
}
sat_lease(){
  case "$1" in
    pg) $PSQL -c "SELECT (lease_id IS NOT NULL)::int FROM headgate_job WHERE ulid='$2';" ;;
    redis) $RED hexists "hg:job:$2" lease_id ;;
    mysql) $HM scalar-i64 stmt="SELECT lease_id IS NOT NULL FROM headgate_job WHERE ulid='$2'" ;;
  esac
}
sat_fence(){
  case "$1" in
    pg) $PSQL -c "SELECT fence FROM headgate_job WHERE ulid='$2';" ;;
    redis) $RED hget "hg:job:$2" fence ;;
    mysql) $HM scalar-i64 stmt="SELECT fence FROM headgate_job WHERE ulid='$2'" ;;
  esac
}
sat_terminal_shape(){ # state-neutrality witness: attempt, crash, finalized?, lease?
  case "$1" in
    pg) $PSQL -c "SELECT attempt||','||crash_attempt||','||
                    (finalized_at_ms IS NOT NULL)::int||','||(lease_id IS NOT NULL)::int
                  FROM headgate_job WHERE ulid='$2';" ;;
    redis)
      echo "$($RED hget "hg:job:$2" attempt),$($RED hget "hg:job:$2" crash_attempt),$($RED hexists "hg:job:$2" finalized_at_ms),$($RED hexists "hg:job:$2" lease_id)"
      ;;
    mysql) $HM scalar-i64 stmt="SELECT attempt*1000 + crash_attempt*100 +
                                  (finalized_at_ms IS NOT NULL)*10 + (lease_id IS NOT NULL)
                                FROM headgate_job WHERE ulid='$2'" ;;
  esac
}
sat_inflight(){
  case "$1" in
    pg) $PSQL -c "SELECT n FROM headgate_inflight WHERE queue='$2' AND partition_key='p';" ;;
    redis) $RED hget "hg:inflight:$2" p ;;
    mysql) $HM scalar-i64 stmt="SELECT n FROM headgate_inflight WHERE queue='$2' AND partition_key='p'" ;;
  esac
}

saturation_cells=0
saturation_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3"
  local fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local q="satq-$suffix" d="satd-$suffix" ci="satci-$suffix" cr="satcr-$suffix"
  local qb="sq-$suffix-" db="sd-$suffix-" cib="si-$suffix-"
  local oldb="so-$suffix-" newb="sn-$suffix-" claims contract stale verdict
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
  esac

  $tool concurrency name="limit-q-$suffix" queue="$q" max=1 strategy=queue >/dev/null
  $tool enqueue count=3 prefix="$qb" queue="$q" partition=p sched=1000 retention=86400000 >/dev/null
  claims=$($tool admit queues="$q" capacity=3 lease_ms=600000 worker=w lease="SQ-$suffix" quantum=1000 | grep -c '|' || true)
  contract="$claims|$(sat_state "$backend" "${qb}1"),$(sat_state "$backend" "${qb}2"),$(sat_state "$backend" "${qb}3")|$(sat_lease "$backend" "${qb}1"),$(sat_lease "$backend" "${qb}2"),$(sat_lease "$backend" "${qb}3")|$(sat_inflight "$backend" "$q")"
  chk "Saturation $label queue: one slot runs while overflow stays available and unleased" \
      "$contract" "1|running,available,available|1,0,0|1"
  chk "Saturation $label queue: admission explain names the concurrency ceiling" \
      "$($tool explain job="${qb}2")" "admissible=false blocked_by=concurrency_limit"

  $tool concurrency name="limit-d-$suffix" queue="$d" max=1 strategy=discard >/dev/null
  $tool enqueue count=3 prefix="$db" queue="$d" partition=p sched=1000 retention=86400000 >/dev/null
  claims=$($tool admit queues="$d" capacity=3 lease_ms=600000 worker=w lease="SD-$suffix" quantum=1000 | grep -c '|' || true)
  contract="$claims|$(sat_state "$backend" "${db}1"),$(sat_state "$backend" "${db}2"),$(sat_state "$backend" "${db}3")|$(sat_terminal_shape "$backend" "${db}2")/$(sat_terminal_shape "$backend" "${db}3")|$(sat_inflight "$backend" "$d")"
  if [ "$backend" = mysql ]; then
    chk "Saturation $label discard: overflow archives visibly with neutral attempts and terminal timestamps" \
        "$contract" "1|running,archived,archived|10/10|1"
  else
    chk "Saturation $label discard: overflow archives visibly with neutral attempts and terminal timestamps" \
        "$contract" "1|running,archived,archived|0,0,1,0/0,0,1,0|1"
  fi

  $tool concurrency name="limit-ci-$suffix" queue="$ci" max=1 strategy=cancel_incoming >/dev/null
  $tool enqueue count=3 prefix="$cib" queue="$ci" partition=p sched=1000 retention=86400000 >/dev/null
  claims=$($tool admit queues="$ci" capacity=3 lease_ms=600000 worker=w lease="SI-$suffix" quantum=1000 | grep -c '|' || true)
  contract="$claims|$(sat_state "$backend" "${cib}1"),$(sat_state "$backend" "${cib}2"),$(sat_state "$backend" "${cib}3")|$(sat_terminal_shape "$backend" "${cib}2")/$(sat_terminal_shape "$backend" "${cib}3")|$(sat_inflight "$backend" "$ci")"
  if [ "$backend" = mysql ]; then
    chk "Saturation $label cancel_incoming: oldest wins and incoming overflow is neutral cancelled state" \
        "$contract" "1|running,cancelled,cancelled|10/10|1"
  else
    chk "Saturation $label cancel_incoming: oldest wins and incoming overflow is neutral cancelled state" \
        "$contract" "1|running,cancelled,cancelled|0,0,1,0/0,0,1,0|1"
  fi

  $tool concurrency name="limit-cr-$suffix" queue="$cr" max=2 strategy=cancel_running >/dev/null
  $tool enqueue count=2 prefix="$oldb" queue="$cr" partition=p sched=1000 retention=86400000 >/dev/null
  $tool admit queues="$cr" capacity=2 lease_ms=600000 worker=w lease="SO-$suffix" quantum=1000 >/dev/null
  $tool enqueue count=1 prefix="$newb" queue="$cr" partition=p sched=1000 retention=86400000 >/dev/null
  chk "Saturation $label cancel_running: explain reports displacement as admissible" \
      "$($tool explain job="${newb}1")" "admissible=true blocked_by=none"
  claims=$($tool admit queues="$cr" capacity=1 lease_ms=600000 worker=w lease="SN-$suffix" quantum=1000 | grep -c '|' || true)
  contract="$claims|$(sat_state "$backend" "${oldb}1"),$(sat_state "$backend" "${oldb}2"),$(sat_state "$backend" "${newb}1")|$(sat_fence "$backend" "${oldb}1"),$(sat_fence "$backend" "${oldb}2"),$(sat_fence "$backend" "${newb}1")|$(sat_lease "$backend" "${oldb}1"),$(sat_lease "$backend" "${oldb}2"),$(sat_lease "$backend" "${newb}1")|$(sat_inflight "$backend" "$cr")"
  chk "Saturation $label cancel_running: newest replaces only the oldest sibling and advances its fence" \
      "$contract" "1|cancelled,running,running|2,1,1|0,1,1|2"
  stale=$($tool ack job="${oldb}1" lease="SO-$suffix" fence=1 outcome=success 2>&1 || true)
  verdict=accepted
  case "$stale" in ERR*) verdict=rejected ;; esac
  chk "Saturation $label cancel_running: the displaced holder's stale ack is rejected" \
      "$verdict|$(sat_state "$backend" "${oldb}2")|$(sat_state "$backend" "${newb}1")" \
      "rejected|running|running"
  [ "$fail" = "$fail_before" ] && saturation_cells=$((saturation_cells+1))
}

echo "== Saturation contract (all strategies, all stores, both languages) =="
saturation_contract pg "$H" "Postgres/Rust"
saturation_contract pg "$G" "Postgres/Go"
saturation_contract redis "$HR" "Redis/Rust"
saturation_contract redis "$GR" "Redis/Go"
if mysql_up; then
  saturation_contract mysql "$HM" "MySQL/Rust"
  saturation_contract mysql "$GM" "MySQL/Go"
  chk "Saturation: all six backend/language cells completed all four strategies" \
      "$saturation_cells" "6"
else
  skipped "Saturation MySQL/Rust + MySQL/Go" "no live MySQL"
fi

# ===================== ROUND 32ak: FIFO-AFTER-RETRY SEMANTICS =====================
# The chosen contract is retry-time ordering, not original-enqueue FIFO. A returned
# Retry re-stamps scheduled_at_ms from the store clock, so later-enqueued, same-priority
# siblings that are already due run first. This is intentionally separate from the
# crash-suspect/reclaim checks above: deleting the Retry arm's re-stamp must fail here.
retry_order_cells=0
retry_order_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3"
  local fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local queue="retry-order-$suffix" base="retry-order-$suffix-" first second third claim
  first="${base}a1"; second="${base}b1"; third="${base}c1"
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
    mysql) $HM sql stmt="DELETE FROM headgate_job WHERE queue='$queue'" >/dev/null ;;
  esac

  $tool enqueue count=1 prefix="${base}a" queue="$queue" partition=p fp="retry-order-a-$suffix" \
        sched=1000 retention=86400000 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=old \
        lease="RETRY-OLD-$suffix" quantum=1000)
  chk "FIFO-after-retry $label: the originally enqueued head is claimed first" \
      "$(echo "$claim" | cut -d'|' -f1)" "$first"

  # These arrive while A is running. Their old due times make the ordering distinction
  # observable after A is re-scheduled; a fixed 1ms retry avoids a one-second test sleep.
  $tool enqueue count=1 prefix="${base}b" queue="$queue" partition=p fp="retry-order-b-$suffix" \
        sched=1001 retention=86400000 >/dev/null
  $tool enqueue count=1 prefix="${base}c" queue="$queue" partition=p fp="retry-order-c-$suffix" \
        sched=1002 retention=86400000 >/dev/null
  $tool ack job="$first" lease="RETRY-OLD-$suffix" fence=1 outcome=retry delay=1 >/dev/null
  sleep 0.02
  $tool promote limit=10 >/dev/null

  claim=$($tool admit queues="$queue" capacity=2 lease_ms=600000 worker=new \
        lease="RETRY-SIB-$suffix" quantum=1000 | idset)
  chk "FIFO-after-retry $label: retry-time ordering yields to later-enqueued due siblings" \
      "$claim" "$second,$third"
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=new \
        lease="RETRY-AGAIN-$suffix" quantum=1000 | cut -d'|' -f1)
  chk "FIFO-after-retry $label: the retried job follows its siblings and is not lost" \
      "$claim" "$first"
  [ "$fail" = "$fail_before" ] && retry_order_cells=$((retry_order_cells+1))
}

echo "== FIFO-after-retry contract (retry-time ordering, all stores, both languages) =="
retry_order_contract pg "$H" "Postgres/Rust"
retry_order_contract pg "$G" "Postgres/Go"
retry_order_contract redis "$HR" "Redis/Rust"
retry_order_contract redis "$GR" "Redis/Go"
if mysql_up; then
  retry_order_contract mysql "$HM" "MySQL/Rust"
  retry_order_contract mysql "$GM" "MySQL/Go"
  chk "FIFO-after-retry: all six backend/language cells chose retry-time ordering" \
      "$retry_order_cells" "6"
else
  skipped "FIFO-after-retry MySQL/Rust + MySQL/Go" "no live MySQL"
fi

leader_resign_cells=0
leader_resign_contract(){ # harness label
  local tool="$1" label="$2" fail_before=$fail
  local duty="resign-${label//[^A-Za-z0-9]/}-$$" first second refused
  first=$($tool duty name="$duty" holder=holder-a lease_ms=60000)
  chk "Leader resign $label: the current holder owns the store-leased duty" "$first" "true"
  $tool duty-release name="$duty" holder=not-the-holder >/dev/null
  refused=$($tool duty name="$duty" holder=holder-b lease_ms=60000)
  chk0 "Leader resign $label: a non-holder cannot release someone else's duty" \
       "$refused" "false" \
       "Leader resign $label: ...witness: holder A acquired the duty" "$first"
  $tool duty-release name="$duty" holder=holder-a >/dev/null
  second=$($tool duty name="$duty" holder=holder-b lease_ms=60000)
  chk "Leader resign $label: the rightful release permits immediate takeover" "$second" "true"
  $tool duty-release name="$duty" holder=holder-b >/dev/null
  [ "$fail" = "$fail_before" ] && leader_resign_cells=$((leader_resign_cells+1))
}

echo "== Leader-resign store contract (all stores, both languages) =="
leader_resign_contract "$H" "Postgres/Rust"
leader_resign_contract "$G" "Postgres/Go"
leader_resign_contract "$HR" "Redis/Rust"
leader_resign_contract "$GR" "Redis/Go"
if mysql_up; then
  leader_resign_contract "$HM" "MySQL/Rust"
  leader_resign_contract "$GM" "MySQL/Go"
  chk "Leader resign: all six backend/language cells fenced release and took over immediately" \
      "$leader_resign_cells" "6"
else
  skipped "Leader resign MySQL/Rust + MySQL/Go" "no live MySQL"
fi

orphan_cells=0
orphan_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3" fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local queue="orphan-$suffix"
  local crash="orphan-crash-$suffix-1" returned="orphan-returned-$suffix-1"
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
    mysql) $HM sql stmt="DELETE FROM headgate_job WHERE queue='$queue'" >/dev/null ;;
  esac
  $tool enqueue count=1 prefix="orphan-crash-$suffix-" queue="$queue" fp="orphan-crash-$suffix" sched=1000 retention=86400000 >/dev/null
  $tool enqueue count=1 prefix="orphan-returned-$suffix-" queue="$queue" fp="orphan-returned-$suffix" sched=1000 retention=86400000 >/dev/null
  $tool admit queues="$queue" capacity=2 lease_ms=600000 worker=lost lease="ORPHAN-$suffix" quantum=1000 >/dev/null
  chk "Orphan provenance $label: fresh claimed jobs are not marked orphaned" \
      "$($tool orphaned job="$crash")|$($tool orphaned job="$returned")" "false|false"
  $tool ack job="$returned" lease="ORPHAN-$suffix" fence=1 outcome=retry delay=1 err=returned >/dev/null
  case "$backend" in
    pg) $PSQL -c "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='$crash'" >/dev/null ;;
    redis) $RED hset "hg:job:$crash" lease_expires_at_ms 0 >/dev/null
           $RED zadd hg:lease XX CH 0 "$crash" >/dev/null ;;
    mysql) $HM sql stmt="UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='$crash'" >/dev/null ;;
  esac
  $tool reclaim limit=10 >/dev/null
  chk "Orphan provenance $label: lease reclaim is surfaced while a returned error is not" \
      "$($tool orphaned job="$crash")|$($tool orphaned job="$returned")" "true|false"
  [ "$fail" = "$fail_before" ] && orphan_cells=$((orphan_cells+1))
}

echo "== Orphan provenance contract (all stores, both languages) =="
orphan_contract pg "$H" "Postgres/Rust"
orphan_contract pg "$G" "Postgres/Go"
orphan_contract redis "$HR" "Redis/Rust"
orphan_contract redis "$GR" "Redis/Go"
if mysql_up; then
  orphan_contract mysql "$HM" "MySQL/Rust"
  orphan_contract mysql "$GM" "MySQL/Go"
  chk "Orphan provenance: all six backend/language cells distinguish reclaim from returned error" \
      "$orphan_cells" "6"
else
  skipped "Orphan provenance MySQL/Rust + MySQL/Go" "no live MySQL"
fi

periodic_origin_cells=0
periodic_origin_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3" fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local queue="origin-$suffix"
  local plain="origin-plain-$suffix-1" periodic="origin-periodic-$suffix-1"
  local plain_origin periodic_origin
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
    mysql) $HM sql stmt="DELETE FROM headgate_job WHERE queue='$queue'" >/dev/null ;;
  esac
  $tool enqueue count=1 prefix="origin-plain-$suffix-" queue="$queue" fp="origin-plain-$suffix" \
        sched=1000 retention=86400000 >/dev/null
  $tool enqueue count=1 prefix="origin-periodic-$suffix-" queue="$queue" fp="origin-periodic-$suffix" \
        schedule_id="daily-report-$suffix" tick=1700000000123 sched=1000 retention=86400000 >/dev/null
  plain_origin=$($tool origin job="$plain")
  periodic_origin=$($tool origin job="$periodic")
  chk "Periodic origin $label: schedule identity and exact tick survive typed storage" \
      "$periodic_origin" "daily-report-$suffix|1700000000123"
  chk0 "Periodic origin $label: ordinary jobs expose no invented schedule origin" \
       "$plain_origin" "none" \
       "Periodic origin $label: ...witness: the typed periodic sibling really exists" \
       "$periodic_origin"
  [ "$fail" = "$fail_before" ] && periodic_origin_cells=$((periodic_origin_cells+1))
}

echo "== Periodic-origin contract (all stores, both languages) =="
periodic_origin_contract pg "$H" "Postgres/Rust"
periodic_origin_contract pg "$G" "Postgres/Go"
periodic_origin_contract redis "$HR" "Redis/Rust"
periodic_origin_contract redis "$GR" "Redis/Go"
if mysql_up; then
  periodic_origin_contract mysql "$HM" "MySQL/Rust"
  periodic_origin_contract mysql "$GM" "MySQL/Go"
  chk "Periodic origin: all six backend/language cells preserved the typed pair" \
      "$periodic_origin_cells" "6"
else
  skipped "Periodic origin MySQL/Rust + MySQL/Go" "no live MySQL"
fi

# ===================== ROUND 32ah: VERSIONED JOB RESULTS =====================
# The result is attempt-local until the success transition. This six-cell contract pins
# the two failure paths that a happy-path-only test cannot see: a displaced fence cannot
# publish bytes, and retention zero/eviction remove the result with the job. The runtime
# unit tests separately use non-UTF-8 bytes and prove failed handlers discard their
# attempt-local value; here the live stores prove atomicity and backend parity.
result_cells=0
result_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3"
  local fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local queue="result-$suffix" base="result-$suffix-" claim job lease fence stale before
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
    mysql) $HM sql stmt="DELETE FROM headgate_job WHERE queue='$queue'" >/dev/null ;;
  esac

  $tool enqueue count=1 prefix="${base}kept" queue="$queue" fp="result-fp-$suffix" \
        sched=1000 retention=86400000 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=w \
        lease="RESULT-$suffix" quantum=1000)
  job=$(echo "$claim" | cut -d'|' -f1)
  lease=$(echo "$claim" | cut -d'|' -f2)
  fence=$(echo "$claim" | cut -d'|' -f3)
  before=$($tool get_result job="$job")
  chk0 "Job results $label: a running job has no implicit result before success" \
       "$before" "none" \
       "Job results $label: ...witness: the job was actually claimed" "$claim"

  stale=$($tool ack_result job="$job" lease="$lease" fence=$((fence+1)) \
        version=6 bytes=stale 2>&1 || true)
  case "$stale" in ERR*) stale=rejected ;; *) stale=accepted ;; esac
  chk "Job results $label: a stale fence cannot publish result bytes" \
      "$stale|$($tool get_result job="$job")" "rejected|none"

  $tool ack_result job="$job" lease="$lease" fence="$fence" version=7 bytes=result-value >/dev/null
  chk "Job results $label: fenced success preserves schema version and exact bytes" \
      "$($tool get_result job="$job")" "7|result-value"

  $tool enqueue count=1 prefix="${base}ephemeral" queue="$queue" fp="result-ephemeral-$suffix" \
        sched=1000 retention=0 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=w \
        lease="RESULT-E-$suffix" quantum=1000)
  job=$(echo "$claim" | cut -d'|' -f1); lease=$(echo "$claim" | cut -d'|' -f2); fence=$(echo "$claim" | cut -d'|' -f3)
  before=$($tool ack_result job="$job" lease="$lease" fence="$fence" version=8 bytes=ephemeral)
  chk0 "Job results $label: retention zero deletes result bytes with the completed job" \
       "$($tool get_result job="$job")" "none" \
       "Job results $label: ...witness: the ephemeral result ack committed" "$before"

  $tool enqueue count=1 prefix="${base}evict" queue="$queue" fp="result-evict-$suffix" \
        sched=1000 retention=1 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=w \
        lease="RESULT-X-$suffix" quantum=1000)
  job=$(echo "$claim" | cut -d'|' -f1); lease=$(echo "$claim" | cut -d'|' -f2); fence=$(echo "$claim" | cut -d'|' -f3)
  $tool ack_result job="$job" lease="$lease" fence="$fence" version=9 bytes=evict-me >/dev/null
  before=$($tool get_result job="$job")
  sleep 0.02
  evicted=$($tool evict limit=10)
  chk "Job results $label: eviction removes the retained result with its job" \
      "$before|$evicted|$($tool get_result job="$job")" "9|evict-me|1|none"
  [ "$fail" = "$fail_before" ] && result_cells=$((result_cells+1))
}

echo "== Versioned job-result contract (all stores, both languages) =="
result_contract pg "$H" "Postgres/Rust"
result_contract pg "$G" "Postgres/Go"
result_contract redis "$HR" "Redis/Rust"
result_contract redis "$GR" "Redis/Go"
if mysql_up; then
  result_contract mysql "$HM" "MySQL/Rust"
  result_contract mysql "$GM" "MySQL/Go"
  chk "Job results: all six backend/language cells completed the fenced retention contract" \
      "$result_cells" "6"
else
  skipped "Job results MySQL/Rust + MySQL/Go" "no live MySQL"
fi

# ===================== ROUND 32ai: VERSIONED MID-RUN OUTPUT =====================
# Unlike a final result, output is replaceable while a job is running. The write is
# nevertheless fenced: after a lease turnover, the former holder must not be able to
# replace the new holder's progress. Store time and writer fence travel with the bytes so
# an operator can tell which attempt produced the currently visible value.
output_shape(){
  echo "$1" | awk -F'|' '
    NF == 4 && $1 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ && $4 ~ /^[0-9]+$/ && $4 > 0 {
      print $1 "|" $2 "|" $3 "|store-time"; next
    }
    { print "invalid-output-shape:" $0 }
  '
}

output_cells=0
output_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3"
  local fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local queue="output-$suffix" base="output-$suffix-" claim job old_lease old_fence
  local new_lease new_fence written stale yielded
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
    mysql) $HM sql stmt="DELETE FROM headgate_job WHERE queue='$queue'" >/dev/null ;;
  esac

  $tool enqueue count=1 prefix="${base}kept" queue="$queue" fp="output-fp-$suffix" \
        sched=1000 retention=86400000 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=old \
        lease="OUTPUT-OLD-$suffix" quantum=1000)
  job=$(echo "$claim" | cut -d'|' -f1)
  old_lease=$(echo "$claim" | cut -d'|' -f2)
  old_fence=$(echo "$claim" | cut -d'|' -f3)
  chk0 "Mid-run output $label: a running job has no implicit output" \
       "$($tool get_output job="$job")" "none" \
       "Mid-run output $label: ...witness: holder A actually claimed the job" "$claim"

  written=$($tool write_output job="$job" lease="$old_lease" fence="$old_fence" \
        version=11 bytes=holder-a)
  chk "Mid-run output $label: the store stamps holder A's fence and update time" \
      "$(output_shape "$written")" "11|holder-a|$old_fence|store-time"

  # A one-millisecond retry is a deterministic lease turnover without coupling the
  # contract to any backend's crash backoff. Holder A's identity becomes stale exactly
  # as it would after expiry/reclaim; the in-memory runtime tests exercise that crash path.
  yielded=$($tool ack job="$job" lease="$old_lease" fence="$old_fence" \
        outcome=retry delay=1)
  sleep 0.02
  $tool promote limit=10 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=new \
        lease="OUTPUT-NEW-$suffix" quantum=1000)
  new_lease=$(echo "$claim" | cut -d'|' -f2)
  new_fence=$(echo "$claim" | cut -d'|' -f3)
  chk "Mid-run output $label: lease turnover advances the fence" \
      "$yielded|$job|$new_fence" "ok|$job|$((old_fence+1))"

  written=$($tool write_output job="$job" lease="$new_lease" fence="$new_fence" \
        version=12 bytes=holder-b)
  chk "Mid-run output $label: holder B replaces output under the new fence" \
      "$(output_shape "$written")" "12|holder-b|$new_fence|store-time"
  stale=$($tool write_output job="$job" lease="$old_lease" fence="$old_fence" \
        version=13 bytes=stale-overwrite 2>&1 || true)
  case "$stale" in ERR*) stale=rejected ;; *) stale=accepted ;; esac
  chk "Mid-run output $label: stale holder A cannot overwrite holder B" \
      "$stale|$(output_shape "$($tool get_output job="$job")")" \
      "rejected|12|holder-b|$new_fence|store-time"

  $tool ack job="$job" lease="$new_lease" fence="$new_fence" outcome=success >/dev/null
  chk "Mid-run output $label: retained completion keeps the latest output" \
      "$(output_shape "$($tool get_output job="$job")")" \
      "12|holder-b|$new_fence|store-time"

  $tool enqueue count=1 prefix="${base}ephemeral" queue="$queue" \
        fp="output-ephemeral-$suffix" sched=1000 retention=0 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=e \
        lease="OUTPUT-E-$suffix" quantum=1000)
  job=$(echo "$claim" | cut -d'|' -f1)
  new_lease=$(echo "$claim" | cut -d'|' -f2)
  new_fence=$(echo "$claim" | cut -d'|' -f3)
  written=$($tool write_output job="$job" lease="$new_lease" fence="$new_fence" \
        version=14 bytes=ephemeral)
  $tool ack job="$job" lease="$new_lease" fence="$new_fence" outcome=success >/dev/null
  chk0 "Mid-run output $label: retention zero deletes output with its job" \
       "$($tool get_output job="$job")" "none" \
       "Mid-run output $label: ...witness: ephemeral output was persisted first" \
       "$(output_shape "$written")"
  [ "$fail" = "$fail_before" ] && output_cells=$((output_cells+1))
}

echo "== Versioned mid-run-output contract (all stores, both languages) =="
output_contract pg "$H" "Postgres/Rust"
output_contract pg "$G" "Postgres/Go"
output_contract redis "$HR" "Redis/Rust"
output_contract redis "$GR" "Redis/Go"
if mysql_up; then
  output_contract mysql "$HM" "MySQL/Rust"
  output_contract mysql "$GM" "MySQL/Go"
  chk "Mid-run output: all six backend/language cells completed the fenced replacement contract" \
      "$output_cells" "6"
else
  skipped "Mid-run output MySQL/Rust + MySQL/Go" "no live MySQL"
fi

# ===================== ROUND 32aj: OPERATOR-FACING JOB PROGRESS =====================
# Progress is deliberately distinct from opaque output: it has one portable numeric
# shape the console can render without application decoding. The same lease-turnover
# adversary proves that a displaced worker cannot make the visible bar move backward.
progress_shape(){
  echo "$1" | awk -F'|' '
    NF == 5 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $4 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ && $5 > 0 {
      print $1 "|" $2 "|" $3 "|" $4 "|store-time"; next
    }
    { print "invalid-progress-shape:" $0 }
  '
}

progress_cells=0
progress_contract(){ # backend harness label
  local backend="$1" tool="$2" label="$3"
  local fail_before=$fail
  local suffix="${backend}-${label//[^A-Za-z0-9]/}-$$"
  local queue="progress-$suffix" base="progress-$suffix-" claim job old_lease old_fence
  local new_lease new_fence written stale yielded
  case "$backend" in
    pg) reset_pg ;;
    redis) $RED flushall >/dev/null ;;
    mysql) $HM sql stmt="DELETE FROM headgate_job WHERE queue='$queue'" >/dev/null ;;
  esac

  $tool enqueue count=1 prefix="${base}kept" queue="$queue" fp="progress-fp-$suffix" \
        sched=1000 retention=86400000 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=old \
        lease="PROGRESS-OLD-$suffix" quantum=1000)
  job=$(echo "$claim" | cut -d'|' -f1)
  old_lease=$(echo "$claim" | cut -d'|' -f2)
  old_fence=$(echo "$claim" | cut -d'|' -f3)
  chk0 "Job progress $label: a running job has no implicit progress" \
       "$($tool get_progress job="$job")" "none" \
       "Job progress $label: ...witness: holder A actually claimed the job" "$claim"

  written=$($tool write_progress job="$job" lease="$old_lease" fence="$old_fence" \
        current=25 total=100 message=preparing)
  chk "Job progress $label: the store preserves exact units, message, fence, and store time" \
      "$(progress_shape "$written")" "25|100|preparing|$old_fence|store-time"

  yielded=$($tool ack job="$job" lease="$old_lease" fence="$old_fence" \
        outcome=retry delay=1)
  sleep 0.02
  $tool promote limit=10 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=new \
        lease="PROGRESS-NEW-$suffix" quantum=1000)
  new_lease=$(echo "$claim" | cut -d'|' -f2)
  new_fence=$(echo "$claim" | cut -d'|' -f3)
  chk "Job progress $label: lease turnover advances the writer fence" \
      "$yielded|$job|$new_fence" "ok|$job|$((old_fence+1))"

  written=$($tool write_progress job="$job" lease="$new_lease" fence="$new_fence" \
        current=80 total=100 message=encoding)
  chk "Job progress $label: holder B replaces progress under the new fence" \
      "$(progress_shape "$written")" "80|100|encoding|$new_fence|store-time"
  stale=$($tool write_progress job="$job" lease="$old_lease" fence="$old_fence" \
        current=90 total=100 message=stale 2>&1 || true)
  case "$stale" in ERR*) stale=rejected ;; *) stale=accepted ;; esac
  chk "Job progress $label: stale holder A cannot overwrite holder B" \
      "$stale|$(progress_shape "$($tool get_progress job="$job")")" \
      "rejected|80|100|encoding|$new_fence|store-time"

  $tool ack job="$job" lease="$new_lease" fence="$new_fence" outcome=success >/dev/null
  chk "Job progress $label: retained completion keeps the latest report" \
      "$(progress_shape "$($tool get_progress job="$job")")" \
      "80|100|encoding|$new_fence|store-time"

  $tool enqueue count=1 prefix="${base}ephemeral" queue="$queue" \
        fp="progress-ephemeral-$suffix" sched=1000 retention=0 >/dev/null
  claim=$($tool admit queues="$queue" capacity=1 lease_ms=600000 worker=e \
        lease="PROGRESS-E-$suffix" quantum=1000)
  job=$(echo "$claim" | cut -d'|' -f1)
  new_lease=$(echo "$claim" | cut -d'|' -f2)
  new_fence=$(echo "$claim" | cut -d'|' -f3)
  written=$($tool write_progress job="$job" lease="$new_lease" fence="$new_fence" \
        current=1 total=1 message=done)
  $tool ack job="$job" lease="$new_lease" fence="$new_fence" outcome=success >/dev/null
  chk0 "Job progress $label: retention zero deletes progress with its job" \
       "$($tool get_progress job="$job")" "none" \
       "Job progress $label: ...witness: ephemeral progress was persisted first" \
       "$(progress_shape "$written")"
  [ "$fail" = "$fail_before" ] && progress_cells=$((progress_cells+1))
}

echo "== Operator job-progress contract (all stores, both languages) =="
progress_contract pg "$H" "Postgres/Rust"
progress_contract pg "$G" "Postgres/Go"
progress_contract redis "$HR" "Redis/Rust"
progress_contract redis "$GR" "Redis/Go"
if mysql_up; then
  progress_contract mysql "$HM" "MySQL/Rust"
  progress_contract mysql "$GM" "MySQL/Go"
  chk "Job progress: all six backend/language cells completed the fenced replacement contract" \
      "$progress_cells" "6"
else
  skipped "Job progress MySQL/Rust + MySQL/Go" "no live MySQL"
fi

# §3.2 behavioral conformance: both languages against ONE store, simultaneously.
reset_pg
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('stripe',5,5,5,1000,1000000);" >/dev/null
$G enqueue count=20 prefix=x queue=default rate=stripe fp=fp sched=1000 >/dev/null
r=$($H admit queues=default capacity=100 lease_ms=30000 worker=rw lease=XL quantum=1000 | wc -l)
chk "Go enqueues; Rust admits under the shared fleet limit" "$r" "5"
r=$($G ack job=x1 lease=XL fence=1 outcome=success 2>&1)
chk "Go acks a lease Rust issued" "$r" "ok"

# §13 round 32d THE ESCALATION PATH THROUGH THE GO DRIVER. The widening signal is a
# SENTINEL ROW — dummy values plus a trailing hg_widen — and the Go adapter scans admit.sql
# POSITIONALLY, so a driver that mishandled it would either claim a phantom job or return
# empty. Neither shows up on any fixture whose narrow window is not consumed, which is
# every other Go fixture in this file, so it gets its own. Both languages must escalate to
# the SAME tail from the same fixture: a mixed-runtime fleet cannot disagree about which
# jobs a blocked head hides.
reset_pg
$G enqueue count=10 prefix=ga queue=default fp=ghd sched=1000 retention=86400000 >/dev/null
$G enqueue count=10 prefix=gb queue=default fp=gtl sched=1000 retention=86400000 >/dev/null
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint,kind,crash_count,quarantined_at_ms,reason) VALUES ('ghd','w',3,1000,'poison');" >/dev/null
r=$($G admit queues=default capacity=5 lease_ms=30000 worker=gw lease=GQW quantum=1000 | cut -d'|' -f1 | sort | paste -sd, -)
chk "Go's gate escalates past a blocked head to the same admissible tail" "$r" "gb1,gb2,gb3,gb4,gb5"
reset_pg
$H enqueue count=10 prefix=ga queue=default fp=ghd sched=1000 retention=86400000 >/dev/null
$H enqueue count=10 prefix=gb queue=default fp=gtl sched=1000 retention=86400000 >/dev/null
$PSQL -c "INSERT INTO headgate_quarantine (fingerprint,kind,crash_count,quarantined_at_ms,reason) VALUES ('ghd','w',3,1000,'poison');" >/dev/null
r=$($H admit queues=default capacity=5 lease_ms=30000 worker=rw lease=RQW quantum=1000 | cut -d'|' -f1 | sort | paste -sd, -)
chk "...and Rust's reaches the identical set from the identical fixture" "$r" "gb1,gb2,gb3,gb4,gb5"
$PSQL -c "DELETE FROM headgate_quarantine WHERE fingerprint='ghd';" >/dev/null

# Racing bursts overlap candidate windows, so one round cannot claim everything — the
# skipped workers claim on their NEXT poll, exactly like the real admission loop. Drain
# in rounds; the invariants are zero double-claims AND full coverage by the end.
reset_pg
$H enqueue count=500 prefix=cj queue=default fp=fp sched=1000 >/dev/null
rm -f /tmp/hgxout*; round=0
while [ $round -lt 20 ]; do
  round=$((round+1))
  for i in 1 2 3 4; do
    $H admit queues=default capacity=80 lease_ms=30000 worker=rw$i lease=RL$round-$i quantum=1000 | cut -d'|' -f1 > /tmp/hgxout-$round-r$i &
    $G admit queues=default capacity=80 lease_ms=30000 worker=gw$i lease=GL$round-$i quantum=1000 | cut -d'|' -f1 > /tmp/hgxout-$round-g$i &
  done; wait
  n=$(cat /tmp/hgxout-$round-* | wc -l | tr -d ' ')
  [ "$n" = "0" ] && break
done
dup=$(cat /tmp/hgxout* | sort | uniq -d | wc -l)
total=$(cat /tmp/hgxout* | wc -l)
chk "every job claimed exactly once across languages" "$total" "500"
chk0 "4 Go + 4 Rust workers race one store: zero double-claims" "$dup" "0" \
     "...witness: rows were actually claimed (a dead gate double-claims nothing)" "$total"

# §13 THE ACTIVE-PARTITION SET'S ONE FORBIDDEN DIRECTION. A listed partition with no work
# is a wasted LATERAL probe; a partition holding an available job that is NOT listed is
# silent permanent starvation. Producers and the pruner are raced here on purpose: 12
# concurrent enqueuers (6 Rust + 6 Go, distinct partitions) against 6 concurrent
# promote_due sweeps, which is what prunes. What makes it safe is the lock protocol, not
# luck — producers upsert with ON CONFLICT DO UPDATE so they TAKE the row lock, and the
# pruner re-checks emptiness in a SECOND statement whose READ COMMITTED snapshot is taken
# after the lock. Fusing those two into one statement would reintroduce the bug: every CTE
# in a statement shares one snapshot, so a producer that committed after it is invisible.
reset_pg
for i in $(seq 1 6); do ( for j in $(seq 1 12); do $H promote >/dev/null 2>&1; done ) & done
for p in $(seq 1 6); do
  ( $H enqueue count=25 prefix=rz$p- queue=race partition=P$p fp=fp sched=1000 retention=86400000 >/dev/null 2>&1 ) &
  ( $G enqueue count=25 prefix=gz$p- queue=race partition=Q$p fp=fp sched=1000 retention=86400000 >/dev/null 2>&1 ) &
done
wait
r=$($PSQL -c "SELECT count(*) FROM (SELECT DISTINCT queue, partition_key FROM headgate_job WHERE state='available') j
              WHERE NOT EXISTS (SELECT 1 FROM headgate_active_partition ap
                                WHERE ap.queue=j.queue AND ap.partition_key=j.partition_key);")
# 12 backgrounded producers with `2>&1 >/dev/null`: a producer that DIED is silent, and
# an empty store has no unlisted partition either. The witness is the produced backlog.
chk0 "active-partition set: no partition with work goes unlisted (12 producers vs 6 pruners)" "$r" "0" \
     "...witness: all 12 producers landed their 300 rows across 12 partitions" \
     "$($PSQL -c "SELECT count(*)||'/'||count(DISTINCT partition_key) FROM headgate_job WHERE queue='race' AND state='available';" | grep -x '300/12')"
total=0
for round in $(seq 1 40); do
  n=$($H admit queues=race capacity=200 lease_ms=600000 worker=w1 lease=RZ$round quantum=1000 | wc -l | tr -d ' ')
  total=$((total+n)); [ "$n" = "0" ] && break
done
chk "...and the gate still reaches every one of the 300 jobs" "$total" "300"

# §7 key derivation is an algorithm, not "whatever the Go code does": both languages
# must fingerprint identical (kind, payload) identically, in the live store.
reset_pg
$G enqueue count=1 prefix=fpg queue=default fp=auto kind=parity sched=1000 >/dev/null
$H enqueue count=1 prefix=fpr queue=default fp=auto kind=parity sched=1000 >/dev/null
r=$($PSQL -c "SELECT count(DISTINCT fingerprint) FROM headgate_job WHERE ulid IN ('fpg1','fpr1');")
chk "Go and Rust derive the same fingerprint in the live store" "$r" "1"

# Same shape as the Rust arm above: the COMMIT is the witness, because "wrote nothing"
# and "rolled back" leave an identical table.
gcommitted=$($G tx mode=commit id=gtx2 >/dev/null 2>&1; $PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='gtx2';")
chk "Go transactional enqueue commits with the caller" "$gcommitted" "1"
r=$($G tx mode=rollback id=gtx1 >/dev/null 2>&1; $PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='gtx1';")
chk0 "Go transactional enqueue rolls back with the caller" "$r" "0" \
     "...witness: the same path COMMITS when told to (so absence means rollback)" "$gcommitted"

# §5.2 the crash-suspect ordering contract ACROSS languages: Go seeds the partition and
# crashes the head, Rust's sweep reclaims it, Rust's gate draws next. Both reclaimers
# write the same column off the same store clock, so a fleet running mixed runtimes must
# not disagree about who is at the front — that is exactly the kind of divergence the
# fail-open rate-class bug was.
reset_pg
$G enqueue count=1 prefix=xhola queue=default partition=xhol fp=fp sched=1000 retention=86400000 >/dev/null
$G enqueue count=1 prefix=xholb queue=default partition=xhol fp=fp sched=1001 retention=86400000 >/dev/null
$G enqueue count=1 prefix=xholc queue=default partition=xhol fp=fp sched=1002 retention=86400000 >/dev/null
r=$($G admit queues=default capacity=1 lease_ms=30000 worker=gw lease=GXH quantum=1 | cut -d'|' -f1)
chk "cross-language: Go's gate takes the same partition head" "$r" "xhola1"
$PSQL -c "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE ulid='xhola1';" >/dev/null
r=$($H reclaim | cut -d'|' -f1)
chk "...Rust's sweep reclaims the lease Go issued" "$r" "xhola1"
sleep 1.2
$H promote >/dev/null
r=$($H admit queues=default capacity=2 lease_ms=30000 worker=rw lease=RXH quantum=10 | idset)
chk "...and Rust's gate yields B and C before the job Go crashed" "$r" "xholb1,xholc1"

# §3.2's headline sentence, executable: each language's REAL runtime (dispatch, handler,
# ack) executes jobs the OTHER language enqueued.
reset_pg
$H enqueue count=3 prefix=r2g queue=xlang payload='{}' fp=auto sched=1000 retention=86400000 >/dev/null
r=$($G drain queues=xlang count=10)
chk "Rust enqueues; the Go runtime executes" "$r" "3"
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE queue='xlang' AND state='completed';")
chk "...and all three completed" "$r" "3"
$G enqueue count=3 prefix=g2r queue=xlang payload='{}' fp=auto sched=1000 retention=86400000 >/dev/null
r=$($H drain queues=xlang count=10)
chk "Go enqueues; the Rust runtime executes" "$r" "3"
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE queue='xlang' AND state='completed';")
chk "...and all six completed" "$r" "6"

# ----- ROUND 32K, ACROSS THE LANGUAGE BOUNDARY -----
# §5.7 WHAT "THE CURSOR CROSSES" ACTUALLY MEANS, asserted rather than assumed.
#
# The two APIs are NOT the same shape. Rust's `set_cursor(Vec<u8>)` takes RAW BYTES and
# hands the closure a raw `Option<Vec<u8>>` back; Go's `SetCursor[C]` / `StepCursor[C]` are
# GENERIC and JSON-encode/decode whatever C is. At the port, the store column
# (`cp_cursor` bytea / the Redis hash field) is opaque bytes in both languages — so the
# languages are interoperable if and ONLY IF the raw side writes bytes the generic side can
# decode into its C. There is no adapter and no negotiation; the BYTES are the contract.
#
# So both harnesses write `{"page":N}`: Go by marshalling `struct{Page int64 \`json:"page"\`}`,
# Rust by formatting those exact bytes by hand. These four assertions are what pins it —
# a Rust runtime that started writing bincode, or a Go cursor type that gained a field,
# turns them red rather than stranding one language's jobs at page 1 in production.
reset_pg
$H enqueue count=1 prefix=xcr queue=xcurq fp=fpxcr sched=1000 retention=86400000 payload='{}' >/dev/null
r=$($H cursor queues=xcurq pages=6 stop=3)
chk "xlang §5.7: Rust's raw-byte cursor step persists and stops where it was interrupted" \
    "$r" "resumed_from=0|processed=1,2,3|outcome=rate_limited"
r=$($PSQL -c "SELECT encode(cp_cursor,'escape') FROM headgate_job WHERE ulid='xcr1';")
chk "xlang §5.7: ...and what it wrote is the JSON Go's generic SetCursor would have written" \
    "$r" '{"page":3}'
r=$($G cursor queues=xcurq pages=6)
chk "xlang §5.7: Go's GENERIC StepCursor decodes the cursor Rust wrote as RAW BYTES and resumes" \
    "$r" "resumed_from=3|processed=4,5,6|outcome=success"
# ...and the other direction, which is the one that could rot silently: Go writes through
# json.Marshal, Rust reads bytes it never negotiated for.
reset_pg
$G enqueue count=1 prefix=xcg queue=xcurq fp=fpxcg sched=1000 retention=86400000 payload='{}' >/dev/null
$G cursor queues=xcurq pages=6 stop=2 >/dev/null
r=$($PSQL -c "SELECT encode(cp_cursor,'escape') FROM headgate_job WHERE ulid='xcg1';")
chk "xlang §5.7: Go's marshalled cursor is the same bytes, not a Go-flavoured encoding" "$r" '{"page":2}'
r=$($H cursor queues=xcurq pages=6)
chk "xlang §5.7: ...and RUST resumes from a cursor Go marshalled, at page 3 and not page 1" \
    "$r" "resumed_from=2|processed=3,4,5,6|outcome=success"

# §4 the per-attempt timeout and absolute deadline, through the GO runtime on the SAME
# store. Go cancellation is COOPERATIVE (no task abort), so this is not the same mechanism
# as Rust's `tokio::time::timeout` — it is a context deadline the handler must observe and
# a rewrite of the resulting error. Same envelope fields, same outcome, same message.
reset_pg
$G enqueue count=1 prefix=gtmo queue=gtmoq fp=fpgtmo sched=1000 retention=86400000 payload='{}' timeout=50 >/dev/null
r=$($G drain queues=gtmoq count=1 sleep=400)
chk "xlang §4 timeout: the Go runtime really drew and ran the job" "$r" "1"
r=$($PSQL -c "SELECT state||'/'||attempt||'/'||crash_attempt FROM headgate_job WHERE ulid='gtmo1';")
chk "xlang §4 timeout: Go's over-running attempt is a RETRY that CONSUMES an attempt too" "$r" "retryable/1/0"
r=$($PSQL -c "SELECT count(*) FROM headgate_job WHERE ulid='gtmo1' AND errors::text LIKE '%attempt timed out after 50ms%';")
chk "xlang §4 timeout: ...naming the timeout in the SAME words Rust uses" "$r" "1"
$G enqueue count=1 prefix=gtmok queue=gtmokq fp=fpgtmok sched=1000 retention=86400000 payload='{}' >/dev/null
$G drain queues=gtmokq count=1 sleep=400 >/dev/null
r=$($PSQL -c "SELECT state||'/'||attempt FROM headgate_job WHERE ulid='gtmok1';")
chk "xlang §4 timeout: ...control: the same 400ms Go handler with NO timeout completes" "$r" "completed/0"
$G enqueue count=1 prefix=gdln queue=gdlnq fp=fpgdln sched=1000 retention=86400000 payload='{}' deadline=1000 >/dev/null
$G drain queues=gdlnq count=1 >/dev/null
r=$($PSQL -c "SELECT state||'/'||attempt FROM headgate_job WHERE ulid='gdln1';")
chk "xlang §4 deadline: Go archives an exceeded deadline and spends NO attempt, exactly as Rust does" \
    "$r" "archived/0"

# §5.5 the BACKLOG DERIVATIVES, computed independently by the two adapters over ONE store.
# The Rust-vs-Go GET /queues byte diff empties `headgate_queue_counter` first, so it has
# never once compared these three numbers; this does.
reset_pg
$H enqueue count=10 prefix=xbd queue=xbdq fp=fpxbd sched=1000 retention=86400000 >/dev/null
seed "xlang §5.5 backlog fixture: 120 arrivals and 180 completions in the current minute" \
     "DELETE FROM headgate_queue_counter WHERE queue='xbdq';
      INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived, completed)
      VALUES ('xbdq', (extract(epoch from clock_timestamp())*1000)::bigint/60000*60000, 120, 180);"
rustrates=$($H qstats queue=xbdq)
gorates=$($G qstats queue=xbdq)
chk "xlang §5.5 derivatives: Rust's adapter computes 2.0 / 3.0 / 10s from the shared counters" \
    "$rustrates" "xbdq|2.000|3.000|10000"
chk "xlang §5.5 derivatives: ...and Go's independent SQL agrees to the byte" \
    "$gorates" "$rustrates"
rusage=$($H qstats queue=xbdq age=1 | awk -F'|' '{print $5}')
goage=$($G qstats queue=xbdq age=1 | awk -F'|' '{print $5}')
age_delta=$((rusage - goage)); [ "$age_delta" -lt 0 ] && age_delta=$((-age_delta))
age_agree="too-far-apart:$rusage/$goage"
if [ "$rusage" -gt 1000 ] && [ "$goage" -gt 1000 ] && [ "$age_delta" -le 2000 ]; then
  age_agree=agree
fi
chk "xlang §5.5 age-of-oldest: Rust and Go independently derive the same store-clock age" \
    "$age_agree" "agree"

# §3.2 THE KEYSPACE DIFF. The same scenario driven once per language against a fresh
# store; the resulting stores must be byte-identical after normalizing what legitimately
# differs across runs (store-clock timestamps, backoff jitter, bucket refill state).
# This is what catches field-ordering and hash-derivation drift before users do.
xlang_scenario(){ # $1 = harness binary
  reset_pg
  $PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('kx',3,3,3,60000,1000000);" >/dev/null
  # Round 32k: a job left MID-CURSOR-STEP, FIRST in the scenario and in its own queue.
  # First because `perform_job` promotes due jobs before admitting and a retry backoff
  # coming due mid-scenario would make the snapshot a coin flip; its own queue because the
  # capacity-1 admit must be deterministic about WHICH job it draws. Until this round the
  # diff's `encode(coalesce(cp_cursor, ''::bytea),'hex')` compared empty against empty on
  # every run — the column, the checkpoint's `cursor_step` field and Redis's
  # checkpoint.lua cursor branch were all transported by a diff that could not see them.
  $1 enqueue count=1 prefix=kc queue=kdiffc payload='{}' fp=auto kind=w sched=1000 retention=86400000 >/dev/null
  $1 cursor queues=kdiffc pages=4 stop=2 >/dev/null
  $1 enqueue count=3 prefix=ks queue=kdiff payload='{"n":1}' fp=auto kind=kx rate=kx sched=1000 retention=86400000 >/dev/null
  $1 enqueue count=1 prefix=ku queue=kdiff payload='{}' fp=auto kind=ku unique=KDU sched=1000 >/dev/null
  $1 enqueue count=1 prefix=kt queue=kdiff payload='{}' fp=auto kind=kt unique=KDT window=3600000 sched=1000 >/dev/null
  $1 enqueue count=1 prefix=kf queue=kdiff payload='{}' fp=auto kind=kf sched=99999999999999 >/dev/null
  $1 admit queues=kdiff capacity=3 lease_ms=60000 worker=wX lease=LX quantum=100 >/dev/null
  $1 ack job=ks1 lease=LX fence=1 outcome=success >/dev/null
  $1 ack job=ks2 lease=LX fence=1 outcome=retry err=boom >/dev/null
  $1 ack job=ks3 lease=LX fence=1 outcome=skip err=nope >/dev/null
  $PSQL <<'SQL'
SELECT ulid, kind, schema_version, encode(payload,'hex'), queue, partition_key,
       rate_class, fingerprint, priority, attempt, crash_attempt, max_attempts,
       state, fence, retention_ms,
       encode(coalesce(unique_key, ''::bytea), 'hex'), unique_states,
       (unique_expires_at_ms IS NOT NULL) AS throttle_held,
       checkpoint::text, encode(coalesce(cp_cursor, ''::bytea), 'hex'),
       COALESCE((SELECT jsonb_agg(e - 'at_ms' ORDER BY ord)
                 FROM jsonb_array_elements(errors) WITH ORDINALITY t(e, ord)),
                '[]'::jsonb)::text,
       (lease_id IS NOT NULL) AS leased
FROM headgate_job ORDER BY ulid;
SELECT name, burst, limit_per_window, window_ms FROM headgate_rate_bucket ORDER BY name;
SELECT queue, partition_key, deficit FROM headgate_partition_deficit ORDER BY 1, 2;
SQL
}
xlang_scenario "$G" > /tmp/hgx-keyspace-go.txt
xlang_scenario "$H" > /tmp/hgx-keyspace-rust.txt
lines=$(wc -l < /tmp/hgx-keyspace-go.txt)
chk "keyspace snapshot is non-trivial (no vacuous pass)" "$((lines >= 8))" "1"
# Round 32k: and specifically NON-EMPTY where it used to be structurally empty. `cp_cursor`
# is hex-encoded in the snapshot; `7b22706167653a` is `{"page` — without this the diff
# would go on comparing two absent cursors and calling it agreement.
chk "keyspace snapshot carries a NON-EMPTY cursor in both languages (it never did before)"     "$(grep -c '7b2270616765' /tmp/hgx-keyspace-go.txt)|$(grep -c '7b2270616765' /tmp/hgx-keyspace-rust.txt)" "1|1"
if cmp -s /tmp/hgx-keyspace-go.txt /tmp/hgx-keyspace-rust.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-keyspace-go.txt /tmp/hgx-keyspace-rust.txt | head -10; fi
chk "keyspace diff: Go-driven and Rust-driven stores match byte-for-byte" "$d" "identical"

# §4.4b/§5.9 ACROSS the language boundary on one Postgres: Go writes, Rust re-enqueues.
# Identical content is idempotent from the other side; different content is the SAME
# typed conflict with the SAME message, and the malformed kind is refused identically.
reset_pg
$G enqueue count=1 prefix=xidc queue=default payload='{"n":1}' fp=auto sched=1000 retention=86400000 >/dev/null
r=$($H enqueue count=1 prefix=xidc queue=default payload='{"n":1}' fp=auto sched=1000 retention=86400000 2>&1)
chk "xlang §4.4b: Go enqueues, Rust re-enqueues IDENTICAL -> idempotent success" "$r" "1"
r=$($PSQL -c "SELECT count(*) FROM headgate_job;")
chk "xlang §4.4b: ...and the job is NOT duplicated" "$r" "1"
r=$($H enqueue count=1 prefix=xidc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "xlang §4.4b: Go enqueues, Rust re-enqueues DIFFERENT -> id conflict" "$r" "ERR id conflict: job xidc1"
r=$($G enqueue count=1 prefix=xidc queue=default payload='{"n":2}' fp=auto sched=1000 retention=86400000 2>&1)
chk "xlang §4.4b: ...and Go reports that conflict byte for byte" "$r" "ERR id conflict: job xidc1"
r=$($G enqueue count=1 prefix=xbk queue=default kind='bad kind' payload='{}' fp=auto sched=1000 2>&1)
chk "xlang §5.9: Go's store refuses the same malformed kind, same message" "$r" "ERR $KINDMSG"

# ----- §8.4 TRACE CONTEXT ON THE ENVELOPE (round 32), cross-language over Postgres.
# The 🔶 this closes was "headers CAN carry traceparent but it is not specified, so
# implementations will diverge". Two properties, both asserted rather than assumed:
#
#  1. ROUND TRIP. Go enqueues with a valid traceparent + tracestate; RUST admits and
#     reads back the IDENTICAL value. The headers are opaque bytes to the store, so
#     "identical" means byte-identical, not semantically equal.
#  2. INVALID = ABSENT, NEVER AN ERROR. A malformed traceparent is a normal enqueue and
#     a normal dispatch; the raw header still round-trips verbatim (the store did not
#     eat it), and the PARSE reports absent. Both languages, both directions — this is
#     the half that would have diverged, because "be lenient" without a written rule
#     means one runtime accepting uppercase hex and the other refusing it.
#
# admit_trace prints: ulid|<raw traceparent>|<parsed, re-rendered>|<tracestate>
reset_pg
TP='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
TPBAD='00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01'   # uppercase: invalid
$G enqueue count=1 prefix=xtp queue=xtp fp=fp sched=1000 retention=86400000 \
   tp="$TP" ts='vendor=opaque,other=1' >/dev/null
r=$($H admit_trace queues=xtp capacity=1 lease_ms=600000 worker=rw lease=XTP quantum=100)
chk "xlang §8.4: Go enqueues traceparent -> Rust reads back the IDENTICAL value" \
    "$r" "xtp1|$TP|$TP|vendor=opaque,other=1"
reset_pg
# ...and the same in the other direction, so neither language is merely echoing itself.
$H enqueue count=1 prefix=xtp queue=xtp fp=fp sched=1000 retention=86400000 \
   tp="$TP" ts='vendor=opaque,other=1' >/dev/null
r=$($G admit_trace queues=xtp capacity=1 lease_ms=600000 worker=gw lease=XTP quantum=100)
chk "xlang §8.4: Rust enqueues traceparent -> Go reads back the IDENTICAL value" \
    "$r" "xtp1|$TP|$TP|vendor=opaque,other=1"
reset_pg
$G enqueue count=1 prefix=xtb queue=xtb fp=fp sched=1000 retention=86400000 tp="$TPBAD" >/dev/null
r=$($H admit_trace queues=xtb capacity=1 lease_ms=600000 worker=rw lease=XTB quantum=100)
chk "xlang §8.4: an INVALID traceparent is ABSENT in Rust, and the raw header survives" \
    "$r" "xtb1|$TPBAD||"
reset_pg
$H enqueue count=1 prefix=xtb queue=xtb fp=fp sched=1000 retention=86400000 tp="$TPBAD" >/dev/null
r=$($G admit_trace queues=xtb capacity=1 lease_ms=600000 worker=gw lease=XTB quantum=100)
chk "xlang §8.4: ...and ABSENT in Go too, byte for byte the same answer" \
    "$r" "xtb1|$TPBAD||"
reset_pg

# §10.1 "both implementations serve this spec, and the conformance suite asserts
# identical responses" — both APIs over ONE store state, JSON key-normalized (jq -S)
# and diffed. Only time-stable responses are compared: paused rate class (frozen
# tokens), counters cleared (rates 0), admission cases whose ETA is 0 or null.
command -v jq >/dev/null || { echo "FATAL: jq not found; the API diff needs it"; exit 2; }
cargo build -q -p headgate-api --bin hg-api || { echo "FATAL: rust api build failed"; exit 2; }
(cd go && go build -o ../target/debug/hg-go-api ./headgateapi/cmd/hg-go-api) \
  || { echo "FATAL: go api build failed"; exit 2; }

reset_pg
seed "§10.1 GET fixtures (rate class, quarantine, schedule, paused queue)" \
     "INSERT INTO headgate_rate_bucket VALUES ('apix-rc',0,5,0,1000,1000);
      INSERT INTO headgate_quarantine VALUES ('apix-fp','k',3,1234,'\x00','crash limit reached');
      INSERT INTO headgate_schedule (id,kind,payload,queue,spec,next_run_ms) VALUES ('apix-s','k','\x','apix','@every:60000',99999999999999);
      INSERT INTO headgate_queue_state (queue,paused) VALUES ('apix-paused',true);"
# Round 32g: `ON CONFLICT (queue) DO UPDATE` above is not cosmetic. `reset_pg` does not
# truncate `headgate_queue_state`, so on EVERY run after the first that INSERT hit a
# duplicate key — and because psql wraps a multi-statement `-c` in ONE transaction, the
# whole seed rolled back with it. `apix-rc` and `apix-fp` were never written, so
# `GET /rate-classes` and `GET /quarantine` compared `[]` against `[]` on both servers
# and both backends: two of the twelve diffed endpoints were passing VACUOUSLY. The
# duplicate-key print that four rounds recorded as harmless noise was the symptom.
#
# §11.2 round 32: the fixed worker registry GET /cluster is diffed over. Its OWN psql -c
# on purpose — psql wraps a multi-statement -c in ONE transaction, so sharing it with a
# seed that can abort would silently roll this back too and leave the registry full of
# whatever earlier tests left behind.
# heartbeat_at_ms far in the future = live under the 15-minute grace; the third row is
# ancient, i.e. stale but still REGISTERED, which is what makes apix-paused the honest
# zero-coverage case rather than a queue nobody ever claimed.
seed "§11.2 the fixed three-worker registry" \
     "DELETE FROM headgate_worker;
      INSERT INTO headgate_worker
        (worker_id,host,pid,queues,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls)
      VALUES ('apix-w1','h1',1,'{apix}',8,1000,99999999999999,6,10,2),
             ('apix-w2','h2',2,'{apix,apix-other}',4,1000,99999999999999,1,10,3),
             ('apix-dead','h3',3,'{apix-paused}',9,1000,1000,0,0,0);"
$H enqueue count=4 prefix=apix queue=apix payload='{}' fp=auto retention=86400000 >/dev/null
$H enqueue count=1 prefix=apxp queue=apix-paused payload='{}' fp=auto >/dev/null
$H admit queues=apix capacity=2 lease_ms=600000 worker=wA lease=LA quantum=1000 >/dev/null
$H ack job=apix1 lease=LA fence=1 outcome=retry err=boom >/dev/null
$H ack job=apix2 lease=LA fence=1 outcome=success >/dev/null
$PSQL -c "DELETE FROM headgate_queue_counter; DELETE FROM headgate_partition_counter;" >/dev/null
# Queue AND quiet-group rates are now time-stable at 0. Clearing only the queue-level
# table left per-partition buckets behind; two sequential API snapshots could straddle a
# minute boundary and disagree when the older bucket aged out between them.

HG_API_ADDR=127.0.0.1:8091 target/debug/hg-api >/dev/null 2>&1 & RUST_API_PID=$!
HG_API_ADDR=127.0.0.1:8092 target/debug/hg-go-api >/dev/null 2>&1 & GO_API_PID=$!
disown $RUST_API_PID $GO_API_PID 2>/dev/null || true
trap 'kill $RUST_API_PID $GO_API_PID 2>/dev/null' EXIT
for i in $(seq 1 50); do
  curl -sf 127.0.0.1:8091/api/v1/healthz >/dev/null && curl -sf 127.0.0.1:8092/api/v1/healthz >/dev/null && break
  sleep 0.2
done

api_snapshot(){ # $1 = port
  for ep in "meta" "jobs?queue=apix&limit=50" "jobs/apix3" "jobs/apix3/admission" \
            "jobs/apxp1/admission" "jobs/counts?queue=apix" "rate-classes" \
            "partitions?queue=apix" "quarantine" "periodic" "queues" "cluster"; do
    echo "== $ep =="
    # jq 1.7 preserves number literals (0.0 vs 0); `+ 0` forces canonical numerics.
    # Ages are sampled from store time on each request, so two sequential servers are
    # expected to differ by milliseconds. Preserve null-vs-present and field shape while
    # normalizing only the dynamic value; the live store assertions above pin arithmetic.
    curl -sf "127.0.0.1:$1/api/v1/$ep" | jq -S '
      walk(
        if type == "object" and has("oldest_available_ms") and .oldest_available_ms != null
        then .oldest_available_ms = "<dynamic-age>"
        elif type == "number" then . + 0
        else . end
      )'
  done
}

# ----- round 32h: THE PER-ENDPOINT FIXTURE WITNESS. This is the round-32g bug's shape,
# generalized and closed. That bug was: the API seed rolled back, so `GET /rate-classes`
# and `GET /quarantine` returned `[]` from BOTH servers and the byte diff of two empty
# lists passed — for many rounds. The only guard was `lines >= 100`, which is an
# AGGREGATE: one endpoint collapsing to `[]` hides comfortably inside eleven that did
# not. So every one of the twelve diffed endpoints is now asserted to carry the identity
# the seed put there, INDIVIDUALLY, on every backend. A seed that half-lands now fails
# on the endpoint it half-landed for, and names it.
#
# Asserted on the Rust snapshot alone on purpose: the byte diff immediately below proves
# the Go snapshot is the same file, so witnessing both would assert the same fact twice.
api_witness(){ # $1 = snapshot file, $2 = label, $3 = the backend /meta must name
  local F="$1" L="$2" B="$3"
  # Print the body of one `== ep ==` block and count a needle in it. Scoped to the block
  # so a value that happens to appear under a DIFFERENT endpoint cannot stand in.
  wit(){ chk "$L GET /$1 carries its fixture ($2)" \
             "$(awk -v e="== $1 ==" '$0==e{f=1;next} /^== /{f=0} f' "$F" | grep -Fc -- "$2")" "1"; }
  wit "meta"                       '"inspect"'
  # Round 32h: /meta's `backend` was the literal "postgres" in BOTH servers, so it
  # claimed postgres while fronting Redis and MySQL — and the byte diff could not see
  # it, because the two servers were wrong in exactly the same way. That is the one
  # failure a diff structurally cannot catch. Pinned per backend, as a literal.
  wit "meta"                       "\"backend\": \"$B\""
  wit "jobs?queue=apix&limit=50"   '"id": "apix3"'
  wit "jobs/apix3"                 '"id": "apix3"'
  wit "jobs/apix3/admission"       '"admissible": true'
  wit "jobs/apxp1/admission"       '"blocked_by": "queue_paused"'
  wit "jobs/counts?queue=apix"     '"available": 2'
  wit "rate-classes"               '"name": "apix-rc"'
  wit "partitions?queue=apix"      '"waiting": 2'
  wit "quarantine"                 '"fingerprint": "apix-fp"'
  wit "periodic"                   '"id": "apix-s"'
  wit "queues"                     '"queue": "apix"'
  # /cluster reports no worker ids, so the witness is the queue only apix-w2 declares:
  # it appears in the union ONLY because the three-row registry landed.
  wit "cluster"                    '"queue": "apix-other"'
}

# ----- §11.2 + §5.5 the CLUSTER VIEW's actual VALUES (round 32). The diff above proves
# the two servers agree; these prove they agree on the RIGHT thing — two servers can
# match each other while both being wrong, which is the one failure a diff cannot see.
# Registry seeded above: w1 live (cap 8, inflight 6, 10 polls / 2 empty, serves apix),
# w2 live (cap 4, inflight 1, 10/3, serves apix + apix-other), dead STALE (cap 9,
# serves apix-paused).
cluster_asserts(){ # $1 = port, $2 = label
  local j; j=$(curl -sf "127.0.0.1:$1/api/v1/cluster")
  chk "$2 /cluster: live/stale/total from the fixed registry" \
      "$(echo "$j" | jq -c '.workers')" '{"live":2,"stale":1,"total":3}'
  chk "$2 /cluster: capacity and in-flight sum over LIVE workers only" \
      "$(echo "$j" | jq -c '[.capacity_total,.inflight_total]')" '[12,7]'
  # A ratio of SUMS, not a mean of per-worker ratios: w1 is 0.75 and w2 is 0.25, whose
  # mean is 0.5 — the honest fleet number is 7/12, because a 4-slot worker must not
  # weigh the same as an 8-slot one.
  chk "$2 /cluster: fleet utilization is 7/12, not the 0.5 mean of the two ratios" \
      "$(echo "$j" | jq '.utilization == 7/12 and .utilization != 0.5')" "true"
  chk "$2 /cluster: fleet empty-poll ratio is 5/20 over the reported windows" \
      "$(echo "$j" | jq -c '[.empty_poll_ratio,.polls_total,.empty_polls_total]')" "[0.25,20,5]"
  # THE operational answer this endpoint exists for. apix-paused HAS a worker row — it
  # is just stale — so a naive "join workers to queues" would have called it covered.
  # `jq` on an absent queue prints NOTHING, which trims to "" — a different failure from
  # the 0 under test, and one `chk` would have called a miss. The witness pins that the
  # queue is LISTED at all, which is the half of this contract that a naive
  # "join workers to queues" implementation gets wrong.
  chk0 "$2 /cluster: a queue served only by a STALE worker reports ZERO live workers" \
      "$(echo "$j" | jq -r '.queues[]|select(.queue=="apix-paused")|.live_workers')" "0" \
      "$2 /cluster: ...and that queue is LISTED at all (the union runs both ways)" \
      "$(echo "$j" | jq -c '[.queues[]|select(.queue=="apix-paused")|.queue]')"
  # ...and the union runs the other way too: a queue with a live worker and no jobs at
  # all is still listed, so coverage is reported from the fleet, not from the backlog.
  chk "$2 /cluster: a queue with live workers and no jobs is still listed" \
      "$(echo "$j" | jq -r '.queues[]|select(.queue=="apix-other")|.live_workers')" "1"
}
# ----- INVARIANT 9, THE PAYLOAD DEFAULT (round 32i) -----
# "Job payloads are not returned unless explicitly requested. `include_payload` defaults
# to false everywhere. Payloads carry PII and this console mounts at /admin."
#
# Nothing asserted this. `api_snapshot` fetches `jobs/apix3` WITHOUT the parameter, so a
# server that defaulted the flag to TRUE emitted a payload into the snapshot — on BOTH
# servers, identically — and the byte diff is structurally blind to that, exactly as it
# was blind to /meta's backend in round 32h. Mutation-tested in round 32i by defaulting
# the flag to true in both servers: 344 of 344 assertions stayed green while every
# `GET /jobs/{id}` leaked its payload.
#
# Run against BOTH servers rather than only the Rust one, because the whole point is a
# default that can drift per language and cancel out in the diff. The second assertion is
# the contrast: with the flag explicitly on, the key IS there — so the first is "withheld",
# not "this store has no payloads".
# ----- INVARIANT 16, THE POLICY WRITE-BACK LOOP (round 32i) -----
# "Any policy the gate reads, the API can write. A fleet-wide limit you cannot change
# without a redeploy is not an operational feature. That includes a `paused` kill switch
# per rate class."
#
# Every rate-class fixture in this file is seeded with psql or redis-cli, and the §10.1
# mutation sequence PUTs rate classes but never READS one back — so a
# `PUT /rate-classes/{name}` that answered 200 and wrote NOTHING was invisible. Both
# servers would be wrong identically, which is the one failure the parity diff cannot see
# (the same shape as round 32h's /meta backend). Mutation-tested in round 32i by making
# both handlers accept and discard: 364 of 364 assertions stayed green while a fleet limit
# became unchangeable without a redeploy.
#
# Two assertions, because the invariant names two things: the LIMIT, and the per-class
# `paused` KILL SWITCH — which is a separate field a handler can drop on its own (Go's
# already shipped one bug of exactly that kind: a body missing `limit` created the class
# at limit 0, i.e. paused it by accident).
policy_writeback_asserts(){ # $1 = port, $2 = label, $3 = url-safe id
  local P="127.0.0.1:$1/api/v1" n="inv16-$3" q="inv16q-$3" cl="inv16cl-$3"
  curl -s -o /dev/null -X PUT "$P/rate-classes/$n" -H 'content-type: application/json' \
       -H "Idempotency-Key: $n-a" -d '{"limit":7,"window_ms":60000,"burst":7}'
  chk "$2 invariant 16: a fleet rate limit WRITTEN through the API is readable back" \
      "$(curl -sf "$P/rate-classes" | jq -c "[.[]|select(.name==\"$n\")|[.limit_per_window,.window_ms]]")" \
      "[[7,60000]]"
  curl -s -o /dev/null -X PUT "$P/rate-classes/$n" -H 'content-type: application/json' \
       -H "Idempotency-Key: $n-b" -d '{"limit":9,"window_ms":60000,"burst":9,"paused":true}'
  # `paused` is not a stored column: the kill switch IS limit 0 with an empty bucket, so
  # the gate needs no extra predicate to honour it (see upsert_rate_class). Asserting the
  # PAIR pins that the route understood `paused` AND that the store encoded it the one way
  # the gate reads — a handler that stored `limit: 9` and dropped the flag reads [[9,false]].
  chk "$2 invariant 16: ...and so is the per-class PAUSED kill switch, on the same route" \
      "$(curl -sf "$P/rate-classes" | jq -c "[.[]|select(.name==\"$n\")|[.limit_per_window,.burst,.paused]]")" \
      "[[0,9,true]]"

  curl -s -o /dev/null -X PUT "$P/queues/$q" -H 'content-type: application/json' \
       -H "Idempotency-Key: $q" -d '{"weight":3}'
  chk "$2 invariant 16: queue-selection WEIGHT written through the API is readable back" \
      "$(curl -sf "$P/queues" | jq -c "[.[]|select(.queue==\"$q\")|.weight]")" \
      "[3]"

  curl -s -o /dev/null -X PUT "$P/concurrency-limits/$cl" -H 'content-type: application/json' \
       -H "Idempotency-Key: $cl" \
       -d "{\"queue\":\"$q\",\"max_concurrent\":2,\"on_saturated\":\"cancel_running\"}"
  chk "$2 invariant 16: concurrency SATURATION strategy is writable and readable as policy" \
      "$(curl -sf "$P/concurrency-limits" | jq -c "[.[]|select(.name==\"$cl\")|[.queue,.max_concurrent,.on_saturated]]")" \
      "[[\"$q\",2,\"cancel_running\"]]"
}

# ----- EXPLICIT GAP 7, PRODUCER BACKPRESSURE (round 32t) -----
# The producer decision is made by the store, but the HTTP contract has to preserve its
# typed details so callers can distinguish capacity pressure from a malformed job.  Use
# a process-scoped queue name: the final assertion deliberately leaves two unfinished
# jobs behind, and a repeated gate run must exercise a fresh limit rather than replaying
# yesterday's ids at an already-full queue.
enqueue_backpressure_api_asserts(){ # $1 = port, $2 = label, $3 = url-safe id
  local P="127.0.0.1:$1/api/v1" q="bpapi-$3-$$" out="/tmp/hgx-bp-$3-$$.json" code

  code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$P/queues/$q/enqueue-limit" \
       -H 'content-type: application/json' -H "Idempotency-Key: $q-limit" \
       -d '{"max_unfinished_jobs":1}')
  chk "$2 enqueue backpressure: the operational limit route accepts the policy" "$code" "200"

  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$P/jobs" \
       -H 'content-type: application/json' -H "Idempotency-Key: $q-job-1" \
       -d "{\"id\":\"$q-job-1\",\"kind\":\"bp\",\"queue\":\"$q\",\"payload\":\"e30=\"}")
  chk "$2 enqueue backpressure: the first job inside capacity is accepted" "$code" "201"

  code=$(curl -s -o "$out" -w '%{http_code}' -X POST "$P/jobs" \
       -H 'content-type: application/json' -H "Idempotency-Key: $q-job-2" \
       -d "{\"id\":\"$q-job-2\",\"kind\":\"bp\",\"queue\":\"$q\",\"payload\":\"e30=\"}")
  chk "$2 enqueue backpressure: a producer over capacity receives HTTP 429" "$code" "429"
  chk "$2 enqueue backpressure: 429 preserves queue, limit, current, and incoming demand" \
      "$(jq -c '[.error,.queue,.limit,.current,.incoming]' "$out")" \
      "[\"enqueue backpressure\",\"$q\",1,1,1]"
  chk "$2 enqueue backpressure: queue inspection reports the same exact counter and limit" \
      "$(curl -sf "$P/queues" | jq -c "[.[]|select(.queue==\"$q\")|[.unfinished_jobs,.max_unfinished_jobs]]")" \
      "[[1,1]]"

  code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$P/queues/$q/enqueue-limit" \
       -H "Idempotency-Key: $q-clear")
  chk "$2 enqueue backpressure: DELETE disables the producer limit" "$code" "204"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$P/jobs" \
       -H 'content-type: application/json' -H "Idempotency-Key: $q-job-3" \
       -d "{\"id\":\"$q-job-3\",\"kind\":\"bp\",\"queue\":\"$q\",\"payload\":\"e30=\"}")
  chk "$2 enqueue backpressure: intake resumes after the limit is disabled" "$code" "201"
  chk "$2 enqueue backpressure: inspection shows growth resumed with no configured limit" \
      "$(curl -sf "$P/queues" | jq -c "[.[]|select(.queue==\"$q\")|[.unfinished_jobs,.max_unfinished_jobs]]")" \
      "[[2,null]]"
}

payload_asserts(){ # $1 = port, $2 = label
  local base="127.0.0.1:$1/api/v1/jobs/apix3" shown
  # The witness is the CONTRAST, and it has to come first: "no payload key" is also what
  # a 404, a broken fixture and an empty store produce. Asking explicitly must return a
  # NON-EMPTY one, or there was nothing to withhold and the default proves nothing.
  shown=$(curl -sf "$base?include_payload=true" \
          | jq -r 'if has("payload") and (.payload|length) > 0 then "payload returned on request" else "" end')
  chk0 "$2 invariant 9: GET /jobs/{id} withholds the payload by DEFAULT (PII, console at /admin)" \
       "$(curl -sf "$base" | jq -c 'has("payload")')" "false" \
       "$2 invariant 9: ...witness/contrast: explicitly asking really does return one" "$shown"
  # The LIST endpoint has no opt-in at all — it passes `false` unconditionally — so a
  # page of 50 jobs can never become a page of 50 payloads.
  chk "$2 invariant 9: ...and the LIST endpoint has no opt-in, whatever the caller asks" \
      "$(curl -sf "127.0.0.1:$1/api/v1/jobs?queue=apix&limit=50&include_payload=true" | jq -c '[.jobs[]|has("payload")]|unique')" \
      "[false]"
}

api_snapshot 8091 > /tmp/hgx-api-rust.txt 2>&1
api_snapshot 8092 > /tmp/hgx-api-go.txt 2>&1
payload_asserts 8091 "PG rust"
payload_asserts 8092 "PG go"
policy_writeback_asserts 8091 "PG rust" pgr
policy_writeback_asserts 8092 "PG go"   pgg
enqueue_backpressure_api_asserts 8091 "PG rust" pgr
enqueue_backpressure_api_asserts 8092 "PG go"   pgg
# ...AND THE LOOP CLOSED, once, where the gate and the API front the same store: a limit
# that ONLY the API ever wrote is then enforced by the admission gate itself. The two
# assertions above prove the API can write the policy; this proves it is the SAME policy
# the gate reads, which is the half invariant 16 actually claims.
curl -s -o /dev/null -X PUT "127.0.0.1:8091/api/v1/rate-classes/inv16gate" \
     -H 'content-type: application/json' -H 'Idempotency-Key: inv16gate-a' \
     -d '{"limit":3,"window_ms":60000,"burst":3}'
$H enqueue count=6 prefix=i16 queue=inv16q rate=inv16gate fp=fp sched=1000 >/dev/null
r=$($H admit queues=inv16q capacity=10 lease_ms=30000 worker=w1 lease=LI16A quantum=1000 | wc -l | tr -d ' ')
chk "invariant 16: the GATE enforces a fleet limit that ONLY the API ever wrote (3 of 6)" "$r" "3"
curl -s -o /dev/null -X PUT "127.0.0.1:8091/api/v1/rate-classes/inv16gate" \
     -H 'content-type: application/json' -H 'Idempotency-Key: inv16gate-b' \
     -d '{"limit":3,"window_ms":60000,"burst":3,"paused":true}'
r=$($H admit queues=inv16q capacity=10 lease_ms=30000 worker=w2 lease=LI16B quantum=1000 | wc -l | tr -d ' ')
chk0 "invariant 16: ...and the API's kill switch stops the gate dead, with no redeploy" "$r" "0" \
     "invariant 16: ...witness: three jobs are still waiting on the killed class" \
     "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='available' AND rate_class='inv16gate';")"
$PSQL -c "DELETE FROM headgate_job WHERE queue='inv16q'; DELETE FROM headgate_active_partition WHERE queue='inv16q'; DELETE FROM headgate_rate_bucket WHERE name LIKE 'inv16%';" >/dev/null

# ----- INVARIANT 6, THE BOUNDED ADMIN READ (round 32i) -----
# "No admin operation is O(queue depth). asynq's GetQueueInfo pinned Redis CPU for seconds
# in production; monitoring caused the outage."
#
# Every inspection read here is bounded by a LIMIT and SAYS SO — a capped position, or an
# `approximate` flag — and nothing asserted the cap, because every fixture in this file is
# a handful of rows and a query with the LIMIT deleted answers identically on four rows.
# Mutation-tested in round 32i by replacing `LIMIT $1` with `AND $1::bigint >= 0` in
# rate_classes' `jobs_waiting` subquery — same parameters, same answer on every existing
# fixture, now a full count over the backlog: 375 of 375 assertions stayed green.
#
# So the fixture is made DEEPER THAN THE CAP, which is the only way a bound is observable
# at all. POSITION_LIMIT is 1000 in both languages and on both store ports; the assertion
# is that a 1200-deep class reports exactly 1000, i.e. the read stopped counting. Asserted
# on BOTH servers: a cap that exists in one language and not the other is a divergence the
# snapshot diff cannot show, because this fixture is created after the snapshot.
# (Postgres only. The Redis and MySQL paths share the same POSITION_LIMIT by construction
# but are NOT separately seeded here — see the round 32i note in CAPABILITY_REGISTER.md.)
$PSQL -c "INSERT INTO headgate_rate_bucket VALUES ('inv6rc',5,5,5,1000,1000000);" >/dev/null
$H enqueue count=600 prefix=i6a- queue=inv6q rate=inv6rc fp=fp sched=1000 >/dev/null
$H enqueue count=600 prefix=i6b- queue=inv6q rate=inv6rc fp=fp sched=1000 >/dev/null
chk "invariant 6: ...the fixture really is deeper than the cap (1200 available on the class)" \
    "$($PSQL -c "SELECT count(*) FROM headgate_job WHERE state='available' AND rate_class='inv6rc';")" "1200"
for p in "8091 PG rust" "8092 PG go"; do
  set -- $p
  chk "$2 $3 invariant 6: jobs_waiting STOPS at the position cap, never a count(*) over the backlog" \
      "$(curl -sf "127.0.0.1:$1/api/v1/rate-classes" | jq -c '[.[]|select(.name=="inv6rc")|.jobs_waiting]')" \
      "[1000]"
done
$PSQL -c "DELETE FROM headgate_job WHERE queue='inv6q'; DELETE FROM headgate_active_partition WHERE queue='inv6q'; DELETE FROM headgate_rate_bucket WHERE name='inv6rc';" >/dev/null
cluster_asserts 8091 "§11.2 PG"
api_witness /tmp/hgx-api-rust.txt "PG" postgres
kill $RUST_API_PID $GO_API_PID 2>/dev/null; trap - EXIT
lines=$(wc -l < /tmp/hgx-api-rust.txt)
chk "API snapshot is non-trivial (no vacuous pass)" "$((lines >= 100))" "1"
if cmp -s /tmp/hgx-api-rust.txt /tmp/hgx-api-go.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-api-rust.txt /tmp/hgx-api-go.txt | head -20; fi
chk "API diff: Rust and Go serve §10.1 with identical responses" "$d" "identical"

# ----- §10.1 MUTATION routes (round 20): same seed, same POST/PUT/DELETE sequence
# against each server, fresh store per side. Status codes are part of the snapshot.
# Normalized: server-computed timestamps -> 0 (non-null only, so null-vs-number stays
# semantic) and server-generated ids -> "X" where we could not supply one.
API_NORM='walk(if type == "number" then . + 0 else . end)
  | walk(if type == "object" then with_entries(
      if (.key | IN("enqueued_at_ms","scheduled_at_ms","finalized_at_ms","next_run_ms",
                    "last_enqueued_ms","updated_at_ms","quarantined_at_ms",
                    "estimated_admission_ms")) and (.value != null)
      then .value = 0 else . end) else . end)'
# ----- round 32g: the diff compares THREE things now, not one.
#
#  1. STATUS + jq-normalized body — what it always did.
#  2. HEADERS. Round 32f named this a blind spot and it was: `x-released-jobs` is a
#     DOCUMENTED part of DELETE /quarantine/{fp} and NOTHING compared it, and
#     `content-type` told two different stories on every rejection — axum's extractors
#     answered text/plain where Go answered JSON, on ~20 paths. Only headers a client
#     can depend on are compared; date, content-length and the connection plumbing are
#     excluded because they are not contract. Lines are lowercased whole: hyper writes
#     header names lowercase and net/http title-cases them, which is legal in HTTP/1.1
#     and not a divergence.
#  3. RAW BYTES, for a representative subset. `jq` normalizes — that is the point of it
#     and also its blind spot. Go appended a TRAILING NEWLINE to every JSON body and
#     HTML-escaped `<`, `>` and `&` into <>&; twelve rounds of a diff
#     that pipes through jq saw neither. Byte-compared here on a 2xx body, a 4xx body
#     carrying `>`, a 4xx body carrying `<` and `>`, and a 204.
API_HDRS='content-type|allow|x-released-jobs'
api_mutate(){ # $1 = port
  local P="127.0.0.1:$1/api/v1"
  local HDRF="/tmp/hgx-mhdr-$$.txt"
  # emit_hdrs renders the comparable subset of a captured header block.
  emit_hdrs(){
    echo "headers: $(tr -d '\r' < "$HDRF" | tr 'A-Z' 'a-z' \
                     | grep -E "^($API_HDRS):" | sort | tr '\n' ' ')"
  }
  mreq(){ # key method path body [extra-jq]
    echo "== $2 $3 (key $1) =="
    local out
    if [ -n "$4" ]; then
      out=$(curl -s -D "$HDRF" -w '\n%{http_code}' -X "$2" "$P/$3" -H "Idempotency-Key: $1" \
                 -H 'content-type: application/json' -d "$4")
    else
      out=$(curl -s -D "$HDRF" -w '\n%{http_code}' -X "$2" "$P/$3" -H "Idempotency-Key: $1")
    fi
    echo "status: $(echo "$out" | tail -1)"
    emit_hdrs
    echo "$out" | sed '$d' | jq -S "$API_NORM ${5:+| $5}" 2>/dev/null || echo "$out" | sed '$d'
  }
  # mreqraw: the same request, with the body compared BYTE FOR BYTE instead of through
  # jq. Reserved for bodies with no server-generated value in them, since nothing
  # normalizes ids or timestamps here.
  mreqraw(){ # key method path body
    echo "== RAW $2 $3 (key $1) =="
    local body
    if [ -n "$4" ]; then
      body=$(curl -s -D "$HDRF" -o "/tmp/hgx-mraw-$$.bin" -w '%{http_code}' \
                  -X "$2" "$P/$3" -H "Idempotency-Key: $1" \
                  -H 'content-type: application/json' -d "$4")
    else
      body=$(curl -s -D "$HDRF" -o "/tmp/hgx-mraw-$$.bin" -w '%{http_code}' \
                  -X "$2" "$P/$3" -H "Idempotency-Key: $1")
    fi
    echo "status: $body"
    emit_hdrs
    echo "bytes: $(wc -c < "/tmp/hgx-mraw-$$.bin" | tr -d ' ')"
    # The body verbatim on ONE line, so a literal `>` is greppable: everywhere else
    # in this file jq has already DECODED the escape, which is exactly why the HTML
    # escaping survived twelve rounds. `bytes:` and the od dump below carry the trailing
    # newline this line strips.
    echo "raw-body: $(tr -d '\n' < "/tmp/hgx-mraw-$$.bin")"
    od -An -c < "/tmp/hgx-mraw-$$.bin"
  }
  # mreqnoct: a bodied request with NO Content-Type. Rust answers 415; Go used to
  # proceed and enqueue, so a proxy that strips the header could write to the queue.
  mreqnoct(){ # key method path body
    echo "== $2 $3 NO-CONTENT-TYPE (key $1) =="
    local out
    out=$(curl -s -D "$HDRF" -w '\n%{http_code}' -X "$2" "$P/$3" -H "Idempotency-Key: $1" \
               -H 'content-type:' --data-binary "$4")
    echo "status: $(echo "$out" | tail -1)"
    emit_hdrs
    echo "$out" | sed '$d' | jq -S "$API_NORM" 2>/dev/null || echo "$out" | sed '$d'
  }
  # mreqnokey: no Idempotency-Key at all — used to prove ROUTING happens first, so an
  # unrouted POST is a 404 and not a complaint about a missing header.
  mreqnokey(){ # method path
    echo "== $1 $2 NO-IDEMPOTENCY-KEY =="
    local out
    out=$(curl -s -D "$HDRF" -w '\n%{http_code}' -X "$1" "$P/$2")
    echo "status: $(echo "$out" | tail -1)"
    emit_hdrs
    echo "$out" | sed '$d'
  }
  mreq k1 POST "jobs" '{"id":"m1","kind":"k","payload":"e30=","queue":"apim","retention_ms":86400000}'
  mreq k2 POST "jobs" '{"id":"m2","kind":"k","payload":"e30=","queue":"apim","scheduled_at_ms":99999999999999,"retention_ms":86400000}'
  # replay: NO caller id, the Idempotency-Key is the dedup key -> second POST is a 201
  # replay of the first, never a second job.
  mreq k3 POST "jobs" '{"kind":"k","payload":"e30=","queue":"apim"}' '.id = "X"'
  mreq k3 POST "jobs" '{"kind":"k","payload":"e30=","queue":"apim"}' '.id = "X"'
  mreq k4 POST "queues/apim/pause" ''
  mreq k5 POST "queues/apim/resume" ''
  mreq k6 POST "jobs/m1/cancel" ''
  mreq k7 POST "jobs/m1/retry" ''                # cancelled, not archived: error parity
  mreq k8 POST "jobs/m2/reschedule" '{"scheduled_at_ms":88888888888888}'
  mreq k9 PUT  "jobs/m2/payload" '{"payload":"eyJhIjoxfQ==","schema_version":2}'
  mreq k10 PUT "rate-classes/apim-rc" '{"limit":5,"window_ms":1000,"burst":5}'
  mreq k11 PUT "rate-classes/apim-bad" '{"limit":5,"window_ms":0}'  # 400 parity
  mreq k12 PUT "periodic/apim-s" '{"kind":"k","spec":"@every:60000"}'
  mreq k13 PUT "periodic/apim-cron" '{"kind":"k","spec":"0 0 * * 1"}' # POSIX DOW parity
  mreq k14 DELETE "periodic/apim-cron" ''
  mreq k15 DELETE "periodic/apim-cron" ''        # 404 parity
  mreq k16 POST "workers/ghost/signal" '{"command":"quiet"}'          # 404 parity
  mreq k17 POST "jobs/bulk" '{"action":"cancel","selector":{"queue":"apim"}}' '.id = "X"'
  mreq k18 POST "jobs/bulk" '{"action":"cancel","selector":{}}'       # empty selector 400
  mreq k19 DELETE "jobs/m1" ''
  mreq k20 DELETE "periodic/apim-s" ''           # leave nothing ticking
  # ----- round 32: §4.4b + §5.9 at the API, in the SAME byte-diffed sequence.
  # A caller-supplied id, replayed under a DIFFERENT Idempotency-Key (so the unique-key
  # dedup cannot be what answers): identical content is a plain 201 for the same job,
  # different content is 409 with the uniform raw message, and the count proves no
  # second job was created. Then §5.9's 400.
  mreq k21 POST "jobs" '{"id":"c1","kind":"k","payload":"e30=","queue":"apic","retention_ms":86400000}'
  mreq k22 POST "jobs" '{"id":"c1","kind":"k","payload":"e30=","queue":"apic","retention_ms":86400000}'
  mreq k23 POST "jobs" '{"id":"c1","kind":"k","payload":"eyJhIjoxfQ==","queue":"apic","retention_ms":86400000}'
  mreq k24 GET  "jobs/counts?queue=apic" ''
  mreq k25 POST "jobs" '{"id":"bk1","kind":"bad kind","payload":"e30=","queue":"apic"}'
  mreq k26 POST "jobs" '{"id":"bk2","kind":"w","payload":"e30=","queue":"apic"}'
  # ----- round 32: §11.2 PER-SCHEDULE TIMEZONE at the API, in the SAME byte-diffed
  # sequence. The zone rides INSIDE the spec string, so the accepted PUT needs no new
  # field and the GET echoes the zone verbatim — that is the entire argument for the
  # in-spec prefix, asserted rather than claimed. The two rejections are the error
  # contract: Rust resolves through chrono-tz's embedded table and Go through stdlib
  # time.LoadLocation, and both must answer with the same bytes and the same 400.
  mreq k27 PUT "periodic/apim-tz" '{"kind":"k","spec":"CRON_TZ=America/New_York 0 9 * * *"}'
  # filtered to this step's own entry: the shared store carries other tests' schedules,
  # and a leaked row is noise the diff would have to keep re-proving
  mreq k28 GET "periodic" '' 'map(select(.id == "apim-tz"))'
  mreq k29 PUT "periodic/apim-badtz" '{"kind":"k","spec":"CRON_TZ=Mars/Phobos 0 9 * * *"}'
  # an interval has no wall clock: a zone on "@every" is an error, never a silent no-op
  mreq k30 PUT "periodic/apim-badtz" '{"kind":"k","spec":"CRON_TZ=America/New_York @every:60000"}'
  mreq k31 DELETE "periodic/apim-tz" ''        # leave nothing ticking
  # ----- round 32f: quarantine_release's NOT-FOUND path. This route had drifted for four
  # rounds precisely because no diff covered it: Rust answered 404 `not found: fingerprint
  # {fp} is not quarantined` (StoreError::NotFound's Display) while all three Go drivers
  # omitted the `not found: ` prefix — and Go has no NotFoundError type, so storeErr
  # classifies on exactly that prefix and everything without it falls through to 400.
  # Same envelope, different STATUS CODE. A divergence no diff covers is a divergence
  # waiting to happen, so the diff now covers it on every backend.
  mreq k32 DELETE "quarantine/apim-never-quarantined" ''
  # ================= round 32g: the ~70 non-2xx paths =================
  # Round 32f audited every API-reachable error path and found the sequence above
  # covered TEN of them. Outside that set the two servers diverged badly — including
  # three Go-side data-corruption bugs — precisely because nothing compared them. Each
  # block below is a divergence that WAS real, so the diff can never lose it again.
  #
  # ----- TIER 1: bodies Go accepted and acted on, where Rust rejects the request.
  # m2 is SCHEDULED at this point (k2 enqueued it, k8 moved it): `{}` used to reschedule
  # it TO EPOCH 0 — "run it now" to every promote sweep — and answer 204.
  mreq g01 POST "jobs/m2/reschedule" '{}'
  # ...and `{}` here used to WIPE the payload and rewrite the §7.1 fingerprint to match.
  mreq g02 PUT  "jobs/m2/payload" '{}'
  # An explicit empty command used to CLEAR a pending signal and answer 204. Rust hands
  # Some("") to the store, which rejects it; Go's port cannot express the difference, so
  # the check lives above the port. Validated BEFORE the worker lookup on both, which is
  # why a nonexistent worker still answers 400 and not 404.
  mreq g03 POST "workers/ghost/signal" '{"command":""}'
  mreq g04 POST "workers/apim-w/signal" '{"command":"quiet"}'
  mreq g05 POST "workers/apim-w/signal" '{"command":""}'   # 400; the quiet SURVIVES (chk'd)
  # ----- TIER 1b: every other required field. Missing `limit` used to create a rate
  # class with limit 0 — invariant 16's KILL SWITCH — and answer 200. Missing `kind`
  # used to create a periodic entry that enqueues jobs no worker can dispatch.
  mreq g06 POST "jobs" '{"queue":"apig"}'
  mreq g07 POST "jobs" '{"kind":"k","queue":"apig"}'
  mreq g08 PUT  "rate-classes/apig-rc" '{"window_ms":1000}'
  mreq g09 PUT  "rate-classes/apig-rc" '{"limit":5}'
  mreq g10 PUT  "periodic/apig-s" '{"spec":"@every:60000"}'
  mreq g11 PUT  "periodic/apig-s" '{"kind":"k"}'
  mreq g12 POST "jobs/actions" '{"ids":["m1"]}'
  mreq g13 POST "jobs/actions" '{"action":"retry"}'
  mreq g14 POST "jobs/bulk" '{"action":"cancel"}'
  mreq g15 POST "jobs/bulk" '{"selector":{"queue":"apim"}}'
  # present-but-NULL is a type error to serde, not a missing field. The two must not be
  # collapsed or the message names the wrong problem.
  mreq g16 POST "jobs/m2/reschedule" '{"scheduled_at_ms":null}'
  mreq g17 POST "jobs" '{"kind":"k","payload":"e30=","priority":"high"}'
  mreq g18 POST "jobs" 'null'
  mreq g19 POST "jobs" '{'
  # ----- TIER 2: `Option<T>` is not `""`.
  mreq g20 POST "jobs" '{"id":"","kind":"k","payload":"e30=","queue":"apig"}'
  # an explicit empty unique_key is a REAL zero-length key: the second enqueue is a 409,
  # not a second job. Go read it as "no key supplied" and created one.
  mreq g21 POST "jobs" '{"id":"u1","kind":"k","payload":"e30=","queue":"apig","unique_key":"","retention_ms":86400000}'
  mreq g22 POST "jobs" '{"id":"u2","kind":"k","payload":"e30=","queue":"apig","unique_key":"","retention_ms":86400000}'
  mreq g23 GET  "jobs/counts?queue=apig" ''    # ...and no second job exists
  # an explicit empty on_missed is NOT "skip": Rust parses it and fails.
  mreq g24 PUT  "periodic/apig-s" '{"kind":"k","spec":"@every:60000","on_missed":""}'
  # the two periodic defaults that are NOT the zero value: queue -> "default" and
  # max_attempts -> 25 only when ABSENT. An explicit "" queue stays "", an explicit 0
  # max_attempts stays 0 — Go's `if x == zero { default }` silently overrode both, so
  # "never retry this" became 25 tries.
  mreq g25 PUT  "periodic/apig-opt" '{"kind":"k","spec":"@every:60000","queue":"","max_attempts":0}'
  mreq g26 GET  "periodic" '' 'map(select(.id == "apig-opt"))'
  mreq g27 DELETE "periodic/apig-opt" ''
  # ----- TIER 2: query-string coercion. Every one of these silently used a DEFAULT in
  # Go, so a client bug that produced a malformed parameter never surfaced.
  mreq g28 GET "jobs?limit=abc" ''
  mreq g29 GET "jobs?cursor=" ''
  mreq g30 GET "jobs?cursor=zzz" ''
  mreq g31 GET "queues/apim/history?bucket_ms=abc" ''
  mreq g32 GET "queues/apim/history?since_ms=" ''
  mreq g33 GET "jobs/m1?include_payload=yes" ''
  mreq g34 GET "partitions" ''                 # the ONE required query parameter
  # ----- TIER 2: media type. A proxy that strips Content-Type must not be able to
  # enqueue, which is exactly what Go let it do.
  mreqnoct g35 POST "jobs" '{"kind":"k","payload":"e30=","queue":"apig"}'
  # ----- TIER 3: bytes. /jobs/actions is the route round 32f found 100% untested, and
  # its per-id `failed[].reason` was the ONE place Rust shipped Display's "invalid
  # request: " prefix — every other route strips it.
  mreq g36 POST "jobs/actions" '{"action":"retry","ids":["apig-ghost"]}'
  mreq g37 POST "jobs/actions" '{"action":"archive","ids":["m1"]}'
  mreq g38 POST "jobs/actions" '{"action":"nope","ids":["m1"]}'
  # `unknown action %q` lived in headgatepgx alone; redis and mysql used backticks, so
  # ONE backend rendered the same rejection differently from the other two and from Rust.
  mreq g39 POST "jobs/bulk" '{"action":"nope","selector":{"queue":"apim"}}'
  # cron and @every rejections: Go quoted the spec with %q (double quotes) where Rust
  # uses backticks, on every spec error there is.
  mreq g40 PUT "periodic/apig-s" '{"kind":"k","spec":"notacron"}'
  mreq g41 PUT "periodic/apig-s" '{"kind":"k","spec":"@every:abc"}'
  mreq g42 PUT "periodic/apig-s" '{"kind":"k","spec":"@every:0"}'
  mreq g43 PUT "periodic/apig-s" '{"kind":"k","spec":""}'
  # ----- routing. Go leaked net/http's "404 page not found" and "Method Not Allowed"
  # strings, rendered Allow with a space, and ran the Idempotency-Key check BEFORE
  # routing so an unrouted POST complained about a header instead of 404ing.
  mreq g44 GET "nosuchroute" ''
  mreqnokey POST "nosuchroute"        # 404: routing decides before the header check
  mreqnokey DELETE "queues"           # 405-shaped, but the header check still runs: 400
  # NOT in the sequence: `GET /queues//pause`. axum matches it with queue="" and answers
  # 405; net/http's ServeMux cannot match an empty path segment and would redirect, so
  # headgateapi answers 404 for an unclean path instead. 404-vs-405 on a doubled slash
  # is a real divergence, recorded in the register rather than papered over — closing it
  # needs a router that matches empty segments, not an API-layer change.
  # ----- HEADERS: `x-released-jobs` is documented and was never compared. apim-fp is
  # seeded quarantined with no jobs behind it, so the count is a stable 0 on every
  # backend — this asserts the header EXISTS and agrees, which is what drifted.
  mreq g45 DELETE "quarantine/apim-fp" ''
  # ----- RAW BYTES, no jq in the way. The 2xx proves no trailing newline; the two 4xx
  # bodies carry `>` and `<>` and prove no HTML escaping; the 204 proves an empty body
  # stays empty.
  mreqraw g46 PUT "rate-classes/apig-bad" '{"limit":5,"window_ms":0}'
  mreqraw g47 POST "jobs" '{"id":"bkr","kind":"bad kind","payload":"e30=","queue":"apig"}'
  mreqraw g48 GET "jobs/counts?queue=apig" ''
  mreqraw g49 POST "queues/apig/pause" ''
  mreqraw g50 POST "queues/apig/resume" ''
  mreq g51 DELETE "jobs/u1" ''
  mreq g52 DELETE "jobs/u2" ''
  # ================= round 32h: THE EMPTY-VALUE FILTER =================
  # Round 32g found this and deferred it: Rust's job filters are `Option<String>`, so
  # `?queue=` / `?state=` / `?kind=` / `q=partition:` filter FOR the empty value; Go's
  # plain-`string` port read the same input as "no filter" and answered with the WHOLE
  # queue. Rust is right — an empty `partition_key` is the DEFAULT partition, the most
  # populated one in any store that never set a partition key, and Go could not express
  # the question at all. The port moved (see headgate.JobFilter).
  #
  # Two jobs, one in the default partition and one in `p`, so the two answers DIFFER.
  # That contrast is the whole test: a filter that is ignored returns both.
  mreq h01 POST "jobs" '{"id":"pk0","kind":"k","payload":"e30=","queue":"apih","retention_ms":86400000}'
  mreq h02 POST "jobs" '{"id":"pk1","kind":"k","payload":"e30=","queue":"apih","partition_key":"p","retention_ms":86400000}'
  mreq h03 GET "jobs?queue=apih&partition_key=" ''
  mreq h04 GET "jobs?queue=apih&partition_key=p" ''
  mreq h05 GET "jobs?queue=apih" ''
  mreq h06 GET "jobs?queue=apih&q=partition:" ''
  # ...and the same rule on the other three fields. `?queue=` asks for the queue named
  # "" (nothing matches, which is the CORRECT empty answer, not the whole store);
  # `?state=` and `?kind=` likewise.
  mreq h07 GET "jobs?queue=" ''
  mreq h08 GET "jobs?queue=apih&state=" ''
  mreq h09 GET "jobs?queue=apih&kind=" ''
  # counts takes `Option<&str>` on both sides now: absent = every queue, `?queue=` = the
  # queue named "". Go's port could only say "every queue".
  mreq h10 GET "jobs/counts?queue=" ''
  mreq h11 GET "jobs/counts" ''
  mreq h12 DELETE "jobs/pk0" ''
  mreq h13 DELETE "jobs/pk1" ''
}
# The literal-bytes half of the empty-value filter contract. The diff proves the two
# servers agree; these prove they agree on the RIGHT answer — and, unlike the diff, they
# would fail if BOTH servers regressed to "empty means no filter", which is exactly the
# state the Go port was in until this round.
h_asserts(){ # $1 = rust snapshot, $2 = go snapshot, $3 = label
  local R="$1" G="$2" L="$3"
  # the body of ONE request block, by its idempotency key
  hblk(){ awk -v k="(key $1)" '/^== /{f=(index($0,k)>0); next} f' "$2" | grep -Fc -- "$3"; }
  hpair(){ echo "$(hblk "$1" "$R" "$2")$(hblk "$1" "$G" "$2")"; }
  chk "$L 32h: ?partition_key= selects the DEFAULT partition, never the whole queue" \
      "$(hpair h03 '"id": "pk0"')|$(hpair h03 '"id": "pk1"')" "11|00"
  chk "$L 32h: ...and ?partition_key=p selects only the other one (the contrast)" \
      "$(hpair h04 '"id": "pk0"')|$(hpair h04 '"id": "pk1"')" "00|11"
  chk "$L 32h: ...and NO partition filter really does return both (so the filter bites)" \
      "$(hpair h05 '"id": "pk0"')|$(hpair h05 '"id": "pk1"')" "11|11"
  chk "$L 32h: q=partition: is the same empty-value filter through the search grammar" \
      "$(hpair h06 '"id": "pk0"')|$(hpair h06 '"id": "pk1"')" "11|00"
  chk "$L 32h: ?queue= asks for the queue named \"\", which nothing is in" \
      "$(hpair h07 '"id": "pk')|$(hpair h05 '"id": "pk')" "00|22"
  chk "$L 32h: ?state= asks for the state named \"\", not for every state" \
      "$(hpair h08 '"id": "pk')|$(hpair h05 '"id": "pk')" "00|22"
  chk "$L 32h: ?kind= asks for the kind named \"\", not for every kind" \
      "$(hpair h09 '"id": "pk')|$(hpair h05 '"id": "pk')" "00|22"
  # counts: `?queue=` is the empty-named queue (no rows -> an empty counts object),
  # `?queue` absent is every queue (which at this point holds the two apih rows).
  # Asserted as SHAPE, not as a number: the fleet-wide total depends on what the rest of
  # the sequence left behind, which differs per backend and would be a fixture nobody
  # controls. What is contractual is that the two answers are DIFFERENT KINDS of answer.
  chk "$L 32h: counts ?queue= is the empty-named queue; absent is every queue" \
      "$(hblk h10 "$R" '"counts": {}')|$(hblk h11 "$R" '"available"')" "1|1"
}
# The mutation fixtures round 32g needs beyond an empty store: ONE quarantined
# fingerprint (so DELETE /quarantine/{fp} takes the 204 path and emits x-released-jobs)
# and ONE registered worker (so the signal sequence can prove an empty command does not
# CLEAR a pending one). Both are fixed literals, so both servers see the same state.
mutation_seed(){
  reset_pg
  seed "§10.1 mutation fixtures (quarantined fingerprint + registered worker)" \
       "INSERT INTO headgate_quarantine VALUES ('apim-fp','k',3,1234,'\x00','crash limit reached');
        INSERT INTO headgate_worker
          (worker_id,host,pid,queues,concurrency,started_at_ms,heartbeat_at_ms)
        VALUES ('apim-w','h',1,'{apim}',1,1000,99999999999999);"
}
# The state half of the tier-1 fixes. The diff proves both servers ANSWER the same way;
# these prove the answer was honest — a 422 that still moved the job would pass a diff.
api_state_asserts(){ # $1 = port, $2 = label, $3 = pending-worker-command ("" = unreadable)
  local j
  j=$(curl -s "127.0.0.1:$1/api/v1/jobs/m2?include_payload=true")
  chk "$2 tier-1: a reschedule with no scheduled_at_ms did NOT move the job to epoch 0" \
      "$(echo "$j" | jq '.scheduled_at_ms > 1000000000000')" "true"
  chk "$2 tier-1: a payload edit with no payload did NOT wipe the payload" \
      "$(echo "$j" | jq -r '.payload')" "eyJhIjoxfQ=="
  if [ -n "$3" ]; then
    chk "$2 tier-1: an empty signal command did NOT clear the pending one" "$3" "quiet"
  else
    # MySQL only: the harness's `sql` is exec_drop, so there is no read path for a
    # worker's pending command. The 400 itself is still diffed (g03/g05) on all three
    # backends; only this STATE half is unreadable here. Noted rather than faked.
    skipped "$2 tier-1: pending-command state check" "needs a MySQL read path; the 400 itself is still diffed (g03/g05)"
  fi
}
HG_API_ADDR=127.0.0.1:8093 target/debug/hg-api    >/dev/null 2>&1 & RUST_API_PID=$!
HG_API_ADDR=127.0.0.1:8094 target/debug/hg-go-api >/dev/null 2>&1 & GO_API_PID=$!
disown $RUST_API_PID $GO_API_PID 2>/dev/null || true
trap 'kill $RUST_API_PID $GO_API_PID 2>/dev/null' EXIT
for i in $(seq 1 50); do
  curl -sf 127.0.0.1:8093/api/v1/healthz >/dev/null && curl -sf 127.0.0.1:8094/api/v1/healthz >/dev/null && break
  sleep 0.2
done
pg_worker_cmd(){ $PSQL -c "SELECT coalesce(command,'<none>') FROM headgate_worker WHERE worker_id='apim-w'"; }
mutation_seed; api_mutate 8093 > /tmp/hgx-mut-rust.txt 2>&1
api_state_asserts 8093 "PG rust" "$(pg_worker_cmd)"
mutation_seed; api_mutate 8094 > /tmp/hgx-mut-go.txt 2>&1
api_state_asserts 8094 "PG go" "$(pg_worker_cmd)"
kill $RUST_API_PID $GO_API_PID 2>/dev/null; trap - EXIT
reset_pg   # hygiene: drop the mutation fixtures
lines=$(wc -l < /tmp/hgx-mut-rust.txt)
chk "mutation snapshot is non-trivial (no vacuous pass)" "$((lines >= 60))" "1"
if cmp -s /tmp/hgx-mut-rust.txt /tmp/hgx-mut-go.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-mut-rust.txt /tmp/hgx-mut-go.txt | head -20; fi
chk "API mutation diff: POST/PUT/DELETE responses and status codes match" "$d" "identical"
chk "§11.2 tz: PUT /periodic stores the zoned spec and GET echoes it VERBATIM" \
    "$(grep -Fc "$TZSPEC" /tmp/hgx-mut-rust.txt)" "1"
chk "§11.2 tz: an unknown zone is a 400 carrying the raw message" \
    "$(grep -Fc "$TZMSG" /tmp/hgx-mut-rust.txt)" "1"
chk "§11.2 tz: a zone on \"@every\" is refused, not silently ignored" \
    "$(grep -Fc "$TZEVERY" /tmp/hgx-mut-rust.txt)" "1"
# round 32f: the ONE quarantine_release not-found contract, beside the diff.
# Both servers agreeing is not enough — they agreed on 400-vs-404 being
# untested for four rounds. These pin the bytes AND the status.
chk "§5.2: quarantine_release not-found is one message in both languages" \
    "$(grep -Fc "$QRELMSG" /tmp/hgx-mut-rust.txt)$(grep -Fc "$QRELMSG" /tmp/hgx-mut-go.txt)" "11"
chk "§5.2: ...and one STATUS — 404, never Go's old fall-through 400" \
    "$(awk '/== DELETE quarantine\//{getline; print; exit}' /tmp/hgx-mut-rust.txt)|$(awk '/== DELETE quarantine\//{getline; print; exit}' /tmp/hgx-mut-go.txt)" \
    "$QRELSTATUS|$QRELSTATUS"
g_asserts /tmp/hgx-mut-rust.txt /tmp/hgx-mut-go.txt "PG"
h_asserts /tmp/hgx-mut-rust.txt /tmp/hgx-mut-go.txt "PG"

# ----- the SAME API diffs over REDIS (round 24): both servers front the Redis store
# (HG_STORE=redis), one shared keyspace for the GET diff, fresh keyspace per side for
# the mutation diff. Seeding mirrors the PG seed with fixed literals; hist counters
# are cleared so rates are time-stable at 0.
redis_api_seed(){
  $RED flushall >/dev/null
  $RED hset hg:rate:apix-rc tokens 0 burst 5 limit 0 window 1000 refilled 1000 >/dev/null
  $RED sadd hg:rate_classes apix-rc >/dev/null
  $RED sadd hg:quarantine apix-fp >/dev/null
  $RED hset hg:qmeta:apix-fp kind k crash_count 3 at_ms 1234 reason 'crash limit reached' >/dev/null
  $RED hset hg:schedule:apix-s kind k payload '' queue apix partition_key '' rate_class '' \
       priority 0 max_attempts 25 retention_ms 0 spec @every:60000 next_run_ms 99999999999999 \
       on_missed skip backfill_limit 0 paused 0 updated_at_ms 1234 >/dev/null
  $RED zadd hg:schedules 99999999999999 apix-s >/dev/null
  $RED sadd hg:paused apix-paused >/dev/null
  $RED sadd hg:queues apix-paused >/dev/null
  $HR enqueue count=4 prefix=apix queue=apix payload='{}' fp=auto retention=86400000 >/dev/null
  $HR enqueue count=1 prefix=apxp queue=apix-paused payload='{}' fp=auto >/dev/null
  $HR admit queues=apix capacity=2 lease_ms=600000 worker=wA lease=LA quantum=1000 >/dev/null
  $HR ack job=apix1 lease=LA fence=1 outcome=retry err=boom >/dev/null
  $HR ack job=apix2 lease=LA fence=1 outcome=success >/dev/null
  for k in $($RED keys 'hg:hist:*') $($RED keys 'hg:histp:*'); do
    $RED del "$k" >/dev/null
  done
  # Both aggregate and per-partition histories must be empty. Quiet-group metrics read
  # histp:, so deleting only hist: still made the sequential snapshot minute-sensitive.
  # §11.2 round 32: the same fixed registry the PG seed writes, so GET /cluster is
  # time-stable on both backends. heartbeat_at_ms far in the future = live by the
  # 15-minute grace rule; the third row is ancient, i.e. stale.
  $RED hset hg:worker:apix-w1 host h1 pid 1 queues apix concurrency 8 \
       started_at_ms 1000 heartbeat_at_ms 99999999999999 inflight 6 polls 10 empty_polls 2 >/dev/null
  $RED hset hg:worker:apix-w2 host h2 pid 2 queues apix,apix-other concurrency 4 \
       started_at_ms 1000 heartbeat_at_ms 99999999999999 inflight 1 polls 10 empty_polls 3 >/dev/null
  $RED hset hg:worker:apix-dead host h3 pid 3 queues apix-paused concurrency 9 \
       started_at_ms 1000 heartbeat_at_ms 1000 inflight 0 polls 0 empty_polls 0 >/dev/null
  $RED sadd hg:workers apix-w1 apix-w2 apix-dead >/dev/null
}
redis_api_seed
HG_STORE=redis HG_REDIS="redis://127.0.0.1:$RP" HG_API_ADDR=127.0.0.1:8095 target/debug/hg-api >/dev/null 2>&1 & RUST_API_PID=$!
HG_STORE=redis HG_REDIS="redis://127.0.0.1:$RP" HG_API_ADDR=127.0.0.1:8096 target/debug/hg-go-api >/dev/null 2>&1 & GO_API_PID=$!
disown $RUST_API_PID $GO_API_PID 2>/dev/null || true
trap 'kill $RUST_API_PID $GO_API_PID 2>/dev/null' EXIT
for i in $(seq 1 50); do
  curl -sf 127.0.0.1:8095/api/v1/healthz >/dev/null && curl -sf 127.0.0.1:8096/api/v1/healthz >/dev/null && break
  sleep 0.2
done
api_snapshot 8095 > /tmp/hgx-rapi-rust.txt 2>&1
api_snapshot 8096 > /tmp/hgx-rapi-go.txt 2>&1
payload_asserts 8095 "Redis rust"
payload_asserts 8096 "Redis go"
policy_writeback_asserts 8095 "Redis rust" rr
policy_writeback_asserts 8096 "Redis go"   rg
enqueue_backpressure_api_asserts 8095 "Redis rust" rr
enqueue_backpressure_api_asserts 8096 "Redis go"   rg
cluster_asserts 8095 "§11.2 Redis"
api_witness /tmp/hgx-rapi-rust.txt "Redis" redis
lines=$(wc -l < /tmp/hgx-rapi-rust.txt)
chk "Redis API snapshot is non-trivial (no vacuous pass)" "$((lines >= 100))" "1"
if cmp -s /tmp/hgx-rapi-rust.txt /tmp/hgx-rapi-go.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-rapi-rust.txt /tmp/hgx-rapi-go.txt | head -20; fi
chk "Redis API diff: both languages serve §10.1 identically over Redis" "$d" "identical"
# The same round-32g fixtures the PG seed writes: one quarantined fingerprint (so
# DELETE /quarantine takes the 204 path and emits x-released-jobs) and one registered
# worker (so the signal sequence can prove an empty command does not CLEAR a pending one).
mutation_seed_redis(){
  $RED flushall >/dev/null
  $RED sadd hg:quarantine apim-fp >/dev/null
  $RED hset hg:qmeta:apim-fp kind k crash_count 3 at_ms 1234 reason 'crash limit reached' >/dev/null
  $RED hset hg:worker:apim-w host h pid 1 queues apim concurrency 1 \
       started_at_ms 1000 heartbeat_at_ms 99999999999999 inflight 0 polls 0 empty_polls 0 >/dev/null
  $RED sadd hg:workers apim-w >/dev/null
}
redis_worker_cmd(){ local c; c=$($RED hget hg:worker:apim-w command); echo "${c:-<none>}"; }
mutation_seed_redis; api_mutate 8095 > /tmp/hgx-rmut-rust.txt 2>&1
api_state_asserts 8095 "Redis rust" "$(redis_worker_cmd)"
mutation_seed_redis; api_mutate 8096 > /tmp/hgx-rmut-go.txt 2>&1
api_state_asserts 8096 "Redis go" "$(redis_worker_cmd)"
kill $RUST_API_PID $GO_API_PID 2>/dev/null; trap - EXIT
$RED flushall >/dev/null   # hygiene: drop the mutation fixtures
lines=$(wc -l < /tmp/hgx-rmut-rust.txt)
chk "Redis mutation snapshot is non-trivial (no vacuous pass)" "$((lines >= 60))" "1"
if cmp -s /tmp/hgx-rmut-rust.txt /tmp/hgx-rmut-go.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-rmut-rust.txt /tmp/hgx-rmut-go.txt | head -20; fi
chk "Redis API mutation diff: POST/PUT/DELETE responses and status codes match" "$d" "identical"
chk "Redis §11.2 tz: the zoned spec round-trips through the sched hash VERBATIM" \
    "$(grep -Fc "$TZSPEC" /tmp/hgx-rmut-rust.txt)" "1"
chk "Redis §11.2 tz: an unknown zone is a 400 carrying the raw message" \
    "$(grep -Fc "$TZMSG" /tmp/hgx-rmut-rust.txt)" "1"
chk "Redis §11.2 tz: a zone on \"@every\" is refused, not silently ignored" \
    "$(grep -Fc "$TZEVERY" /tmp/hgx-rmut-rust.txt)" "1"
# round 32f: the ONE quarantine_release not-found contract, beside the diff.
# Both servers agreeing is not enough — they agreed on 400-vs-404 being
# untested for four rounds. These pin the bytes AND the status.
chk "Redis §5.2: quarantine_release not-found is one message in both languages" \
    "$(grep -Fc "$QRELMSG" /tmp/hgx-rmut-rust.txt)$(grep -Fc "$QRELMSG" /tmp/hgx-rmut-go.txt)" "11"
chk "Redis §5.2: ...and one STATUS — 404, never Go's old fall-through 400" \
    "$(awk '/== DELETE quarantine\//{getline; print; exit}' /tmp/hgx-rmut-rust.txt)|$(awk '/== DELETE quarantine\//{getline; print; exit}' /tmp/hgx-rmut-go.txt)" \
    "$QRELSTATUS|$QRELSTATUS"
g_asserts /tmp/hgx-rmut-rust.txt /tmp/hgx-rmut-go.txt "Redis"
h_asserts /tmp/hgx-rmut-rust.txt /tmp/hgx-rmut-go.txt "Redis"

# ----- the SAME API diffs over MYSQL (round 32c): §10.1 parity reaches 6/6 server
# configurations — 2 languages × 3 backends. The Go side is new: go/driver/headgatemysql
# declined InspectStore until this round, so hg-go-api had no `mysql` arm to select and
# this pair could not exist. Both servers front ONE MySQL (HG_STORE=mysql), one shared
# state for the GET diff, a fresh one per side for the mutation diff.
#
# The whole section sits behind the SAME watchdog gate as the MySQL store section above,
# so it soft-skips on a dead or WEDGED container rather than hanging the suite.
echo "== API parity over MySQL =="
if mysql_up; then
  # Fixture reset. Everything the two snapshots read must be either time-STABLE or
  # scoped to these queues; the two hazards are rate buckets (whose `avail` is a
  # function of NOW, so a bucket with a real limit would differ between the two
  # snapshots) and the arrival/drain counters. Both are emptied, and the one bucket
  # that stays is PAUSED — limit 0, tokens 0, refill adds nothing — which is exactly
  # why the Postgres seed uses a paused class too.
  mysql_api_reset(){
    for t in \
      "DELETE FROM headgate_job WHERE queue IN ('apix','apix-paused','apim','apic','apig','apih')" \
      "DELETE FROM headgate_active_partition WHERE queue IN ('apix','apix-paused','apim','apic','apig','apih')" \
      "DELETE FROM headgate_inflight WHERE queue IN ('apix','apix-paused','apim','apic','apig','apih')" \
      "DELETE FROM headgate_schedule" \
      "DELETE FROM headgate_operation" \
      "DELETE FROM headgate_rate_bucket" \
      "DELETE FROM headgate_queue_state" \
      "DELETE FROM headgate_quarantine" \
      "DELETE FROM headgate_worker" \
      "DELETE FROM headgate_queue_counter" ; do
      $HM sql stmt="$t" >/dev/null
    done
  }
  # The PG seed's fixed literals, translated: MySQL's `sample_payload` is nullable and
  # invisible to the API so it is omitted; `updated_at_ms` is NOT NULL with no default
  # here (Postgres defaults it) so it is supplied as a FIXED value, which is what keeps
  # GET /periodic time-stable; and `queues` is JSON, not text[].
  # One statement per `sql` call — the harness runs exec_drop, not a multi-statement.
  mysql_api_seed(){
    mysql_api_reset
    $HM sql stmt="INSERT INTO headgate_rate_bucket VALUES ('apix-rc',0,5,0,1000,1000)" >/dev/null
    $HM sql stmt="INSERT INTO headgate_quarantine (fingerprint,kind,crash_count,quarantined_at_ms,reason) VALUES ('apix-fp','k',3,1234,'crash limit reached')" >/dev/null
    $HM sql stmt="INSERT INTO headgate_schedule (id,kind,payload,queue,spec,next_run_ms,updated_at_ms) VALUES ('apix-s','k','','apix','@every:60000',99999999999999,1234)" >/dev/null
    $HM sql stmt="INSERT INTO headgate_queue_state (queue,paused) VALUES ('apix-paused',TRUE)" >/dev/null
    # §11.2 the fixed three-worker registry GET /cluster is diffed over and asserted on.
    # heartbeat_at_ms far in the future = live under the 15-minute grace; the third row
    # is ancient, i.e. stale but still REGISTERED, which is what makes apix-paused the
    # honest zero-coverage case rather than a queue nobody ever claimed.
    $HM sql stmt="INSERT INTO headgate_worker (worker_id,host,pid,queues,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls) VALUES ('apix-w1','h1',1,JSON_ARRAY('apix'),8,1000,99999999999999,6,10,2)" >/dev/null
    $HM sql stmt="INSERT INTO headgate_worker (worker_id,host,pid,queues,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls) VALUES ('apix-w2','h2',2,JSON_ARRAY('apix','apix-other'),4,1000,99999999999999,1,10,3)" >/dev/null
    $HM sql stmt="INSERT INTO headgate_worker (worker_id,host,pid,queues,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls) VALUES ('apix-dead','h3',3,JSON_ARRAY('apix-paused'),9,1000,1000,0,0,0)" >/dev/null
    $HM enqueue count=4 prefix=apix queue=apix payload='{}' fp=auto retention=86400000 >/dev/null
    $HM enqueue count=1 prefix=apxp queue=apix-paused payload='{}' fp=auto >/dev/null
    $HM admit queues=apix capacity=2 lease_ms=600000 worker=wA lease=LA quantum=1000 >/dev/null
    $HM ack job=apix1 lease=LA fence=1 outcome=retry err=boom >/dev/null
    $HM ack job=apix2 lease=LA fence=1 outcome=success >/dev/null
    $HM sql stmt="DELETE FROM headgate_queue_counter" >/dev/null
    $HM sql stmt="DELETE FROM headgate_partition_counter" >/dev/null
    # Queue AND quiet-group rates are time-stable at 0. Round 32w caught the old reset
    # deleting only queue counters: Rust and Go snapshots straddled a minute boundary,
    # and stale per-partition buckets aged out between the two reads.
  }
  mysql_api_seed
  HG_STORE=mysql HG_MYSQL="$HG_MYSQL" HG_API_ADDR=127.0.0.1:8097 target/debug/hg-api >/dev/null 2>&1 & RUST_API_PID=$!
  HG_STORE=mysql HG_MYSQL="$HG_MYSQL" HG_API_ADDR=127.0.0.1:8098 target/debug/hg-go-api >/dev/null 2>&1 & GO_API_PID=$!
  disown $RUST_API_PID $GO_API_PID 2>/dev/null || true
  trap 'kill $RUST_API_PID $GO_API_PID 2>/dev/null' EXIT
  for i in $(seq 1 50); do
    curl -sf 127.0.0.1:8097/api/v1/healthz >/dev/null && curl -sf 127.0.0.1:8098/api/v1/healthz >/dev/null && break
    sleep 0.2
  done
  api_snapshot 8097 > /tmp/hgx-mapi-rust.txt 2>&1
  api_snapshot 8098 > /tmp/hgx-mapi-go.txt 2>&1
  payload_asserts 8097 "MySQL rust"
  payload_asserts 8098 "MySQL go"
  policy_writeback_asserts 8097 "MySQL rust" mr
  policy_writeback_asserts 8098 "MySQL go"   mg
  enqueue_backpressure_api_asserts 8097 "MySQL rust" mr
  enqueue_backpressure_api_asserts 8098 "MySQL go"   mg
  cluster_asserts 8097 "§11.2 MySQL"
  api_witness /tmp/hgx-mapi-rust.txt "MySQL" mysql
  lines=$(wc -l < /tmp/hgx-mapi-rust.txt)
  chk "MySQL API snapshot is non-trivial (no vacuous pass)" "$((lines >= 100))" "1"
  if cmp -s /tmp/hgx-mapi-rust.txt /tmp/hgx-mapi-go.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-mapi-rust.txt /tmp/hgx-mapi-go.txt | head -20; fi
  chk "MySQL API diff: both languages serve §10.1 identically over MySQL" "$d" "identical"
  # Round 32g's fixtures, translated: one quarantined fingerprint and one registered
  # worker, so the new x-released-jobs and signal requests run here too.
  mutation_seed_mysql(){
    mysql_api_reset
    $HM sql stmt="INSERT INTO headgate_quarantine (fingerprint,kind,crash_count,quarantined_at_ms,reason) VALUES ('apim-fp','k',3,1234,'crash limit reached')" >/dev/null
    $HM sql stmt="INSERT INTO headgate_worker (worker_id,host,pid,queues,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls) VALUES ('apim-w','h',1,JSON_ARRAY('apim'),1,1000,99999999999999,0,0,0)" >/dev/null
  }
  mutation_seed_mysql; api_mutate 8097 > /tmp/hgx-mmut-rust.txt 2>&1
  # "" for the worker command: the MySQL harness's `sql` is exec_drop, so there is no
  # read path for it here. api_state_asserts prints a skip line rather than faking it.
  api_state_asserts 8097 "MySQL rust" ""
  mutation_seed_mysql; api_mutate 8098 > /tmp/hgx-mmut-go.txt 2>&1
  api_state_asserts 8098 "MySQL go" ""
  kill $RUST_API_PID $GO_API_PID 2>/dev/null; trap - EXIT
  mysql_api_reset   # hygiene: drop the mutation fixtures
  lines=$(wc -l < /tmp/hgx-mmut-rust.txt)
  chk "MySQL mutation snapshot is non-trivial (no vacuous pass)" "$((lines >= 60))" "1"
  if cmp -s /tmp/hgx-mmut-rust.txt /tmp/hgx-mmut-go.txt; then d=identical; else d=DIFFERENT; diff /tmp/hgx-mmut-rust.txt /tmp/hgx-mmut-go.txt | head -20; fi
  chk "MySQL API mutation diff: POST/PUT/DELETE responses and status codes match" "$d" "identical"
  # Literal-bytes assertions beside the diff, for the same reason the other two backends
  # carry them: two servers can match each other while both being wrong.
  chk "MySQL §11.2 tz: the zoned spec round-trips through headgate_schedule VERBATIM" \
      "$(grep -Fc "$TZSPEC" /tmp/hgx-mmut-rust.txt)" "1"
  chk "MySQL §11.2 tz: an unknown zone is a 400 carrying the raw message" \
      "$(grep -Fc "$TZMSG" /tmp/hgx-mmut-rust.txt)" "1"
  chk "MySQL §11.2 tz: a zone on \"@every\" is refused, not silently ignored" \
      "$(grep -Fc "$TZEVERY" /tmp/hgx-mmut-rust.txt)" "1"
  # round 32f: the ONE quarantine_release not-found contract, beside the diff.
  # Both servers agreeing is not enough — they agreed on 400-vs-404 being
  # untested for four rounds. These pin the bytes AND the status.
  chk "MySQL §5.2: quarantine_release not-found is one message in both languages" \
      "$(grep -Fc "$QRELMSG" /tmp/hgx-mmut-rust.txt)$(grep -Fc "$QRELMSG" /tmp/hgx-mmut-go.txt)" "11"
  chk "MySQL §5.2: ...and one STATUS — 404, never Go's old fall-through 400" \
      "$(awk '/== DELETE quarantine\//{getline; print; exit}' /tmp/hgx-mmut-rust.txt)|$(awk '/== DELETE quarantine\//{getline; print; exit}' /tmp/hgx-mmut-go.txt)" \
      "$QRELSTATUS|$QRELSTATUS"
  g_asserts /tmp/hgx-mmut-rust.txt /tmp/hgx-mmut-go.txt "MySQL"
  h_asserts /tmp/hgx-mmut-rust.txt /tmp/hgx-mmut-go.txt "MySQL"
else
  skipped "§10.1 API parity over MySQL (whole section)" "no MySQL at $HG_MYSQL — see the MySQL store section above"
fi

# `guarded=` is the anti-vacuity number: how many assertions in this run compared
# against an empty/zero value AND carried a witness proving the fixture was there. A
# jump in it is visible; a zero WITHOUT a witness cannot exist, because `chk` refuses
# one. `skipped=` is separate from `passed=` on purpose — a soft-skipped section is not
# a green section, and reading it as one is how the MySQL half went five rounds unrun.
echo; echo "passed=$pass failed=$fail skipped=$skip guarded-zero-assertions=$guarded"
[ "$skip" -gt 0 ] && echo "NOTE: $skip section(s)/assertion(s) were SKIPPED — this run did not prove them."
printf '#\tfinished\tpassed=%s\tfailed=%s\tskipped=%s\n' "$pass" "$fail" "$skip" >> "$TRANSCRIPT"
echo "assertion transcript: $TRANSCRIPT ($(grep -c '^PASS' "$TRANSCRIPT") executed and green)"
[ "$fail" -eq 0 ]
