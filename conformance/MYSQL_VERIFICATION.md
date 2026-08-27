# MySQL verification runbook

**Why this file exists.** Rounds 32b, 32c, 32d and 32e each shipped MySQL work while no
MySQL server was reachable (127.0.0.1:3307 refuses, no `mysql` client on the host, Docker
off-limits). Four rounds of "it compiles" is not four rounds of verification, and the
per-round checklists those rounds left behind were four partial lists in four different
paragraphs of the capability register. This is the one ordered list that replaces them.

**Where it lives and why.** `conformance/`, beside `CAPABILITY_REGISTER.md` — the register
rows it discharges — and beside `scenarios/`. It is a procedure for the conformance
suite, not competitor research (`docs/`) and not design (`ARCHITECTURE.md`).

**What it is for.** Every step below names what it is the FIRST LIVE PARSE of. The order is
riskiest-and-cheapest first: the constructs that have never been seen by any SQL parser
come first, and they are checked by parsing rather than by running a suite, so a syntax
error surfaces in a second instead of after a ten-minute build. Nothing here is expected
to change behavior. If a step fails, the failure is information about the WRITTEN code,
not about the server.

**Current status (round 32m, 2026-08-26): the written-only era is over.** A fresh
`mysql:8.4` server on `127.0.0.1:3307` accepted the current migration and the complete
MySQL portion of `scripts/test-admission.sh`. The latest three-backend run finished **756
passed, 0 failed, 2 announced skips**; both skips are the pre-existing pending-worker-command
checks whose MySQL harness read path still does not exist. The transcript is
`target/conformance/assertions.tsv` and records `mysql_live=yes`. This run live-parsed the
shared eligibility query through both drivers, exercised both harnesses and both HTTP
servers, and includes age-of-oldest, quiet groups, weighted queues, cost-weighted limits
and every saturation strategy. It also caught the disposable database lagging behind the
current `0001_init.sql` (`weight`/`dispatch_count`, saturation `name`/`on_saturated`, and
`claimed_at_ms`); applying those additive definitions restored a clean live parse. The
separately gated
MySQL suites have since also run serially: **12/12 Rust tests** across the four binaries and
**9/9 Go tests** in the driver package. The first live uniqueness run exposed a broken test
fixture: it scheduled a row at `99_999_999_999_999` and immediately expected the
store-clock promoter to consider it due. The fixture now makes that same row due before
promotion; the corrected test passed. `scripts/verify.sh` as a whole has not yet run after
the round-32m additions, and the current ledger at the end distinguishes that fact. The
ordered steps below remain the recovery and reproduction procedure, while language such
as “first live parse” records why each step was originally introduced.

**Round 32n migration follow-up.** The schema drift above now has a first-class failure
boundary. Rust and Go each ran a new isolated MySQL database through fresh versioned up,
manifest validation, dry-run down, destructive down, reinstall, checksum corruption,
refusal to treat an unversioned schema as fresh, validated adoption, and rejection of the
same missing `headgate_queue_state.dispatch_count` column. Both live tests passed, as did
the matching Postgres cells and manual live runs through both CLIs. Version 1 is recorded
as offline/fresh-install, and `scripts/check-migrations.py` byte-compares the MySQL driver
schema with both embedded language assets. The main `hg` database was not mutated by these
tests; each created and dropped a process-scoped temporary database.

---

## Step 0 — a server, and proof it answers the protocol

```bash
docker run -d --name headgate-mysql -p 127.0.0.1:3307:3306 \
  -e MYSQL_ROOT_PASSWORD=hg -e MYSQL_DATABASE=hg mysql:8.4
```

MySQL **8.0.19 or newer is required**, and not by preference:

- `JOIN LATERAL` (`eligible.sql:117`) is 8.0.14+.
- the `INSERT … VALUES (…) AS new ON DUPLICATE KEY UPDATE` row alias is **8.0.19+**, and
  it is used at seven sites in `crates/headgate-mysql/src/lib.rs` alone
  (`grep -n "AS new"`). On 8.0.18 every upsert in the driver is a syntax error.
- `WITH … AS` CTEs are 8.0+.

`mysql:8.4` satisfies all three. MariaDB does **not** — it has no `AS new` row alias.

A wedged container ACCEPTS TCP through the docker proxy and never answers the protocol,
which is why the suite's probe is a `perl alarm` and not a connect. Confirm liveness the
way the suite will:

```bash
cargo build -q -p headgate-mysql --bin hg-mysql-harness
export HG_MYSQL='mysql://root:hg@127.0.0.1:3307/hg'
perl -e 'alarm 5; exec @ARGV' -- target/debug/hg-mysql-harness promote
```

> **TRAP, and the first thing that will happen to you.** That probe runs `promote_due`,
> and `promote_due` calls `reconcile_inflight` (`crates/headgate-mysql/src/lib.rs:713`),
> which reads `headgate_inflight` — a table round 32b ADDED and no live database has.
> So against a perfectly healthy pre-32b container the probe fails and
> `scripts/test-admission.sh` prints **"skipped: no MySQL"**. A schema gap is
> indistinguishable from a down server at this probe. Do Step 1 before believing it.

## Step 1 — the schema

Apply `crates/headgate-mysql/migrations/0001_init.sql` (231 lines). Prefer a **fresh**
database, because the migration is entirely `CREATE TABLE IF NOT EXISTS` — it adds missing
TABLES to an existing database but never missing COLUMNS.

With a client:

```bash
mysql -h 127.0.0.1 -P 3307 -u root -phg hg < crates/headgate-mysql/migrations/0001_init.sql
```

Without one, the harness is the only tool on this host, and it runs **exactly one
statement per call** (`exec_drop`, not a multi-statement):

```bash
target/debug/hg-mysql-harness sql stmt="CREATE TABLE IF NOT EXISTS headgate_inflight ( ... )"
```

**First live parse of:** the `headgate_inflight` table + its `headgate_inflight_stale`
KEY (round 32b, `migrations/0001_init.sql:136`), and `headgate_active_partition`
(round 32). On a pre-existing container, also the three round-32 `headgate_worker`
columns — those were applied live once, so an old container has them and a fresh one
gets them from the migration.

**Watch for:** a `CREATE TABLE` that fails leaves the rest of the file unapplied and every
later step then fails for the wrong reason. Verify before moving on:

```bash
target/debug/hg-mysql-harness sql stmt="SELECT 1 FROM headgate_inflight LIMIT 1"
target/debug/hg-mysql-harness sql stmt="SELECT 1 FROM headgate_active_partition LIMIT 1"
```

## Step 2 — THE RISKIEST PARSE, and the cheapest: prepare `eligible.sql`

This is the whole point of the ordering. `crates/headgate-mysql/queries/eligible.sql` is
293 lines, 13 CTEs and 13 placeholders, and **not one byte of it in its current form has
ever been seen by a SQL parser.** `PREPARE` parses and plans without reading a row, so it
costs milliseconds and discharges more risk than every other step combined.

```bash
python3 - <<'EOF' > /tmp/hg-prepare.sql
sql = open('crates/headgate-mysql/queries/eligible.sql').read()
sql = sql.replace('/*QUEUES*/', '?')          # what both drivers do, with ONE queue
print("PREPARE hgelig FROM '" + sql.replace("\\", "\\\\").replace("'", "''") + "';")
EOF
mysql -h 127.0.0.1 -P 3307 -u root -phg hg < /tmp/hg-prepare.sql
```

(Without a client: read `/tmp/hg-prepare.sql` and pass it as one `sql stmt=` call. Both
`PREPARE` and the statement it names are session-scoped, so a pooled connection is fine
for a parse check — `PREPARE` alone errors on a parse failure, which is all this step
asks.)

**First live parse of — in the order they appear, all never-parsed:**

| Construct | Site | Round | If it fails, it means |
|---|---|---|---|
| `pol`: three `NOT EXISTS` probes as one boolean CTE | `:86` | 32e | the fast-path predicate is not expressible as written; the arm needs restructuring, not a tweak |
| `pol` → `active_parts` join in the ceiling probe | `:89` | 32e | the ceiling probe must go back to a second `/*QUEUES*/` marker — which breaks the "exactly 2 occurrences" contract both drivers' `ReplaceAll` depends on |
| nested derived table inside `JOIN LATERAL` | `:117` | 32b | the combined-window-sort rewrite is invalid; `rank_part` cannot be applied at the LATERAL's outer level and the two-sort version has to come back |
| `LIMIT ?` inside that derived table | `:129` | 32d | the driver-computed `draw_limit` cannot be a placeholder; adaptive widening needs literal interpolation |
| `(SELECT free FROM pol)` scalar subquery in a `WHERE` | `:179`, `:216` | 32e | a CTE is not referenceable from a scalar subquery here; the predicate must be lifted into a join |
| `elig_free`, the second arm | `:212` | 32e | the policy-free fast path does not exist on MySQL |
| `part_tail` self-join over `candidates` | `:241` | 32d | the escalation verdict cannot see which partitions truncated |
| `elig_n` / `elig_z` reverse-sort trick | `:251` | 32d | z is not computable without an `OFFSET` re-scan |
| CTE-referencing `EXISTS` in `verdict` | `:276` | 32d | the verdict cannot be computed in-statement, and a driver-side one is unsound (it cannot see what the draw dropped) |
| `LEFT JOIN elig_z z ON 1 = 1` | `:279` | 32d | ditto |
| row-constructor comparison `(-t.priority, …) < (-z.priority, …)` | `:284` | 32d | the tail-vs-z test needs three chained comparisons instead |
| scalar subquery over a CTE in the final `SELECT` list | `:293` | 32d | the `'w'` verdict tag cannot ride the same result set |

**Watch for:** MySQL reports only the FIRST syntax error and points at a byte offset in a
293-line string, so fix and re-prepare rather than reading ahead. A `Reference 'pol' not
supported (forward reference)` class of error is the one to expect: MySQL requires a CTE
to be defined before it is referenced, and `pol` is referenced from `elig_policy` and
`elig_free`, both textually after it.

**Do NOT skip to Step 3 on a failure here.** Every later step runs this statement.

## Step 3 — the gate on an EMPTY store: the FAST arm

```bash
target/debug/hg-mysql-harness sql stmt="DELETE FROM headgate_rate_bucket"
target/debug/hg-mysql-harness sql stmt="DELETE FROM headgate_quarantine"
target/debug/hg-mysql-harness sql stmt="DELETE FROM headgate_concurrency_limit"
target/debug/hg-mysql-harness enqueue count=6 prefix=v1- queue=vq partition=A fp=vfp sched=1000
target/debug/hg-mysql-harness admit queues=vq capacity=4 lease_ms=30000 worker=w1 lease=V1 quantum=2
```

With those three tables empty, `pol.free` is TRUE, so this is the **first live execution of
the round-32e fast arm** — and of the driver machinery around it: `/*QUEUES*/` `ReplaceAll`
(round 31's live catch was that a `Replace(…,1)` hit the marker in the file's own header
comment first), the 13-placeholder positional binding order documented in the file header,
the active-partition count statement the driver divides `capacity` by, and the READ
COMMITTED transaction that wraps the whole thing.

**Watch for:** a placeholder-count mismatch surfaces as `Incorrect arguments to EXECUTE`
rather than as a wrong answer, and it is the single most likely failure of round 32e's
`11 → 13` change. A wrong ORDER, by contrast, surfaces as a plausible-looking wrong answer
— which is why Step 5 diffs the arms instead of eyeballing them.

## Step 4 — force the POLICY arm

```bash
target/debug/hg-mysql-harness sql stmt="INSERT INTO headgate_rate_bucket VALUES ('vunused',9,9,9,1000,1000)"
target/debug/hg-mysql-harness admit queues=vq capacity=4 lease_ms=30000 worker=w1 lease=V2 quantum=2
```

A bucket for a class no job uses constrains nothing, so the admitted set must not change —
but `pol.free` is now FALSE. **First live execution of:** `elig_policy`, `bucket_state`,
and the `inflight` CTE's read of `headgate_inflight` (round 32b).

**Watch for:** this is the err-toward-slow assertion. If the answer CHANGES between Step 3
and Step 4, the two arms are not equivalent and the fast path is wrong — which is the one
failure mode that would make round 32e unsafe rather than merely unverified.

## Step 5 — force the ESCALATION

Round 32d's widening needs a fixture where the narrow window binds: many partitions, a
small capacity.

```bash
for p in A B C D E; do
  target/debug/hg-mysql-harness enqueue count=12 prefix=v5$p- queue=vq5 partition=$p fp=vfp5 sched=1000
done
target/debug/hg-mysql-harness admit queues=vq5 capacity=12 lease_ms=30000 worker=w1 lease=V5 quantum=8
```

Narrow limit is `ceil(12/5)+1 = 4`; the fair share is 8. **First live execution of** the
`part_tail`/`elig_n`/`elig_z`/`verdict` chain (MySQL has no `part_stats` — it folds that
aggregate into `part_tail`'s own derived table), the `'w'` tag, and the driver's
second pass (`crates/headgate-mysql/src/lib.rs:874`, the `for draw_lim in [narrow, wide]`
loop; `go/driver/headgatemysql/store.go` has the twin).

**Watch for:** silent UNDER-admission. A verdict that never fires returns fewer rows and
raises no error — so compare against the wide pass by hand:

```bash
# same fixture, quantum 3 => narrow limit ceil(12/5)+1 = 4 = the fair share + 1, no widen
# vs quantum 8 above. The two must agree on the first 12 ids.
```

## Step 6 — the Rust store suite (the 13 `running → *` decrements)

```bash
export HG_TEST_MYSQL='mysql://root:hg@127.0.0.1:3307/hg'
cargo test -p headgate-mysql --test store   -- --test-threads=1
cargo test -p headgate-mysql --test inspect -- --test-threads=1
cargo test -p headgate-mysql --test unique  -- --test-threads=1
cargo test -p headgate-mysql --test orm_interop -- --test-threads=1
```

**First live execution of:** every round-32b `headgate_inflight` decrement on the MySQL
side — all thirteen `running → *` edges of `state_machine.yaml` plus `complete_tx` (which
MySQL duplicates rather than sharing, having no data-modifying CTEs) and the async bulk
`cancel` batch. Also of `crates/headgate-mysql/tests/unique.rs` (round 32c, 439 lines):
the §4.4 GENERATED-column mechanism across all four live states and every terminal state,
and the §4.4-vs-§4.4b classification order.

**Watch for:** the ORDERING TRAP round 32b called out — MySQL has no `RETURNING`, so the
decrement runs FIRST inside the transition's transaction under the identical fence clause.
A decrement that ran after the UPDATE would read the NEW state and never fire, leaving the
counter high; a counter that is too high chokes a partition against its ceiling
**permanently, with no self-healing path**, which is precisely why `reconcile_inflight`
exists and why the suite asserts it by BREAKING the counter by hand.

Also watch for the ONE recorded open corner, which the tests assert rather than fix:
throttle-mode uniqueness with `retention_ms = 0` deletes the row and its `unique_throttle`
value at ack, so a 10-minute window dies immediately. That prediction is from reading
`crates/headgate-mysql/src/lib.rs`; this is the first time it is executed. If it does NOT
hold, the semantic changed and the register's Unique/dedup row must change with it.

**MUST NOT run concurrently** — see `## Serialization` below.

## Step 7 — the Go store + inspect suite

```bash
cd go/driver/headgatemysql
HG_TEST_MYSQL='mysql://root:hg@127.0.0.1:3307/hg' go test -p 1 -parallel 1 ./...
```

**First live execution of:** `go/driver/headgatemysql/inspect.go` — the entire 30-method
surface ported statement-for-statement in round 32c, of which **every statement has been
parsed by Go's compiler and by nothing else**. Also `scheduler_test.go` (the `@every` +
sub-minute cron tick identity and the `CRON_TZ=` re-anchoring through the ODKU's
`IF(headgate_schedule.spec = new.spec, …)`), `inspect_test.go`, and
`TestGoMysqlReclaimExpiredAttributesCrashesAndQuarantines` — reclaim's first direct Go
unit test.

**Watch for:** `database/sql` scans POSITIONALLY. The two deliberate divergences from the
Rust twin exist for that reason (`ListSchedules`/`DueSchedules` name their columns instead
of `SELECT *`; `HeartbeatWorker` pins a `*sql.Conn` for its upsert-then-select). A column
order surprise shows up as a type-conversion error, not as wrong data — that is the good
case. The bad case is two same-typed columns swapped.

## Step 8 — the conformance suite's MySQL store gate

```bash
export PATH="/Applications/Postgres.app/Contents/Versions/16/bin:$PATH"
PGHOST=/tmp PGPORT=5432 REDIS_PORT=6380 \
  HG_MYSQL='mysql://root:hg@127.0.0.1:3307/hg' \
  HG_TEST_PG='host=/tmp port=5432 user=postgres dbname=hg' \
  HG_TEST_REDIS=redis://127.0.0.1:6380 \
  scripts/test-admission.sh
```

**50 assertions** in the store gate. Includes the cross-language MySQL section (Go
enqueues / Rust admits, cross-language ack, drain both directions, byte-identical TABLE
DIFF) and the fail-open + kill-switch pair.

## Step 9 — the API-over-MySQL parity gate

Runs in the same invocation as Step 8, behind the same `mysql_up` probe: **15 assertions**
(the 12-endpoint GET snapshot diff, the 33-request mutation diff, the cluster literals,
and round 32f's two `quarantine_release` contract assertions). Ports 8097 (Rust) / 8098
(Go).

**First live execution of:** `hg-go-api`'s `HG_STORE=mysql` arm — which could not exist
before round 32c, because `go/driver/headgatemysql` declined `InspectStore` and the third
backend was therefore a Rust-only server. This is what takes §10.1 parity from 4/6 to 6/6
server configurations (2 languages × 3 backends).

**Watch for:** the seed's three dialect translations, each of which is what makes a
snapshot time-STABLE rather than merely present — `sample_payload` omitted,
`updated_at_ms` supplied as a fixed value (NOT NULL with no default here, where Postgres
defaults it), `queues` as `JSON_ARRAY(...)` rather than a `text[]` literal. And the reset
EMPTIES `headgate_rate_bucket` before seeding the one PAUSED class, because `avail` is a
function of `NOW()` and two snapshots taken seconds apart would differ.

## Step 10 — the whole gate

```bash
scripts/verify.sh      # same env as Step 8
```

Historical round-32h expectation: **444 assertions** — 335 over PG+Redis + 53 (Step 8) +
56 (Step 9), plus 2 announced SKIPS (the pending-command state check, twice; see the end
of this section). Round 32h changed all three numbers. PG+Redis went 254 → 335 because the
vacuity audit added witnesses (the guarded-zero form asserts the witness as its own
assertion) and thirteen empty-value-filter parity requests with eight literal assertions
beside them. Step 8 went 50 → 53: the kill-switch zero now carries a witness, and the
retention-0 "the row is gone" check is preceded by an assertion that the row was RUNNING
first — `$HM state` prints nothing for a row that never existed, so the old empty-string
comparison could not tell a deletion from an absence. Step 9 went 30 → 56: thirteen
per-endpoint fixture witnesses on the GET snapshot (`api_witness`, including the literal
`"backend": "mysql"` that /meta had been reporting as `postgres` on every backend), one
`/cluster` witness, four in `g_asserts` (two new literals for the explicit-empty periodic
fields, two witnesses on the "must never appear" counts), and eight in `h_asserts`.
Round 32g's figures were: 334 = 254 + 50 + 30. Step 9 grew from round 32f's 15 because round 32g's §10.1 audit added 11
literal-byte assertions per backend (the missing-required-field 422, the empty-signal 400,
the 415, the two query-parameter 400s, the backticked `unknown action` and `bad cron`, the
`x-released-jobs` header, the absent `invalid request: ` Display prefix, and the two
byte-level checks jq was hiding) plus 2 tier-1 STATE assertions per server × 2 servers.
The THIRD state assertion — that an empty signal command did not clear a worker's pending
one — needs to READ `headgate_worker.command`, and this harness's `sql` is `exec_drop`
only, so it prints a skip line rather than a vacuous pass. Giving the MySQL harness a
scalar-read command would recover it and is the one thing worth adding to Step 9 next.

Round 32m's latest actual `scripts/test-admission.sh` checkpoint is **756 passed, 0 failed, 2
skipped** across all three backends, with 43 guarded-zero assertions. That number supersedes the historical expectation
above; it is not called a Step-10 completion because `scripts/verify.sh` itself has not yet
been run after the round-32m additions.

---

## Serialization — what must not run concurrently

One rule, and it is not about load: **`ReclaimExpired`, `PromoteDue`, `EvictRetained` and
the quarantine sweep are FLEET-WIDE.** They do not filter by queue, test, or run. Two
suites against one database will reclaim each other's leases and evict each other's rows,
and the failures look like flaky assertions rather than like interference.

- Steps 6, 7, 8/9 must run **one at a time**. Not in parallel, not in two terminals.
- Within Step 6, the Rust crate's four test binaries must run one at a time
  (`cargo test --test <name>`, not `cargo test -p headgate-mysql`), and each with
  `--test-threads=1`. The source files say so at
  `crates/headgate-mysql/tests/orm_interop.rs:12` and `crates/headgate-mysql/tests/unique.rs:24`.
- Within Step 7, `go test -p 1 -parallel 1`. `go/driver/headgatemysql/store_test.go:11`
  records why: a default-config container has been WEDGED by full-parallel suites before,
  and a wedged container accepts TCP without answering, which sends you back to Step 0
  chasing a phantom.
- Steps 3–5 are hand-driven single statements and are safe beside nothing else running.

Assertions that could not be scoped are already written defensively (reclaim assertions
scoped to their own job ids; `distinct_kinds` sampled wide, because bounded samples fill
with strays first). Do not add unscoped ones.

---

## The ledger — what is verified, what is only written

The detailed table below is the **historical pre-live snapshot from round 32h**. It is kept
because it explains which step was intended to discharge each artifact; its `NO` cells are
not current status. The compact round-32m ledger immediately after it is authoritative.
"Compiles" means `cargo build` / `go build` accept it. "Parsed" means a MySQL server has
read the statement. "Run" means an assertion observed its effect.

| Artifact | Round | Compiles | Parsed by a server | Test ever run | Discharged by |
|---|---|---|---|---|---|
| `eligible.sql` — combined window sort (`rank_part` at the LATERAL's outer level, nested derived table) | 32b | yes | **NO** | **NO** | Step 2, 3 |
| `eligible.sql` — `inflight` CTE reading `headgate_inflight` | 32b | yes | **NO** | **NO** | Step 2, 4 |
| `eligible.sql` — adaptive widening chain (`part_tail`, `elig_n`, `elig_z`, `verdict`, tag `'w'`) | 32d | yes | **NO** | **NO** | Step 2, 5 |
| `eligible.sql` — `pol` predicate + `elig_free` fast arm | 32e | yes | **NO** | **NO** | Step 2, 3, 4 |
| `eligible.sql` — 13-placeholder positional order (was 11) | 32d/32e | yes (both drivers) | **NO** | **NO** | Step 3 |
| `headgate_inflight` DDL + `headgate_inflight_stale` KEY | 32b | n/a | **NO** | **NO** | Step 1 |
| `headgate_active_partition` maintenance + `DELETE … FOR UPDATE SKIP LOCKED` pruner | 32 | yes | **NO** (MySQL half) | **NO** | Step 3, 8 |
| The 13 `running → *` inflight decrements, Rust | 32b | yes | **NO** | **NO** | Step 6 |
| The 13 `running → *` inflight decrements, Go | 32b | yes | **NO** | **NO** | Step 7 |
| `reconcile_inflight` (Rust + Go) | 32b | yes | **NO** | **NO** | Step 6, 7, 8 |
| Driver-side escalation loop (Rust `lib.rs:874`, Go twin) | 32d | yes | **NO** | **NO** | Step 5 |
| `go/driver/headgatemysql/inspect.go`, all 30 methods | 32c | yes | **NO** | **NO** | Step 7 |
| `go/driver/headgatemysql/scheduler_test.go` | 32c | yes | **NO** | **NO** | Step 7 |
| `TestGoMysqlReclaimExpiredAttributesCrashesAndQuarantines` | 32c | yes | **NO** | **NO** | Step 7 |
| `crates/headgate-mysql/tests/unique.rs` (439 lines, §4.4 generated columns) | 32c | yes | **NO** | **NO** | Step 6 |
| The recorded throttle + `retention_ms = 0` open corner | 32c | n/a — read from source, never executed | **NO** | **NO** | Step 6 |
| `hg-go-mysql-harness` inspect commands | 32c | yes | **NO** | **NO** | Step 8 |
| `hg-go-api` `HG_STORE=mysql` arm | 32c | yes | **NO** | **NO** | Step 9 |
| API-over-MySQL parity section (15 assertions) | 32c/32f | yes | **NO** | **NO** | Step 9 |
| `quarantine_release` `not found:` prefix, Go MySQL driver | 32f | yes | **NO** | **NO** | Step 9 |
| `JobFilter`/`Counts` empty-value port change, `go/driver/headgatemysql/inspect.go` | 32h | yes (vet + build) | **NO** | **NO** | Step 7, 9 |
| `h_asserts` empty-value filter parity (8 assertions) over MySQL | 32h | yes | **NO** | **NO** | Step 9 |
| `api_witness` per-endpoint fixture witnesses (13) over MySQL, incl. `"backend": "mysql"` | 32h | yes | **NO** | **NO** | Step 9 |
| MySQL store-section vacuity fixes (kill-switch witness, retention-0 RUNNING precondition) | 32h | yes | **NO** | **NO** | Step 8 |
| `crates/headgate-mysql/tests/inspect.rs` — the tz ADVANCE assertion and the bulk-cancel `affected` precondition | 32h | yes | **NO** | **NO** | Step 6 |
| `go/driver/headgatemysql/{inspect,store,orm_interop}_test.go` — `found` flag, CAS error check, commit-first control, committed-sibling control | 32h | yes | **NO** | **NO** | Step 7 |
| `priority=` on `hg-mysql-harness` and `hg-go-mysql-harness` | 32j | yes (cargo + go build) | **NO** | **NO** | Step 8 |
| MySQL priority-ordering assertions (2): the SQL gate draws `priority DESC` first; the column holds 0/9/5 | 32j | yes | **NO** | **NO** | Step 8 |

At round 32h, everything the register's §13 row claimed about MySQL — the restructures,
the widening and the fast arm — rested on this table's third and fourth columns being
**NO**. That historical warning did its job: round 32m ran the gate against a real server.

**Round 32j adds two more rows and one number.** The MySQL store gate is now **55**
assertions rather than 53, so the whole-suite expectation once a server is reachable is
**521** (400 measured over PG + Redis, plus 55 here, plus 66 in the API gate). The two new
ones pin that `eligible.sql` orders by `priority DESC` ahead of `scheduled_at_ms` — the
SQL-side half of the divergence documented in ARCHITECTURE §6, whose Redis half IS measured.
At round 32j they were typed only; round 32m's live store section now executes them.

**Round 32j also makes the written/run distinction MECHANICAL rather than editorial.**
`conformance/EVIDENCE.md` cites MySQL evidence as `sh-mysql:` / `rust-mysql:` / `go-mysql:`,
and `scripts/check-evidence.py` DERIVES which marking is correct — from the assertion
transcript for shell labels, and from `HG_TEST_MYSQL` in the file for test functions — then
fails if the author's marking disagrees, in either direction. So a MySQL claim can no longer
be quietly cited as though it ran, and the day a server appears the same linter fails on any
`-mysql` citation that still does not run. Two register rows (**Go MySQL driver** and
**MySQL Inspect**) had every citation MySQL-gated. With the round-32m transcript carrying
`mysql_live=yes`, the linter now requires every cited shell label to have actually run.

### Round 32m live ledger (authoritative)

| Scope | Parsed by MySQL 8.4 | Observed by a running assertion | Remaining gap |
|---|---|---|---|
| Fresh `0001_init.sql`, including inflight/active-partition and the new partition-counter/oldest-age indexes | yes | yes — the store and metrics fixtures write/read them | Existing installations still need the project's eventual migration-upgrade path; this run intentionally used a fresh database |
| `eligible.sql` through Rust and Go, including policy/fast/widening arms reached by the store matrix | yes | yes — the MySQL store gate and cross-language claims passed | Full mutation sweep was not repeated specifically on MySQL |
| Rust and Go MySQL harnesses | yes | yes — enqueue/admit/ack/inspect plus age/quiet `qstats` paths passed | The worker pending-command scalar read remains absent |
| Rust and Go APIs with `HG_STORE=mysql` | yes | yes — GET snapshots, mutation parity, headers and raw bytes passed | Two pending-command state assertions remain announced skips |
| Age of oldest + quiet-group metrics | yes | yes — 10 MySQL assertions, both languages, including empty/balanced controls | The >1,000-partition `approximate` branch is not exercised |
| Queue-weight + saturation-policy configuration surfaces | yes | yes — Rust and Go APIs independently write/read both policies, and the six-cell admission matrix exercises queue weighting plus every saturation strategy | No gap in the live language/backend matrix |
| `crates/headgate-mysql/tests/*` under `HG_TEST_MYSQL` | yes | yes — 14/14 across `bounded_pool`, `inspect`, `multi_instance`, `orm_interop`, `store` and `unique`; the crate's generated-index SQL-shape test makes 15 total | No behavior gap in these binaries |
| `go/driver/headgatemysql/*_test.go` under `HG_TEST_MYSQL` | yes | yes — 12/12 across the six test files | No behavior gap in this package |
| Entire `scripts/verify.sh` after round 32r | n/a | **yes — ALL GREEN**: 756 shell assertions, 96 scenario assertions, 714 inventory-guarded tests and 501 evidence citations | Two MySQL pending-command scalar-read assertions remain explicitly skipped in the shell corpus; no Rust or Go test skipped |

### Round 32q named-lock ledger

The migration lock is a live MySQL behavior, not a source-only claim. With
`HG_TEST_MYSQL=mysql://root:hg@127.0.0.1:3307/hg`, both
`live_mysql_configured_lock_namespace_avoids_an_application_lock` and
`TestLiveMySQLConfiguredLockNamespaceAvoidsAnApplicationLock` ran against MySQL 8.4 before
the register row changed. Each test held the default/application key and the configured
key simultaneously, observed the migration remain pending, released only the configured
key, observed migration complete, and then observed the default/application key still
held. Rust and Go pure tests additionally pin the long-database hash encoding and CLI
rejection paths. Remaining gap: the repository-wide verification run after round 32q is
recorded by the normal gate, not by this focused ledger entry.

### Round 32r connection-budget ledger

Both MySQL runtime cells ran live with a caller pool capped at four: Rust
`connection_budget_keeps_renewal_acks_and_duties_live_behind_held_transactions` and Go
`TestConnectionBudgetKeepsRenewalAcksAndDutiesLiveBehindHeldTransactions`. Two `once`
callbacks synchronized after retaining their connections and held them for 2.5s under a
900ms lease. Each test captured the then-current store-issued deadline and, after the
store clock crossed it, observed both jobs still running with later lease deadlines, four
sibling jobs already acked, and all six duty rows held by the test worker. All six jobs
then completed. `mysql_async::Pool::metrics` and `sql.DB.Stats` never reported a fifth
physical connection. The deadline-relative witness replaced a timing-sensitive 300ms
wall-clock assumption found by the repository-wide gate; both MySQL cells and both
Postgres cells reran live afterward. This is the live MySQL half of the documented
`P = T + 2` contract; it is not inferred from the Postgres tests.

The full-gate run also found two real InnoDB cycles outside the pool test. Lazy uniqueness
release filtered on unindexed `unique_key`, so a duplicate enqueue could next-key-lock
unrelated jobs; it now releases through `unique_throttle` and finds winners through
`unique_active` / `unique_throttle`. Enqueue also used the inverse lock order from the
active-partition pruner and inflight reconciler (`job -> route` versus `route -> job`).
Both Rust and Go now reserve sorted route/counter rows before inserting jobs. The generated
index choices have pure source-shape tests, the five live uniqueness cases passed ten
consecutive parallel runs, both complete MySQL driver suites passed, and the final
repository gate passed with MySQL live.

### Round 32aj progress and migration-v5 ledger

Migration v5 (`job_progress`) ran live through both migrators on isolated PostgreSQL and
MySQL databases: fresh up, validation, dry-run/down/up, adoption, drift rejection, and the
configured MySQL lock namespace all passed. Its driver, Rust, and Go assets are byte-
identical in both SQL dialects. The main MySQL test database was upgraded to v5 before the
six-cell store contract ran.

Rust and Go each wrote and read exact `current / total` progress, an optional message,
writer fence, and store-clock timestamp through MySQL. The contract then turned over the
lease, accepted the new holder's replacement, rejected the stale holder, retained the
application's last report through completion, and deleted progress with a retention-zero
job. The same sequence passed on PostgreSQL and Redis. The final repository gate ended
`passed=957 failed=0 skipped=2 guarded-zero-assertions=79`, scenarios 96/96, Rust/Go test
skip ledger 0/0, evidence debt 0, and `ALL GREEN`; the two shell skips remain the announced
pending-command scalar reads and do not touch progress.

That final run also made the pruner/enqueue deadlock reproducible. InnoDB reported the
active-partition pruner holding a `headgate_job` index gap while enqueue held the route row
and waited to insert. The pruner's documentation required READ COMMITTED, but the Rust
transaction used MySQL's default REPEATABLE READ. It now selects the documented isolation,
and Rust plain enqueue matches Go's READ COMMITTED path. The concurrent four-test MySQL
store suite passed twice before passing again inside the all-up gate.
