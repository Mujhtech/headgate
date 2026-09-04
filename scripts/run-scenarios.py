#!/usr/bin/env python3
"""Execute conformance/scenarios/*.yaml against a live backend, in both languages.

WHY THIS EXISTS (round 32j)
---------------------------
Round 32i's mutation testing found that `conformance/scenarios/admission.yaml` — which
AGENTS.md cites as the regression guard for trap 0, "time comes from the store, never the
caller" — was executed by NOTHING. `crates/headgate-conformance/src` and `go/conformance`
were empty directories, and `scripts/verify.sh` only `yaml.safe_load`ed the files, i.e. it
proved they parsed. A guard that does not run is not a guard, and a citation pointing at
one is worse than no citation because it retires the question.

WHY PYTHON, AND NOT RUST OR GO
------------------------------
The scenarios are a CROSS-LANGUAGE corpus: §3.2's first line is "every scenario runs
against every backend, in BOTH languages". A runner written in Rust proves the Rust store;
a runner written in Go proves the Go store; neither proves the claim the file makes. What
already exists in both languages, with ONE identical CLI grammar and ONE identical output
format, is the harness binaries the shell suite drives (`hg-pg-harness` / `hg-go-harness`,
`hg-redis-harness` / `hg-go-redis-harness`). Driving those from a third language covers
2 languages x 2 backends with zero new dependencies in either implementation language —
and adding a YAML dependency to the Rust workspace or the Go core is exactly the kind of
thing invariant 8 exists to prevent creeping in. python3 + PyYAML is already a hard
dependency of `scripts/verify.sh`.

WHAT CHANGED IN THE SCENARIO FILES
----------------------------------
The `then:` clauses were half prose ("the skewed worker claimed 0", "lease_expires_at_ms
is within lease_ms of STORE time, not worker time"). Prose cannot be executed, so those
clauses were rewritten into the small check grammar below. Every `why:` block is preserved
VERBATIM — the prose is the load-bearing part of the file, and it is the half that records
what was measured and why the obvious implementation was wrong.

`conformance/scenarios/lifecycle.yaml` was DELETED rather than ported: its verbs
(`fail_at_step`, `deploy`, `complete_step`, `run_janitor`, `w1_attempts_next_step`) need
the WORKER RUNTIME, not the store port, so a runner for it would be a second test
framework standing beside `headgate::testing::drain` / `Runner.Drain`, which already run
those exact cases. One of its scenarios also specified `step_weights`, which exists nowhere
in the tree — an unmarked feature claim. Its coverage is now cited row by row in
`conformance/EVIDENCE.md`.

THE GRAMMAR
-----------
given:
  rate_class: {name, limit, window_ms, burst, tokens?}   seed one bucket
  quarantine: [fingerprint, ...]                          seed quarantined fingerprints
  jobs: [{count, prefix, queue?, partition_key?, rate_class?, fingerprint?,
          scheduled_at_ms?, priority?}, ...]
when:  a list of one-key steps
  admit: {worker, capacity, quantum?, lease_ms?, queue?}
  admit_concurrently: {workers, capacity, quantum?, lease_ms?, queue?}
  sleep_ms: N
then:  a list of check strings, grammar in CHECKS below
"""

import glob
import json
import os
import re
import subprocess
import sys
import time

import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(ROOT)

PGHOST = os.environ.get("PGHOST", "/tmp")
PGPORT = os.environ.get("PGPORT", "5433")
PGDATABASE = os.environ.get("PGDATABASE", "hg")
REDIS_PORT = os.environ.get("REDIS_PORT", "6380")
PGCONN = f"host={PGHOST} port={PGPORT} user=postgres dbname={PGDATABASE}"
REDIS_URL = f"redis://127.0.0.1:{REDIS_PORT}"

PASSED = 0
FAILED = 0
TRANSCRIPT = os.environ.get(
    "HG_SCENARIO_TRANSCRIPT", "target/conformance/scenarios.tsv"
)


def rec(status, label):
    with open(TRANSCRIPT, "a") as f:
        f.write(f"{status}\t{label}\n")


def check(label, got, want):
    global PASSED, FAILED
    if got == want:
        print(f"  ✅ {label} ({got})")
        PASSED += 1
        rec("PASS", label)
    else:
        print(f"  ❌ {label}: got {got!r} want {want!r}")
        FAILED += 1
        rec("FAIL", label)


def run(cmd, env=None, check_rc=True):
    e = dict(os.environ)
    if env:
        e.update(env)
    p = subprocess.run(cmd, capture_output=True, text=True, env=e)
    if check_rc and p.returncode != 0:
        # The harnesses print their RAW error to STDOUT (that is deliberate — the shell
        # suite diffs the two languages' messages against each other), so both streams
        # go in the diagnostic or a failure reads as empty.
        raise RuntimeError(
            f"{' '.join(cmd)} failed rc={p.returncode}: "
            f"{(p.stdout.strip() + ' ' + p.stderr.strip()).strip()}"
        )
    return p.stdout


# --------------------------------------------------------------------------
# Backend adapters. Each supplies the fixture seeding and the READ-BACK the
# checks need; the ADMIT itself always goes through a harness binary, so the
# thing under test is the store port and never this file.
# --------------------------------------------------------------------------
class PgBackend:
    name = "postgres"
    env = {"HG_PG": PGCONN}

    def psql(self, sql):
        return run(
            [
                "psql",
                "-h",
                PGHOST,
                "-p",
                PGPORT,
                "-U",
                "postgres",
                "-d",
                PGDATABASE,
                "-qtA",
                "-c",
                sql,
            ]
        ).strip()

    def reset(self):
        self.psql(
            "TRUNCATE headgate_job_tag, headgate_job, headgate_queue_sample, "
            "headgate_rate_bucket, headgate_quarantine, "
            "headgate_partition_deficit, headgate_concurrency_limit, headgate_queue_counter, "
            "headgate_active_partition, headgate_inflight, headgate_queue_state;"
        )

    def seed_rate_class(self, c):
        tokens = c.get("tokens", c.get("burst", c["limit"]))
        self.psql(
            "INSERT INTO headgate_rate_bucket VALUES "
            f"('{c['name']}',{tokens},{c.get('burst', c['limit'])},{c['limit']},"
            f"{c['window_ms']},{c.get('refilled_at_ms', 1000)});"
        )

    def empty_bucket_at_store_now(self, c):
        """Stamp a bucket EMPTY at the STORE's own clock. Trap 0's refill half: a gate
        measuring elapsed time against the store reads ~0ms of refill; one measuring
        against a 60s-fast caller would refill a whole second bucket."""
        self.psql(
            "INSERT INTO headgate_rate_bucket VALUES "
            f"('{c['name']}',0,{c.get('burst', c['limit'])},{c['limit']},{c['window_ms']},"
            "(extract(epoch from clock_timestamp())*1000)::bigint);"
        )

    def seed_quarantine(self, fps):
        for fp in fps:
            self.psql(
                "INSERT INTO headgate_quarantine (fingerprint, kind, crash_count, "
                f"quarantined_at_ms, reason) VALUES ('{fp}','w',3,1000,'scenario') "
                "ON CONFLICT DO NOTHING;"
            )

    def bucket_tokens(self, name):
        return self.psql(f"SELECT tokens FROM headgate_rate_bucket WHERE name='{name}';")

    def store_now_ms(self):
        return int(self.psql("SELECT (extract(epoch from clock_timestamp())*1000)::bigint;"))

    def job_states(self, where):
        return self.psql(
            f"SELECT string_agg(DISTINCT state::text, ',') FROM headgate_job WHERE {where};"
        )

    def states_of_fingerprint(self, fp):
        return self.job_states(f"fingerprint='{fp}'")

    def leases_outside_running(self):
        return int(
            self.psql(
                "SELECT count(*) FROM headgate_job WHERE lease_id IS NOT NULL AND state<>'running';"
            )
        )

    def lease_expiry(self, job_id):
        return int(self.psql(f"SELECT lease_expires_at_ms FROM headgate_job WHERE ulid='{job_id}';"))

    def running_count(self):
        return int(self.psql("SELECT count(*) FROM headgate_job WHERE state='running';"))


class RedisBackend:
    name = "redis"
    env = {"HG_REDIS": REDIS_URL, "HG_REDIS_PREFIX": "hg"}

    def cli(self, *args):
        return run(["redis-cli", "-p", REDIS_PORT] + [str(a) for a in args]).strip()

    def reset(self):
        self.cli("flushall")

    def seed_rate_class(self, c):
        tokens = c.get("tokens", c.get("burst", c["limit"]))
        self.cli(
            "hset", f"hg:rate:{c['name']}", "tokens", tokens, "burst", c.get("burst", c["limit"]),
            "limit", c["limit"], "window", c["window_ms"], "refilled", c.get("refilled_at_ms", 1000),
        )

    def empty_bucket_at_store_now(self, c):
        now = self.store_now_ms()
        self.cli(
            "hset", f"hg:rate:{c['name']}", "tokens", 0, "burst", c.get("burst", c["limit"]),
            "limit", c["limit"], "window", c["window_ms"], "refilled", now,
        )

    def seed_quarantine(self, fps):
        for fp in fps:
            self.cli("sadd", "hg:quarantine", fp)

    def bucket_tokens(self, name):
        return self.cli("hget", f"hg:rate:{name}", "tokens")

    def store_now_ms(self):
        out = self.cli("time").split()
        return int(out[0]) * 1000 + int(out[1]) // 1000

    def states_of_fingerprint(self, fp):
        states = set()
        for key in self.cli("keys", "hg:job:*").split():
            if self.cli("hget", key, "fingerprint") == fp:
                states.add(self.cli("hget", key, "state"))
        return ",".join(sorted(states))

    def leases_outside_running(self):
        n = 0
        for key in self.cli("keys", "hg:job:*").split():
            if self.cli("hget", key, "lease_id") and self.cli("hget", key, "state") != "running":
                n += 1
        return n

    def lease_expiry(self, job_id):
        return int(self.cli("hget", f"hg:job:{job_id}", "lease_expires_at_ms"))

    def running_count(self):
        n = 0
        for key in self.cli("keys", "hg:job:*").split():
            if self.cli("hget", key, "state") == "running":
                n += 1
        return n


# --------------------------------------------------------------------------
# The four (language, backend) cells. Both harnesses in a pair take the SAME
# arguments and print the SAME `id|lease_id|fence|partition_key|rate_class`
# line, which is what makes one runner able to drive both.
# --------------------------------------------------------------------------
CELLS = [
    ("rust", PgBackend, "target/debug/hg-pg-harness"),
    ("go", PgBackend, "target/debug/hg-go-harness"),
    ("rust", RedisBackend, "target/debug/hg-redis-harness"),
    ("go", RedisBackend, "target/debug/hg-go-redis-harness"),
]


def harness(bin_path, backend, *args):
    return run([bin_path] + list(args), env=backend.env)


def parse_claims(out):
    claims = []
    for line in out.splitlines():
        if "|" not in line:
            continue
        f = line.split("|")
        claims.append({"id": f[0], "lease_id": f[1], "fence": int(f[2]),
                       "partition": f[3], "rate_class": f[4]})
    return claims


# --------------------------------------------------------------------------
# CHECKS. The executable replacement for the prose `then:` clauses.
# --------------------------------------------------------------------------
def evaluate(expr, st, backend, label):
    """`st` is the scenario's accumulated state: every admit's claims, plus the
    fixture. Each check returns (got, want) as strings so the diagnostic prints
    the value, never just `False`."""
    last = st["steps"][-1] if st["steps"] else []
    allc = [c for s in st["steps"] for c in s]

    m = re.fullmatch(r"claimed == (\d+)", expr)
    if m:
        return str(len(last)), m.group(1)

    m = re.fullmatch(r"claimed_total == (\d+)", expr)
    if m:
        return str(len(allc)), m.group(1)

    m = re.fullmatch(r"step\[(\d+)\]\.claimed == (\d+)", expr)
    if m:
        return str(len(st["steps"][int(m.group(1))])), m.group(2)

    m = re.fullmatch(r"claimed_partitions == \{(.*)\}", expr)
    if m:
        want = {}
        for part in m.group(1).split(","):
            k, v = part.split(":")
            want[k.strip()] = int(v)
        got = {}
        for c in last:
            got[c["partition"]] = got.get(c["partition"], 0) + 1
        return json.dumps(got, sort_keys=True), json.dumps(want, sort_keys=True)

    m = re.fullmatch(r"claimed_ids == \[(.*)\]", expr)
    if m:
        want = sorted(x.strip() for x in m.group(1).split(",") if x.strip())
        return ",".join(sorted(c["id"] for c in last)), ",".join(want)

    m = re.fullmatch(r"claimed_id_prefixes == \[(.*)\]", expr)
    if m:
        want = sorted(x.strip() for x in m.group(1).split(",") if x.strip())
        got = sorted({re.sub(r"\d+$", "", c["id"]) for c in last})
        return ",".join(got), ",".join(want)

    m = re.fullmatch(r"rate_bucket\((\w[\w-]*)\)\.tokens == (\d+)", expr)
    if m:
        return str(backend.bucket_tokens(m.group(1))), m.group(2)

    if expr == "distinct_claimed == claimed_total":
        return str(len({c["id"] for c in allc})), str(len(allc))

    if expr == "claimed_total > 0":
        return "yes" if len(allc) > 0 else "no", "yes"

    if expr == "every_claim_carries_a_lease_id":
        bad = [c["id"] for c in allc if not c["lease_id"]]
        return f"{len(allc)} claims, {len(bad)} without a lease", f"{len(allc)} claims, 0 without a lease"

    if expr == "every_claim_carries_a_fence_above_zero":
        bad = [c["id"] for c in allc if c["fence"] < 1]
        return f"{len(allc)} claims, {len(bad)} at fence 0", f"{len(allc)} claims, 0 at fence 0"

    if expr == "the_store_running_set_is_exactly_what_was_claimed":
        return str(backend.running_count()), str(len({c["id"] for c in allc}))

    if expr == "no_job_holds_a_lease_outside_running":
        return f"{len(allc)} claims, {backend.leases_outside_running()} stray leases", \
               f"{len(allc)} claims, 0 stray leases"

    m = re.fullmatch(r"jobs_with_fingerprint\((\S+)\)\.states == \[(.*)\]", expr)
    if m:
        want = ",".join(sorted(x.strip() for x in m.group(2).split(",") if x.strip()))
        return backend.states_of_fingerprint(m.group(1)), want

    # TRAP 0. The scenario asks whether lease expiry is a function of the STORE's clock.
    # There is no caller clock to skew — the admit path takes no `now_ms` — so the
    # executable form of "not the worker's clock" is: expiry lands within a tight window
    # of (store clock read from a SECOND connection) + lease_ms. A gate stamping from a
    # caller's clock would need that caller to be within the tolerance of the store, which
    # is precisely the NTP assumption trap 0 exists to remove.
    m = re.fullmatch(r"lease_expiry_tracks_the_store_clock\(lease_ms=(\d+), tolerance_ms=(\d+)\)", expr)
    if m:
        lease_ms, tol = int(m.group(1)), int(m.group(2))
        now = backend.store_now_ms()
        offs = [backend.lease_expiry(c["id"]) - now - lease_ms for c in allc]
        bad = [o for o in offs if abs(o) > tol]
        return f"{len(offs)} leases, {len(bad)} outside +-{tol}ms of store now + {lease_ms}", \
               f"{len(offs)} leases, 0 outside +-{tol}ms of store now + {lease_ms}"

    raise RuntimeError(f"{label}: unknown check expression {expr!r}")


# --------------------------------------------------------------------------
def run_scenario(sc, lang, backend, bin_path):
    label_prefix = f"{lang}/{backend.name} {sc['id']}"
    backend.reset()

    given = sc.get("given") or {}
    if "rate_class" in given:
        rc = given["rate_class"]
        if rc.get("empty_at_store_now"):
            backend.empty_bucket_at_store_now(rc)
        else:
            backend.seed_rate_class(rc)
    for spec in given.get("jobs", []):
        count = spec["count"]
        prefix = spec.get("prefix", "j")
        for batch, offset in enumerate(range(0, count, 1000)):
            size = min(1000, count - offset)
            # Enqueue has a deliberate 1,000-job request ceiling. Large adversarial
            # fixtures still need their full cardinality, so build them through several
            # bounded calls with distinct, original-prefix-preserving job IDs.
            batch_prefix = prefix if count <= 1000 else f"{prefix}{batch}-"
            args = [
                "enqueue",
                f"count={size}",
                f"prefix={batch_prefix}",
                f"queue={spec.get('queue', 'default')}",
                f"partition={spec.get('partition_key', '')}",
                f"rate={spec.get('rate_class', '')}",
                f"fp={spec.get('fingerprint', 'fp')}",
                f"sched={spec.get('scheduled_at_ms', 1000)}",
                f"priority={spec.get('priority', 0)}",
                "retention=86400000",
            ]
            harness(bin_path, backend, *args)
    # Quarantine is seeded AFTER the jobs, and that ORDER IS THE SEMANTIC: §5.2 makes
    # `enqueue` of a quarantined fingerprint a hard rejection, so seeding it first would
    # make the fixture unbuildable. The state the scenario describes — jobs already
    # waiting when their fingerprint is quarantined — is exactly what the sweeper and the
    # gate exclusion exist to handle.
    if "quarantine" in given:
        backend.seed_quarantine(given["quarantine"])

    st = {"steps": []}
    for i, step in enumerate(sc.get("when", [])):
        (verb, arg), = step.items()
        if verb == "sleep_ms":
            time.sleep(arg / 1000.0)
            # A placeholder so `step[N]` indexes the `when:` list the scenario author
            # reads, not a compacted list of admits only — off-by-one in a check
            # expression is a silently wrong assertion, which is the class this whole
            # round exists to kill.
            st["steps"].append([])
            continue
        if verb == "admit":
            out = harness(
                bin_path, backend, "admit",
                f"queues={arg.get('queue', 'default')}",
                f"capacity={arg.get('capacity', 1)}",
                f"lease_ms={arg.get('lease_ms', 30000)}",
                f"worker={arg.get('worker', 'w1')}",
                f"lease=L{sc['id'][:8]}{i}",
                f"quantum={arg.get('quantum', 1000)}",
            )
            st["steps"].append(parse_claims(out))
            continue
        if verb == "admit_concurrently":
            procs = []
            for w in range(arg["workers"]):
                e = dict(os.environ)
                e.update(backend.env)
                procs.append(subprocess.Popen(
                    [bin_path, "admit",
                     f"queues={arg.get('queue', 'default')}",
                     f"capacity={arg.get('capacity', 1)}",
                     f"lease_ms={arg.get('lease_ms', 30000)}",
                     f"worker=cw{w}", f"lease=CL{w}",
                     f"quantum={arg.get('quantum', 1000)}"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=e))
            claims = []
            for p in procs:
                out, _ = p.communicate()
                claims += parse_claims(out)
            st["steps"].append(claims)
            continue
        raise RuntimeError(f"{label_prefix}: unknown verb {verb!r}")

    for expr in sc["then"]:
        got, want = evaluate(expr, st, backend, label_prefix)
        check(f"{label_prefix}: {expr}", got, want)


def main():
    os.makedirs(os.path.dirname(TRANSCRIPT), exist_ok=True)
    open(TRANSCRIPT, "w").close()

    files = sorted(glob.glob("conformance/scenarios/*.yaml"))
    if not files:
        print("FATAL: no scenario files — conformance/scenarios/ is empty, which is the "
              "exact state round 32j existed to end")
        return 2

    # Preflight, the same rule the shell suite runs on: a runner that goes green because
    # the store is unreachable is worse than no runner.
    for path, why in [("target/debug/hg-pg-harness", "cargo build -p headgate-postgres --bin hg-pg-harness"),
                      ("target/debug/hg-redis-harness", "cargo build -p headgate-redis --bin hg-redis-harness"),
                      ("target/debug/hg-go-harness", "go build -o target/debug/hg-go-harness ./driver/headgatepgx/cmd/hg-go-harness"),
                      ("target/debug/hg-go-redis-harness", "go build -o target/debug/hg-go-redis-harness ./driver/headgateredis/cmd/hg-go-redis-harness")]:
        if not os.path.exists(path):
            print(f"FATAL: {path} missing — build it with: {why}")
            return 2

    total_scenarios = 0
    for path in files:
        doc = yaml.safe_load(open(path))
        print(f"== {path} (capability: {doc['capability']}) ==")
        for lang, BE, binp in CELLS:
            backend = BE()
            print(f"-- {lang} x {backend.name} --")
            for sc in doc["scenarios"]:
                run_scenario(sc, lang, backend, binp)
                total_scenarios += 1

    print()
    print(f"scenarios={total_scenarios} passed={PASSED} failed={FAILED}")
    with open(TRANSCRIPT, "a") as f:
        f.write(f"#\tfinished\tpassed={PASSED}\tfailed={FAILED}\n")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
