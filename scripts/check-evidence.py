#!/usr/bin/env python3
"""Invariant 5's missing layer: resolve every ✅/🔶 register row to real, RUNNING evidence.

THE PROBLEM THIS EXISTS FOR (round 32i's finding, round 32j's fix)
------------------------------------------------------------------
AGENTS.md invariant 5: "No capability is declared unless its conformance scenarios pass."
Round 32i mutation-tested all sixteen invariants and could not test this one, because
NOTHING MECHANICALLY CONNECTED A ✅ ROW TO EVIDENCE. The register is 129 rows of honest,
dense, hand-written prose — and prose is exactly what a status symbol can drift away from
without anything going red. apalis's `reenqueue_orphaned_after()` (public, settable,
documented, never called) is the cautionary tale invariant 5 quotes; round 32i then found
this repo's own copy of it, `Weighted queues`, sitting at ✅ with no implementation at all.

WHY A SIDECAR FILE AND NOT A COLUMN OR A TRAILER
------------------------------------------------
The register's Notes cells run past 10,000 characters of load-bearing prose, and that
prose MUST survive — it is the reasoning, the measurements, and the record of what each
round found. Three shapes were considered:

  * a fourth TABLE COLUMN — pushes the machine-readable field to the far right of a cell
    nobody can already read to the end of, and every future annotation widens the row
    further;
  * a STRUCTURED TRAILER inside the Notes cell — buries the parse target at the end of a
    10k-character cell, where the next round's "**Round 32k:**" note lands AFTER it and
    silently breaks the parse. Rounds 32c through 32i all appended exactly there;
  * a SIDECAR keyed by row name — small parse target, diff-legible, and populating it
    never edits the register at all.

The sidecar's one real risk is drift between the two files, and that is precisely what
this linter removes: the join is enforced in BOTH directions (a ✅ row with no block is a
failure, and a block naming no row is a failure), so the pair cannot fall out of step
without the gate going red.

WHY THE TRANSCRIPT AND NOT A GREP OF THE SUITE
-----------------------------------------------
"A label exists in scripts/test-admission.sh" and "a label RAN" are different facts, and a
✅ is a claim about the second. ~55 assertions in that file have never once executed —
MySQL has been unreachable for eight rounds (conformance/MYSQL_VERIFICATION.md is the
ledger) — and a source-only linter resolves their citations exactly as happily as it
resolves a Postgres one. So `sh:` resolves against the RUN transcript, `sh-mysql:` against
the source, and the linter DERIVES which one a citation should be from the transcript
rather than trusting the author: a citation marked `sh:` whose label did not run is a hard
failure, and one marked `sh-mysql:` that DID run is a hard failure too. The same derivation
runs for Rust and Go tests, from whether the test's file is MySQL-gated. Neither marking
can rot into a lie.

THE ACKNOWLEDGED-DEBT RATCHET
------------------------------
"A ✅ row with no evidence = hard failure" and "where a ✅ row genuinely has NO evidence, do
not invent any" are in tension, and inventing evidence is far worse than declaring none.
So a row may cite `none: <reason>` — and the file header must declare `evidence-debt: N`
matching the count EXACTLY. Adding an evidence-free ✅ row therefore fails the gate until
someone edits that number, which makes the debt a deliberate, reviewable act; and removing
one fails it too, until the number comes down. A budget that only ever needs raising is
not a ratchet.

USAGE
-----
  scripts/check-evidence.py
Requires a fresh `target/conformance/assertions.tsv` (written by scripts/test-admission.sh)
and `target/conformance/scenarios.tsv` (written by scripts/run-scenarios.py). Run those
first; verify.sh does.
"""

import os
import re
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(ROOT)

REGISTER = "conformance/CAPABILITY_REGISTER.md"
EVIDENCE = "conformance/EVIDENCE.md"
SUITE = "scripts/test-admission.sh"
ASSERT_TSV = "target/conformance/assertions.tsv"
SCENARIO_TSV = "target/conformance/scenarios.tsv"
SCENARIO_DIR = "conformance/scenarios"

CLAIMED = ("✅", "🔶")
FAILURES = []


def fail(msg):
    FAILURES.append(msg)


def norm(name):
    """One spelling for a row name across the register and the sidecar. The register
    writes emphasis and code spans into its own row names (`**CLI**`, `` `pending` state ``,
    ``Admission explain (`GET /jobs/{id}/admission`)``), and a sidecar that had to
    reproduce that markup byte-for-byte would break on the next cosmetic edit."""
    n = name.replace("**", "").replace("`", "")
    return re.sub(r"\s+", " ", n).strip()


# ---------------------------------------------------------------- the register
def parse_register():
    rows = []          # (section, display_name, status)
    section = None
    for line in open(REGISTER):
        h = re.match(r"^##\s+(.*)$", line)
        if h:
            section = h.group(1).strip()
            continue
        if not line.startswith("|"):
            continue
        cells = line.split("|")
        if len(cells) < 4:
            continue
        name, status = cells[1].strip(), cells[2].strip()
        if status not in ("✅", "🔶", "❌", "⏸"):
            continue
        rows.append((section, name, status))
    return rows


# ---------------------------------------------------------------- the sidecar
CITE_KINDS = ("sh", "sh-mysql", "rust", "rust-mysql", "go", "go-mysql", "scenario", "none")


def parse_evidence():
    blocks = {}        # norm name -> {"line": n, "cites": [(kind, value, line)]}
    order = []
    debt = None
    cur = None
    for i, line in enumerate(open(EVIDENCE), 1):
        m = re.match(r"^evidence-debt:\s*(\d+)\s*$", line)
        if m:
            debt = int(m.group(1))
            continue
        m = re.match(r"^###\s+(.*)$", line)
        if m:
            cur = norm(m.group(1))
            if cur in blocks:
                fail(f"{EVIDENCE}:{i}: duplicate block for row '{cur}'")
            blocks[cur] = {"line": i, "cites": []}
            order.append(cur)
            continue
        # A bullet that does not parse as a citation must be an ERROR, never a silent
        # skip: "the linter passed" and "the linter never looked at this line" have to be
        # distinguishable, or a typo'd citation is indistinguishable from a resolved one —
        # which is this repo's single most-repeated bug shape.
        if line.startswith("- ") and not re.match(r"^-\s+[a-z-]+:\s*.+?\s*$", line):
            fail(f"{EVIDENCE}:{i}: bullet is not a `- <kind>: <value>` citation and would "
                 f"have been silently ignored: {line.rstrip()}")
            continue
        m = re.match(r"^-\s+([a-z-]+):\s*(.+?)\s*$", line)
        if m and not cur:
            fail(f"{EVIDENCE}:{i}: citation outside any `### <row>` block.")
            continue
        if m and cur:
            kind, val = m.group(1), m.group(2)
            # Citations read better in markdown wrapped in a code span, and a stray pair
            # of backticks must not be the reason a real citation fails to resolve.
            if len(val) > 1 and val.startswith("`") and val.endswith("`"):
                val = val[1:-1]
            if kind not in CITE_KINDS:
                fail(f"{EVIDENCE}:{i}: unknown evidence kind '{kind}:' "
                     f"(allowed: {', '.join(CITE_KINDS)})")
                continue
            blocks[cur]["cites"].append((kind, val, i))
    return blocks, order, debt


# ------------------------------------------------------------------- the facts
def load_transcript(path, what):
    if not os.path.exists(path):
        print(f"FATAL: {path} missing. {what}")
        sys.exit(2)
    passed, meta = set(), {}
    for line in open(path):
        parts = line.rstrip("\n").split("\t")
        if parts[0] == "#":
            if len(parts) >= 3:
                meta[parts[1]] = parts[2]
            continue
        if len(parts) >= 2 and parts[0] == "PASS":
            passed.add(parts[1])
    return passed, meta


def suite_source_labels():
    src = open(SUITE).read()
    joined = re.sub(r"\\\n\s*", " ", src)
    return [m.group(2) for m in re.finditer(
        r'(?m)^\s*(chk0|chk|seed|skipped)\s+"((?:[^"\\]|\\.)*)"', joined)]


def rust_tests():
    """path::fn -> is_mysql_gated"""
    out = {}
    for dirpath, _dirs, files in os.walk("crates"):
        for f in files:
            if not f.endswith(".rs"):
                continue
            p = os.path.join(dirpath, f)
            src = open(p, encoding="utf-8", errors="replace").read()
            gated = "HG_TEST_MYSQL" in src or "headgate-mysql" in p
            # Round 32l: `(?:\([^)]*\))?` — the pattern used to require a literal `]` right
            # after `test`, so `#[tokio::test(start_paused = true)]` was invisible and a
            # citation to such a test failed as "no such #[test] function". That attribute
            # is exactly what a test needs to assert anything about TIME without a
            # stopwatch, so the blind spot was pointed at the tests hardest to write.
            # scripts/check-inventory.py had the identical bug and is fixed the same way.
            for m in re.finditer(
                    r"#\[(?:tokio::)?test(?:\([^)]*\))?\][^\n]*\n(?:\s*#\[[^\]]*\]\s*\n)*\s*(?:async\s+)?fn\s+(\w+)",
                    src):
                out[f"{p}::{m.group(1)}"] = gated
    return out


def go_tests():
    out = {}
    for dirpath, _dirs, files in os.walk("go"):
        for f in files:
            if not f.endswith("_test.go"):
                continue
            p = os.path.join(dirpath, f)
            src = open(p, encoding="utf-8", errors="replace").read()
            gated = "HG_TEST_MYSQL" in src or "headgatemysql" in p
            for m in re.finditer(r"(?m)^func (Test\w+)\(", src):
                rel = os.path.relpath(p, "go")
                out[f"{rel}::{m.group(1)}"] = gated
    return out


def scenario_ids():
    ids = set()
    for dirpath, _dirs, files in os.walk(SCENARIO_DIR):
        for f in files:
            if not f.endswith(".yaml"):
                continue
            p = os.path.join(dirpath, f)
            for m in re.finditer(r"(?m)^\s*-\s+id:\s*(\S+)\s*$", open(p).read()):
                ids.add(f"{os.path.relpath(p)}#{m.group(1)}")
    return ids


# ------------------------------------------------------------------ resolution
def main():
    ran, meta = load_transcript(
        ASSERT_TSV, "Run scripts/test-admission.sh first — it writes the assertion transcript.")
    sran, _ = load_transcript(
        SCENARIO_TSV, "Run scripts/run-scenarios.py first — it writes the scenario transcript.")
    mysql_live = meta.get("mysql_live") == "yes"

    src_labels = suite_source_labels()
    rtests, gtests = rust_tests(), go_tests()
    scen = scenario_ids()

    rows = parse_register()
    by_norm = {}
    for section, name, status in rows:
        n = norm(name)
        if n in by_norm:
            fail(f"{REGISTER}: two rows normalize to the same name '{n}' — "
                 f"the sidecar is keyed by name and cannot address either")
        by_norm[n] = (section, status)

    blocks, order, debt = parse_evidence()

    claimed_rows = [n for n, (_s, st) in by_norm.items() if st in CLAIMED]

    # --- direction 1: every claimed row has a block with at least one citation
    for n in claimed_rows:
        if n not in blocks:
            fail(f"{REGISTER}: row '{n}' is {by_norm[n][1]} but has NO block in {EVIDENCE}. "
                 f"A declared capability with no evidence is invariant 5's own failure mode.")
        elif not blocks[n]["cites"]:
            fail(f"{EVIDENCE}:{blocks[n]['line']}: row '{n}' has a block but cites nothing.")

    # --- direction 2: every block names a real, claimed row
    for n in order:
        if n not in by_norm:
            fail(f"{EVIDENCE}:{blocks[n]['line']}: block '{n}' names no row in {REGISTER}.")
        elif by_norm[n][1] not in CLAIMED:
            fail(f"{EVIDENCE}:{blocks[n]['line']}: block '{n}' is for a "
                 f"{by_norm[n][1]} row — only ✅/🔶 rows are claims that need evidence.")

    # --- direction 3: every citation resolves
    resolved = 0
    for n in order:
        for kind, val, ln in blocks[n]["cites"]:
            where = f"{EVIDENCE}:{ln}: row '{n}': {kind}: {val}"
            if kind == "none":
                if not val.strip():
                    fail(f"{where} — `none:` must carry a reason.")
                continue
            if kind in ("sh", "sh-mysql"):
                in_source = any(val in lbl for lbl in src_labels)
                in_run = any(val in lbl for lbl in ran)
                if not in_source:
                    fail(f"{where} — no assertion label in {SUITE} contains this text.")
                elif kind == "sh" and not in_run:
                    fail(f"{where} — the label exists in {SUITE} but DID NOT RUN in this "
                         f"run. If it is MySQL-gated, cite it as `sh-mysql:`.")
                elif kind == "sh-mysql" and in_run and not mysql_live:
                    fail(f"{where} — marked MySQL-gated, but it RAN with no MySQL server. "
                         f"Cite it as `sh:`.")
                elif kind == "sh-mysql" and mysql_live and not in_run:
                    fail(f"{where} — MySQL was live and this label still did not run.")
                else:
                    resolved += 1
                continue
            if kind in ("rust", "rust-mysql"):
                if val not in rtests:
                    fail(f"{where} — no such Rust #[test] function.")
                elif rtests[val] != (kind == "rust-mysql"):
                    want = "rust-mysql" if rtests[val] else "rust"
                    fail(f"{where} — this test is{'' if rtests[val] else ' NOT'} MySQL-gated; "
                         f"cite it as `{want}:`.")
                else:
                    resolved += 1
                continue
            if kind in ("go", "go-mysql"):
                if val not in gtests:
                    fail(f"{where} — no such Go test function.")
                elif gtests[val] != (kind == "go-mysql"):
                    want = "go-mysql" if gtests[val] else "go"
                    fail(f"{where} — this test is{'' if gtests[val] else ' NOT'} MySQL-gated; "
                         f"cite it as `{want}:`.")
                else:
                    resolved += 1
                continue
            if kind == "scenario":
                if val not in scen:
                    fail(f"{where} — no such scenario id under {SCENARIO_DIR}/.")
                elif not any(val.split("#")[1] in lbl for lbl in sran):
                    fail(f"{where} — the scenario exists but did not RUN. "
                         f"scripts/run-scenarios.py must execute it.")
                else:
                    resolved += 1

    # --- direction 4: the acknowledged-debt ratchet
    debt_rows = sorted(
        n for n in order
        if blocks[n]["cites"] and all(k == "none" for k, _v, _l in blocks[n]["cites"]))
    if debt is None:
        fail(f"{EVIDENCE}: no `evidence-debt: N` line. It is the ratchet on ✅ rows that "
             f"cite nothing but a reason.")
    elif debt != len(debt_rows):
        fail(f"{EVIDENCE}: `evidence-debt: {debt}` but {len(debt_rows)} row(s) cite only "
             f"`none:` — {', '.join(debt_rows) or '(none)'}. Change the number "
             f"DELIBERATELY; a budget that drifts on its own is not a ratchet.")

    print(f"  register rows: {len(rows)} ({len(claimed_rows)} claimed ✅/🔶)")
    print(f"  evidence blocks: {len(order)}, citations resolved: {resolved}")
    print(f"  MySQL evidence: {'RUN (server live)' if mysql_live else 'WRITTEN, NOT RUN (no server)'}")
    print(f"  acknowledged evidence debt: {len(debt_rows)} row(s) declaring `none:`")
    for n in debt_rows:
        why = next(v for k, v, _l in blocks[n]["cites"] if k == "none")
        print(f"    · {n} — {why}")

    if FAILURES:
        print()
        print(f"FAILED: {len(FAILURES)} evidence problem(s)")
        for f in FAILURES:
            print(f"  ❌ {f}")
        return 1
    print("  ok: every ✅/🔶 row resolves to named, existing evidence")
    return 0


if __name__ == "__main__":
    sys.exit(main())
