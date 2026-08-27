#!/usr/bin/env python3
"""The deleted-test guard: a disappearing test is a FAILURE, not a quieter green.

THE BLIND SPOT (round 32i, found the hard way)
----------------------------------------------
Round 32i's own restore script silently deleted an implementation fix AND the tests that
covered it. The gate went GREEN — because the tests went with the code. Every mechanism in
this repo answers "does the assertion have teeth?"; not one of them answers "is the
assertion still there?". A suite can only fail on what it still contains, so subtraction is
invisible to it by construction, and the more thorough the suite the more convincing the
resulting green.

THE DESIGN, AND ITS FRICTION BUDGET
------------------------------------
A floor per test FILE, in `conformance/TEST_INVENTORY.tsv`:

    <kind>\t<path>\t<floor>

  * ADDING a test to an existing file: zero friction. The check is `count >= floor`, so a
    higher count is fine and nothing needs editing. This is the common case by a wide
    margin, and a guard that taxed it would be dodged inside two rounds.
  * ADDING a new test file: one command — `scripts/check-inventory.py --update`. The
    inventory must be COMPLETE or the guard is unsound (tests could be parked in an
    unwatched file and then deleted), so a new file is a hard failure until it is claimed.
  * DELETING a test: a deliberate hand-edit of the floor. That is the whole point, and
    `--update` REFUSES to do it for you — it only ever raises floors and adds files, and
    prints what a lowering would have discarded. An updater that rubber-stamps deletions
    reintroduces exactly the hole it exists to close.

Per FILE rather than per module, because a module-level total lets a file lose three tests
while a sibling gains three — which is the shape of the accident, not a contrived one.
Counts rather than names, because a RENAME is normal test authoring and should not require
a manifest edit; the residual (delete one test and add another in the same file, in the
same commit) is stated here rather than hidden, and it is a much narrower hole than the one
this closes.

`scripts/test-admission.sh` is in the inventory too, floored by its ASSERTION LABEL count.
AGENTS.md's rule that "no expected total is hardcoded — the gate is failed=0" is about the
PASS count of a run, which legitimately varies with which backends are reachable; a
monotone lower bound on how many assertions the file CONTAINS is a different statement and
does not conflict with it. Same for the scenario corpus, whose entire failure mode in round
32i was being empty of anything that ran.
"""

import os
import re
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(ROOT)

MANIFEST = "conformance/TEST_INVENTORY.tsv"
SUITE = "scripts/test-admission.sh"
SCENARIO_DIR = "conformance/scenarios"


def rust_counts():
    out = {}
    for dirpath, _d, files in os.walk("crates"):
        for f in files:
            if not f.endswith(".rs"):
                continue
            p = os.path.join(dirpath, f)
            src = open(p, encoding="utf-8", errors="replace").read()
            # Round 32l: the `(?:\([^)]*\))?` is not cosmetic. The pattern used to require
            # a literal `]` right after `test`, so `#[tokio::test(start_paused = true)]`
            # counted as ZERO tests — and that attribute is exactly what a test needs to
            # assert anything about TIME on a virtual clock. A test the guard cannot see is
            # a test the guard cannot notice the deletion of, which is the one thing this
            # file exists to prevent. Found by adding such a test and watching the floor
            # not move; `#[test(flavor = "...")]` and `#[tokio::test(start_paused)]` are
            # now both counted.
            n = len(re.findall(
                r"#\[(?:tokio::)?test(?:\([^)]*\))?\][^\n]*\n(?:\s*#\[[^\]]*\]\s*\n)*\s*(?:async\s+)?fn\s+\w+",
                src))
            if n:
                out[p] = n
    return out


def go_counts():
    out = {}
    for dirpath, _d, files in os.walk("go"):
        for f in files:
            if not f.endswith("_test.go"):
                continue
            p = os.path.join(dirpath, f)
            src = open(p, encoding="utf-8", errors="replace").read()
            n = len(re.findall(r"(?m)^func Test\w+\(", src))
            if n:
                out[p] = n
    return out


def sh_counts():
    if not os.path.exists(SUITE):
        return {}
    joined = re.sub(r"\\\n\s*", " ", open(SUITE).read())
    n = len(re.findall(r'(?m)^\s*(?:chk0|chk|seed|skipped)\s+"', joined))
    return {SUITE: n}


def scenario_counts():
    out = {}
    if not os.path.isdir(SCENARIO_DIR):
        return out
    for f in sorted(os.listdir(SCENARIO_DIR)):
        if not f.endswith(".yaml"):
            continue
        p = os.path.join(SCENARIO_DIR, f)
        n = len(re.findall(r"(?m)^\s*-\s+id:\s*\S+", open(p).read()))
        if n:
            out[p] = n
    return out


def current():
    cur = {}
    for kind, fn in (("rust", rust_counts), ("go", go_counts),
                     ("sh", sh_counts), ("scenario", scenario_counts)):
        for p, n in fn().items():
            cur[p] = (kind, n)
    return cur


def load_manifest():
    floors = {}
    if not os.path.exists(MANIFEST):
        return floors
    for i, line in enumerate(open(MANIFEST), 1):
        line = line.rstrip("\n")
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            print(f"FATAL: {MANIFEST}:{i}: expected <kind>\\t<path>\\t<floor>")
            sys.exit(2)
        floors[parts[1]] = (parts[0], int(parts[2]))
    return floors


HEADER = """\
# Test inventory — the deleted-test guard's floors. See scripts/check-inventory.py.
#
# <kind>\\t<path>\\t<floor>. A file's current test count must be >= its floor. Raising a
# floor (or adding a file) is `scripts/check-inventory.py --update`; LOWERING one is a
# deliberate hand-edit, because that is what deleting a test is.
"""


def write_manifest(entries):
    with open(MANIFEST, "w") as f:
        f.write(HEADER)
        for p in sorted(entries):
            kind, n = entries[p]
            f.write(f"{kind}\t{p}\t{n}\n")


def main():
    update = "--update" in sys.argv
    cur, floors = current(), load_manifest()

    problems, refused = [], []
    merged = {}

    for p, (kind, n) in sorted(cur.items()):
        if p not in floors:
            if update:
                merged[p] = (kind, n)
            else:
                problems.append(
                    f"{p} holds {n} test(s) and is NOT in {MANIFEST}. The inventory has to "
                    f"be complete or the guard is unsound. Fix: scripts/check-inventory.py --update")
            continue
        floor = floors[p][1]
        if n < floor:
            problems.append(
                f"{p}: {n} test(s) but the floor is {floor} — {floor - n} DISAPPEARED. "
                f"If that was deliberate, lower the floor in {MANIFEST} by hand.")
            merged[p] = (kind, floor)
            refused.append(f"{p}: would have lowered {floor} -> {n}")
        else:
            merged[p] = (kind, max(n, floor))

    for p, (kind, floor) in sorted(floors.items()):
        if p not in cur:
            problems.append(
                f"{p} is in {MANIFEST} with a floor of {floor} and now holds NO tests "
                f"(deleted, renamed, or emptied). If deliberate, remove its line by hand.")
            merged[p] = (kind, floor)
            refused.append(f"{p}: would have dropped the file entirely")

    if update:
        write_manifest(merged)
        print(f"  {MANIFEST} updated: {len(merged)} file(s) floored")
        for r in refused:
            print(f"  ⚠ REFUSED (a floor never goes down on its own): {r}")
        return 1 if refused else 0

    total = sum(n for _k, n in cur.values())
    print(f"  inventory: {len(cur)} file(s), {total} test(s)/assertion(s) floored")
    if problems:
        print()
        print(f"FAILED: {len(problems)} inventory problem(s)")
        for p in problems:
            print(f"  ❌ {p}")
        return 1
    print("  ok: no test disappeared")
    return 0


if __name__ == "__main__":
    sys.exit(main())
