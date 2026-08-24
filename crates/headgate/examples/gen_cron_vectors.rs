//! Regenerate `conformance/cron_ticks.json` — the cross-language tick pin both test
//! suites read (`crates/headgate/tests/cron_vectors.rs`, `go/cronspec_test.go`).
//!
//!     cargo run -p headgate --example gen_cron_vectors
//!
//! Rust is the generator because Rust is where the semantics are decided (the `cron`
//! crate plus this crate's POSIX normalization and, with the current contract, the timezone layer);
//! Go's dependency-free evaluator then has to MATCH, which is the whole point.
//!
//! Two rules make regeneration safe:
//!
//! 1. Existing vectors are RECOMPUTED and must come out unchanged. A drift is a
//!    semantics change to an already-pinned case and the run aborts naming it, rather
//!    than quietly rewriting the contract Go is tested against.
//! 2. New cases are APPENDED, and a case already in the file is not appended twice — so
//!    running this repeatedly is a no-op and the file stays byte-stable.

use std::fmt::Write as _;

use headgate::schedule_spec::next_after;

/// The round-32 timezone cases. Grouped by what each one PINS, because a vector nobody
/// can explain is a vector nobody dares change. Times are epoch-ms UTC — tick ids are
/// epoch-ms whatever the zone.
const TZ_CASES: &[(&str, i64)] = &[
    // ---- `CRON_TZ=UTC` is exactly the un-prefixed spec (the prefix is not a mode
    // switch, it is a zone). Same after_ms as the plain vectors above them.
    ("CRON_TZ=UTC 0 * * * *", 1_704_069_000_000),
    ("CRON_TZ=UTC */15 * * * *", 1_704_069_000_000),
    // ---- a plain wall-clock shift: 09:00 New York in January is 14:00Z.
    ("CRON_TZ=America/New_York 0 9 * * *", 1_704_067_200_000),
    ("CRON_TZ=America/New_York 0 9 * * *", 1_704_117_600_000),
    // ---- SPRING FORWARD, America/New_York 2024-03-10: 02:00 EST -> 03:00 EDT.
    // A daily 02:30 exists on Mar 9, does NOT exist on Mar 10 (SKIPPED — not dragged to
    // 03:00), and returns on Mar 11.
    ("CRON_TZ=America/New_York 30 2 * * *", 1_709_942_400_000), // 2024-03-09T00:00:00Z
    ("CRON_TZ=America/New_York 30 2 * * *", 1_709_969_400_000), // Mar 9 02:30 EST
    ("CRON_TZ=America/New_York 30 2 * * *", 1_710_138_600_000), // Mar 11 02:30 EDT
    // hourly across the same gap: 01:00 EST -> 03:00 EDT, consecutive in UTC.
    ("CRON_TZ=America/New_York 0 * * * *", 1_710_050_400_000), // 2024-03-10T06:00:00Z
    ("CRON_TZ=America/New_York 0 * * * *", 1_710_054_000_000),
    // ---- FALL BACK, America/New_York 2024-11-03: 02:00 EDT -> 01:00 EST, so 01:30
    // happens TWICE. It fires ONCE, at the FIRST (EDT) instant; the repeat is not a
    // tick, so the successor is the next day.
    ("CRON_TZ=America/New_York 30 1 * * *", 1_730_548_800_000), // 2024-11-02T12:00:00Z
    ("CRON_TZ=America/New_York 30 1 * * *", 1_730_611_800_000), // the FIRST 01:30
    // hourly through the fold: 01:00 EDT (05:00Z) then 02:00 EST (07:00Z) — 06:00Z, the
    // repeated 01:00, is skipped.
    ("CRON_TZ=America/New_York 0 * * * *", 1_730_610_000_000), // 2024-11-03T05:00:00Z
    ("CRON_TZ=America/New_York 0 * * * *", 1_730_617_200_000),
    // ---- HALF-HOUR OFFSET, Asia/Kolkata (+05:30, no DST).
    ("CRON_TZ=Asia/Kolkata 30 9 * * *", 1_704_067_200_000),
    // local midnight is 18:30Z the PREVIOUS UTC day — the local/UTC date divergence.
    ("CRON_TZ=Asia/Kolkata 0 0 * * *", 1_704_067_200_000),
    ("CRON_TZ=Asia/Kolkata 0 0 * * *", 1_704_133_800_000),
    // day-of-week on the LOCAL calendar: Monday 00:30 IST is a SUNDAY instant in UTC.
    ("CRON_TZ=Asia/Kolkata 30 0 * * 1", 1_703_980_800_000),
    ("CRON_TZ=Asia/Kolkata 30 0 * * 1", 1_704_049_200_000),
    // day-of-month likewise: local the 1st, UTC still the last day of the month before.
    ("CRON_TZ=Asia/Kolkata 0 0 1 * *", 1_705_276_800_000), // 2024-01-15T00:00:00Z
    // DOM+DOW union (crontab(5)) evaluated on the LOCAL calendar.
    ("CRON_TZ=Asia/Kolkata 0 12 13 * 5", 1_704_067_200_000),
    ("CRON_TZ=Asia/Kolkata 0 12 13 * 5", 1_704_171_600_000),
    // ---- SOUTHERN HEMISPHERE, Australia/Sydney: DST ENDS in April, STARTS in October.
    // 2024-04-07 03:00 AEDT -> 02:00 AEST, so 02:30 repeats: fires once, at +11.
    ("CRON_TZ=Australia/Sydney 30 2 * * *", 1_712_361_600_000), // 2024-04-06T00:00:00Z
    ("CRON_TZ=Australia/Sydney 30 2 * * *", 1_712_417_400_000),
    // 2024-10-06 02:00 AEST -> 03:00 AEDT: 02:30 does not exist, Oct 6 is skipped.
    ("CRON_TZ=Australia/Sydney 30 2 * * *", 1_728_086_400_000), // 2024-10-05T00:00:00Z
    ("CRON_TZ=Australia/Sydney 0 9 * * 1", 1_728_086_400_000),
    // ---- a HALF-HOUR DST STEP: Australia/Lord_Howe shifts by 00:30, so the gap is
    // 02:00-02:30 on 2024-10-06 and 02:15 is a local time that does not exist.
    ("CRON_TZ=Australia/Lord_Howe 15 2 * * *", 1_728_086_400_000),
    ("CRON_TZ=Australia/Lord_Howe 15 2 * * *", 1_728_242_100_000),
    // ---- SIX FIELDS (seconds, wire-time contract) with a zone, including across the spring-forward
    // gap — the skip is a property of the DAY, not of the minute granularity.
    ("CRON_TZ=America/New_York 15 30 9 * * *", 1_704_067_200_000),
    ("CRON_TZ=America/New_York 45 30 2 * * *", 1_709_942_400_000),
    ("CRON_TZ=America/New_York 45 30 2 * * *", 1_709_969_445_000),
    ("CRON_TZ=America/New_York */30 * * * * *", 1_704_067_200_000),
];

fn main() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../conformance/cron_ticks.json"
    );
    let raw = std::fs::read_to_string(path).expect("read cron_ticks.json");
    let existing: Vec<serde_json::Value> = serde_json::from_str(&raw).expect("valid json");

    let mut cases: Vec<(String, i64)> = Vec::new();
    for v in &existing {
        let spec = v["spec"].as_str().expect("spec").to_string();
        let after = v["after_ms"].as_i64().expect("after_ms");
        let want = v["next_ms"].as_i64().expect("next_ms");
        let got = next_after(&spec, after).unwrap_or_else(|e| panic!("{spec} @ {after}: {e}"));
        assert_eq!(
            got, want,
            "REFUSING to rewrite a pinned vector: `{spec}` after {after} was {want}, is now {got}"
        );
        cases.push((spec, after));
    }
    let kept = cases.len();
    for (spec, after) in TZ_CASES {
        if !cases.iter().any(|(s, a)| s == spec && a == after) {
            cases.push((spec.to_string(), *after));
        }
    }

    let mut out = String::from("[\n");
    for (i, (spec, after)) in cases.iter().enumerate() {
        let next = next_after(spec, *after).unwrap_or_else(|e| panic!("{spec} @ {after}: {e}"));
        let comma = if i + 1 == cases.len() { "" } else { "," };
        writeln!(
            out,
            "  {{\"spec\": {}, \"after_ms\": {after}, \"next_ms\": {next}}}{comma}",
            serde_json::to_string(spec).unwrap()
        )
        .unwrap();
    }
    out.push_str("]\n");
    std::fs::write(path, &out).expect("write cron_ticks.json");
    eprintln!("cron_ticks.json: {kept} kept, {} total", cases.len());
}
