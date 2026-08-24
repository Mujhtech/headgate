//! surveyed policy behavior cross-language tick vectors. Tick times feed the per-tick unique keys
//! (`sched:{id}:{tick_ms}`) that make the scheduler leaderless, so tick derivation is
//! CONTRACT: conformance/cron_ticks.json pins next_after for both languages — the Go
//! suite reads the same file. Semantics pinned here are POSIX crontab: numeric
//! day-of-week 0-7 with 0 and 7 both Sunday, names accepted, and DOM/DOW BOTH
//! restricted = UNION (either matching fires), plus epoch-aligned @every — and, from
//! round 32, `CRON_TZ=<IANA zone>`: local-calendar day/DOW, a nonexistent local time
//! (spring forward) SKIPPED, and a repeated one (fall back) firing ONCE at the first
//! occurrence.
//!
//! Regenerate with `cargo run -p headgate --example gen_cron_vectors`, which refuses to
//! rewrite an existing vector and only appends.

use headgate::schedule_spec::{next_after, validate};

const VECTORS: &str = include_str!("../../../conformance/cron_ticks.json");

#[test]
fn every_vector_matches() {
    let vectors: serde_json::Value = serde_json::from_str(VECTORS).expect("valid json");
    let vectors = vectors.as_array().expect("array");
    assert!(
        vectors.len() >= 60,
        "vector file looks truncated: {}",
        vectors.len()
    );
    // The timezone vectors must actually BE here. Without this the suite passes just as
    // loudly against a file that lost them, which is the one way a cross-language pin
    // fails silently.
    let zoned = vectors
        .iter()
        .filter(|v| {
            v["spec"]
                .as_str()
                .is_some_and(|s| s.starts_with("CRON_TZ="))
        })
        .count();
    assert!(
        zoned >= 25,
        "timezone vectors missing: only {zoned} of {} carry CRON_TZ=",
        vectors.len()
    );
    for v in vectors {
        let spec = v["spec"].as_str().unwrap();
        let after = v["after_ms"].as_i64().unwrap();
        let want = v["next_ms"].as_i64().unwrap();
        let got = next_after(spec, after).unwrap_or_else(|e| panic!("{spec}: {e}"));
        assert_eq!(got, want, "spec `{spec}` after {after}");
    }
}

#[test]
fn the_edges_the_vectors_cannot_encode() {
    // Impossible dates error rather than spin.
    assert!(next_after("0 0 31 4 *", 0).is_err(), "April 31 must error");
    // Quartz 7-field (years) is rejected in BOTH languages — the design says 5 or 6.
    assert!(
        next_after("0 0 0 1 1 * 2099", 0).is_err(),
        "year field must be rejected"
    );
    // POSIX 0 = Sunday parses (the raw crate rejects it; the translation layer owns it).
    assert!(next_after("0 0 * * 0", 0).is_ok());
}

/// Round 32. The vectors pin every ACCEPTED timezone tick; this pins the REJECTIONS and
/// their exact text, which the API serves verbatim as a 400 (control API contract) and the mutation
/// diff compares byte for byte against Go's.
#[test]
fn timezone_rejections_are_the_error_contract() {
    for (spec, want) in [
        (
            "CRON_TZ=Mars/Phobos 0 9 * * *",
            "unknown timezone `Mars/Phobos`",
        ),
        // The syntax gate answers with the SAME message as a database miss, so a client
        // cannot tell which layer refused — that is what keeps Go's filesystem-backed
        // lookup in line with this exact-match table on a case-insensitive filesystem.
        (
            "CRON_TZ=america/new_york 0 9 * * *",
            "unknown timezone `america/new_york`",
        ),
        (
            "CRON_TZ=posix/America/New_York 0 9 * * *",
            "unknown timezone `posix/America/New_York`",
        ),
        ("CRON_TZ= 0 9 * * *", "unknown timezone ``"),
        (
            "CRON_TZ=America/New_York @every:1000",
            "`@every` is epoch-aligned UTC and takes no CRON_TZ: `CRON_TZ=America/New_York @every:1000`",
        ),
    ] {
        assert_eq!(validate(spec).unwrap_err(), want, "spec `{spec}`");
    }
}
