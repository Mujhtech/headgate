//! surveyed policy behavior schedule-spec evaluation. Two forms:
//!
//! - `"@every:<ms>"` — a fixed interval, EPOCH-ALIGNED: ticks are exact multiples of
//!   the interval, so every node derives the same tick times with no coordination.
//!   It is therefore **always UTC** and takes no timezone: an interval has no wall
//!   clock to be wrong about, and epoch alignment is the whole mechanism. A
//!   `CRON_TZ=` prefix on an `@every` spec is an ERROR rather than a silent no-op —
//!   quietly ignoring a timezone the operator asked for is the failure mode this
//!   codebase refuses everywhere else.
//! - a cron expression. Five-field crontab or six-field (with seconds, wire-time contract makes
//!   sub-minute legal) both accepted. Evaluated in UTC unless the spec carries a
//!   `CRON_TZ=<IANA zone>` prefix — robfig/cron's convention, chosen over a
//!   separate column so the spec stays ONE string: no storage migration, no API or UI
//!   field, and — the load-bearing part — a changed timezone is automatically a
//!   *changed spec*, which is what makes the idempotent upsert re-anchor the schedule's
//!   phase instead of keeping a phase computed under the old zone.
//!
//! # Timezone semantics, pinned by `conformance/cron_ticks.json`
//!
//! A cron spec with a zone is a LOCAL WALL-CLOCK expression: hour, minute, second,
//! day-of-month, month and day-of-week are all read off the LOCAL calendar. So the
//! (timezone-free) cron engine is run in wall-clock space and each candidate wall clock
//! is then mapped back to an instant. Two mappings can fail, and that is where the DST
//! contract lives:
//!
//! - a local time that **does not exist** (spring forward) is **SKIPPED** — the tick
//!   does not fire that day, it is not dragged to the edge of the gap;
//! - a local time that **occurs twice** (fall back) fires **ONCE**, at the FIRST
//!   (pre-transition) occurrence.
//!
//! Both rules are choices, not accidents: firing twice would break nothing structurally
//! (the tick key `sched:{id}:{tick_ms}` is epoch-ms, so the two instants are distinct
//! keys and both would enqueue) which is exactly why the rule has to be *stated* and
//! *pinned* rather than left to whichever library each language happens to use.
//!
//! Tick ids stay epoch-ms in every case. Nothing downstream — the unique key, the
//! missed-schedule policies, `advance_schedule`'s CAS — learns about timezones at all.
//!
//! Tick times feed the per-tick unique keys (`sched:{id}:{tick_ms}`), which is what
//! makes the scheduler leaderless: N nodes race the enqueue and the unique index picks
//! one winner (GoodJob's trick). That also means tick derivation is CONTRACT: the Go
//! implementation must produce identical times, and the cron edge cases (day-of-week /
//! day-of-month union semantics, and now every DST rule above) must match. The
//! cross-language pin is `conformance/cron_ticks.json`, read by both test suites.

use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;

/// robfig/cron's in-spec prefix. One form only — `TZ=` is deliberately NOT accepted
/// (robfig deprecated it), because two spellings of one thing is two things to pin.
const TZ_PREFIX: &str = "CRON_TZ=";

/// How many candidate wall clocks may be discarded as nonexistent before a zoned spec
/// is declared unsatisfiable. Generous: a per-second spec across the 24h skip of
/// Pacific/Apia's 2011 date-line change needs 86400. Same bound in Go.
const MAX_WALL_CANDIDATES: usize = 100_000;

/// The next tick STRICTLY AFTER `after_ms`.
pub fn next_after(spec: &str, after_ms: i64) -> Result<i64, String> {
    let (tz, body) = split_tz(spec)?;
    if let Some(ms) = body.strip_prefix("@every:") {
        if tz.is_some() {
            // See the module docs: an interval has no wall clock, and pretending
            // otherwise would silently un-align it from every other node.
            return Err(format!(
                "`@every` is epoch-aligned UTC and takes no CRON_TZ: `{spec}`"
            ));
        }
        let n: i64 = ms
            .parse()
            .map_err(|_| format!("bad @every spec `{spec}`"))?;
        if n < 1 {
            // boundary validation a period that rounds to zero is an error, never a busy loop.
            return Err("@every period must be >= 1ms".into());
        }
        return Ok((after_ms.div_euclid(n) + 1) * n);
    }
    let scheds = compile(body, spec)?;
    match tz {
        None => next_wall(&scheds, spec, after_ms),
        Some(tz) => next_local(&scheds, spec, after_ms, tz),
    }
}

/// Compile a spec body to the one-or-two `cron` schedules whose union is the POSIX
/// answer.
///
/// POSIX union semantics: when BOTH day-of-month and day-of-week are restricted, a day
/// matching EITHER fires (crontab(5); this module's stated contract). The cron crate
/// intersects instead, so evaluate each restriction alone and take the earlier tick —
/// exact, and it keeps the tested crate underneath untouched.
fn compile(body: &str, spec: &str) -> Result<Vec<cron::Schedule>, String> {
    let fields = normalized_fields(body, spec)?;
    let (dom, dow) = (fields[3].as_str(), fields[5].as_str());
    let restricted = |f: &str| f != "*" && f != "?";
    if restricted(dom) && restricted(dow) {
        let mut dom_only = fields.clone();
        dom_only[5] = "*".into();
        let mut dow_only = fields;
        dow_only[3] = "*".into();
        return Ok(vec![
            parse_one(&dom_only.join(" "), spec)?,
            parse_one(&dow_only.join(" "), spec)?,
        ]);
    }
    Ok(vec![parse_one(&fields.join(" "), spec)?])
}

fn parse_one(normalized: &str, original: &str) -> Result<cron::Schedule, String> {
    cron::Schedule::from_str(normalized).map_err(|e| format!("bad cron `{original}`: {e}"))
}

fn instant(ms: i64) -> Result<DateTime<Utc>, String> {
    DateTime::<Utc>::from_timestamp_millis(ms).ok_or_else(|| format!("timestamp {ms} out of range"))
}

/// The earliest occurrence across the union, strictly after `after_ms`. With no zone
/// this IS the answer; with a zone it is the answer in WALL-CLOCK space (the caller
/// hands in a local wall clock encoded as if it were UTC, and maps the result back).
fn next_wall(scheds: &[cron::Schedule], spec: &str, after_ms: i64) -> Result<i64, String> {
    let after = instant(after_ms)?;
    scheds
        .iter()
        .filter_map(|s| s.after(&after).next())
        .map(|dt| dt.timestamp_millis())
        .min()
        .ok_or_else(|| format!("cron `{spec}` has no future occurrence"))
}

/// Zoned evaluation. See the module docs for the two DST rules; this is where they are.
fn next_local(scheds: &[cron::Schedule], spec: &str, after_ms: i64, tz: Tz) -> Result<i64, String> {
    let mut wall = instant(after_ms)?.with_timezone(&tz).naive_local();
    for _ in 0..MAX_WALL_CANDIDATES {
        let next = next_wall(scheds, spec, wall.and_utc().timestamp_millis())?;
        wall = instant(next)?.naive_utc();
        // `.earliest()` IS the contract, exactly: `None` for a wall clock inside a
        // spring-forward gap (skip it, take the next candidate), and the FIRST of the
        // two instants for a wall clock the fall-back repeats (fire once).
        if let Some(dt) = tz.from_local_datetime(&wall).earliest() {
            let ms = dt.timestamp_millis();
            // Collapsing an ambiguous wall clock to its first instant can leave a
            // candidate at or behind `after_ms` when `after_ms` sits in the repeated
            // hour; the guard keeps next_after strictly increasing regardless.
            if ms > after_ms {
                return Ok(ms);
            }
        }
    }
    Err(format!("cron `{spec}` has no future occurrence"))
}

/// Split an optional `CRON_TZ=<IANA zone>` prefix off the front of a spec. Absent
/// prefix returns the spec untouched, so the UTC path is byte-for-byte what it was.
fn split_tz(spec: &str) -> Result<(Option<Tz>, &str), String> {
    let Some(rest) = spec.strip_prefix(TZ_PREFIX) else {
        return Ok((None, spec));
    };
    let (name, body) = match rest.find(char::is_whitespace) {
        Some(i) => (&rest[..i], rest[i..].trim_start()),
        None => (rest, ""),
    };
    // ONE message whichever layer rejects, so a syntax refusal here and a
    // database miss in Go's `time.LoadLocation` cannot be told apart by a client. The
    // API serves it raw as a 400 (control API contract's error contract), byte-identical from both
    // servers, so the two must not differ in wording OR in which condition fires.
    let bad = || format!("unknown timezone `{name}`");
    if !zone_name_is_wellformed(name) {
        return Err(bad());
    }
    Tz::from_str(name)
        .map(|tz| (Some(tz), body))
        .map_err(|_| bad())
}

/// The shared syntax gate, mirrored byte-for-byte in Go's `cronspec.go`.
///
/// 1..=64 bytes, `/`-separated non-empty segments of `[A-Za-z0-9_+-]`, each segment
/// starting with an ASCII UPPERCASE letter.
///
/// The uppercase rule is not cosmetic — it is what keeps the two runtimes from
/// disagreeing. Go resolves through `time.LoadLocation`, which reads the FILESYSTEM: on
/// a case-insensitive filesystem (macOS, measured) `america/new_york` loads there while
/// chrono-tz's exact-match table rejects it, and on Linux the same door admits the
/// `posix/` and `right/` trees that exist in no Rust build. No IANA zone has a
/// lowercase-initial segment (checked against all 604 entries of a current tzdata), so
/// requiring one closes both holes with no zone list to hand-maintain.
///
/// Residual, stated rather than hidden: an ALL-CAPS spelling of a real zone
/// (`AMERICA/NEW_YORK`) still resolves on a case-insensitive filesystem and not in
/// Rust. Closing that needs an embedded zone list on the Go side, which is a second
/// copy of tzdata to keep in sync — a worse trade than a documented typo that fails
/// closed on every case-sensitive deployment.
fn zone_name_is_wellformed(name: &str) -> bool {
    if name.is_empty() || name.len() > 64 {
        return false;
    }
    name.split('/').all(|seg| {
        seg.starts_with(|c: char| c.is_ascii_uppercase())
            && seg
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'+' || b == b'-')
    })
}

/// Due ticks: `first_ms` (the stored next_run, inclusive) plus every successor up to
/// and including `now_ms`, capped at `cap` MOST RECENT ticks. Ordered oldest→newest.
pub fn due_ticks(spec: &str, first_ms: i64, now_ms: i64, cap: usize) -> Result<Vec<i64>, String> {
    if first_ms > now_ms {
        return Ok(Vec::new());
    }
    let mut ticks = vec![first_ms];
    let mut t = first_ms;
    // Generous scan bound: keep only the last `cap`, but never loop unboundedly on a
    // long outage x short period.
    for _ in 0..10_000 {
        t = next_after(spec, t)?;
        if t > now_ms {
            break;
        }
        ticks.push(t);
        if ticks.len() > cap {
            ticks.remove(0);
        }
    }
    Ok(ticks)
}

pub fn validate(spec: &str) -> Result<(), String> {
    // Any valid spec has a next occurrence after "now-ish"; 0 works for both forms.
    next_after(spec, 0).map(|_| ())
}

/// Normalize a spec BODY (the spec minus any `CRON_TZ=` prefix) to the crate's 6-field
/// form with POSIX day-of-week numbers; `spec` is the caller's original text and is
/// only used for error messages. Classic five-field crontab gets `0` seconds. Numeric
/// DOW is translated from POSIX (0-7, 0 and 7 both Sunday) to the crate's Quartz
/// numbering (1-7 = SUN-SAT) — left untranslated, `0 0 * * 1` would fire on SUNDAY, the
/// classic off-by-one trap. Names (MON..SUN) pass through untouched; so does anything
/// non-numeric.
fn normalized_fields(body: &str, spec: &str) -> Result<Vec<String>, String> {
    let mut fields: Vec<String> = body.split_whitespace().map(String::from).collect();
    if fields.len() == 5 {
        fields.insert(0, "0".into());
    }
    if fields.len() != 6 {
        // The design says five-field crontab or six-field with seconds — a 7th (years,
        // a Quartz-ism the crate would take) is rejected so Go never has to match it.
        return Err(format!(
            "bad cron `{spec}`: want 5 fields, or 6 with seconds"
        ));
    }
    fields[5] = translate_posix_dow(&fields[5]);
    Ok(fields)
}

fn translate_posix_dow(field: &str) -> String {
    let shift = |tok: &str| -> Option<i64> {
        let d: i64 = tok.parse().ok()?;
        (0..=7).contains(&d).then_some(d % 7 + 1)
    };
    field
        .split(',')
        .map(|part| {
            let (range, step) = match part.split_once('/') {
                Some((r, s)) => (r, Some(s)),
                None => (part, None),
            };
            let translated = match range.split_once('-') {
                Some((a, b)) => match (shift(a), shift(b)) {
                    // A range ending on POSIX 7 (Sunday) wraps past SAT: split it.
                    (Some(a), Some(b)) if b < a => format!("{a}-7,1"),
                    (Some(a), Some(b)) => format!("{a}-{b}"),
                    _ => range.to_string(),
                },
                None => shift(range)
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| range.to_string()),
            };
            match step {
                Some(s) => format!("{translated}/{s}"),
                None => translated,
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_is_epoch_aligned_and_deterministic() {
        // Alignment is what lets racing nodes agree on tick identity.
        assert_eq!(next_after("@every:1000", 0).unwrap(), 1000);
        assert_eq!(next_after("@every:1000", 999).unwrap(), 1000);
        assert_eq!(next_after("@every:1000", 1000).unwrap(), 2000);
        assert!(
            next_after("@every:0", 0).is_err(),
            "zero period is rejected, not looped"
        );
    }

    #[test]
    fn cron_five_and_six_field_forms() {
        // hourly at :00 — 2024-01-01T00:30:00Z is 1704069000000
        let t = 1_704_069_000_000i64;
        assert_eq!(next_after("0 * * * *", t).unwrap(), 1_704_070_800_000); // 01:00Z
        // with seconds: every 30s (wire-time contract sub-minute)
        let n = next_after("*/30 * * * * *", t).unwrap();
        assert_eq!(n, t + 30_000);
    }

    #[test]
    fn due_ticks_caps_to_most_recent() {
        // 10s of backlog at 1s period, cap 3 → the three most recent ticks.
        let ticks = due_ticks("@every:1000", 1000, 10_000, 3).unwrap();
        assert_eq!(ticks, vec![8000, 9000, 10_000]);
    }

    #[test]
    fn tz_prefix_shifts_the_wall_clock() {
        // 2024-01-01T00:00:00Z. 09:00 New York (EST, -5) is 14:00Z.
        let t = 1_704_067_200_000i64;
        assert_eq!(
            next_after("CRON_TZ=America/New_York 0 9 * * *", t).unwrap(),
            1_704_117_600_000
        );
        // CRON_TZ=UTC is exactly the un-prefixed spec.
        assert_eq!(
            next_after("CRON_TZ=UTC 0 * * * *", t).unwrap(),
            next_after("0 * * * *", t).unwrap()
        );
    }

    #[test]
    fn unknown_zone_is_rejected_and_at_validate_time() {
        // The API turns this string into a 400 verbatim, so it is the contract.
        assert_eq!(
            validate("CRON_TZ=Mars/Phobos 0 9 * * *").unwrap_err(),
            "unknown timezone `Mars/Phobos`"
        );
        // The syntax gate answers with the SAME message: a client cannot tell which
        // layer refused, which is what keeps Go's filesystem lookup in line.
        assert_eq!(
            validate("CRON_TZ=america/new_york 0 9 * * *").unwrap_err(),
            "unknown timezone `america/new_york`"
        );
        assert!(validate("CRON_TZ=posix/America/New_York 0 9 * * *").is_err());
        assert!(validate("CRON_TZ= 0 9 * * *").is_err());
    }

    #[test]
    fn every_refuses_a_timezone() {
        // Silently ignoring it would un-align the interval from every other node.
        assert!(next_after("CRON_TZ=America/New_York @every:1000", 0).is_err());
    }

    #[test]
    fn dst_gap_is_skipped_and_fold_fires_once() {
        // 2024-03-10: America/New_York jumps 02:00 EST -> 03:00 EDT. A daily 02:30
        // does not exist that day and is SKIPPED (not dragged to 03:00).
        let before = 1_709_942_400_000i64; // 2024-03-09T00:00:00Z
        let next = next_after("CRON_TZ=America/New_York 30 2 * * *", before).unwrap();
        assert_eq!(
            next, 1_709_969_400_000,
            "2024-03-09T07:30:00Z (Mar 9 02:30 EST)"
        );
        let after_gap = next_after("CRON_TZ=America/New_York 30 2 * * *", next).unwrap();
        assert_eq!(
            after_gap, 1_710_138_600_000,
            "2024-03-11T06:30:00Z — Mar 10 skipped"
        );

        // 2024-11-03: 02:00 EDT -> 01:00 EST, so 01:30 happens twice. It fires ONCE,
        // at the FIRST (EDT, -4) instant, and the next tick is the following day.
        let before = 1_730_548_800_000i64; // 2024-11-02T12:00:00Z
        let fold = next_after("CRON_TZ=America/New_York 30 1 * * *", before).unwrap();
        assert_eq!(
            fold, 1_730_611_800_000,
            "2024-11-03T05:30:00Z — the FIRST 01:30"
        );
        let after_fold = next_after("CRON_TZ=America/New_York 30 1 * * *", fold).unwrap();
        assert_eq!(
            after_fold, 1_730_701_800_000,
            "2024-11-04T06:30:00Z — the second 01:30 (06:30Z) is NOT a tick"
        );
    }

    #[test]
    fn day_of_week_is_the_local_calendar() {
        // Monday 00:30 in Kolkata (+05:30) is SUNDAY 19:00Z — if DOW were read off the
        // UTC calendar this would never fire. 2024-01-01 was a Monday.
        let t = 1_703_980_800_000i64; // 2023-12-31T00:00:00Z (a Sunday, UTC)
        assert_eq!(
            next_after("CRON_TZ=Asia/Kolkata 30 0 * * 1", t).unwrap(),
            1_704_049_200_000,
            "2023-12-31T19:00:00Z = Mon 2024-01-01 00:30 IST"
        );
    }

    #[test]
    fn due_ticks_and_the_scheduler_path_take_a_zone() {
        // The sweep only ever calls due_ticks/next_after, so a zone rides through it
        // untouched — and tick ids stay epoch-ms.
        let first = 1_704_117_600_000i64; // 2024-01-01T14:00:00Z = 09:00 New York
        let ticks = due_ticks(
            "CRON_TZ=America/New_York 0 9 * * *",
            first,
            first + 200_000_000,
            5,
        )
        .unwrap();
        assert_eq!(
            ticks,
            vec![1_704_117_600_000, 1_704_204_000_000, 1_704_290_400_000]
        );
    }
}
