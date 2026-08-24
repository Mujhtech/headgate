package headgate

// surveyed policy behavior cron evaluation, Go side — POSIX crontab semantics, implemented directly
// rather than through a third-party library: tick identity is a cross-language
// CONTRACT (the per-tick unique keys), and two cron libraries silently disagreeing on
// DOW numbering or DOM/DOW union is exactly the drift that kept cron declined here
// until conformance/cron_ticks.json pinned it. Semantics pinned by the vectors:
// five-field crontab or six with leading seconds; numeric day-of-week 0-7 with 0 and 7
// both Sunday; month/day names accepted; DOM and DOW BOTH restricted = UNION (a day
// matching either fires — crontab(5)).
//
// a spec may carry a `CRON_TZ=<IANA zone>` prefix and is then a LOCAL
// wall-clock expression — hour/minute/second AND day-of-month/day-of-week are read off
// the local calendar. Zone resolution is stdlib time.LoadLocation; no new dependency.
// The DST contract, identical to Rust's schedule_spec.rs and pinned by the same
// vectors:
//
//   - a local time that does NOT EXIST (spring forward) is SKIPPED;
//   - a local time that occurs TWICE (fall back) fires ONCE, at the FIRST
//     (pre-transition) occurrence.
//
// Tick ids stay epoch-ms, so nothing downstream — the unique key `sched:{id}:{tick_ms}`,
// the missed-schedule policies, the CAS advance — learns about timezones at all.

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	// Embeds the IANA database as a FALLBACK behind the system zoneinfo (stdlib, not a
	// dependency). Rust's side carries chrono-tz's embedded copy, so a Go node in a
	// scratch container must not be the one that rejects `CRON_TZ=Asia/Kolkata` — a
	// zone that resolves in one runtime and not the other is a split-brain scheduler
	// wearing a config error's clothes.
	_ "time/tzdata"
	"unicode"
)

// robfig/cron's in-spec prefix. One form only — `TZ=` is deliberately NOT accepted
// (robfig deprecated it), because two spellings of one thing is two things to pin.
const tzPrefix = "CRON_TZ="

// How many candidate wall clocks may be discarded as nonexistent before a zoned spec is
// declared unsatisfiable. Same bound as Rust's MAX_WALL_CANDIDATES.
const maxWallCandidates = 100_000

type cronSpec struct {
	sec, min, hour uint64 // bitmasks over their POSIX ranges
	dom            uint64 // 1..31
	month          uint64 // 1..12
	dow            uint64 // 0..6, Sunday = 0
	domStar        bool   // the crontab(5) union rule needs "was it '*'", not the mask
	dowStar        bool
}

var monthNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
	"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

var dowNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
}

// splitTZ pulls an optional `CRON_TZ=<IANA zone>` prefix off the front of a spec.
// Absent prefix returns a nil Location and the spec untouched, so the UTC path is
// byte-for-byte what it was.
func splitTZ(spec string) (*time.Location, string, error) {
	rest, ok := strings.CutPrefix(spec, tzPrefix)
	if !ok {
		return nil, spec, nil
	}
	name, body := rest, ""
	if i := strings.IndexFunc(rest, unicode.IsSpace); i >= 0 {
		name, body = rest[:i], strings.TrimLeftFunc(rest[i:], unicode.IsSpace)
	}
	// ONE message whichever layer rejects, so this filesystem-backed lookup and Rust's
	// embedded table cannot be told apart by a client. The API serves it raw as a 400
	// (control API contract's error contract), byte-identical from both servers.
	bad := fmt.Errorf("headgate: unknown timezone `%s`", name)
	if !zoneNameIsWellformed(name) {
		return nil, "", bad
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, "", bad
	}
	return loc, body, nil
}

// zoneNameIsWellformed is the shared syntax gate, mirrored byte-for-byte in Rust's
// schedule_spec.rs: 1..64 bytes, '/'-separated non-empty segments of [A-Za-z0-9_+-],
// each segment starting with an ASCII UPPERCASE letter.
//
// The uppercase rule is not cosmetic — it is what keeps the two runtimes from
// disagreeing. time.LoadLocation reads the FILESYSTEM: on a case-insensitive filesystem
// (macOS, measured) `america/new_york` loads HERE while chrono-tz's exact-match table
// rejects it, and on Linux the same door admits the `posix/` and `right/` trees that
// exist in no Rust build. No IANA zone has a lowercase-initial segment (checked against
// all 604 entries of a current tzdata), so requiring one closes both holes with no zone
// list to hand-maintain. Residual, stated rather than hidden: an ALL-CAPS spelling of a
// real zone still resolves on a case-insensitive filesystem and not in Rust — a typo
// that fails closed on every case-sensitive deployment, and closing it would mean a
// second copy of tzdata to keep in sync.
func zoneNameIsWellformed(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || seg[0] < 'A' || seg[0] > 'Z' {
			return false
		}
		for i := 0; i < len(seg); i++ {
			b := seg[i]
			switch {
			case b >= '0' && b <= '9', b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z',
				b == '_', b == '+', b == '-':
			default:
				return false
			}
		}
	}
	return true
}

// parseCron parses a spec BODY (the spec minus any CRON_TZ= prefix); `spec` is the
// caller's original text and is used only for error messages.
func parseCron(body, spec string) (*cronSpec, error) {
	fields := strings.Fields(body)
	if len(fields) == 5 {
		fields = append([]string{"0"}, fields...) // classic crontab: seconds = 0
	}
	if len(fields) != 6 {
		return nil, fmt.Errorf("headgate: bad cron `%s`: want 5 fields, or 6 with seconds", spec)
	}
	c := &cronSpec{domStar: fieldIsStar(fields[3]), dowStar: fieldIsStar(fields[5])}
	var err error
	if c.sec, err = parseField(fields[0], 0, 59, nil, false); err == nil {
		if c.min, err = parseField(fields[1], 0, 59, nil, false); err == nil {
			if c.hour, err = parseField(fields[2], 0, 23, nil, false); err == nil {
				if c.dom, err = parseField(fields[3], 1, 31, nil, false); err == nil {
					if c.month, err = parseField(fields[4], 1, 12, monthNames, false); err == nil {
						c.dow, err = parseField(fields[5], 0, 6, dowNames, true)
					}
				}
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("headgate: bad cron `%s`: %w", spec, err)
	}
	return c, nil
}

func fieldIsStar(f string) bool { return f == "*" || f == "?" }

// parseField turns one crontab field (lists, ranges, steps, names) into a bitmask.
// isDow admits 7 as Sunday and wraps descending ranges (5-7 = FRI,SAT,SUN).
func parseField(field string, lo, hi int, names map[string]int, isDow bool) (uint64, error) {
	var mask uint64
	for _, part := range strings.Split(field, ",") {
		rangePart, step := part, 1
		if r, s, ok := strings.Cut(part, "/"); ok {
			n, err := strconv.Atoi(s)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("bad step %q", part)
			}
			rangePart, step = r, n
		}
		start, end := lo, hi
		switch {
		case fieldIsStar(rangePart):
			// full range
		default:
			a, b, isRange := strings.Cut(rangePart, "-")
			var err error
			if start, err = parseValue(a, lo, hi, names, isDow); err != nil {
				return 0, err
			}
			if isRange {
				if end, err = parseValue(b, lo, hi, names, isDow); err != nil {
					return 0, err
				}
			} else if step == 1 {
				end = start
			} // a bare value with a step ("3/5") ranges to hi, per crontab(5)
		}
		if end < start {
			if !isDow {
				return 0, fmt.Errorf("descending range %q", part)
			}
			// DOW ranges through Sunday wrap: 5-7 -> FRI,SAT then SUN.
			for v := start; v <= hi; v += step {
				mask |= 1 << v
			}
			start = lo
		}
		for v := start; v <= end; v += step {
			mask |= 1 << v
		}
	}
	if mask == 0 {
		return 0, fmt.Errorf("empty field %q", field)
	}
	return mask, nil
}

func parseValue(tok string, lo, hi int, names map[string]int, isDow bool) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToUpper(tok)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(tok)
	if err != nil {
		return 0, fmt.Errorf("bad value %q", tok)
	}
	if isDow && v == 7 {
		v = 0 // POSIX: 0 and 7 are both Sunday
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("value %q out of range %d-%d", tok, lo, hi)
	}
	return v, nil
}

func (c *cronSpec) dayMatches(t time.Time) bool {
	if c.month&(1<<int(t.Month())) == 0 {
		return false
	}
	domHit := c.dom&(1<<t.Day()) != 0
	dowHit := c.dow&(1<<int(t.Weekday())) != 0
	if !c.domStar && !c.dowStar {
		return domHit || dowHit // crontab(5): both restricted -> UNION
	}
	return domHit && dowHit // a '*' side matches every day, so AND is exact
}

// nextAfter returns the first matching instant STRICTLY AFTER afterMs. With no zone
// that is a plain UTC scan. With a zone the spec is a LOCAL wall-clock expression, so
// the same (timezone-free) scan runs in WALL-CLOCK space — fed the local wall clock
// encoded as if it were UTC — and each candidate is mapped back to a real instant.
// That mapping is where the DST contract lives (see the package comment): a wall clock
// inside a spring-forward gap is skipped, and a wall clock the fall-back repeats
// resolves to the FIRST of its two instants, so it fires once.
func (c *cronSpec) nextAfter(spec string, afterMs int64, loc *time.Location) (int64, error) {
	if loc == nil {
		return c.nextWall(spec, afterMs)
	}
	wallMs := wallClockMillis(time.UnixMilli(afterMs).In(loc))
	for i := 0; i < maxWallCandidates; i++ {
		next, err := c.nextWall(spec, wallMs)
		if err != nil {
			return 0, err
		}
		wallMs = next
		// Collapsing an ambiguous wall clock to its first instant can leave a candidate
		// at or behind afterMs when afterMs sits in the repeated hour; the > guard keeps
		// nextAfter strictly increasing regardless.
		if ms, ok := resolveLocal(next, loc); ok && ms > afterMs {
			return ms, nil
		}
	}
	return 0, fmt.Errorf("headgate: cron `%s` has no future occurrence", spec)
}

// wallClockMillis encodes a local wall clock as if it were UTC, which is the space the
// timezone-free scan runs in.
func wallClockMillis(t time.Time) int64 {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(),
		t.Nanosecond(), time.UTC).UnixMilli()
}

// resolveLocal maps a wall clock (encoded as UTC millis) to the instant it names in
// loc. ok is false when that wall clock does NOT exist (spring forward); when it
// happens TWICE (fall back) the FIRST — smaller — instant wins.
//
// Deliberately not time.Date(..., loc): that resolves a nonexistent wall clock to
// *something* and picks one of an ambiguous pair by rules the docs decline to promise,
// and this is a value two languages must agree on byte for byte. Instead, probe the
// offsets in force a day either side (which bracket any single transition), and keep
// the candidates that actually round-trip to the wall clock asked for.
func resolveLocal(wallMs int64, loc *time.Location) (int64, bool) {
	w := time.UnixMilli(wallMs).UTC()
	const day = int64(86_400_000)
	var best int64
	found := false
	seen := map[int]bool{}
	for _, probe := range []int64{wallMs - day, wallMs, wallMs + day} {
		_, off := time.UnixMilli(probe).In(loc).Zone()
		if seen[off] {
			continue
		}
		seen[off] = true
		cand := wallMs - int64(off)*1000
		t := time.UnixMilli(cand).In(loc)
		if t.Year() == w.Year() && t.Month() == w.Month() && t.Day() == w.Day() &&
			t.Hour() == w.Hour() && t.Minute() == w.Minute() && t.Second() == w.Second() {
			if !found || cand < best {
				best, found = cand, true
			}
		}
	}
	return best, found
}

// nextWall is the timezone-free scan: the first matching instant STRICTLY AFTER
// afterMs, reading every field off the UTC calendar. Bounded at 12 years: enough to
// cross the longest Feb-29 gap (8 years around a century non-leap), then "no future
// occurrence" — an error, never a spin.
func (c *cronSpec) nextWall(spec string, afterMs int64) (int64, error) {
	start := time.UnixMilli(afterMs).UTC().Truncate(time.Second).Add(time.Second)
	day := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	for i := 0; i < 366*12; i++ {
		if c.dayMatches(day) {
			floor := int64(0)
			if !day.After(start) && day.Add(24*time.Hour).After(start) {
				floor = int64(start.Sub(day) / time.Second) // first day: skip past `start`
			}
			for h := 0; h < 24; h++ {
				if c.hour&(1<<h) == 0 {
					continue
				}
				for m := 0; m < 60; m++ {
					if c.min&(1<<m) == 0 {
						continue
					}
					for s := 0; s < 60; s++ {
						if c.sec&(1<<s) == 0 {
							continue
						}
						at := int64(h*3600 + m*60 + s)
						if at >= floor {
							return day.Add(time.Duration(at) * time.Second).UnixMilli(), nil
						}
					}
				}
			}
		}
		day = day.Add(24 * time.Hour)
	}
	return 0, fmt.Errorf("headgate: cron `%s` has no future occurrence", spec)
}
