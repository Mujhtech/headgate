package headgate

// surveyed policy behavior schedule-spec evaluation, Go side. Tick times feed the per-tick unique keys
// (`sched:{id}:{tick_ms}`), so BOTH languages must derive identical times — that is
// what makes the scheduler leaderless. "@every:<ms>" is epoch-aligned and exact here;
// cron (POSIX crontab semantics, UTC or a `CRON_TZ=<IANA zone>` prefix — see
// cronspec.go) is pinned against Rust by the shared vectors in
// conformance/cron_ticks.json.

import (
	"fmt"
	"strconv"
	"strings"
)

// ScheduleNextAfter returns the next tick STRICTLY AFTER afterMs. Mirrors Rust's
// schedule_spec::next_after for "@every:<ms>".
func ScheduleNextAfter(spec string, afterMs int64) (int64, error) {
	loc, body, err := splitTZ(spec)
	if err != nil {
		return 0, err
	}
	if ms, ok := strings.CutPrefix(body, "@every:"); ok {
		if loc != nil {
			// An interval has no wall clock to be wrong about, and epoch alignment is
			// the whole mechanism — silently ignoring a timezone the operator asked for
			// would un-align this node from every other one.
			return 0, fmt.Errorf(
				"headgate: `@every` is epoch-aligned UTC and takes no CRON_TZ: `%s`", spec)
		}
		n, err := strconv.ParseInt(ms, 10, 64)
		if err != nil {
			// Backticks, not %q. Rust renders every spec error with `{spec}`; Go's %q
			// wraps it in DOUBLE QUOTES, so the two servers answered the same 400
			// with different bytes on every cron and @every rejection.
			return 0, fmt.Errorf("headgate: bad @every spec `%s`", spec)
		}
		if n < 1 {
			// boundary validation a period that rounds to zero is an error, never a busy loop.
			return 0, &InvalidError{Msg: "@every period must be >= 1ms"}
		}
		// Epoch alignment is what lets racing nodes agree on tick identity.
		return (floorDiv(afterMs, n) + 1) * n, nil
	}
	c, err := parseCron(body, spec)
	if err != nil {
		return 0, err
	}
	return c.nextAfter(spec, afterMs, loc)
}

// ScheduleDueTicks mirrors Rust's schedule_spec::due_ticks: firstMs (the stored
// next_run, inclusive) plus every successor up to and including nowMs, capped at the
// `cap` MOST RECENT ticks, oldest first.
func ScheduleDueTicks(spec string, firstMs, nowMs int64, cap int) ([]int64, error) {
	if firstMs > nowMs {
		return nil, nil
	}
	ticks := []int64{firstMs}
	t := firstMs
	for i := 0; i < 10_000; i++ {
		next, err := ScheduleNextAfter(spec, t)
		if err != nil {
			return nil, err
		}
		if next > nowMs {
			break
		}
		ticks = append(ticks, next)
		if len(ticks) > cap {
			ticks = ticks[1:]
		}
		t = next
	}
	return ticks, nil
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
