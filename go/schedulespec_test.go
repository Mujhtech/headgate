package headgate

import "testing"

// These vectors are copied from the Rust schedule_spec tests VERBATIM — the two
// implementations must agree on tick identity or the leaderless scheduler splits.
func TestScheduleSpecMatchesRustVectors(t *testing.T) {
	for _, c := range []struct{ after, want int64 }{
		{0, 1000}, {999, 1000}, {1000, 2000},
	} {
		got, err := ScheduleNextAfter("@every:1000", c.after)
		if err != nil || got != c.want {
			t.Fatalf("next_after(%d) = %d, %v; want %d", c.after, got, err, c.want)
		}
	}
	if _, err := ScheduleNextAfter("@every:0", 0); err == nil {
		t.Fatal("zero period must be rejected, not looped")
	}
	ticks, err := ScheduleDueTicks("@every:1000", 1000, 10_000, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{8000, 9000, 10_000}
	if len(ticks) != 3 || ticks[0] != want[0] || ticks[1] != want[1] || ticks[2] != want[2] {
		t.Fatalf("due_ticks = %v, want %v", ticks, want)
	}
	// Cron is no longer declined: conformance/cron_ticks.json pins tick identity
	// against Rust (cronspec_test.go). Hourly from the epoch = 3600000.
	if next, err := ScheduleNextAfter("0 * * * *", 0); err != nil || next != 3_600_000 {
		t.Fatalf("cron hourly: %d %v", next, err)
	}
}
