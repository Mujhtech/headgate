package headgate

// The cross-language tick-vector suite: the SAME conformance/cron_ticks.json the Rust
// test reads. A mismatch here is a split-brain scheduler (two nodes firing different
// "identical" ticks), so this suite is what un-declined cron in Go.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestCronTickVectorsMatchRust(t *testing.T) {
	raw, err := os.ReadFile("../conformance/cron_ticks.json")
	if err != nil {
		t.Fatalf("vector file: %v", err)
	}
	var vectors []struct {
		Spec    string `json:"spec"`
		AfterMs int64  `json:"after_ms"`
		NextMs  int64  `json:"next_ms"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("vector json: %v", err)
	}
	if len(vectors) < 60 {
		t.Fatalf("vector file looks truncated: %d", len(vectors))
	}
	// Round 32: the timezone vectors must actually BE here. Without this the suite
	// passes just as loudly against a file that lost them, which is the one way a
	// cross-language pin fails silently.
	zoned := 0
	for _, v := range vectors {
		if strings.HasPrefix(v.Spec, "CRON_TZ=") {
			zoned++
		}
	}
	if zoned < 25 {
		t.Fatalf("timezone vectors missing: only %d of %d specs carry CRON_TZ=", zoned, len(vectors))
	}
	for _, v := range vectors {
		got, err := ScheduleNextAfter(v.Spec, v.AfterMs)
		if err != nil {
			t.Errorf("spec %q after %d: %v", v.Spec, v.AfterMs, err)
			continue
		}
		if got != v.NextMs {
			t.Errorf("spec %q after %d: got %d want %d", v.Spec, v.AfterMs, got, v.NextMs)
		}
	}
}

func TestCronEdgesTheVectorsCannotEncode(t *testing.T) {
	// Impossible dates error rather than spin.
	if _, err := ScheduleNextAfter("0 0 31 4 *", 0); err == nil {
		t.Fatal("April 31 must error")
	}
	// Quartz 7-field (years) is rejected in BOTH languages — the design says 5 or 6.
	if _, err := ScheduleNextAfter("0 0 0 1 1 * 2099", 0); err == nil {
		t.Fatal("year field must be rejected")
	}
	// POSIX 0 = Sunday parses.
	if _, err := ScheduleNextAfter("0 0 * * 0", 0); err != nil {
		t.Fatalf("DOW 0 must parse: %v", err)
	}
}

// Round 32. The vectors pin every ACCEPTED timezone tick; these pin the REJECTIONS and
// the error text, which the API serves verbatim as a 400 and the mutation diff compares
// byte for byte against Rust's.
func TestCronTimezoneRejections(t *testing.T) {
	for _, c := range []struct{ spec, want string }{
		{"CRON_TZ=Mars/Phobos 0 9 * * *", "headgate: unknown timezone `Mars/Phobos`"},
		// The syntax gate answers with the SAME message as a database miss, so a client
		// cannot tell which layer refused — that is what keeps this filesystem-backed
		// lookup in line with Rust's exact-match table on a case-insensitive filesystem.
		{"CRON_TZ=america/new_york 0 9 * * *", "headgate: unknown timezone `america/new_york`"},
		{"CRON_TZ=posix/America/New_York 0 9 * * *",
			"headgate: unknown timezone `posix/America/New_York`"},
		{"CRON_TZ= 0 9 * * *", "headgate: unknown timezone ``"},
		{"CRON_TZ=America/New_York @every:1000",
			"headgate: `@every` is epoch-aligned UTC and takes no CRON_TZ: " +
				"`CRON_TZ=America/New_York @every:1000`"},
	} {
		_, err := ScheduleNextAfter(c.spec, 0)
		if err == nil {
			t.Fatalf("%q must be rejected", c.spec)
		}
		if err.Error() != c.want {
			t.Fatalf("%q: got %q, want %q", c.spec, err.Error(), c.want)
		}
	}
	// CRON_TZ=UTC is not a mode switch, it is a zone: same answer as no prefix at all.
	a, err := ScheduleNextAfter("CRON_TZ=UTC 0 * * * *", 1_704_069_000_000)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ScheduleNextAfter("0 * * * *", 1_704_069_000_000)
	if err != nil || a != b {
		t.Fatalf("CRON_TZ=UTC diverged: %d vs %d (%v)", a, b, err)
	}
}

// The sweep only ever calls ScheduleDueTicks/ScheduleNextAfter, so a zone rides through
// the missed-schedule machinery untouched — and tick ids stay epoch-ms.
func TestZonedDueTicksAreEpochMillis(t *testing.T) {
	first := int64(1_704_117_600_000) // 2024-01-01T14:00:00Z = 09:00 New York
	ticks, err := ScheduleDueTicks("CRON_TZ=America/New_York 0 9 * * *", first, first+200_000_000, 5)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1_704_117_600_000, 1_704_204_000_000, 1_704_290_400_000}
	if len(ticks) != len(want) {
		t.Fatalf("ticks = %v, want %v", ticks, want)
	}
	for i := range want {
		if ticks[i] != want[i] {
			t.Fatalf("ticks = %v, want %v", ticks, want)
		}
	}
}
