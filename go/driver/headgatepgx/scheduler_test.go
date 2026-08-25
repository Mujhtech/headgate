package headgatepgx

// surveyed policy behavior the Go scheduler duty, live: a real Runner fires a periodic schedule through
// SchedulerSweep — @every AND cron (cron un-declined once conformance/cron_ticks.json
// pinned tick identity against Rust). Assertions are scoped to THIS test's schedule
// ids and each test pauses/deletes its schedules at the end — global sweep counts are
// unassertable in a shared DB (other tests' schedules become due).

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

// Round 32: a PER-SCHEDULE TIMEZONE, live through the Go store and SchedulerSweep. The
// zone rides inside the ONE spec string (`CRON_TZ=<IANA> <cron>`), so the schema, the
// store port and the sweep learn nothing about timezones — and the tick id is still
// epoch-ms. What the tick proves is that 09:00 is NEW YORK's 09:00: that instant is
// 14:00Z under EST and 13:00Z under EDT, never 09:00Z.
func TestGoSchedulerZonedSpecFiresOnTheLocalWallClock(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM headgate_job WHERE queue = 'gotz';
		DELETE FROM headgate_schedule WHERE id LIKE 'gotz-%'`); err != nil {
		t.Fatal(err)
	}
	mk := func(spec string, next int64) headgate.ScheduleEntry {
		return headgate.ScheduleEntry{
			ID: "gotz-1", Kind: "gort:msg", Payload: []byte(`{"mode":"ok"}`), Queue: "gotz",
			Spec: spec, NextRunMs: next, MaxAttempts: 25, RetentionMs: 86_400_000,
		}
	}
	const ny = "CRON_TZ=America/New_York 0 9 * * *"
	// 2024-01-01T14:00:00Z = 09:00 New York, well in the past: the tick is due now.
	if err := s.UpsertSchedule(ctx, mk(ny, 1_704_117_600_000)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.DeleteSchedule(ctx, "gotz-1") }() // leave nothing ticking
	if _, err := headgate.SchedulerSweep(ctx, s); err != nil {
		t.Fatal(err)
	}
	var id, originSchedule string
	var originTick int64
	if err := s.pool.QueryRow(ctx,
		`SELECT ulid, periodic_schedule_id, periodic_tick_ms
		 FROM headgate_job WHERE ulid LIKE 'sched-gotz-1-%'`).Scan(&id, &originSchedule, &originTick); err != nil {
		t.Fatalf("zoned schedule fired no tick: %v", err)
	}
	tick, err := strconv.ParseInt(id[strings.LastIndex(id, "-")+1:], 10, 64)
	if err != nil {
		t.Fatalf("tick id %q is not epoch-ms: %v", id, err)
	}
	if originSchedule != "gotz-1" || originTick != tick {
		t.Fatalf("typed periodic origin = %q/%d, want gotz-1/%d", originSchedule, originTick, tick)
	}
	utcHour := func(ms int64) int64 { return ((ms % 86_400_000) + 86_400_000) % 86_400_000 / 3_600_000 }
	if h := utcHour(tick); h != 13 && h != 14 {
		t.Fatalf("tick %d is at %d:00Z — 09:00 New York is 14:00Z (EST) or 13:00Z (EDT)", tick, h)
	}
	nextRun := func() int64 {
		ss, err := s.ListSchedules(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ss {
			if e.ID == "gotz-1" {
				return e.NextRunMs
			}
		}
		t.Fatal("gotz-1 vanished")
		return 0
	}
	advanced := nextRun()
	if h := utcHour(advanced); h != 13 && h != 14 {
		t.Fatalf("advance %d is at %d:00Z, not on the New York wall clock", advanced, h)
	}
	// The whole reason the zone rides IN the spec: an unchanged spec keeps the phase,
	// and changing ONLY the timezone is a changed spec, so the phase re-anchors.
	if err := s.UpsertSchedule(ctx, mk(ny, 8_888)); err != nil {
		t.Fatal(err)
	}
	if got := nextRun(); got != advanced {
		t.Fatalf("same zone, same spec must keep the phase: %d != %d", got, advanced)
	}
	if err := s.UpsertSchedule(ctx, mk("CRON_TZ=Asia/Kolkata 0 9 * * *", 9_999)); err != nil {
		t.Fatal(err)
	}
	if got := nextRun(); got != 9_999 {
		t.Fatalf("a changed TIMEZONE is a changed spec and must re-anchor: %d", got)
	}
}

func TestGoSchedulerDutyFiresEveryAndCron(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM headgate_job WHERE queue = 'gosched';
		DELETE FROM headgate_schedule WHERE id LIKE 'gosched-%'`); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"mode":"ok"}`)
	mk := func(id, spec string) headgate.ScheduleEntry {
		return headgate.ScheduleEntry{
			ID: id, Kind: "gort:msg", Payload: payload, Queue: "gosched",
			Spec: spec, NextRunMs: 1, MaxAttempts: 25, RetentionMs: 86_400_000,
		}
	}
	if err := s.UpsertSchedule(ctx, mk("gosched-e", "@every:300")); err != nil {
		t.Fatal(err)
	}
	// Sub-minute six-field cron so the test observes REAL cron ticks, not a mock.
	if err := s.UpsertSchedule(ctx, mk("gosched-c", "*/1 * * * * *")); err != nil {
		t.Fatal(err)
	}

	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg, func(context.Context, *headgate.Job[rtMsg]) error {
		return nil
	})
	var hookMu sync.Mutex
	hookEvents := make([]struct {
		phase headgate.PeriodicEnqueueHookPhase
		id    string
		tick  int64
	}, 0)
	periodicHook := headgate.PeriodicEnqueueHookFunc(func(
		_ context.Context,
		event headgate.PeriodicEnqueueHookEvent,
	) {
		attempt := event.Attempt()
		if attempt.ScheduleID() != "gosched-e" && attempt.ScheduleID() != "gosched-c" {
			return
		}
		hookMu.Lock()
		hookEvents = append(hookEvents, struct {
			phase headgate.PeriodicEnqueueHookPhase
			id    string
			tick  int64
		}{event.Phase(), attempt.ScheduleID(), attempt.TickMs()})
		hookMu.Unlock()
	})
	cfg := headgate.Config{
		Queues:               map[string]headgate.QueueConfig{"gosched": {MaxWorkers: 4}},
		WorkerID:             "gosched-w",
		LeaseDuration:        5 * time.Second,
		DutyInterval:         50 * time.Millisecond,
		PeriodicEnqueueHooks: []headgate.PeriodicEnqueueHook{periodicHook},
	}
	r := headgate.NewRunner(s, reg, cfg)
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(runCtx) }()

	completedLike := func(prefix string) int64 {
		var n int64
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM headgate_job
			WHERE queue = 'gosched' AND ulid LIKE $1 AND state = 'completed'`,
			prefix+"%").Scan(&n)
		return n
	}
	waitFor(t, 60*time.Second, func() bool {
		return completedLike("sched-gosched-e-") >= 2 && completedLike("sched-gosched-c-") >= 2
	})

	// Tick identity: every fired cron job id embeds a whole-second tick (the shared
	// vector contract in action, not just "something ran").
	rows, err := s.pool.Query(ctx,
		`SELECT ulid FROM headgate_job WHERE queue = 'gosched' AND ulid LIKE 'sched-gosched-c-%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	// Round 32h: `rows.Next()` returns false on ITERATION ERROR as well as on
	// exhaustion, so a mid-stream failure ended this loop with no complaint — and a
	// zero-row result asserted nothing at all. Both are checked now, the way the MySQL
	// twin already did.
	seen := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		seen++
		tick := strings.TrimPrefix(id, "sched-gosched-c-")
		if !strings.HasSuffix(tick, "000") {
			t.Fatalf("cron tick %s is not second-aligned", tick)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("tick scan: %v", err)
	}
	if seen < 2 {
		t.Fatalf("the tick-identity loop examined %d rows; it must not pass vacuously", seen)
	}
	hookMu.Lock()
	observedHooks := append([]struct {
		phase headgate.PeriodicEnqueueHookPhase
		id    string
		tick  int64
	}(nil), hookEvents...)
	hookMu.Unlock()
	for _, scheduleID := range []string{"gosched-e", "gosched-c"} {
		phasesByTick := map[int64][]headgate.PeriodicEnqueueHookPhase{}
		for _, event := range observedHooks {
			if event.id == scheduleID {
				phasesByTick[event.tick] = append(phasesByTick[event.tick], event.phase)
			}
		}
		if len(phasesByTick) < 2 {
			t.Fatalf("%s hook observed %d ticks, want at least two", scheduleID, len(phasesByTick))
		}
		for tick, phases := range phasesByTick {
			if len(phases) >= 2 && (phases[0] != headgate.PeriodicEnqueueHookBegin ||
				phases[1] != headgate.PeriodicEnqueueHookEnd) {
				t.Fatalf("%s tick %d phases = %#v, want begin/end", scheduleID, tick, phases)
			}
		}
	}

	// Hygiene: leave nothing ticking in the shared DB.
	if err := s.DeleteSchedule(ctx, "gosched-e"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSchedule(ctx, "gosched-c"); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestPeriodicEnqueueHooksSurroundReplayWithoutBreakingTickIdempotency(t *testing.T) {
	s, ctx := testStore(t)
	const scheduleID = "gohook-1"
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM headgate_job WHERE queue = 'gohook';
		DELETE FROM headgate_schedule WHERE id = 'gohook-1'`); err != nil {
		t.Fatal(err)
	}
	entry := headgate.ScheduleEntry{
		ID: scheduleID, Kind: "gort:hook", Payload: []byte(`{"ok":true}`), Queue: "gohook",
		Spec: "@every:60000", NextRunMs: 1, MaxAttempts: 25, RetentionMs: 86_400_000,
	}
	if err := s.UpsertSchedule(ctx, entry); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.DeleteSchedule(ctx, scheduleID) }()

	// The first hook attacks its owned snapshots. The second hook and the Store must
	// still see the original schedule and immutable `sched:<id>:<tick>` identity.
	mutator := headgate.PeriodicEnqueueHookFunc(func(
		_ context.Context,
		event headgate.PeriodicEnqueueHookEvent,
	) {
		attempt := event.Attempt()
		if attempt.ScheduleID() != scheduleID {
			return
		}
		schedule := attempt.Schedule()
		schedule.Payload[0] = 'X'
		envelope := attempt.Envelope()
		envelope.UniqueKey[0] = 'X'
	})
	type hookRecord struct {
		phase   headgate.PeriodicEnqueueHookPhase
		id      string
		tick    int64
		jobID   string
		success bool
	}
	var records []hookRecord
	recorder := headgate.PeriodicEnqueueHookFunc(func(
		_ context.Context,
		event headgate.PeriodicEnqueueHookEvent,
	) {
		attempt := event.Attempt()
		if attempt.ScheduleID() != scheduleID {
			return
		}
		outcome, hasOutcome := event.Outcome()
		records = append(records, hookRecord{
			phase: event.Phase(), id: attempt.ScheduleID(), tick: attempt.TickMs(),
			jobID:   attempt.Envelope().ID,
			success: hasOutcome && outcome.Kind == headgate.InsertOutcomeSucceeded,
		})
	})
	hooks := []headgate.PeriodicEnqueueHook{mutator, recorder}
	if _, err := headgate.SchedulerSweepWithHooks(ctx, s, hooks...); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].phase != headgate.PeriodicEnqueueHookBegin ||
		records[1].phase != headgate.PeriodicEnqueueHookEnd {
		t.Fatalf("first tick events = %#v, want begin/end", records)
	}
	if records[0].id != scheduleID || records[0].tick != records[1].tick {
		t.Fatalf("schedule identity drifted: %#v", records)
	}
	if want := "sched-" + scheduleID + "-" + strconv.FormatInt(records[0].tick, 10); records[0].jobID != want {
		t.Fatalf("job id = %q, want %q", records[0].jobID, want)
	}
	if records[0].success || !records[1].success {
		t.Fatalf("outcomes = %#v, want only end success", records)
	}

	if _, err := s.pool.Exec(ctx,
		`UPDATE headgate_schedule SET next_run_ms = $2 WHERE id = $1`,
		scheduleID, int64(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := headgate.SchedulerSweepWithHooks(ctx, s, hooks...); err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || records[2].tick != records[0].tick ||
		records[2].jobID != records[0].jobID || !records[3].success {
		t.Fatalf("replayed tick events = %#v", records)
	}
	var jobs int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM headgate_job WHERE ulid LIKE 'sched-gohook-1-%'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("replayed tick created %d rows, want exactly one", jobs)
	}
}
