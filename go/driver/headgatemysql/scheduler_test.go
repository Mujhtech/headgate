package headgatemysql

// surveyed policy behavior the Go scheduler duty over MySQL, live — the duty that was compiled but
// unreachable on this driver until InspectStore existed here (round 32c). A real
// Runner fires a periodic schedule through SchedulerSweep: @every AND cron, plus the
// per-schedule timezone contract, mirroring headgatepgx/scheduler_test.go on the third
// backend.
//
// Assertions are scoped to THIS test's schedule ids and each test deletes its
// schedules at the end — global sweep counts are unassertable in a shared database
// (other tests' schedules become due, and MySQL is a single shared container here).
//
// Opt-in via HG_TEST_MYSQL; skips cleanly without it.

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// Round 32's per-schedule timezone, live through the Go MySQL store and SchedulerSweep.
// The zone rides inside the ONE spec string (`CRON_TZ=<IANA> <cron>`), so the schema,
// the store port and the sweep learn nothing about timezones — and the tick id is still
// epoch-ms. What the tick proves is that 09:00 is NEW YORK's 09:00: that instant is
// 14:00Z under EST and 13:00Z under EDT, never 09:00Z.
func TestGoMysqlSchedulerZonedSpecFiresOnTheLocalWallClock(t *testing.T) {
	s, ctx := testStore(t)
	q := scope("tz")
	sid := q + "-1"
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_job WHERE queue = ?", q); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_schedule WHERE id = ?", sid); err != nil {
		t.Fatal(err)
	}
	mk := func(spec string, next int64) headgate.ScheduleEntry {
		return headgate.ScheduleEntry{
			ID: sid, Kind: "gmy:msg", Payload: []byte(`{"mode":"ok"}`), Queue: q,
			Spec: spec, NextRunMs: next, MaxAttempts: 25, RetentionMs: 86_400_000,
		}
	}
	const ny = "CRON_TZ=America/New_York 0 9 * * *"
	// 2024-01-01T14:00:00Z = 09:00 New York, well in the past: the tick is due now.
	if err := s.UpsertSchedule(ctx, mk(ny, 1_704_117_600_000)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.DeleteSchedule(ctx, sid) }() // leave nothing ticking
	if _, err := headgate.SchedulerSweep(ctx, s); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := s.db.QueryRowContext(ctx,
		"SELECT ulid FROM headgate_job WHERE ulid LIKE ?", "sched-"+sid+"-%").Scan(&id); err != nil {
		t.Fatalf("zoned schedule fired no tick: %v", err)
	}
	tick, err := strconv.ParseInt(id[strings.LastIndex(id, "-")+1:], 10, 64)
	if err != nil {
		t.Fatalf("tick id %q is not epoch-ms: %v", id, err)
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
			if e.ID == sid {
				return e.NextRunMs
			}
		}
		t.Fatalf("%s vanished", sid)
		return 0
	}
	advanced := nextRun()
	if h := utcHour(advanced); h != 13 && h != 14 {
		t.Fatalf("advance %d is at %d:00Z, not on the New York wall clock", advanced, h)
	}
	// The whole reason the zone rides IN the spec: an unchanged spec keeps the phase,
	// and changing ONLY the timezone is a changed spec, so the phase re-anchors. On
	// MySQL that comparison is the ODKU's `IF(headgate_schedule.spec = new.spec, ...)`,
	// which must read the OLD spec — assert it here rather than trust statement order.
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

func TestGoMysqlSchedulerDutyFiresEveryAndCron(t *testing.T) {
	s, ctx := testStore(t)
	q := scope("sched")
	every, cron := q+"-e", q+"-c"
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_job WHERE queue = ?", q); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM headgate_schedule WHERE id IN (?, ?)", every, cron); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"mode":"ok"}`)
	mk := func(id, spec string) headgate.ScheduleEntry {
		return headgate.ScheduleEntry{
			ID: id, Kind: "gmy:msg", Payload: payload, Queue: q,
			Spec: spec, NextRunMs: 1, MaxAttempts: 25, RetentionMs: 86_400_000,
		}
	}
	if err := s.UpsertSchedule(ctx, mk(every, "@every:300")); err != nil {
		t.Fatal(err)
	}
	// Sub-minute six-field cron so the test observes REAL cron ticks, not a mock.
	if err := s.UpsertSchedule(ctx, mk(cron, "*/1 * * * * *")); err != nil {
		t.Fatal(err)
	}

	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[gmMsg](reg, func(context.Context, *headgate.Job[gmMsg]) error {
		return nil
	})
	var hookMu sync.Mutex
	hookPhases := map[string]map[int64][]headgate.PeriodicEnqueueHookPhase{}
	periodicHook := headgate.PeriodicEnqueueHookFunc(func(
		_ context.Context,
		event headgate.PeriodicEnqueueHookEvent,
	) {
		attempt := event.Attempt()
		if attempt.ScheduleID() != every && attempt.ScheduleID() != cron {
			return
		}
		hookMu.Lock()
		if hookPhases[attempt.ScheduleID()] == nil {
			hookPhases[attempt.ScheduleID()] = map[int64][]headgate.PeriodicEnqueueHookPhase{}
		}
		hookPhases[attempt.ScheduleID()][attempt.TickMs()] = append(
			hookPhases[attempt.ScheduleID()][attempt.TickMs()], event.Phase())
		hookMu.Unlock()
	})
	cfg := headgate.Config{
		Queues:               map[string]headgate.QueueConfig{q: {MaxWorkers: 4}},
		WorkerID:             q + "-w",
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
		_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM headgate_job
			WHERE queue = ? AND ulid LIKE ? AND state = 'completed'`,
			q, prefix+"%").Scan(&n)
		return n
	}
	waitFor(t, 60*time.Second, func() bool {
		return completedLike("sched-"+every+"-") >= 2 && completedLike("sched-"+cron+"-") >= 2
	})

	// Tick identity: every fired cron job id embeds a whole-second tick (the shared
	// vector contract in action, not just "something ran").
	rows, err := s.db.QueryContext(ctx,
		`SELECT ulid FROM headgate_job WHERE queue = ? AND ulid LIKE ?`,
		q, "sched-"+cron+"-%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		tick := strings.TrimPrefix(id, "sched-"+cron+"-")
		if !strings.HasSuffix(tick, "000") {
			t.Fatalf("cron tick %s is not second-aligned", tick)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	hookMu.Lock()
	for _, scheduleID := range []string{every, cron} {
		completedPairs := 0
		for tick, phases := range hookPhases[scheduleID] {
			if len(phases) < 2 {
				continue
			}
			if phases[0] != headgate.PeriodicEnqueueHookBegin ||
				phases[1] != headgate.PeriodicEnqueueHookEnd {
				hookMu.Unlock()
				t.Fatalf("%s tick %d phases = %#v, want begin/end", scheduleID, tick, phases)
			}
			completedPairs++
		}
		if completedPairs < 2 {
			hookMu.Unlock()
			t.Fatalf("%s has %d completed hook pairs, want at least two", scheduleID, completedPairs)
		}
	}
	hookMu.Unlock()

	// Hygiene: leave nothing ticking in the shared container.
	if err := s.DeleteSchedule(ctx, every); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSchedule(ctx, cron); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not stop")
	}
	if _, err := s.db.ExecContext(context.Background(),
		"DELETE FROM headgate_worker WHERE worker_id = ?", q+"-w"); err != nil {
		t.Fatal(err)
	}
}
