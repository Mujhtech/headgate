package headgateapi

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

func scheduleJSON(s headgate.ScheduleEntry) map[string]any {
	var last any
	if s.LastEnqueued != nil {
		last = *s.LastEnqueued
	}
	onMissed := "skip"
	switch s.OnMissed {
	case headgate.MissedRunOnce:
		onMissed = "run_once"
	case headgate.MissedBackfill:
		onMissed = "backfill"
	}
	return map[string]any{
		"id": s.ID, "kind": s.Kind, "queue": s.Queue, "spec": s.Spec,
		"next_run_ms": s.NextRunMs, "last_enqueued_ms": last, "on_missed": onMissed,
		"backfill_limit": s.BackfillLimit, "paused": s.Paused,
		"partition_key": s.PartitionKey, "rate_class": s.RateClass,
		"priority": s.Priority, "max_attempts": s.MaxAttempts, "retention_ms": s.RetentionMs,
	}
}

func (a *api) listPeriodic(w http.ResponseWriter, r *http.Request) {
	ss, err := a.store.ListSchedules(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	start, end, ok := controlPage(w, r, len(ss))
	if !ok {
		return
	}
	out := []map[string]any{}
	for _, s := range ss[start:end] {
		out = append(out, scheduleJSON(s))
	}
	writeJSON(w, 200, out)
}

func (a *api) periodicEvents(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryUint32(w, r, "limit", 30)
	if !ok {
		return
	}
	cursor, ok := queryUint64(w, r, "cursor", 0)
	if !ok {
		return
	}
	events, err := a.store.ListScheduleEvents(r.Context(), r.PathValue("id"), cursor, limit)
	if err != nil {
		storeErr(w, err)
		return
	}
	out := make([]map[string]any, 0, len(events))
	for _, event := range events {
		out = append(out, map[string]any{
			"event_id":       event.EventID,
			"schedule_id":    event.ScheduleID,
			"tick_ms":        event.TickMs,
			"job_id":         event.JobID,
			"outcome":        event.Outcome,
			"reason":         event.Reason,
			"recorded_at_ms": event.RecordedAtMs,
		})
	}
	var nextCursor any
	if len(events) == int(limit) {
		nextCursor = events[len(events)-1].EventID
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out, "next_cursor": nextCursor})
}

func (a *api) putPeriodic(w http.ResponseWriter, r *http.Request) {
	// `queue` and `max_attempts` are pointers because their Rust defaults are NOT the
	// zero value — `unwrap_or("default")` and `unwrap_or(25)`. Reading them as values
	// made `{"queue":""}` mean "default" in Go and "" in Rust, and `{"max_attempts":0}`
	// mean 25 in Go and 0 in Rust: an explicit "never retry" silently became 25 tries.
	var b struct {
		Kind          string  `json:"kind"`
		Spec          string  `json:"spec"`
		Payload       *string `json:"payload"`
		Queue         *string `json:"queue"`
		PartitionKey  string  `json:"partition_key"`
		RateClass     string  `json:"rate_class"`
		Priority      int32   `json:"priority"`
		MaxAttempts   *uint32 `json:"max_attempts"`
		RetentionMs   int64   `json:"retention_ms"`
		OnMissed      *string `json:"on_missed"`
		BackfillLimit uint32  `json:"backfill_limit"`
		Paused        bool    `json:"paused"`
	}
	// kind and spec are REQUIRED. A body with only `spec` used to create a schedule
	// whose kind was "", i.e. a periodic entry that enqueues jobs no worker can
	// dispatch. 200, no signal.
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "kind", "spec") {
		return
	}
	payload := []byte{}
	if b.Payload != nil {
		var err error
		if payload, err = base64.StdEncoding.DecodeString(*b.Payload); err != nil {
			errJSON(w, http.StatusBadRequest, "payload must be base64")
			return
		}
	}
	// ABSENT means skip; an explicit "" does NOT. Rust parses `Some("")` and fails, so
	// Go's `case "", "skip":` silently accepted a value Rust rejects.
	onMissed := headgate.MissedSkip
	if b.OnMissed != nil {
		switch *b.OnMissed {
		case "skip":
		case "run_once":
			onMissed = headgate.MissedRunOnce
		case "backfill":
			onMissed = headgate.MissedBackfill
		default:
			errJSON(w, http.StatusBadRequest, "on_missed must be skip|run_once|backfill")
			return
		}
	}
	// "@every:<ms>" and cron both validate here; tick identity is pinned against Rust
	// by conformance/cron_ticks.json (see cronspec.go).
	nextRun, err := headgate.ScheduleNextAfter(b.Spec, time.Now().UnixMilli())
	if err != nil {
		errJSON(w, http.StatusBadRequest, strings.TrimPrefix(err.Error(), "headgate: "))
		return
	}
	queue := "default"
	if b.Queue != nil {
		queue = *b.Queue
	}
	maxAttempts := uint32(25)
	if b.MaxAttempts != nil {
		maxAttempts = *b.MaxAttempts
	}
	schedule := headgate.ScheduleEntry{
		ID: r.PathValue("id"), Kind: b.Kind, Payload: payload, Queue: queue,
		PartitionKey: b.PartitionKey, RateClass: b.RateClass, Priority: b.Priority,
		MaxAttempts: maxAttempts, RetentionMs: b.RetentionMs, Spec: b.Spec,
		NextRunMs: nextRun, OnMissed: onMissed, BackfillLimit: b.BackfillLimit,
		Paused: b.Paused,
	}
	preview := headgate.Envelope{
		ID: "schedule:" + schedule.ID, Kind: schedule.Kind, Payload: schedule.Payload,
		Fingerprint: headgate.Fingerprint(schedule.Kind, schedule.Payload), Queue: schedule.Queue,
		PartitionKey: schedule.PartitionKey, RateClass: schedule.RateClass,
		Priority: schedule.Priority, MaxAttempts: schedule.MaxAttempts,
		RetentionMs: schedule.RetentionMs,
	}
	if !a.authorizeEnqueue(w, r, []headgate.Envelope{preview}) {
		return
	}
	err = a.store.UpsertSchedule(r.Context(), schedule)
	if err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) deletePeriodic(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteSchedule(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) runPeriodic(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ss, err := a.store.ListSchedules(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	var sched *headgate.ScheduleEntry
	for i := range ss {
		if ss[i].ID == id {
			sched = &ss[i]
			break
		}
	}
	if sched == nil {
		errJSON(w, http.StatusNotFound, "no such schedule")
		return
	}
	jobID := a.genID()
	idem := r.Header.Get("Idempotency-Key")
	env := headgate.Envelope{
		ID: jobID, Kind: sched.Kind,
		Fingerprint: headgate.Fingerprint(sched.Kind, sched.Payload),
		Payload:     sched.Payload, Queue: sched.Queue, PartitionKey: sched.PartitionKey,
		RateClass: sched.RateClass, Priority: sched.Priority,
		MaxAttempts: sched.MaxAttempts, RetentionMs: sched.RetentionMs,
		UniqueKey: []byte("schedrun:" + id + ":" + idem),
	}
	err = a.producer.EnqueueWithSource(
		r.Context(), headgate.EnqueueSourceHTTP, []headgate.Envelope{env},
	)
	var dup *headgate.DuplicateError
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]any{"id": jobID})
	case errors.As(err, &dup):
		writeJSON(w, http.StatusAccepted, map[string]any{"id": dup.ExistingID, "replayed": true})
	default:
		enqueueClientErr(w, err)
	}
}

// workerStaleMs is the stale-aging rule, defined ONCE: 15 minutes of heartbeat grace.
// GET /workers and GET /cluster must agree about which workers are live, or the cluster
// view contradicts the list it summarizes.
const workerStaleMs = 900_000

// workerAllMs is the window meaning "every worker the registry still remembers" —
// 10,000 years, which is not math.MaxInt64 because the SQL adapters compute now_ms - ?
// and that would overflow BIGINT. Live + stale = this; stale is the difference.
const workerAllMs = 315_576_000_000_000
