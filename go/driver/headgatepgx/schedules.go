package headgatepgx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	headgate "github.com/mujhtech/headgate/go"
)

// ---------- surveyed policy behavior periodic schedules ----------

const scheduleCols = `id, kind, payload, queue, partition_key, rate_class, priority,
	max_attempts, retention_ms, spec, next_run_ms, last_enqueued_ms, on_missed,
	backfill_limit, paused`

func scanSchedule(row pgx.Row) (headgate.ScheduleEntry, error) {
	var e headgate.ScheduleEntry
	var prio, maxAtt, backfill int32
	var onMissed string
	err := row.Scan(&e.ID, &e.Kind, &e.Payload, &e.Queue, &e.PartitionKey, &e.RateClass,
		&prio, &maxAtt, &e.RetentionMs, &e.Spec, &e.NextRunMs, &e.LastEnqueued,
		&onMissed, &backfill, &e.Paused)
	if err != nil {
		return e, err
	}
	e.Priority = prio
	e.MaxAttempts = uint32(maxAtt)
	e.BackfillLimit = uint32(backfill)
	switch onMissed {
	case "run_once":
		e.OnMissed = headgate.MissedRunOnce
	case "backfill":
		e.OnMissed = headgate.MissedBackfill
	default:
		e.OnMissed = headgate.MissedSkip
	}
	return e, nil
}

func missedName(p headgate.MissedPolicy) string {
	return p.String()
}

// UpsertSchedule creates or updates a periodic schedule.
func (s *PgxStore) UpsertSchedule(ctx context.Context, e headgate.ScheduleEntry) error {
	if e.Payload == nil {
		e.Payload = []byte{} // nil would write SQL NULL into a NOT NULL column
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_schedule AS d
		       (id, kind, payload, queue, partition_key, rate_class, priority,
		        max_attempts, retention_ms, spec, next_run_ms, on_missed,
		        backfill_limit, paused, updated_at_ms)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, `+nowMS+`
		ON CONFLICT (id) DO UPDATE SET
		  kind = EXCLUDED.kind, payload = EXCLUDED.payload, queue = EXCLUDED.queue,
		  partition_key = EXCLUDED.partition_key, rate_class = EXCLUDED.rate_class,
		  priority = EXCLUDED.priority, max_attempts = EXCLUDED.max_attempts,
		  retention_ms = EXCLUDED.retention_ms, spec = EXCLUDED.spec,
		  next_run_ms = CASE WHEN d.spec = EXCLUDED.spec
		                     THEN d.next_run_ms ELSE EXCLUDED.next_run_ms END,
		  on_missed = EXCLUDED.on_missed, backfill_limit = EXCLUDED.backfill_limit,
		  paused = EXCLUDED.paused, updated_at_ms = EXCLUDED.updated_at_ms`,
		e.ID, e.Kind, e.Payload, e.Queue, e.PartitionKey, e.RateClass, e.Priority,
		int32(e.MaxAttempts), e.RetentionMs, e.Spec, e.NextRunMs, missedName(e.OnMissed),
		int32(e.BackfillLimit), e.Paused)
	return err
}

// DeleteSchedule removes a periodic schedule.
func (s *PgxStore) DeleteSchedule(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM headgate_schedule WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return headgate.NotFoundf("schedule %s", id)
	}
	return nil
}

// ListSchedules returns all periodic schedules.
func (s *PgxStore) ListSchedules(ctx context.Context) ([]headgate.ScheduleEntry, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+scheduleCols+` FROM headgate_schedule ORDER BY id LIMIT 10000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.ScheduleEntry
	for rows.Next() {
		e, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DueSchedules returns schedules due according to the store clock.
func (s *PgxStore) DueSchedules(ctx context.Context, limit int64) ([]headgate.ScheduleEntry, int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+scheduleCols+`, `+nowMS+` AS now_ms FROM headgate_schedule
		WHERE NOT paused AND next_run_ms <= `+nowMS+`
		ORDER BY next_run_ms LIMIT $1`, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []headgate.ScheduleEntry
	var now int64
	for rows.Next() {
		var e headgate.ScheduleEntry
		var prio, maxAtt, backfill int32
		var onMissed string
		if err := rows.Scan(&e.ID, &e.Kind, &e.Payload, &e.Queue, &e.PartitionKey,
			&e.RateClass, &prio, &maxAtt, &e.RetentionMs, &e.Spec, &e.NextRunMs,
			&e.LastEnqueued, &onMissed, &backfill, &e.Paused, &now); err != nil {
			return nil, 0, err
		}
		e.Priority, e.MaxAttempts, e.BackfillLimit = prio, uint32(maxAtt), uint32(backfill)
		switch onMissed {
		case "run_once":
			e.OnMissed = headgate.MissedRunOnce
		case "backfill":
			e.OnMissed = headgate.MissedBackfill
		}
		out = append(out, e)
	}
	return out, now, rows.Err()
}

// AdvanceSchedule conditionally moves a schedule to its next run time.
func (s *PgxStore) AdvanceSchedule(ctx context.Context, id string, fromNextRunMs, toNextRunMs int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE headgate_schedule
		SET next_run_ms = $3, last_enqueued_ms = `+nowMS+`
		WHERE id = $1 AND next_run_ms = $2`, id, fromNextRunMs, toNextRunMs)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// RecordScheduleEvent appends an event to a periodic schedule's history.
func (s *PgxStore) RecordScheduleEvent(ctx context.Context, event headgate.ScheduleEvent) error {
	if !event.Outcome.Valid() {
		return headgate.Invalidf("invalid schedule event outcome")
	}
	if len(event.Reason) > 64 {
		return headgate.Invalidf("schedule event reason exceeds 64 bytes")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning schedule event transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize racing scheduler attempts before append-and-trim. Otherwise their
	// READ COMMITTED snapshots can each miss the other's insert and leave 101 rows.
	var lockedSchedule string
	err = tx.QueryRow(ctx, `SELECT id FROM headgate_schedule WHERE id = $1 FOR UPDATE`, event.ScheduleID).Scan(&lockedSchedule)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("locking schedule for event retention: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO headgate_schedule_event
		       (schedule_id, tick_ms, job_id, outcome, reason, recorded_at_ms)
		SELECT $1, $2, $3, $4, $5, `+nowMS,
		event.ScheduleID, event.TickMs, event.JobID, string(event.Outcome), event.Reason)
	if err != nil {
		return fmt.Errorf("inserting schedule event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		DELETE FROM headgate_schedule_event
		WHERE schedule_id = $1 AND id NOT IN (
		  SELECT id FROM headgate_schedule_event WHERE schedule_id = $1
		  ORDER BY id DESC LIMIT $2
		)`, event.ScheduleID, int64(headgate.ScheduleEventLimit))
	if err != nil {
		return fmt.Errorf("trimming schedule events: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing schedule event: %w", err)
	}
	return nil
}

// ListScheduleEvents returns a newest-first page of events for scheduleID.
func (s *PgxStore) ListScheduleEvents(ctx context.Context, scheduleID string, beforeEventID uint64, limit uint32) ([]headgate.ScheduleEvent, error) {
	if err := headgate.ValidateScheduleEventLimit(limit); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, schedule_id, tick_ms, job_id, outcome, reason, recorded_at_ms
		FROM headgate_schedule_event WHERE schedule_id = $1
		  AND ($2 = 0 OR id < $2)
		ORDER BY id DESC LIMIT $3`, scheduleID, int64(beforeEventID), int64(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.ScheduleEvent
	for rows.Next() {
		var event headgate.ScheduleEvent
		var eventID int64
		if err := rows.Scan(&eventID, &event.ScheduleID, &event.TickMs, &event.JobID,
			&event.Outcome, &event.Reason, &event.RecordedAtMs); err != nil {
			return nil, err
		}
		event.EventID = uint64(eventID)
		out = append(out, event)
	}
	return out, rows.Err()
}

// AppendDurableEvent stores an event once per idempotency key.
func (s *PgxStore) AppendDurableEvent(ctx context.Context, event headgate.DurableEvent) (headgate.DurableEvent, bool, error) {
	if err := validateDurableEvent(event); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return headgate.DurableEvent{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `INSERT INTO headgate_durable_event_scope(scope) VALUES($1) ON CONFLICT DO NOTHING`, event.Scope); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	var locked string
	if err = tx.QueryRow(ctx, `SELECT scope FROM headgate_durable_event_scope WHERE scope=$1 FOR UPDATE`, event.Scope).Scan(&locked); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	var id uint64
	var recorded int64
	err = tx.QueryRow(ctx, `INSERT INTO headgate_durable_event(scope,topic,idempotency_key,payload,source,recorded_at_ms) VALUES($1,$2,$3,$4,$5,`+nowMS+`) ON CONFLICT(scope,idempotency_key) DO NOTHING RETURNING id,recorded_at_ms`, event.Scope, event.Topic, event.IdempotencyKey, event.Payload, event.Source).Scan(&id, &recorded)
	inserted := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		var topic string
		var payload, source []byte
		if err = tx.QueryRow(ctx, `SELECT id,topic,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=$1 AND idempotency_key=$2`, event.Scope, event.IdempotencyKey).Scan(&id, &topic, &payload, &source, &recorded); err != nil {
			return headgate.DurableEvent{}, false, err
		}
		if topic != event.Topic || !bytes.Equal(payload, event.Payload) || !bytes.Equal(source, event.Source) {
			return headgate.DurableEvent{}, false, &headgate.InvalidError{Msg: "durable event idempotency key was reused with different content"}
		}
	} else if err != nil {
		return headgate.DurableEvent{}, false, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM headgate_durable_event WHERE scope=$1 AND id NOT IN (SELECT id FROM headgate_durable_event WHERE scope=$1 ORDER BY id DESC LIMIT $2)`, event.Scope, headgate.DurableEventLimit); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	event.EventID, event.RecordedAtMs = id, recorded
	return event, inserted, nil
}

// ListDurableEvents returns a newest-first page of events in scope.
func (s *PgxStore) ListDurableEvents(ctx context.Context, scope string, beforeEventID uint64, limit uint32) ([]headgate.DurableEvent, error) {
	if err := headgate.ValidateDurableEventLimit(limit); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT id,scope,topic,idempotency_key,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=$1 AND ($2=0 OR id<$2) ORDER BY id DESC LIMIT $3`, scope, beforeEventID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]headgate.DurableEvent, 0, limit)
	for rows.Next() {
		var event headgate.DurableEvent
		if err := rows.Scan(&event.EventID, &event.Scope, &event.Topic, &event.IdempotencyKey, &event.Payload, &event.Source, &event.RecordedAtMs); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func validateDurableEvent(event headgate.DurableEvent) error {
	if event.Scope == "" || len(event.Scope) > 512 || event.Topic == "" || len(event.Topic) > 255 || event.IdempotencyKey == "" || len(event.IdempotencyKey) > 255 {
		return &headgate.InvalidError{Msg: "durable event scope, topic, or idempotency key is invalid"}
	}
	if len(event.Payload) > headgate.MaxDurableEventPayloadBytes || !json.Valid(event.Payload) {
		return &headgate.InvalidError{Msg: "durable event payload must be valid JSON of at most 65536 bytes"}
	}
	if len(event.Source) > headgate.MaxDurableEventSourceBytes || !json.Valid(event.Source) {
		return &headgate.InvalidError{Msg: "durable event source must be valid JSON of at most 16384 bytes"}
	}
	return nil
}
