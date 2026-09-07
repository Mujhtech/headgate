package headgatemysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
)

// ---------- surveyed policy behavior periodic schedules ----------

// scheduleCols is the ONE deliberate divergence from the Rust adapter's `SELECT *`:
// database/sql scans POSITIONALLY, so a column added to the table would silently shift
// every field. The column list is the same set the Rust row-by-name reader consumes.
const scheduleCols = `id, kind, payload, queue, partition_key, rate_class, priority,
	max_attempts, retention_ms, spec, next_run_ms, last_enqueued_ms, on_missed,
	backfill_limit, paused`

func scanSchedule(row rowScanner, extra ...any) (headgate.ScheduleEntry, error) {
	var e headgate.ScheduleEntry
	var prio, maxAtt, backfill int64
	var onMissed string
	var lastEnqueued sql.NullInt64
	dest := []any{&e.ID, &e.Kind, &e.Payload, &e.Queue, &e.PartitionKey, &e.RateClass,
		&prio, &maxAtt, &e.RetentionMs, &e.Spec, &e.NextRunMs, &lastEnqueued,
		&onMissed, &backfill, &e.Paused}
	dest = append(dest, extra...)
	if err := row.Scan(dest...); err != nil {
		return e, err
	}
	e.Priority = int32(prio)
	e.MaxAttempts = uint32(maxAtt)
	e.BackfillLimit = uint32(backfill)
	if lastEnqueued.Valid {
		v := lastEnqueued.Int64
		e.LastEnqueued = &v
	}
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
func (s *MysqlStore) UpsertSchedule(ctx context.Context, e headgate.ScheduleEntry) error {
	if e.Payload == nil {
		e.Payload = []byte{} // nil would write SQL NULL into a NOT NULL column
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_schedule
		       (id, kind, payload, queue, partition_key, rate_class, priority,
		        max_attempts, retention_ms, spec, next_run_ms, on_missed,
		        backfill_limit, paused, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, `+nowMS+`) AS new
		ON DUPLICATE KEY UPDATE
		  kind = new.kind, payload = new.payload, queue = new.queue,
		  partition_key = new.partition_key, rate_class = new.rate_class,
		  priority = new.priority, max_attempts = new.max_attempts,
		  retention_ms = new.retention_ms,
		  -- Idempotent (BullMQ): an unchanged spec keeps its phase; only a NEW spec
		  -- resets next_run. Compare BEFORE spec is overwritten.
		  next_run_ms = IF(headgate_schedule.spec = new.spec,
		                   headgate_schedule.next_run_ms, new.next_run_ms),
		  spec = new.spec,
		  on_missed = new.on_missed, backfill_limit = new.backfill_limit,
		  paused = new.paused, updated_at_ms = new.updated_at_ms`,
		e.ID, e.Kind, e.Payload, e.Queue, e.PartitionKey, e.RateClass, int64(e.Priority),
		int64(e.MaxAttempts), e.RetentionMs, e.Spec, e.NextRunMs, missedName(e.OnMissed),
		int64(e.BackfillLimit), e.Paused)
	return err
}

// DeleteSchedule removes a periodic schedule.
func (s *MysqlStore) DeleteSchedule(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM headgate_schedule WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return headgate.NotFoundf("schedule %s", id)
	}
	return nil
}

// ListSchedules returns all periodic schedules.
func (s *MysqlStore) ListSchedules(ctx context.Context) ([]headgate.ScheduleEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+scheduleCols+` FROM headgate_schedule ORDER BY id LIMIT 10000`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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
func (s *MysqlStore) DueSchedules(ctx context.Context, limit int64) ([]headgate.ScheduleEntry, int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+scheduleCols+`, `+nowMS+` AS now_ms FROM headgate_schedule
		WHERE NOT paused AND next_run_ms <= `+nowMS+`
		ORDER BY next_run_ms LIMIT ?`, limit)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.ScheduleEntry
	var now int64
	for rows.Next() {
		e, err := scanSchedule(rows, &now)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, e)
	}
	return out, now, rows.Err()
}

// AdvanceSchedule conditionally moves a schedule to its next run time.
func (s *MysqlStore) AdvanceSchedule(ctx context.Context, id string, fromNextRunMs, toNextRunMs int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE headgate_schedule
		SET next_run_ms = ?, last_enqueued_ms = `+nowMS+`
		WHERE id = ? AND next_run_ms = ?`, toNextRunMs, id, fromNextRunMs)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// RecordScheduleEvent appends an event to a periodic schedule's history.
func (s *MysqlStore) RecordScheduleEvent(ctx context.Context, event headgate.ScheduleEvent) error {
	if !event.Outcome.Valid() {
		return headgate.Invalidf("invalid schedule event outcome")
	}
	if len(event.Reason) > 64 {
		return headgate.Invalidf("schedule event reason exceeds 64 bytes")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning schedule event transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Serialize racing scheduler attempts before append-and-trim. Otherwise their
	// READ COMMITTED snapshots can each miss the other's insert and leave 101 rows.
	var lockedSchedule string
	err = tx.QueryRowContext(ctx, `SELECT id FROM headgate_schedule WHERE id = ? FOR UPDATE`, event.ScheduleID).Scan(&lockedSchedule)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("locking schedule for event retention: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO headgate_schedule_event
		       (schedule_id, tick_ms, job_id, outcome, reason, recorded_at_ms)
		VALUES (?, ?, ?, ?, ?, `+nowMS+`)`, event.ScheduleID, event.TickMs,
		event.JobID, string(event.Outcome), event.Reason)
	if err != nil {
		return fmt.Errorf("inserting schedule event: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		DELETE e FROM headgate_schedule_event e
		LEFT JOIN (
		  SELECT id FROM headgate_schedule_event WHERE schedule_id = ?
		  ORDER BY id DESC LIMIT ?
		) keep ON keep.id = e.id
		WHERE e.schedule_id = ? AND keep.id IS NULL`, event.ScheduleID,
		headgate.ScheduleEventLimit, event.ScheduleID)
	if err != nil {
		return fmt.Errorf("trimming schedule events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing schedule event: %w", err)
	}
	return nil
}

// ListScheduleEvents returns a newest-first page of events for scheduleID.
func (s *MysqlStore) ListScheduleEvents(ctx context.Context, scheduleID string, beforeEventID uint64, limit uint32) ([]headgate.ScheduleEvent, error) {
	if err := headgate.ValidateScheduleEventLimit(limit); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, schedule_id, tick_ms, job_id, CAST(outcome AS CHAR), reason, recorded_at_ms
		FROM headgate_schedule_event WHERE schedule_id = ?
		  AND (? = 0 OR id < ?)
		ORDER BY id DESC LIMIT ?`, scheduleID, beforeEventID, beforeEventID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.ScheduleEvent
	for rows.Next() {
		var event headgate.ScheduleEvent
		if err := rows.Scan(&event.EventID, &event.ScheduleID, &event.TickMs, &event.JobID,
			&event.Outcome, &event.Reason, &event.RecordedAtMs); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

// AppendDurableEvent stores an event once per idempotency key.
func (s *MysqlStore) AppendDurableEvent(ctx context.Context, event headgate.DurableEvent) (headgate.DurableEvent, bool, error) {
	if err := validateDurableEvent(event); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return headgate.DurableEvent{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `INSERT IGNORE INTO headgate_durable_event_scope(scope) VALUES(?)`, event.Scope); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	var locked string
	if err = tx.QueryRowContext(ctx, `SELECT scope FROM headgate_durable_event_scope WHERE scope=? FOR UPDATE`, event.Scope).Scan(&locked); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	var existingID uint64
	err = tx.QueryRowContext(ctx, `SELECT id FROM headgate_durable_event WHERE scope=? AND idempotency_key=?`, event.Scope, event.IdempotencyKey).Scan(&existingID)
	inserted := errors.Is(err, sql.ErrNoRows)
	if err != nil && !inserted {
		return headgate.DurableEvent{}, false, err
	}
	if inserted {
		if _, err = tx.ExecContext(ctx, `INSERT INTO headgate_durable_event(scope,topic,idempotency_key,payload,source,recorded_at_ms) VALUES(?,?,?,?,?,`+nowMS+`)`, event.Scope, event.Topic, event.IdempotencyKey, event.Payload, event.Source); err != nil {
			return headgate.DurableEvent{}, false, err
		}
	}
	var topic string
	var payload, source []byte
	if err = tx.QueryRowContext(ctx, `SELECT id,topic,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=? AND idempotency_key=?`, event.Scope, event.IdempotencyKey).Scan(&event.EventID, &topic, &payload, &source, &event.RecordedAtMs); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	if topic != event.Topic || !bytes.Equal(payload, event.Payload) || !bytes.Equal(source, event.Source) {
		return headgate.DurableEvent{}, false, &headgate.InvalidError{Msg: "durable event idempotency key was reused with different content"}
	}
	if _, err = tx.ExecContext(ctx, `DELETE e FROM headgate_durable_event e LEFT JOIN (SELECT id FROM headgate_durable_event WHERE scope=? ORDER BY id DESC LIMIT ?) keep ON keep.id=e.id WHERE e.scope=? AND keep.id IS NULL`, event.Scope, headgate.DurableEventLimit, event.Scope); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	return event, inserted, nil
}

// ListDurableEvents returns a newest-first page of events in scope.
func (s *MysqlStore) ListDurableEvents(ctx context.Context, scope string, beforeEventID uint64, limit uint32) ([]headgate.DurableEvent, error) {
	if err := headgate.ValidateDurableEventLimit(limit); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,scope,topic,idempotency_key,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=? AND (?=0 OR id<?) ORDER BY id DESC LIMIT ?`, scope, beforeEventID, beforeEventID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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
