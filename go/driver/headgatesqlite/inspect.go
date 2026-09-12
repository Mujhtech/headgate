package headgatesqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

var _ headgate.InspectStore = (*SqliteStore)(nil)
var _ headgate.PendingScheduleStore = (*SqliteStore)(nil)
var _ headgate.DurableEventStore = (*SqliteStore)(nil)

const summaryColumns = `id,kind,queue,state,schema_version,priority,attempt,crash_attempt,max_attempts,
	partition_key,rate_class,sticky_worker,weight,fingerprint,enqueued_at_ms,scheduled_at_ms,
	claimed_at_ms,periodic_schedule_id,periodic_tick_ms,finalized_at_ms,payload,headers_json,errors_json,tags_json`

func scanSummary(row interface{ Scan(...any) error }, includePayload bool) (*headgate.JobSummary, error) {
	var j headgate.JobSummary
	var schema, attempt, crash, maxAttempts, weight int64
	var claimed, finalized sql.NullInt64
	var payload []byte
	var headers, tags sql.NullString
	if err := row.Scan(&j.ID, &j.Kind, &j.Queue, &j.State, &schema, &j.Priority, &attempt, &crash, &maxAttempts,
		&j.PartitionKey, &j.RateClass, &j.StickyWorker, &weight, &j.Fingerprint, &j.EnqueuedAtMs, &j.ScheduledAtMs,
		&claimed, &j.PeriodicScheduleID, &j.PeriodicTickMs, &finalized, &payload, &headers, &j.ErrorsJSON, &tags); err != nil {
		return nil, err
	}
	j.SchemaVersion = uint32(schema)
	j.Attempt, j.CrashAttempt, j.MaxAttempts, j.Weight = uint32(attempt), uint32(crash), uint32(maxAttempts), uint32(weight)
	if claimed.Valid {
		j.ClaimedAtMs = &claimed.Int64
	}
	if finalized.Valid {
		j.FinalizedAtMs = &finalized.Int64
	}
	if includePayload {
		j.Payload = append([]byte(nil), payload...)
		j.Headers = headgate.DecodeHeaders([]byte(headers.String))
	}
	_ = json.Unmarshal([]byte(tags.String), &j.Tags)
	return &j, nil
}

func (s *SqliteStore) GetJob(ctx context.Context, id string, includePayload bool) (*headgate.JobSummary, error) {
	j, err := scanSummary(s.db.QueryRowContext(ctx, `SELECT `+summaryColumns+` FROM headgate_job WHERE id=?`, id), includePayload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading SQLite job: %w", err)
	}
	return j, nil
}

func (s *SqliteStore) ListJobs(ctx context.Context, f headgate.JobFilter, cursor string, limit uint32) (headgate.JobPage, error) {
	if limit == 0 || limit > 100 {
		return headgate.JobPage{}, &headgate.InvalidError{Msg: "limit must be between 1 and 100"}
	}
	where := []string{"1=1"}
	args := []any{}
	add := func(col string, v *string) {
		if v != nil {
			where = append(where, col+"=?")
			args = append(args, *v)
		}
	}
	add("queue", f.Queue)
	add("state", f.State)
	add("kind", f.Kind)
	add("partition_key", f.PartitionKey)
	add("id", f.ID)
	add("fingerprint", f.Fingerprint)
	add("rate_class", f.RateClass)
	if f.KindPrefix != nil {
		where = append(where, "kind LIKE ?")
		args = append(args, *f.KindPrefix+"%")
	}
	if f.Priority != nil {
		where = append(where, "priority=?")
		args = append(args, *f.Priority)
	}
	for _, tag := range f.TagsAll {
		where = append(where, "EXISTS(SELECT 1 FROM json_each(tags_json) WHERE value=?)")
		args = append(args, tag)
	}
	if len(f.TagsAny) > 0 {
		where = append(where, "EXISTS(SELECT 1 FROM json_each(tags_json) WHERE value IN ("+placeholders(len(f.TagsAny))+"))")
		for _, tag := range f.TagsAny {
			args = append(args, tag)
		}
	}
	if cursor != "" {
		where = append(where, "id>?")
		args = append(args, cursor)
	}
	args = append(args, int64(limit)+1)
	rows, err := s.db.QueryContext(ctx, `SELECT `+summaryColumns+` FROM headgate_job WHERE `+strings.Join(where, " AND ")+` ORDER BY id LIMIT ?`, args...)
	if err != nil {
		return headgate.JobPage{}, err
	}
	defer rows.Close()
	page := headgate.JobPage{}
	for rows.Next() {
		j, err := scanSummary(rows, false)
		if err != nil {
			return page, err
		}
		page.Jobs = append(page.Jobs, *j)
	}
	if err := rows.Err(); err != nil {
		return page, err
	}
	if len(page.Jobs) > int(limit) {
		page.NextCursor = page.Jobs[limit-1].ID
		page.Jobs = page.Jobs[:limit]
	}
	return page, nil
}

func (s *SqliteStore) Counts(ctx context.Context, queue *string) (headgate.StateCounts, error) {
	args := []any{}
	where := ""
	if queue != nil {
		where = " WHERE queue=?"
		args = append(args, *queue)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT state,count(*) FROM (SELECT state FROM headgate_job`+where+` LIMIT 10001) GROUP BY state`, args...)
	if err != nil {
		return headgate.StateCounts{}, err
	}
	defer rows.Close()
	out := headgate.StateCounts{Counts: map[string]int64{}}
	var total int64
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			return out, err
		}
		out.Counts[state] = n
		total += n
	}
	out.Approximate = total > 10000
	return out, rows.Err()
}

func (s *SqliteStore) QueueStats(ctx context.Context) ([]headgate.QueueStatsView, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT q.queue,COALESCE(qs.weight,1),COALESCE(qs.paused,0),ep.max_unfinished_jobs,qs.memory_bytes FROM (SELECT queue FROM headgate_queue_state UNION SELECT queue FROM headgate_job UNION SELECT queue FROM headgate_enqueue_policy) q LEFT JOIN headgate_queue_state qs USING(queue) LEFT JOIN headgate_enqueue_policy ep USING(queue) ORDER BY q.queue LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	var out []headgate.QueueStatsView
	for rows.Next() {
		var v headgate.QueueStatsView
		var weight int64
		var max, memory sql.NullInt64
		if err := rows.Scan(&v.Queue, &weight, &v.Paused, &max, &memory); err != nil {
			return nil, err
		}
		v.Weight = uint32(weight)
		if max.Valid {
			x := uint64(max.Int64)
			v.MaxUnfinishedJobs = &x
		}
		if memory.Valid {
			x := uint64(memory.Int64)
			v.MemoryBytes = &x
		}
		out = append(out, v)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var now int64
	if err := s.db.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return nil, err
	}
	for i := range out {
		v := &out[i]
		counts, err := s.Counts(ctx, &v.Queue)
		if err != nil {
			return nil, err
		}
		v.ByState, v.CountsApproximate = counts.Counts, counts.Approximate
		for state, n := range counts.Counts {
			if state == "pending" || state == "scheduled" || state == "available" || state == "running" || state == "retryable" {
				v.UnfinishedJobs += uint64(n)
			}
		}
		var arrived, completed int64
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(arrived),0),COALESCE(sum(completed),0) FROM headgate_queue_counter WHERE queue=? AND bucket_ms>=?`, v.Queue, now-60_000).Scan(&arrived, &completed); err != nil {
			return nil, err
		}
		v.ArrivalRate = float64(arrived) / 60
		v.DrainRate = float64(completed) / 60
		v.TimeToDrainMs = headgate.TimeToDrainMillis(int64(v.UnfinishedJobs), v.ArrivalRate, v.DrainRate)
		var oldest sql.NullInt64
		if err := s.db.QueryRowContext(ctx, `SELECT min(enqueued_at_ms) FROM (SELECT enqueued_at_ms FROM headgate_job WHERE queue=? AND state='available' LIMIT 10000)`, v.Queue).Scan(&oldest); err != nil {
			return nil, err
		}
		if oldest.Valid {
			x := headgate.AgeMillis(now, oldest.Int64)
			v.OldestAvailableMs = &x
		}
		v.QuietGroups = headgate.QuietGroupMetrics{ArrivalRate: v.ArrivalRate, DrainRate: v.DrainRate, TimeToDrainMs: v.TimeToDrainMs, OldestAvailableMs: v.OldestAvailableMs}
	}
	return out, nil
}

func (s *SqliteStore) QuarantineList(ctx context.Context) ([]headgate.QuarantineEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT q.fingerprint,COALESCE((SELECT kind FROM headgate_job j WHERE j.fingerprint=q.fingerprint LIMIT 1),''),q.crash_count,q.quarantined_at_ms,'' FROM headgate_quarantine q ORDER BY q.quarantined_at_ms DESC LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.QuarantineEntry
	for rows.Next() {
		var v headgate.QuarantineEntry
		if err := rows.Scan(&v.Fingerprint, &v.Kind, &v.CrashCount, &v.QuarantinedAtMs, &v.Reason); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SqliteStore) QuarantineRelease(ctx context.Context, fingerprint string) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state='available',scheduled_at_ms=`+nowMS+`,finalized_at_ms=NULL WHERE fingerprint=? AND state='quarantined'`, fingerprint)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	del, err := tx.ExecContext(ctx, `DELETE FROM headgate_quarantine WHERE fingerprint=?`, fingerprint)
	if err != nil {
		return 0, err
	}
	dn, _ := del.RowsAffected()
	if n == 0 && dn == 0 {
		return 0, headgate.NotFoundf("fingerprint %s is not quarantined", fingerprint)
	}
	return uint64(n), tx.Commit()
}

func (s *SqliteStore) transitionByID(ctx context.Context, id string, from []string, to string, scheduled any) error {
	q := `UPDATE headgate_job SET state=?,scheduled_at_ms=COALESCE(?,scheduled_at_ms),finalized_at_ms=CASE WHEN ?='cancelled' THEN ` + nowMS + ` ELSE NULL END,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL WHERE id=? AND state IN (` + placeholders(len(from)) + `)`
	args := []any{to, scheduled, to, id}
	for _, v := range from {
		args = append(args, v)
	}
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	var state string
	err = s.db.QueryRowContext(ctx, "SELECT state FROM headgate_job WHERE id=?", id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return headgate.NotFoundf("job %s", id)
	}
	if err != nil {
		return err
	}
	return headgate.Invalidf("transition to %s is not defined from %s", to, state)
}
func (s *SqliteStore) OperatorRetry(ctx context.Context, id string) error {
	return s.transitionByID(ctx, id, []string{"archived", "cancelled", "undecodable"}, "available", nil)
}
func (s *SqliteStore) OperatorCancel(ctx context.Context, id string) error {
	return s.transitionByID(ctx, id, []string{"pending", "scheduled", "available", "running", "retryable"}, "cancelled", nil)
}
func (s *SqliteStore) PromoteJob(ctx context.Context, id string) error {
	return s.transitionByID(ctx, id, []string{"pending"}, "available", nil)
}
func (s *SqliteStore) SchedulePendingJob(ctx context.Context, id string, atMs int64) error {
	return s.transitionPending(ctx, id, atMs)
}
func (s *SqliteStore) transitionPending(ctx context.Context, id string, atMs int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET state='scheduled',scheduled_at_ms=? WHERE id=? AND state='pending'`, atMs, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	return headgate.Invalidf("job %s is not pending", id)
}
func (s *SqliteStore) DeleteJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, "DELETE FROM headgate_job WHERE id=? AND state<>'running'", id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	var state string
	err = s.db.QueryRowContext(ctx, "SELECT state FROM headgate_job WHERE id=?", id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return headgate.NotFoundf("job %s", id)
	}
	if err != nil {
		return err
	}
	return &headgate.InvalidError{Msg: "cannot delete a running job; cancel it first"}
}

func (s *SqliteStore) ExplainAdmission(ctx context.Context, id string) (*headgate.AdmissionExplain, error) {
	var state, queue, rate, part, fingerprint string
	var scheduled, weight int64
	err := s.db.QueryRowContext(ctx, `SELECT state,queue,rate_class,partition_key,fingerprint,scheduled_at_ms,weight FROM headgate_job WHERE id=?`, id).Scan(&state, &queue, &rate, &part, &fingerprint, &scheduled, &weight)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var now int64
	_ = s.db.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now)
	var paused bool
	_ = s.db.QueryRowContext(ctx, `SELECT paused FROM headgate_queue_state WHERE queue=?`, queue).Scan(&paused)
	var quarantined int
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM headgate_quarantine WHERE fingerprint=?`, fingerprint).Scan(&quarantined)
	facts := headgateshared.AdmissionFacts{State: state, NowMs: now, ScheduledAtMs: scheduled, QueuePaused: paused, Quarantined: quarantined > 0}
	var tokens, burst, limit, window, refilled int64
	if err := s.db.QueryRowContext(ctx, `SELECT tokens,burst,limit_per_window,window_ms,refilled_at_ms FROM headgate_rate_bucket WHERE name=?`, rate).Scan(&tokens, &burst, &limit, &window, &refilled); err == nil {
		available := min(burst, tokens+max(int64(0), now-refilled)*limit/window)
		facts.TokensAvailable = &available
		facts.Weight = weight
		facts.LimitPerWindow = limit
		facts.WindowMs = window
	}
	var maxConcurrent int64
	if err := s.db.QueryRowContext(ctx, `SELECT max_concurrent FROM headgate_concurrency_limit WHERE queue=?`, queue).Scan(&maxConcurrent); err == nil {
		facts.MaxConcurrent = &maxConcurrent
		_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM headgate_job WHERE queue=? AND partition_key=? AND state='running'`, queue, part).Scan(&facts.Inflight)
	}
	return headgate.EvaluateAdmission(facts), nil
}

func (s *SqliteStore) History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]headgate.HistoryBucket, error) {
	if bucketMs < 1 {
		return nil, &headgate.InvalidError{Msg: "bucket_ms must be >= 1"}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT bucket_ms/?*?,sum(arrived),sum(completed) FROM headgate_queue_counter WHERE queue=? AND bucket_ms>=? GROUP BY 1 ORDER BY 1 LIMIT 10000`, bucketMs, bucketMs, queue, sinceMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.HistoryBucket
	for rows.Next() {
		var v headgate.HistoryBucket
		if err := rows.Scan(&v.AtMs, &v.Arrived, &v.Completed); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SqliteStore) QuarantineSweep(ctx context.Context, limit int64) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET state='quarantined',finalized_at_ms=`+nowMS+` WHERE id IN (SELECT j.id FROM headgate_job j JOIN headgate_quarantine q USING(fingerprint) WHERE j.state IN ('pending','scheduled','available','retryable') ORDER BY j.id LIMIT ?)`, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (s *SqliteStore) RescheduleJob(ctx context.Context, id string, atMs int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET scheduled_at_ms=? WHERE id=? AND state IN ('scheduled','retryable')`, atMs, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	return headgate.Invalidf("job %s is not scheduled or retryable", id)
}
func (s *SqliteStore) EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error {
	if schemaVersion == 0 || schemaVersion > headgate.MaxOpaqueSchemaVersion {
		return &headgate.InvalidError{Msg: "schema_version is outside the portable range"}
	}
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET payload=?,schema_version=?,fingerprint=? WHERE id=? AND state<>'running'`, payload, schemaVersion, fingerprint, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	return headgate.Invalidf("job %s cannot be edited while running or does not exist", id)
}

const scheduleColumns = `id,kind,payload,queue,partition_key,rate_class,priority,max_attempts,retention_ms,spec,next_run_ms,last_enqueued_ms,on_missed,backfill_limit,paused`

func scanSqliteSchedule(row interface{ Scan(...any) error }) (headgate.ScheduleEntry, error) {
	var v headgate.ScheduleEntry
	var priority, maxAttempts, backfill int64
	var last sql.NullInt64
	var missed string
	if err := row.Scan(&v.ID, &v.Kind, &v.Payload, &v.Queue, &v.PartitionKey, &v.RateClass, &priority, &maxAttempts, &v.RetentionMs, &v.Spec, &v.NextRunMs, &last, &missed, &backfill, &v.Paused); err != nil {
		return v, err
	}
	v.Priority = int32(priority)
	v.MaxAttempts = uint32(maxAttempts)
	v.BackfillLimit = uint32(backfill)
	if last.Valid {
		x := last.Int64
		v.LastEnqueued = &x
	}
	v.OnMissed, _ = headgateshared.ParseMissedPolicy(missed)
	return v, nil
}
func (s *SqliteStore) UpsertSchedule(ctx context.Context, v headgate.ScheduleEntry) error {
	if v.ID == "" || v.Kind == "" || v.Spec == "" {
		return &headgate.InvalidError{Msg: "schedule id, kind, and spec must not be empty"}
	}
	if v.Payload == nil {
		v.Payload = []byte{}
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO headgate_schedule(id,kind,payload,queue,partition_key,rate_class,priority,max_attempts,retention_ms,spec,next_run_ms,on_missed,backfill_limit,paused,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,`+nowMS+`) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,payload=excluded.payload,queue=excluded.queue,partition_key=excluded.partition_key,rate_class=excluded.rate_class,priority=excluded.priority,max_attempts=excluded.max_attempts,retention_ms=excluded.retention_ms,next_run_ms=CASE WHEN headgate_schedule.spec=excluded.spec THEN headgate_schedule.next_run_ms ELSE excluded.next_run_ms END,spec=excluded.spec,on_missed=excluded.on_missed,backfill_limit=excluded.backfill_limit,paused=excluded.paused,updated_at_ms=excluded.updated_at_ms`, v.ID, v.Kind, v.Payload, v.Queue, v.PartitionKey, v.RateClass, v.Priority, v.MaxAttempts, v.RetentionMs, v.Spec, v.NextRunMs, v.OnMissed.String(), v.BackfillLimit, v.Paused)
	return err
}
func (s *SqliteStore) DeleteSchedule(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, "DELETE FROM headgate_schedule WHERE id=?", id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return headgate.NotFoundf("schedule %s", id)
	}
	return nil
}
func (s *SqliteStore) ListSchedules(ctx context.Context) ([]headgate.ScheduleEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+scheduleColumns+` FROM headgate_schedule ORDER BY id LIMIT 10000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.ScheduleEntry
	for rows.Next() {
		v, err := scanSqliteSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *SqliteStore) DueSchedules(ctx context.Context, limit int64) ([]headgate.ScheduleEntry, int64, error) {
	if limit <= 0 {
		return nil, 0, nil
	}
	var now int64
	if err := s.db.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+scheduleColumns+` FROM headgate_schedule WHERE paused=0 AND next_run_ms<=? ORDER BY next_run_ms,id LIMIT ?`, now, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []headgate.ScheduleEntry
	for rows.Next() {
		v, err := scanSqliteSchedule(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, v)
	}
	return out, now, rows.Err()
}
func (s *SqliteStore) AdvanceSchedule(ctx context.Context, id string, from, to int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_schedule SET next_run_ms=?,last_enqueued_ms=`+nowMS+` WHERE id=? AND next_run_ms=?`, to, id, from)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
func (s *SqliteStore) RecordScheduleEvent(ctx context.Context, e headgate.ScheduleEvent) error {
	if !e.Outcome.Valid() {
		return headgate.Invalidf("invalid schedule event outcome")
	}
	if len(e.Reason) > 64 {
		return headgate.Invalidf("schedule event reason exceeds 64 bytes")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO headgate_schedule_event(schedule_id,tick_ms,job_id,outcome,reason,recorded_at_ms) VALUES(?,?,?,?,?,`+nowMS+`)`, e.ScheduleID, e.TickMs, e.JobID, e.Outcome, e.Reason); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM headgate_schedule_event WHERE schedule_id=? AND event_id NOT IN (SELECT event_id FROM headgate_schedule_event WHERE schedule_id=? ORDER BY event_id DESC LIMIT 100)`, e.ScheduleID, e.ScheduleID); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *SqliteStore) ListScheduleEvents(ctx context.Context, scheduleID string, before uint64, limit uint32) ([]headgate.ScheduleEvent, error) {
	if err := headgate.ValidateScheduleEventLimit(limit); err != nil {
		return nil, err
	}
	query := `SELECT event_id,schedule_id,tick_ms,job_id,outcome,reason,recorded_at_ms FROM headgate_schedule_event WHERE schedule_id=?`
	args := []any{scheduleID}
	if before > 0 {
		query += " AND event_id<?"
		args = append(args, before)
	}
	query += " ORDER BY event_id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.ScheduleEvent
	for rows.Next() {
		var v headgate.ScheduleEvent
		if err := rows.Scan(&v.EventID, &v.ScheduleID, &v.TickMs, &v.JobID, &v.Outcome, &v.Reason, &v.RecordedAtMs); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SqliteStore) HeartbeatWorker(ctx context.Context, w headgate.WorkerMeta) (string, error) {
	if w.WorkerID == "" {
		return "", &headgate.InvalidError{Msg: "worker_id must not be empty"}
	}
	queues, _ := json.Marshal(w.Queues)
	var command sql.NullString
	err := s.db.QueryRowContext(ctx, `INSERT INTO headgate_worker(worker_id,host,pid,queues_json,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls,status,duties_active) VALUES(?,?,?,?,?,?,`+nowMS+`,?,?,?,?,?) ON CONFLICT(worker_id) DO UPDATE SET host=excluded.host,pid=excluded.pid,queues_json=excluded.queues_json,concurrency=excluded.concurrency,started_at_ms=excluded.started_at_ms,heartbeat_at_ms=excluded.heartbeat_at_ms,inflight=excluded.inflight,polls=excluded.polls,empty_polls=excluded.empty_polls,status=excluded.status,duties_active=excluded.duties_active RETURNING command`, w.WorkerID, w.Host, w.PID, string(queues), w.Concurrency, w.StartedAtMs, w.Inflight, w.Polls, w.EmptyPolls, w.Status, w.DutiesActive).Scan(&command)
	if err != nil {
		return "", err
	}
	return command.String, nil
}
func (s *SqliteStore) ListWorkers(ctx context.Context, staleAfterMs int64) ([]headgate.WorkerMeta, error) {
	if staleAfterMs < 0 {
		return nil, &headgate.InvalidError{Msg: "stale_after_ms must be non-negative"}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT worker_id,host,pid,queues_json,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls,status,duties_active,command FROM headgate_worker WHERE heartbeat_at_ms>=`+nowMS+`-? ORDER BY worker_id LIMIT 10000`, staleAfterMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.WorkerMeta
	for rows.Next() {
		var v headgate.WorkerMeta
		var queues string
		var command sql.NullString
		if err := rows.Scan(&v.WorkerID, &v.Host, &v.PID, &queues, &v.Concurrency, &v.StartedAtMs, &v.HeartbeatAtMs, &v.Inflight, &v.Polls, &v.EmptyPolls, &v.Status, &v.DutiesActive, &command); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(queues), &v.Queues)
		v.PendingCommand = command.String
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *SqliteStore) SignalWorker(ctx context.Context, workerID, command string) error {
	if command != "" && !headgate.ValidWorkerCommand(command) {
		return headgate.Invalidf("unknown worker command `%s`", command)
	}
	var value any
	if command != "" {
		value = command
	}
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_worker SET command=? WHERE worker_id=?`, value, workerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return headgate.NotFoundf("worker %s", workerID)
	}
	return nil
}
func (s *SqliteStore) DistinctKinds(ctx context.Context, limit int64) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT kind FROM (SELECT kind FROM headgate_job WHERE state IN ('pending','scheduled','available','retryable') ORDER BY id LIMIT ?) ORDER BY kind`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return nil, err
		}
		out = append(out, kind)
	}
	return out, rows.Err()
}

func operationWhere(op headgate.BulkOp) (string, []any, error) {
	states, ok := headgate.BulkActionStates(op.Action)
	if !ok {
		return "", nil, headgate.Invalidf("unknown bulk action `%s`", op.Action)
	}
	where := []string{"state IN (" + placeholders(len(states)) + ")"}
	args := make([]any, 0, len(states)+5)
	for _, v := range states {
		args = append(args, v)
	}
	if op.Queue != "" {
		where = append(where, "queue=?")
		args = append(args, op.Queue)
	}
	if op.State != "" {
		where = append(where, "state=?")
		args = append(args, op.State)
	}
	if op.Kind != "" {
		where = append(where, "kind=?")
		args = append(args, op.Kind)
	}
	if op.PartitionKey != "" {
		where = append(where, "partition_key=?")
		args = append(args, op.PartitionKey)
	}
	if op.OlderThanMs != nil {
		where = append(where, "enqueued_at_ms<?")
		args = append(args, *op.OlderThanMs)
	}
	return strings.Join(where, " AND "), args, nil
}
func (s *SqliteStore) CreateOperation(ctx context.Context, op headgate.BulkOp) error {
	if op.ID == "" || !op.HasSelector() {
		return &headgate.InvalidError{Msg: "bulk operation requires id and a non-empty selector"}
	}
	where, args, err := operationWhere(op)
	if err != nil {
		return err
	}
	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM headgate_job WHERE `+where+` LIMIT 50001)`, args...).Scan(&total); err != nil {
		return err
	}
	selector, _ := json.Marshal(op)
	_, err = s.db.ExecContext(ctx, `INSERT INTO headgate_operation(id,action,selector_json,total_estimated,dry_run,created_at_ms) VALUES(?,?,?,?,?,`+nowMS+`)`, op.ID, op.Action, string(selector), total, op.DryRun)
	return err
}
func (s *SqliteStore) GetOperation(ctx context.Context, id string) (*headgate.OperationStatus, error) {
	var v headgate.OperationStatus
	var failure sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT id,status,affected,total_estimated,dry_run,error FROM headgate_operation WHERE id=?`, id).Scan(&v.ID, &v.Status, &v.Affected, &v.TotalEstimated, &v.DryRun, &failure)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v.Error = failure.String
	return &v, nil
}
func (s *SqliteStore) RunPendingOperations(ctx context.Context, batch int64) (uint64, error) {
	if batch <= 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,selector_json,dry_run FROM headgate_operation WHERE status='pending' ORDER BY created_at_ms,id LIMIT 100`)
	if err != nil {
		return 0, err
	}
	type pending struct {
		id, raw string
		dry     bool
	}
	var ops []pending
	for rows.Next() {
		var v pending
		if err := rows.Scan(&v.id, &v.raw, &v.dry); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ops = append(ops, v)
	}
	_ = rows.Close()
	var affected uint64
	for _, p := range ops {
		var op headgate.BulkOp
		if err := json.Unmarshal([]byte(p.raw), &op); err != nil {
			return 0, err
		}
		where, args, err := operationWhere(op)
		if err != nil {
			return 0, err
		}
		args = append(args, batch)
		idRows, err := tx.QueryContext(ctx, `SELECT id FROM headgate_job WHERE `+where+` ORDER BY id LIMIT ?`, args...)
		if err != nil {
			return 0, err
		}
		var ids []string
		for idRows.Next() {
			var id string
			if err := idRows.Scan(&id); err != nil {
				_ = idRows.Close()
				return 0, err
			}
			ids = append(ids, id)
		}
		_ = idRows.Close()
		n := int64(len(ids))
		if !p.dry && n > 0 {
			marks := placeholders(len(ids))
			vals := make([]any, len(ids))
			for i, id := range ids {
				vals[i] = id
			}
			var mutation string
			switch op.Action {
			case "delete":
				mutation = `DELETE FROM headgate_job WHERE id IN (` + marks + `)`
			case "cancel":
				mutation = `UPDATE headgate_job SET state='cancelled',finalized_at_ms=` + nowMS + `,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL WHERE id IN (` + marks + `)`
			case "retry":
				mutation = `UPDATE headgate_job SET state='available',scheduled_at_ms=` + nowMS + `,finalized_at_ms=NULL WHERE id IN (` + marks + `)`
			}
			res, err := tx.ExecContext(ctx, mutation, vals...)
			if err != nil {
				return 0, err
			}
			n, _ = res.RowsAffected()
		}
		affected += uint64(n)
		status := "pending"
		if n < batch || p.dry {
			status = "completed"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE headgate_operation SET affected=affected+?,status=? WHERE id=?`, n, status, p.id); err != nil {
			return 0, err
		}
	}
	return affected, tx.Commit()
}

var operationSequence atomic.Uint64

func (s *SqliteStore) DeleteQueue(ctx context.Context, queue string, force bool) (string, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM headgate_job WHERE queue=? LIMIT 2)`, queue).Scan(&count); err != nil {
		return "", err
	}
	if count == 0 {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM headgate_queue_state WHERE queue=?`, queue)
		_, _ = s.db.ExecContext(ctx, `DELETE FROM headgate_enqueue_policy WHERE queue=?`, queue)
		return "", nil
	}
	if !force {
		return "", &headgate.InvalidError{Msg: "queue is not empty; force creates an asynchronous delete operation"}
	}
	var now int64
	if err := s.db.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return "", err
	}
	id := headgate.FormatGeneratedID(now, os.Getpid(), operationSequence.Add(1))
	if err := s.CreateOperation(ctx, headgate.BulkOp{ID: id, Action: "delete", Queue: queue}); err != nil {
		return "", err
	}
	return id, nil
}
func (s *SqliteStore) SampleQueueMemory(ctx context.Context, limit uint32) (uint32, error) {
	if limit == 0 {
		return 0, nil
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT queue FROM (SELECT queue FROM headgate_job UNION SELECT queue FROM headgate_queue_state) ORDER BY queue LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	var queues []string
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			_ = rows.Close()
			return 0, err
		}
		queues = append(queues, q)
	}
	_ = rows.Close()
	for _, q := range queues {
		var bytes uint64
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(length(payload)+length(id)+length(kind)+length(fingerprint)),0) FROM (SELECT payload,id,kind,fingerprint FROM headgate_job WHERE queue=? LIMIT 1000)`, q).Scan(&bytes); err != nil {
			return 0, err
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO headgate_queue_state(queue,memory_bytes) VALUES(?,?) ON CONFLICT(queue) DO UPDATE SET memory_bytes=excluded.memory_bytes`, q, bytes); err != nil {
			return 0, err
		}
	}
	return uint32(len(queues)), nil
}

func (s *SqliteStore) AppendDurableEvent(ctx context.Context, e headgate.DurableEvent) (headgate.DurableEvent, bool, error) {
	if e.Scope == "" || e.Topic == "" || e.IdempotencyKey == "" {
		return e, false, &headgate.InvalidError{Msg: "durable event scope, topic, and idempotency_key must not be empty"}
	}
	if len(e.Payload) > headgate.MaxDurableEventPayloadBytes || len(e.Source) > headgate.MaxDurableEventSourceBytes || !json.Valid(e.Payload) || !json.Valid(e.Source) {
		return e, false, &headgate.InvalidError{Msg: "durable event payload/source must be valid bounded JSON"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return e, false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT INTO headgate_durable_event(scope,topic,idempotency_key,payload,source,recorded_at_ms) VALUES(?,?,?,?,?,`+nowMS+`) ON CONFLICT(scope,idempotency_key) DO NOTHING`, e.Scope, e.Topic, e.IdempotencyKey, e.Payload, e.Source)
	if err != nil {
		return e, false, err
	}
	n, _ := res.RowsAffected()
	stored := headgate.DurableEvent{}
	if err := tx.QueryRowContext(ctx, `SELECT event_id,scope,topic,idempotency_key,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=? AND idempotency_key=?`, e.Scope, e.IdempotencyKey).Scan(&stored.EventID, &stored.Scope, &stored.Topic, &stored.IdempotencyKey, &stored.Payload, &stored.Source, &stored.RecordedAtMs); err != nil {
		return e, false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM headgate_durable_event WHERE scope=? AND event_id NOT IN (SELECT event_id FROM headgate_durable_event WHERE scope=? ORDER BY event_id DESC LIMIT 100)`, e.Scope, e.Scope); err != nil {
		return e, false, err
	}
	return stored, n == 1, tx.Commit()
}
func (s *SqliteStore) ListDurableEvents(ctx context.Context, scope string, before uint64, limit uint32) ([]headgate.DurableEvent, error) {
	if err := headgate.ValidateDurableEventLimit(limit); err != nil {
		return nil, err
	}
	query := `SELECT event_id,scope,topic,idempotency_key,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=?`
	args := []any{scope}
	if before > 0 {
		query += " AND event_id<?"
		args = append(args, before)
	}
	query += " ORDER BY event_id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.DurableEvent
	for rows.Next() {
		var e headgate.DurableEvent
		if err := rows.Scan(&e.EventID, &e.Scope, &e.Topic, &e.IdempotencyKey, &e.Payload, &e.Source, &e.RecordedAtMs); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
