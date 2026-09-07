package headgatemysql

// control plane the inspection/control surface over MySQL, ported statement-for-statement from
// crates/headgate-mysql/src/inspect.rs — the same discipline headgatepgx and
// headgateredis followed for their Rust twins. Same bounds, same SQL, same error
// texts, so the two languages' consoles read one store identically and the control API contract
// mutation-parity diff can compare them byte for byte.
//
// Every read is BOUNDED (invariant 6): counting queries scan at most sampleLimit rows
// and report approximate instead of paying for exactness.
//
// The MySQL idioms this file inherits from its Rust twin, all load-bearing:
//   - NO data-modifying CTEs and NO RETURNING, so where Postgres fuses "transition +
//     maintain the derived set" into one statement, this uses a short TRANSACTION with
//     the maintenance statement FIRST (it reads the pre-transition rows).
//   - ER_UPDATE_TABLE_USED: MySQL cannot reference the updated table in its own
//     subquery, so every bounded UPDATE/DELETE takes the
//     `UPDATE t JOIN (SELECT id ... LIMIT n) pick ON pick.id = t.id` form.
//   - ON DUPLICATE KEY UPDATE with the 8.0.19 `AS new` row alias (VALUES() is
//     deprecated) — except where the ODKU body is a no-op lock take, which keeps
//     `VALUES(...)` to stay byte-identical with store.go's existing statements.
//   - affected-rows via CLIENT_FOUND_ROWS (see the package docs): matched, not changed,
//     so "0 rows" unambiguously means "no such row".

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

const (
	// sampleLimit is the most rows any counting query may touch. Past this, counts are
	// approximate — a queue console must never be able to pin the database.
	sampleLimit = int64(headgateshared.InspectionSampleLimit)
	// positionLimit caps queue-position lookups; "position >= 1000" is answer enough.
	positionLimit       = int64(headgateshared.InspectionPositionLimit)
	quietPartitionLimit = headgateshared.InspectionQuietPartitionLimit
	maxPage             = uint32(headgateshared.InspectionMaxPage)
)

type quietPartMetric struct {
	partition          string
	inflight           int64
	arrived, completed int64
	oldestAt           sql.NullInt64
}

func (s *MysqlStore) quietGroupMetrics(ctx context.Context, queue string, nowMs int64) (headgate.QuietGroupMetrics, error) {
	cutoff := nowMs/60_000*60_000 - 60_000
	rows, err := s.db.QueryContext(ctx, `
		SELECT n.partition_key,
		       COALESCE((SELECT i.n FROM headgate_inflight i
		                 WHERE i.queue = ? AND i.partition_key = n.partition_key), 0),
		       COALESCE((SELECT SUM(pc.arrived) FROM headgate_partition_counter pc
		                 WHERE pc.queue = ? AND pc.partition_key = n.partition_key
		                   AND pc.bucket_ms >= ?), 0),
		       COALESCE((SELECT SUM(pc.completed) FROM headgate_partition_counter pc
		                 WHERE pc.queue = ? AND pc.partition_key = n.partition_key
		                   AND pc.bucket_ms >= ?), 0),
		       (SELECT j.scheduled_at_ms FROM headgate_job j
		        WHERE j.queue = ? AND j.partition_key = n.partition_key
		          AND j.state = 'available'
		        ORDER BY j.scheduled_at_ms, j.id LIMIT 1)
		FROM (
		  SELECT partition_key FROM (
		    SELECT partition_key FROM headgate_active_partition WHERE queue = ?
		    UNION SELECT partition_key FROM headgate_inflight WHERE queue = ? AND n > 0
		    UNION SELECT partition_key FROM headgate_partition_counter
		          WHERE queue = ? AND bucket_ms >= ?
		  ) all_names ORDER BY partition_key LIMIT ?
		) n`, queue, queue, cutoff, queue, cutoff, queue,
		queue, queue, queue, cutoff, quietPartitionLimit+1)
	if err != nil {
		return headgate.QuietGroupMetrics{}, err
	}
	defer func() { _ = rows.Close() }()
	parts := make([]quietPartMetric, 0)
	for rows.Next() {
		var p quietPartMetric
		if err := rows.Scan(&p.partition, &p.inflight, &p.arrived, &p.completed, &p.oldestAt); err != nil {
			return headgate.QuietGroupMetrics{}, err
		}
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return headgate.QuietGroupMetrics{}, err
	}
	approx := len(parts) > quietPartitionLimit
	if approx {
		parts = parts[:quietPartitionLimit]
	}
	loads := make(map[string]int64, len(parts))
	for _, p := range parts {
		loads[p.partition] = p.inflight
	}
	noisy := headgate.NoisyPartitionKeys(loads)
	quietParts := make([]string, 0, len(parts)-len(noisy))
	var arrived, completed int64
	var oldestAt *int64
	for _, p := range parts {
		if noisy[p.partition] {
			continue
		}
		quietParts = append(quietParts, p.partition)
		arrived += p.arrived
		completed += p.completed
		if p.oldestAt.Valid && (oldestAt == nil || p.oldestAt.Int64 < *oldestAt) {
			v := p.oldestAt.Int64
			oldestAt = &v
		}
	}
	var backlog int64
	if len(quietParts) > 0 {
		args := make([]any, 0, len(quietParts)+1)
		args = append(args, queue)
		for _, p := range quietParts {
			args = append(args, p)
		}
		err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (
			SELECT 1 FROM headgate_job
			WHERE queue = ? AND partition_key IN (`+placeholders(len(quietParts))+`)
			  AND state IN ('pending','scheduled','available','running','retryable')
			LIMIT `+strconv.FormatInt(sampleLimit, 10)+`) bounded`, args...).Scan(&backlog)
		if err != nil {
			return headgate.QuietGroupMetrics{}, err
		}
	}
	q := headgate.QuietGroupMetrics{
		ArrivalRate: float64(arrived) / 60.0, DrainRate: float64(completed) / 60.0,
		NoisyPartitions: uint32(len(noisy)), Approximate: approx || backlog >= sampleLimit,
	}
	q.TimeToDrainMs = headgate.TimeToDrainMillis(backlog, q.ArrivalRate, q.DrainRate)
	if oldestAt != nil {
		age := headgate.AgeMillis(nowMs, *oldestAt)
		q.OldestAvailableMs = &age
	}
	return q, nil
}

// transactional API's compile-time capability check: the Caps() bit and the method set cannot drift.
var _ headgate.InspectStore = (*MysqlStore)(nil)
var _ headgate.ResultInspectStore = (*MysqlStore)(nil)
var _ headgate.OutputInspectStore = (*MysqlStore)(nil)
var _ headgate.ProgressInspectStore = (*MysqlStore)(nil)
var _ headgate.CheckpointInspectStore = (*MysqlStore)(nil)

const jobCols = `j.ulid, j.kind, j.queue, CAST(j.state AS CHAR) AS state_text,
	j.schema_version, j.priority, j.attempt, j.crash_attempt, j.max_attempts,
	j.partition_key, j.rate_class, j.sticky_worker, j.weight, j.fingerprint, j.enqueued_at_ms, j.scheduled_at_ms, j.claimed_at_ms,
	j.periodic_schedule_id, j.periodic_tick_ms, j.finalized_at_ms, j.payload, CAST(j.headers AS CHAR),
	CAST(j.errors AS CHAR) AS errors_text, j.id,
	COALESCE((SELECT JSON_ARRAYAGG(t.tag) FROM headgate_job_tag t WHERE t.job_id=j.id),JSON_ARRAY()) AS tags_text`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner, includePayload bool) (*headgate.JobSummary, int64, error) {
	var j headgate.JobSummary
	var schemaVersion, attempt, crash, maxAtt int64
	var payload []byte
	var headersJSON []byte
	var finalized sql.NullInt64
	var claimed sql.NullInt64
	var errorsText sql.NullString
	var internalID int64
	var tagsJSON []byte
	err := row.Scan(&j.ID, &j.Kind, &j.Queue, &j.State, &schemaVersion, &j.Priority,
		&attempt, &crash, &maxAtt, &j.PartitionKey, &j.RateClass, &j.StickyWorker, &j.Weight, &j.Fingerprint,
		&j.EnqueuedAtMs, &j.ScheduledAtMs, &claimed, &j.PeriodicScheduleID, &j.PeriodicTickMs,
		&finalized, &payload, &headersJSON, &errorsText,
		&internalID, &tagsJSON)
	if err != nil {
		return nil, 0, err
	}
	j.SchemaVersion = uint32(schemaVersion)
	j.Attempt, j.CrashAttempt, j.MaxAttempts = uint32(attempt), uint32(crash), uint32(maxAtt)
	if finalized.Valid {
		v := finalized.Int64
		j.FinalizedAtMs = &v
	}
	if claimed.Valid {
		v := claimed.Int64
		j.ClaimedAtMs = &v
	}
	j.ErrorsJSON = errorsText.String
	_ = json.Unmarshal(tagsJSON, &j.Tags)
	if j.ErrorsJSON == "" {
		j.ErrorsJSON = "[]"
	}
	if includePayload {
		j.Payload = payload // invariant 9: withheld unless explicitly requested
		j.Headers = headgate.DecodeHeaders(headersJSON)
	}
	return &j, internalID, nil
}

// GetJob returns a job summary, optionally including its payload and headers.
func (s *MysqlStore) GetJob(ctx context.Context, id string, includePayload bool) (*headgate.JobSummary, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+jobCols+` FROM headgate_job j WHERE j.ulid = ?`, id)
	j, _, err := scanJob(row, includePayload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

// GetJobResult returns a completed job's durable result, if present.
func (s *MysqlStore) GetJobResult(ctx context.Context, id string) (*headgate.JobResult, error) {
	var version uint32
	var bytes []byte
	err := s.db.QueryRowContext(ctx, `SELECT result_schema_version, result_bytes
		FROM headgate_job WHERE ulid = ? AND result_schema_version IS NOT NULL`, id).
		Scan(&version, &bytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &headgate.JobResult{SchemaVersion: version, Bytes: bytes}, nil
}

// GetJobOutput returns a job's latest durable output, if present.
func (s *MysqlStore) GetJobOutput(ctx context.Context, id string) (*headgate.JobOutput, error) {
	var version uint32
	var bytes []byte
	var fence uint64
	var updatedAtMs int64
	err := s.db.QueryRowContext(ctx, `SELECT output_schema_version, output_bytes, output_fence,
		output_updated_at_ms FROM headgate_job
		WHERE ulid = ? AND output_schema_version IS NOT NULL`, id).
		Scan(&version, &bytes, &fence, &updatedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &headgate.JobOutput{
		SchemaVersion: version, Bytes: bytes, Fence: fence, UpdatedAtMs: updatedAtMs,
	}, nil
}

// GetJobProgress returns a job's latest progress update, if present.
func (s *MysqlStore) GetJobProgress(ctx context.Context, id string) (*headgate.JobProgress, error) {
	var current, total, fence uint64
	var message sql.NullString
	var updatedAtMs int64
	err := s.db.QueryRowContext(ctx, `SELECT progress_current, progress_total, progress_message,
		progress_fence, progress_updated_at_ms FROM headgate_job
		WHERE ulid = ? AND progress_current IS NOT NULL`, id).
		Scan(&current, &total, &message, &fence, &updatedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &headgate.JobProgress{
		Current: current, Total: total, Message: message.String,
		Fence: fence, UpdatedAtMs: updatedAtMs,
	}, nil
}

// GetJobCheckpoint returns a job's latest resumable checkpoint.
func (s *MysqlStore) GetJobCheckpoint(ctx context.Context, id string) (*headgate.Checkpoint, error) {
	var raw sql.NullString
	var cursor []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT CAST(checkpoint AS CHAR), cp_cursor FROM headgate_job WHERE ulid = ?`, id).
		Scan(&raw, &cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	checkpoint := decodeCheckpoint(raw, cursor)
	return &checkpoint, nil
}

// ListJobs returns a newest-first page matching f.
func (s *MysqlStore) ListJobs(ctx context.Context, f headgate.JobFilter, cursor string, limit uint32) (headgate.JobPage, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > maxPage {
		limit = maxPage
	}
	var clauses []string
	var args []any
	bind := func(sqlText string, v any) {
		args = append(args, v)
		clauses = append(clauses, sqlText)
	}
	// `!= nil`, not `!= ""` — see the note in headgatepgx.ListJobs and the
	// JobFilter doc comment. An explicit "" is a filter FOR the empty value.
	if f.Queue != nil {
		bind("j.queue = ?", *f.Queue)
	}
	if f.Kind != nil {
		bind("j.kind = ?", *f.Kind)
	}
	if f.KindPrefix != nil {
		// No starts_with() on MySQL: LIKE, with the caller's % and _ escaped so a
		// prefix containing either is a literal prefix and not a pattern.
		bind(`j.kind LIKE CONCAT(REPLACE(REPLACE(?, '%', '\\%'), '_', '\\_'), '%')`, *f.KindPrefix)
	}
	if f.PartitionKey != nil {
		bind("j.partition_key = ?", *f.PartitionKey)
	}
	if f.State != nil {
		bind("CAST(j.state AS CHAR) = ?", *f.State)
	}
	if f.ID != nil {
		bind("j.ulid = ?", *f.ID)
	}
	if f.Fingerprint != nil {
		bind("j.fingerprint = ?", *f.Fingerprint)
	}
	if f.RateClass != nil {
		bind("j.rate_class = ?", *f.RateClass)
	}
	if f.Priority != nil {
		bind("j.priority = ?", *f.Priority)
	}
	for _, tag := range f.TagsAll {
		bind("EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag=?)", tag)
	}
	if len(f.TagsAny) > 0 {
		marks := make([]string, len(f.TagsAny))
		for i, tag := range f.TagsAny {
			marks[i] = "?"
			args = append(args, tag)
		}
		clauses = append(clauses, "EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag IN ("+strings.Join(marks, ",")+"))")
	}
	// Newest first; the cursor is the last row's internal id — same as Postgres.
	cursorID := int64(1<<63 - 1)
	if cursor != "" {
		v, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return headgate.JobPage{}, &headgate.InvalidError{Msg: "bad cursor"}
		}
		cursorID = v
	}
	bind("j.id < ?", cursorID)
	args = append(args, int64(limit))
	q := `SELECT ` + jobCols + ` FROM headgate_job j WHERE ` +
		strings.Join(clauses, " AND ") + ` ORDER BY j.id DESC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return headgate.JobPage{}, err
	}
	defer func() { _ = rows.Close() }()
	var page headgate.JobPage
	var lastInternal int64
	for rows.Next() {
		j, internal, err := scanJob(rows, false)
		if err != nil {
			return headgate.JobPage{}, err
		}
		page.Jobs = append(page.Jobs, *j)
		lastInternal = internal
	}
	if err := rows.Err(); err != nil {
		return headgate.JobPage{}, err
	}
	if uint32(len(page.Jobs)) == limit {
		page.NextCursor = strconv.FormatInt(lastInternal, 10)
	}
	return page, nil
}

// Counts returns bounded per-state counts for one queue or all queues.
func (s *MysqlStore) Counts(ctx context.Context, queue *string) (headgate.StateCounts, error) {
	// nil = every queue; a pointer to "" = the queue literally named "". See headgatepgx.
	var q any
	if queue != nil {
		q = *queue
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT CAST(state AS CHAR), COUNT(*) FROM (
		  SELECT state FROM headgate_job
		  WHERE (? IS NULL OR queue = ?) LIMIT ?
		) s GROUP BY 1`, q, q, sampleLimit)
	if err != nil {
		return headgate.StateCounts{}, err
	}
	defer func() { _ = rows.Close() }()
	out := headgate.StateCounts{Counts: map[string]int64{}}
	var total int64
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return out, err
		}
		out.Counts[st] = n
		total += n
	}
	out.Approximate = total >= sampleLimit
	return out, rows.Err()
}

// QueueStats returns bounded operational statistics for known queues.
func (s *MysqlStore) QueueStats(ctx context.Context) ([]headgate.QueueStatsView, error) {
	// Queue discovery is bounded: configured queues, recently active counters, and a
	// bounded sample of job rows. Same three-arm UNION as the Rust adapter, and the
	// same per-queue follow-ups (MySQL has no LATERAL-free way to fuse them that the
	// planner handles better than three keyed lookups).
	names, err := func() ([]string, error) {
		rows, err := s.db.QueryContext(ctx, `
			SELECT queue FROM headgate_queue_state
			UNION SELECT queue FROM headgate_enqueue_policy
			UNION SELECT queue FROM headgate_queue_counter
			      WHERE bucket_ms >= `+nowMS+` - 3600000
			UNION SELECT DISTINCT queue FROM
			      (SELECT queue FROM headgate_job LIMIT ?) s
			ORDER BY 1 LIMIT 10000`, sampleLimit)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var q string
			if err := rows.Scan(&q); err != nil {
				return nil, err
			}
			out = append(out, q)
		}
		return out, rows.Err()
	}()
	if err != nil {
		return nil, err
	}
	out := make([]headgate.QueueStatsView, 0, len(names))
	for _, q := range names {
		v := headgate.QueueStatsView{Queue: q, ByState: map[string]int64{}}
		_ = s.db.QueryRowContext(ctx, `SELECT memory_bytes FROM headgate_queue_sample WHERE queue = ?`, q).Scan(&v.MemoryBytes)
		var nowMs int64
		if err := s.db.QueryRowContext(ctx, `SELECT `+nowMS).Scan(&nowMs); err != nil {
			return nil, err
		}
		byState, _, total, err := func() (map[string]int64, int64, int64, error) {
			rows, err := s.db.QueryContext(ctx, `
				SELECT CAST(state AS CHAR), COUNT(*) FROM (
				  SELECT state FROM headgate_job WHERE queue = ? LIMIT ?
				) s GROUP BY 1`, q, sampleLimit)
			if err != nil {
				return nil, 0, 0, err
			}
			defer func() { _ = rows.Close() }()
			m := map[string]int64{}
			var backlog, total int64
			for rows.Next() {
				var st string
				var n int64
				if err := rows.Scan(&st, &n); err != nil {
					return nil, 0, 0, err
				}
				m[st] = n
				total += n
				switch st {
				case "pending", "available", "scheduled", "retryable", "running":
					backlog += n
				}
			}
			return m, backlog, total, rows.Err()
		}()
		if err != nil {
			return nil, err
		}
		v.ByState = byState
		v.CountsApproximate = total >= sampleLimit
		var maxUnfinished sql.NullInt64
		var entered, exited int64
		err = s.db.QueryRowContext(ctx, `
			SELECT p.max_unfinished_jobs, COALESCE(ent.n, 0), COALESCE(ext.n, 0)
			FROM headgate_enqueue_policy p
			LEFT JOIN headgate_enqueue_counter ent
			  ON ent.queue = p.queue AND ent.counter_kind = 'entered'
			LEFT JOIN headgate_enqueue_counter ext
			  ON ext.queue = p.queue AND ext.counter_kind = 'exited'
			WHERE p.queue = ?`, q).Scan(&maxUnfinished, &entered, &exited)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		v.UnfinishedJobs = uint64(max(int64(0), entered-exited))
		if maxUnfinished.Valid {
			n := uint64(maxUnfinished.Int64)
			v.MaxUnfinishedJobs = &n
		}
		var arrived, completed sql.NullInt64
		err = s.db.QueryRowContext(ctx, `
			SELECT SUM(arrived), SUM(completed) FROM headgate_queue_counter
			WHERE queue = ? AND bucket_ms >= (`+nowMS+` DIV 60000) * 60000 - 60000`, q).
			Scan(&arrived, &completed)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		var paused sql.NullBool
		var queueWeight sql.NullInt64
		err = s.db.QueryRowContext(ctx,
			`SELECT paused, weight FROM headgate_queue_state WHERE queue = ?`, q).
			Scan(&paused, &queueWeight)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		v.Paused = paused.Bool
		v.Weight = 1
		if queueWeight.Valid {
			v.Weight = uint32(queueWeight.Int64)
		}
		var oldestAt sql.NullInt64
		err = s.db.QueryRowContext(ctx, `
			SELECT scheduled_at_ms FROM headgate_job
			WHERE queue = ? AND state = 'available'
			ORDER BY scheduled_at_ms, id LIMIT 1`, q).Scan(&oldestAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if oldestAt.Valid {
			age := headgate.AgeMillis(nowMs, oldestAt.Int64)
			v.OldestAvailableMs = &age
		}
		v.ArrivalRate = float64(arrived.Int64) / 60.0
		v.DrainRate = float64(completed.Int64) / 60.0
		// backlog metrics time-to-drain: nil when arrival >= drain — the alert condition.
		v.TimeToDrainMs = headgate.TimeToDrainMillis(
			int64(v.UnfinishedJobs), v.ArrivalRate, v.DrainRate,
		)
		v.QuietGroups, err = s.quietGroupMetrics(ctx, q, nowMs)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// SetQueuePaused changes whether admission may draw from queue.
