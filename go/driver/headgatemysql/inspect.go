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
	"fmt"
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

func (s *MysqlStore) GetJob(ctx context.Context, id string, includePayload bool) (*headgate.JobSummary, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+jobCols+` FROM headgate_job j WHERE j.ulid = ?`, id)
	j, _, err := scanJob(row, includePayload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

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

func (s *MysqlStore) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_queue_state (queue, paused) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE paused = new.paused`, queue, paused)
	return err
}

func (s *MysqlStore) SetQueueWeight(ctx context.Context, queue string, weight uint32) error {
	if weight == 0 {
		return &headgate.InvalidError{Msg: "weight must be >= 1"}
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_queue_state (queue, weight) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE
		  dispatch_count = FLOOR(headgate_queue_state.dispatch_count
		                         * new.weight / headgate_queue_state.weight),
		  weight = new.weight`, queue, weight)
	return err
}

func (s *MysqlStore) SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_enqueue_policy (queue, max_unfinished_jobs) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE max_unfinished_jobs = new.max_unfinished_jobs`, queue, maxUnfinishedJobs)
	return err
}

func (s *MysqlStore) RateClasses(ctx context.Context) ([]headgate.RateClassState, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT b.name, b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((`+nowMS+` - b.refilled_at_ms) * b.limit_per_window DIV b.window_ms))
		            ELSE b.tokens END AS avail,
		       (SELECT COUNT(*) FROM (
		          SELECT 1 FROM headgate_job w
		          WHERE w.state = 'available' AND w.rate_class = b.name LIMIT ?
		       ) t) AS waiting
		FROM headgate_rate_bucket b ORDER BY b.name`, positionLimit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.RateClassState
	for rows.Next() {
		var r headgate.RateClassState
		if err := rows.Scan(&r.Name, &r.Burst, &r.LimitPerWindow, &r.WindowMs,
			&r.TokensAvailable, &r.JobsWaiting); err != nil {
			return nil, err
		}
		r.Paused = r.LimitPerWindow == 0 // the kill switch is limit 0 + empty bucket
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *MysqlStore) UpsertRateClass(ctx context.Context, cfg headgate.RateClassConfig) error {
	if err := headgate.ValidateRateClassConfig(cfg); err != nil {
		return err
	}
	// Invariant 16 kill switch: paused = limit 0 AND tokens 0, refill adds nothing.
	limit, tokensInsert := cfg.Limit, cfg.Burst
	if cfg.Paused {
		limit, tokensInsert = 0, 0
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_rate_bucket
		       (name, tokens, burst, limit_per_window, window_ms, refilled_at_ms)
		VALUES (?, ?, ?, ?, ?, `+nowMS+`) AS new
		ON DUPLICATE KEY UPDATE
		  burst = new.burst, limit_per_window = new.limit_per_window,
		  window_ms = new.window_ms,
		  tokens = IF(?, 0, LEAST(headgate_rate_bucket.tokens, new.burst)),
		  refilled_at_ms = new.refilled_at_ms`,
		cfg.Name, tokensInsert, cfg.Burst, limit, cfg.WindowMs, cfg.Paused)
	return err
}

func (s *MysqlStore) ConcurrencyLimits(ctx context.Context) ([]headgate.ConcurrencyLimit, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, queue, max_concurrent, CAST(on_saturated AS CHAR)
		FROM headgate_concurrency_limit ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.ConcurrencyLimit
	for rows.Next() {
		var v headgate.ConcurrencyLimit
		if err := rows.Scan(&v.Name, &v.Queue, &v.MaxConcurrent, &v.OnSaturated); err != nil {
			return nil, err
		}
		if !v.OnSaturated.Valid() {
			return nil, fmt.Errorf("headgate: invalid saturation strategy `%s` in store", v.OnSaturated)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *MysqlStore) UpsertConcurrencyLimit(ctx context.Context, cfg headgate.ConcurrencyLimit) error {
	if err := headgate.ValidateConcurrencyLimit(cfg); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_concurrency_limit
		       (name, queue, max_concurrent, on_saturated)
		VALUES (?, ?, ?, ?) AS new
		ON DUPLICATE KEY UPDATE
		  queue = new.queue,
		  max_concurrent = new.max_concurrent,
		  on_saturated = new.on_saturated`,
		cfg.Name, cfg.Queue, cfg.MaxConcurrent, cfg.OnSaturated)
	return err
}

func (s *MysqlStore) Partitions(ctx context.Context, queue string) ([]headgate.PartitionState, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.partition_key, COALESCE(d.deficit, 0), p.n
		FROM (SELECT partition_key, COUNT(*) AS n FROM
		        (SELECT partition_key FROM headgate_job
		         WHERE queue = ? AND state = 'available' LIMIT ?) s
		      GROUP BY 1) p
		LEFT JOIN headgate_partition_deficit d
		       ON d.queue = ? AND d.partition_key = p.partition_key
		UNION
		SELECT d.partition_key, d.deficit, 0
		FROM headgate_partition_deficit d
		WHERE d.queue = ?
		  AND d.partition_key NOT IN
		      (SELECT partition_key FROM
		         (SELECT DISTINCT partition_key FROM headgate_job
		          WHERE queue = ? AND state = 'available' LIMIT 1000) t)
		ORDER BY 1 LIMIT 10000`, queue, sampleLimit, queue, queue, queue)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.PartitionState
	for rows.Next() {
		var p headgate.PartitionState
		if err := rows.Scan(&p.PartitionKey, &p.Deficit, &p.Waiting); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *MysqlStore) QuarantineList(ctx context.Context) ([]headgate.QuarantineEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT fingerprint, kind, crash_count, quarantined_at_ms, reason
		FROM headgate_quarantine ORDER BY quarantined_at_ms DESC LIMIT ?`, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.QuarantineEntry
	for rows.Next() {
		var q headgate.QuarantineEntry
		var reason sql.NullString
		if err := rows.Scan(&q.Fingerprint, &q.Kind, &q.CrashCount, &q.QuarantinedAtMs,
			&reason); err != nil {
			return nil, err
		}
		q.Reason = reason.String
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *MysqlStore) QuarantineRelease(ctx context.Context, fingerprint string) (uint64, error) {
	// tenant fairness/adaptive admission one transaction: released rows become available, so their partitions
	// must be listed in the same commit. The INSERT reads the still-quarantined rows,
	// so it goes first (MySQL has no data-modifying CTEs to fuse the two).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO headgate_active_partition (queue, partition_key)
		SELECT DISTINCT queue, partition_key FROM headgate_job
		WHERE fingerprint = ? AND state = 'quarantined'
		ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, fingerprint); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE headgate_job SET state = 'available', scheduled_at_ms = `+nowMS+`,
		       finalized_at_ms = NULL
		WHERE fingerprint = ? AND state = 'quarantined'`, fingerprint)
	if err != nil {
		return 0, err
	}
	released, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	res, err = tx.ExecContext(ctx,
		`DELETE FROM headgate_quarantine WHERE fingerprint = ?`, fingerprint)
	if err != nil {
		return 0, err
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if released == 0 && deleted == 0 {
		// `not found: ` is load-bearing — see the note in headgatepgx/inspect.go.
		return 0, headgate.NotFoundf("fingerprint %s is not quarantined", fingerprint)
	}
	return uint64(released), nil
}

func (s *MysqlStore) jobState(ctx context.Context, id string) (string, bool, error) {
	var st string
	err := s.db.QueryRowContext(ctx,
		`SELECT CAST(state AS CHAR) FROM headgate_job WHERE ulid = ?`, id).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return st, true, nil
}

func (s *MysqlStore) OperatorRetry(ctx context.Context, id string) error {
	// tenant fairness/adaptive admission same commit as the transition that makes the row available.
	retried, err := func() (int64, error) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO headgate_active_partition (queue, partition_key)
			SELECT queue, partition_key FROM headgate_job
			WHERE ulid = ? AND state = 'archived'
			ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, id); err != nil {
			return 0, err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE headgate_job SET state = 'available', scheduled_at_ms = `+nowMS+`,
			       finalized_at_ms = NULL
			WHERE ulid = ? AND state = 'archived'`, id)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		return n, tx.Commit()
	}()
	if err != nil {
		return err
	}
	if retried == 1 {
		return nil
	}
	st, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return headgate.Invalidf("operator_retry is only defined from archived; job %s is %s", id, st)
}

func (s *MysqlStore) OperatorCancel(ctx context.Context, id string) error {
	// adaptive admission cancelling a RUNNING job releases its slot; cancelling a scheduled or
	// available one must not decrement a slot it never took. The decrement therefore
	// carries `state = 'running'` in its own guard and runs FIRST, while that is still
	// true — after the UPDATE the row is 'cancelled' and unjoinable. (Postgres reads
	// was_running out of a locking pick CTE; MySQL has neither, so ORDER is the fence.)
	n, err := func() (int64, error) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
		if _, err := tx.ExecContext(ctx, `
			UPDATE headgate_inflight f
			  JOIN headgate_job j ON j.queue = f.queue AND j.partition_key = f.partition_key
			   SET f.n = GREATEST(0, f.n - 1)
			 WHERE j.ulid = ? AND j.state = 'running'`, id); err != nil {
			return 0, err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE headgate_job SET state = 'cancelled', lease_id = NULL,
			       lease_expires_at_ms = NULL, claimed_by = NULL,
			       finalized_at_ms = `+nowMS+`
			WHERE ulid = ? AND state IN ('pending', 'scheduled', 'available', 'running')`, id)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		return n, tx.Commit()
	}()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	st, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return headgate.Invalidf("operator_cancel is not defined from %s", st)
}

func (s *MysqlStore) DeleteJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM headgate_job WHERE ulid = ? AND state <> 'running'`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	_, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return &headgate.InvalidError{Msg: "cannot delete a running job; cancel it first"}
}

// ExplainAdmission replays the gate's own clause order read-only for one job — the
// admission policy/control plane endpoint only this design needs. Same query and assembly as the Rust MySQL
// adapter's explain_admission, whose clause order is in turn the Postgres one (the two
// SQL gates share it).
func (s *MysqlStore) ExplainAdmission(ctx context.Context, id string) (*headgate.AdmissionExplain, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT CAST(j.state AS CHAR) AS state, j.queue, j.scheduled_at_ms, j.priority,
		       j.rate_class, j.partition_key, j.fingerprint, j.id,
		       CAST(j.weight AS SIGNED) AS weight,
		       `+nowMS+` AS now_ms,
		       COALESCE(qs.paused, FALSE) AS paused,
		       (q.fingerprint IS NOT NULL) AS quarantined,
		       b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.name IS NULL THEN NULL
		            WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((`+nowMS+` - b.refilled_at_ms) * b.limit_per_window DIV b.window_ms))
		            ELSE b.tokens END AS avail,
		       COALESCE(d.deficit, 0) AS deficit,
		       cl.max_concurrent, CAST(cl.on_saturated AS CHAR) AS on_saturated,
		       -- adaptive admission read the counter the GATE reads, not a fresh count of running
		       -- rows. Why-is-this-job-not-running must answer for the gate that is
		       -- actually deciding: if headgate_inflight ever drifts, an explain that
		       -- quietly recomputed the truth would report a ceiling as clear while
		       -- admission kept refusing -- the one failure this endpoint exists to
		       -- make visible. Also O(1) instead of O(running).
		       COALESCE((SELECT f.n FROM headgate_inflight f
		                 WHERE f.queue = j.queue
		                   AND f.partition_key = j.partition_key), 0) AS inflight,
		       (SELECT CAST(COALESCE(SUM(t.weight), 0) AS SIGNED) FROM (
		          SELECT a.weight FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.rate_class = j.rate_class
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms < j.scheduled_at_ms
		                          OR (a.scheduled_at_ms = j.scheduled_at_ms AND a.id < j.id))))
		          ORDER BY a.priority DESC, a.scheduled_at_ms, a.id
		          LIMIT ?
		       ) t) AS cost_ahead_in_class,
		       (SELECT COUNT(*) FROM (
		          SELECT 1 FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.partition_key = j.partition_key
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms < j.scheduled_at_ms
		                          OR (a.scheduled_at_ms = j.scheduled_at_ms AND a.id < j.id))))
		          LIMIT ?
		       ) t) AS ahead_in_partition
		FROM headgate_job j
		LEFT JOIN headgate_queue_state qs ON qs.queue = j.queue
		LEFT JOIN headgate_quarantine q ON q.fingerprint = j.fingerprint
		LEFT JOIN headgate_rate_bucket b ON b.name = j.rate_class AND j.rate_class <> ''
		LEFT JOIN headgate_partition_deficit d
		       ON d.queue = j.queue AND d.partition_key = j.partition_key
		LEFT JOIN headgate_concurrency_limit cl ON cl.queue = j.queue
		WHERE j.ulid = ?`, positionLimit, positionLimit, id)

	var (
		state, queue, rateClass, partitionKey, fingerprint string
		scheduledAt, nowMs, deficit, inflight, weight      int64
		aheadInClass, aheadInPartition, internalID         int64
		priority                                           int32
		paused, quarantined                                bool
		burst, limitPerWindow, windowMs, avail, maxConc    sql.NullInt64
		onSaturated                                        sql.NullString
	)
	err := row.Scan(&state, &queue, &scheduledAt, &priority, &rateClass, &partitionKey,
		&fingerprint, &internalID, &weight, &nowMs, &paused, &quarantined,
		&burst, &limitPerWindow, &windowMs, &avail, &deficit, &maxConc, &onSaturated,
		&inflight, &aheadInClass, &aheadInPartition)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	nullableInt := func(value sql.NullInt64) *int64 {
		if !value.Valid {
			return nil
		}
		copy := value.Int64
		return &copy
	}
	strategy := ""
	if onSaturated.Valid {
		strategy = onSaturated.String
	}
	return headgate.EvaluateAdmission(headgateshared.AdmissionFacts{
		State: state, NowMs: nowMs, ScheduledAtMs: scheduledAt,
		QueuePaused: paused, Quarantined: quarantined, Fingerprint: fingerprint,
		RateClass: rateClass, Weight: weight, TokensAvailable: nullableInt(avail),
		TokensAhead: aheadInClass, LimitPerWindow: limitPerWindow.Int64,
		WindowMs: windowMs.Int64, MaxConcurrent: nullableInt(maxConc), Inflight: inflight,
		Saturation: strategy, Position: aheadInPartition, Deficit: deficit,
	}), nil
}

func (s *MysqlStore) History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]headgate.HistoryBucket, error) {
	if bucketMs < 60_000 {
		return nil, &headgate.InvalidError{Msg: "bucket_ms must be >= 60000 (the stored granularity)"}
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT (bucket_ms DIV ?) * ? AS at_ms,
		       CAST(SUM(arrived) AS SIGNED), CAST(SUM(completed) AS SIGNED)
		FROM headgate_queue_counter
		WHERE queue = ? AND bucket_ms >= ?
		GROUP BY 1 ORDER BY 1 LIMIT 10000`, bucketMs, bucketMs, queue, sinceMs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.HistoryBucket
	for rows.Next() {
		var b headgate.HistoryBucket
		if err := rows.Scan(&b.AtMs, &b.Arrived, &b.Completed); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *MysqlStore) QuarantineSweep(ctx context.Context, limit int64) (int64, error) {
	// crash quarantine quarantined is TERMINAL and VISIBLE; the generated column releases any
	// lifecycle unique key these jobs held. (MySQL cannot self-join the updated table
	// in a subquery; the join form sidesteps ER_UPDATE_TABLE_USED.)
	res, err := s.db.ExecContext(ctx, `
		UPDATE headgate_job j
		JOIN (SELECT id FROM headgate_job
		      WHERE state IN ('pending', 'available', 'scheduled', 'retryable')
		        AND fingerprint IN (SELECT fingerprint FROM headgate_quarantine)
		      LIMIT ?) pick ON pick.id = j.id
		SET j.state = 'quarantined', j.finalized_at_ms = `+nowMS, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *MysqlStore) RescheduleJob(ctx context.Context, id string, atMs int64) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE headgate_job SET scheduled_at_ms = ?
		WHERE ulid = ? AND state IN ('scheduled', 'retryable')`, atMs, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	st, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return headgate.Invalidf("reschedule is only defined for scheduled/retryable; job %s is %s", id, st)
}

func (s *MysqlStore) EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE headgate_job SET payload = ?, schema_version = ?, fingerprint = ?
		WHERE ulid = ? AND state <> 'running'`,
		payload, int64(schemaVersion), fingerprint, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	_, found, err := s.jobState(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return headgate.NotFoundf("job %s", id)
	}
	return &headgate.InvalidError{Msg: "cannot edit a running job's payload"}
}

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

// ---------- worker registry + surveyed policy behavior control channel ----------

func (s *MysqlStore) HeartbeatWorker(ctx context.Context, w headgate.WorkerMeta) (string, error) {
	status := w.Status
	if status == "" {
		status = "running"
	}
	queues, err := json.Marshal(w.Queues)
	if err != nil || w.Queues == nil {
		queues = []byte("[]")
	}
	// No RETURNING on MySQL: upsert, then read the command on the SAME connection. The
	// surveyed policy behavior channel is sticky, so the window between the two reads nothing away — but
	// the connection is PINNED anyway, because a pooled second statement could in
	// principle land on a replica-lagged session and the Rust twin holds one Conn.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck // returns the connection to the pool
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO headgate_worker
		       (worker_id, host, pid, queues, concurrency, started_at_ms, heartbeat_at_ms,
		        inflight, polls, empty_polls, status, duties_active)
		VALUES (?, ?, ?, CAST(? AS JSON), ?, ?, `+nowMS+`, ?, ?, ?, ?, ?) AS new
		ON DUPLICATE KEY UPDATE
		  queues = new.queues, concurrency = new.concurrency,
		  heartbeat_at_ms = new.heartbeat_at_ms,
		  -- ADDITIVE: LEVELS, so the beat overwrites rather than
		  -- accumulating (same rule as the PG adapter).
		  inflight = new.inflight, polls = new.polls,
		  empty_polls = new.empty_polls, status = new.status,
		  duties_active = new.duties_active`,
		w.WorkerID, w.Host, int64(w.PID), string(queues), int64(w.Concurrency),
		w.StartedAtMs, int64(w.Inflight), int64(w.Polls), int64(w.EmptyPolls),
		status, w.DutiesActive); err != nil {
		return "", err
	}
	var cmd sql.NullString
	err = conn.QueryRowContext(ctx,
		`SELECT command FROM headgate_worker WHERE worker_id = ?`, w.WorkerID).Scan(&cmd)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return cmd.String, nil
}

func (s *MysqlStore) ListWorkers(ctx context.Context, staleAfterMs int64) ([]headgate.WorkerMeta, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT worker_id, host, pid, CAST(queues AS CHAR) AS queues_json,
		       concurrency, started_at_ms, heartbeat_at_ms,
		       inflight, polls, empty_polls, status, duties_active, command
		FROM headgate_worker
		WHERE heartbeat_at_ms >= `+nowMS+` - ?
		ORDER BY worker_id LIMIT 10000`, staleAfterMs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []headgate.WorkerMeta
	for rows.Next() {
		var w headgate.WorkerMeta
		var pid, conc, inflight, polls, emptyPolls int64
		var queuesJSON, command sql.NullString
		if err := rows.Scan(&w.WorkerID, &w.Host, &pid, &queuesJSON, &conc,
			&w.StartedAtMs, &w.HeartbeatAtMs, &inflight, &polls, &emptyPolls,
			&w.Status, &w.DutiesActive, &command); err != nil {
			return nil, err
		}
		w.PID = int32(pid)
		if queuesJSON.Valid && queuesJSON.String != "" {
			_ = json.Unmarshal([]byte(queuesJSON.String), &w.Queues)
		}
		w.Concurrency = uint32(conc)
		w.Inflight, w.Polls, w.EmptyPolls = uint32(inflight), uint64(polls), uint64(emptyPolls)
		if command.Valid {
			w.PendingCommand = command.String
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *MysqlStore) SignalWorker(ctx context.Context, workerID, command string) error {
	var cmd any
	if command != "" {
		if !headgate.ValidWorkerCommand(command) {
			return &headgate.InvalidError{Msg: "command must be quiet, resume, restart, terminate, or resign"}
		}
		cmd = command
	}
	// CLIENT_FOUND_ROWS (package contract): matched-rows semantics, so clearing an
	// already-NULL command still counts the row and 0 truly means "no such worker".
	res, err := s.db.ExecContext(ctx,
		`UPDATE headgate_worker SET command = ? WHERE worker_id = ?`, cmd, workerID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return headgate.NotFoundf("worker %s", workerID)
	}
	return nil
}

func (s *MysqlStore) DistinctKinds(ctx context.Context, limit int64) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT kind FROM (
		  SELECT kind FROM headgate_job
		  WHERE state IN ('available', 'scheduled', 'retryable') LIMIT ?
		) t ORDER BY kind`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// ---------- control API contract async bulk operations ----------

// actionStates: which states each bulk action may touch — the transition table's rows,
// nothing more.
func actionStates(action string) (string, bool) {
	states, ok := headgate.BulkActionStates(action)
	if !ok {
		return "", false
	}
	return "('" + strings.Join(states, "', '") + "')", true
}

func selectorWhere(req headgate.BulkOp, allowedStates string) (string, []any) {
	clauses := []string{"j.state IN " + allowedStates}
	var args []any
	if req.Queue != "" {
		args = append(args, req.Queue)
		clauses = append(clauses, "j.queue = ?")
	}
	if req.State != "" {
		args = append(args, req.State)
		clauses = append(clauses, "CAST(j.state AS CHAR) = ?")
	}
	if req.Kind != "" {
		args = append(args, req.Kind)
		clauses = append(clauses, "j.kind = ?")
	}
	if req.PartitionKey != "" {
		args = append(args, req.PartitionKey)
		clauses = append(clauses, "j.partition_key = ?")
	}
	if req.OlderThanMs != nil {
		args = append(args, *req.OlderThanMs)
		clauses = append(clauses, "j.enqueued_at_ms < "+nowMS+" - ?")
	}
	return strings.Join(clauses, " AND "), args
}

func nz(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *MysqlStore) CreateOperation(ctx context.Context, req headgate.BulkOp) error {
	if !req.HasSelector() {
		return &headgate.InvalidError{Msg: "empty selector is rejected"} // control API contract
	}
	allowed, ok := actionStates(req.Action)
	if !ok {
		return headgate.Invalidf("unknown action `%s`", req.Action)
	}
	where, args := selectorWhere(req, allowed)
	estArgs := append(append([]any{}, args...), sampleLimit)
	var estimated int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM (SELECT 1 FROM headgate_job j WHERE `+where+` LIMIT ?) t`,
		estArgs...).Scan(&estimated); err != nil {
		return err
	}
	selector, _ := json.Marshal(map[string]any{
		"queue": nz(req.Queue), "state": nz(req.State), "kind": nz(req.Kind),
		"partition_key": nz(req.PartitionKey), "older_than_ms": req.OlderThanMs,
	})
	status := "pending"
	if req.DryRun {
		status = "completed"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_operation
		       (id, action, selector, status, total_estimated, dry_run, created_at_ms)
		VALUES (?, ?, CAST(? AS JSON), ?, ?, ?, `+nowMS+`)`,
		req.ID, req.Action, string(selector), status, estimated, req.DryRun)
	return err
}

func (s *MysqlStore) GetOperation(ctx context.Context, id string) (*headgate.OperationStatus, error) {
	op := headgate.OperationStatus{ID: id}
	var errText sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT status, affected, total_estimated, dry_run, error
		FROM headgate_operation WHERE id = ?`, id).
		Scan(&op.Status, &op.Affected, &op.TotalEstimated, &op.DryRun, &errText)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	op.Error = errText.String
	return &op, nil
}

func (s *MysqlStore) RunPendingOperations(ctx context.Context, batch int64) (uint64, error) {
	type opRow struct{ id, action, selector string }
	var ops []opRow
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, action, CAST(selector AS CHAR) FROM headgate_operation
		WHERE status IN ('pending', 'running')
		ORDER BY created_at_ms LIMIT 5`)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var o opRow
		if err := rows.Scan(&o.id, &o.action, &o.selector); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ops = append(ops, o)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var total uint64
	for _, o := range ops {
		var sel struct {
			Queue        string `json:"queue"`
			State        string `json:"state"`
			Kind         string `json:"kind"`
			PartitionKey string `json:"partition_key"`
			OlderThanMs  *int64 `json:"older_than_ms"`
		}
		_ = json.Unmarshal([]byte(o.selector), &sel)
		req := headgate.BulkOp{
			ID: o.id, Action: o.action, Queue: sel.Queue, State: sel.State,
			Kind: sel.Kind, PartitionKey: sel.PartitionKey, OlderThanMs: sel.OlderThanMs,
		}
		n, err := s.runOperationBatch(ctx, req, batch)
		if err != nil {
			_, _ = s.db.ExecContext(ctx,
				`UPDATE headgate_operation SET status = 'failed', error = ? WHERE id = ?`,
				err.Error(), o.id)
			continue
		}
		total += uint64(n)
		status := "running"
		if n < batch {
			status = "completed"
		}
		if _, err := s.db.ExecContext(ctx,
			`UPDATE headgate_operation SET status = ?, affected = affected + ? WHERE id = ?`,
			status, n, o.id); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (s *MysqlStore) PromoteJob(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var q, p string
	if err = tx.QueryRowContext(ctx, "SELECT queue,partition_key FROM headgate_job WHERE ulid=? AND state='pending' FOR UPDATE", id).Scan(&q, &p); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return headgate.Invalidf("operator_promote is defined only from pending")
		}
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE headgate_job SET state='available',scheduled_at_ms=CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED) WHERE ulid=?", id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO headgate_active_partition(queue,partition_key) VALUES(?,?) AS new ON DUPLICATE KEY UPDATE queue=new.queue", q, p); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MysqlStore) DeleteQueue(ctx context.Context, queue string, force bool) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err = tx.ExecContext(ctx, `INSERT INTO headgate_enqueue_policy(queue) VALUES(?) AS new ON DUPLICATE KEY UPDATE queue=new.queue`, queue); err != nil {
		return "", err
	}
	var locked string
	if err = tx.QueryRowContext(ctx, `SELECT queue FROM headgate_enqueue_policy WHERE queue=? FOR UPDATE`, queue).Scan(&locked); err != nil {
		return "", err
	}
	var depth int64
	err = tx.QueryRowContext(ctx, `SELECT GREATEST(0,
	 COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=? AND counter_kind='entered'),0)-
	 COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=? AND counter_kind='exited'),0))`, queue, queue).Scan(&depth)
	if err != nil {
		return "", err
	}
	if depth > 0 && !force {
		return "", headgate.Invalidf("queue is not empty; retry with force=true")
	}
	if depth == 0 {
		if _, err = tx.ExecContext(ctx, "DELETE FROM headgate_queue_state WHERE queue=?", queue); err != nil {
			return "", err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM headgate_enqueue_policy WHERE queue=?", queue); err != nil {
			return "", err
		}
		return "", tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE headgate_enqueue_policy SET max_unfinished_jobs=0 WHERE queue=?`, queue); err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	var now int64
	if err = s.db.QueryRowContext(ctx, "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)").Scan(&now); err != nil {
		return "", err
	}
	id := fmt.Sprintf("qdel-%d-%s", now, strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, queue))
	err = s.CreateOperation(ctx, headgate.BulkOp{ID: id, Action: "delete", Queue: queue})
	return id, err
}

func (s *MysqlStore) SampleQueueMemory(ctx context.Context, limit uint32) (uint32, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > headgateshared.InspectionMemorySampleLimit {
		limit = headgateshared.InspectionMemorySampleLimit
	}
	rows, err := s.db.QueryContext(ctx, "SELECT queue FROM headgate_queue_state ORDER BY queue LIMIT 200")
	if err != nil {
		return 0, err
	}
	var qs []string
	for rows.Next() {
		var q string
		if err = rows.Scan(&q); err != nil {
			_ = rows.Close()
			return 0, err
		}
		qs = append(qs, q)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, q := range qs {
		var bytes uint64
		var n uint32
		err = s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(OCTET_LENGTH(payload)+OCTET_LENGTH(COALESCE(headers,''))+256),0),COUNT(*) FROM (SELECT payload,headers FROM headgate_job WHERE queue=? ORDER BY id DESC LIMIT ?) sampled`, q, limit).Scan(&bytes, &n)
		if err != nil {
			return 0, err
		}
		_, err = s.db.ExecContext(ctx, `INSERT INTO headgate_queue_sample(queue,memory_bytes,sampled_jobs,sampled_at_ms) VALUES(?,?,?,CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)) AS new ON DUPLICATE KEY UPDATE memory_bytes=new.memory_bytes,sampled_jobs=new.sampled_jobs,sampled_at_ms=new.sampled_at_ms`, q, bytes, n)
		if err != nil {
			return 0, err
		}
	}
	return uint32(len(qs)), nil
}

// runOperationBatch runs one bounded batch of a bulk operation — the same transitions
// as the single-job ops.
func (s *MysqlStore) runOperationBatch(ctx context.Context, req headgate.BulkOp, batch int64) (int64, error) {
	allowed, ok := actionStates(req.Action)
	if !ok {
		return 0, headgate.Invalidf("unknown action `%s`", req.Action)
	}
	where, args := selectorWhere(req, allowed)
	// MySQL cannot reference the updated table in its own subquery; the JOIN-on-
	// derived-picked-ids form sidesteps ER_UPDATE_TABLE_USED.
	pick := `SELECT j.id FROM headgate_job j WHERE ` + where + ` ORDER BY j.id LIMIT ?`
	var stmt string
	switch req.Action {
	case "retry":
		stmt = `UPDATE headgate_job j JOIN (` + pick + `) picked ON picked.id = j.id
			SET j.state = 'available', j.scheduled_at_ms = ` + nowMS + `,
			    j.finalized_at_ms = NULL`
	case "cancel":
		stmt = `UPDATE headgate_job j JOIN (` + pick + `) picked ON picked.id = j.id
			SET j.state = 'cancelled', j.lease_id = NULL,
			    j.lease_expires_at_ms = NULL, j.claimed_by = NULL,
			    j.finalized_at_ms = ` + nowMS
	case "delete":
		stmt = `DELETE j FROM headgate_job j JOIN (` + pick + `) picked ON picked.id = j.id`
	default:
		return 0, headgate.Invalidf("unknown action `%s`", req.Action)
	}
	all := append(append([]any{}, args...), batch)
	if req.Action == "cancel" {
		// adaptive admission cancel is the only bulk action whose allowed states include 'running', so
		// it is the only one that moves the inflight counter. Same pair, same order,
		// same `pick` predicate as the retry branch below: decrement the rows that are
		// still running, then cancel them, inside one transaction.
		return s.inTx(ctx, func(tx *sql.Tx) (int64, error) {
			if _, err := tx.ExecContext(ctx, `
				UPDATE headgate_inflight f
				  JOIN headgate_job j
				    ON j.queue = f.queue AND j.partition_key = f.partition_key
				  JOIN (`+pick+`) picked ON picked.id = j.id
				   SET f.n = GREATEST(0, f.n - 1)
				 WHERE j.state = 'running'`, all...); err != nil {
				return 0, err
			}
			res, err := tx.ExecContext(ctx, stmt, all...)
			if err != nil {
				return 0, err
			}
			return res.RowsAffected()
		})
	}
	if req.Action != "retry" {
		res, err := s.db.ExecContext(ctx, stmt, all...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}
	// tenant fairness/adaptive admission retry makes rows available, so the partitions are listed in the SAME
	// transaction — and from the SAME `pick` predicate, so the two statements cannot
	// disagree about which rows they are talking about.
	return s.inTx(ctx, func(tx *sql.Tx) (int64, error) {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO headgate_active_partition (queue, partition_key)
			SELECT DISTINCT j.queue, j.partition_key
			FROM headgate_job j JOIN (`+pick+`) picked ON picked.id = j.id
			ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, all...); err != nil {
			return 0, err
		}
		res, err := tx.ExecContext(ctx, stmt, all...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	})
}

// inTx runs f in one transaction and commits only when it succeeds.
func (s *MysqlStore) inTx(ctx context.Context, f func(*sql.Tx) (int64, error)) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	n, err := f(tx)
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}
