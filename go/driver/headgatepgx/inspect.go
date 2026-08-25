package headgatepgx

// control plane the inspection/control surface, ported statement-for-statement from the Rust
// adapter's inspect.rs — same bounds, same SQL, same semantics, so the two languages'
// consoles read one store identically. Every read is BOUNDED (invariant 6): counting
// queries scan at most sampleLimit rows and report approximate instead of paying for
// exactness.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	headgate "github.com/mujhtech/headgate"
)

const (
	sampleLimit         = int64(50_000)
	positionLimit       = int64(1_000)
	quietPartitionLimit = int64(1_000)
	maxPage             = uint32(200)
)

type quietPartMetric struct {
	partition          string
	inflight           int64
	arrived, completed int64
	oldestAt           *int64
}

func (s *PgxStore) quietGroupMetrics(ctx context.Context, queue string, nowMs int64) (headgate.QuietGroupMetrics, error) {
	rows, err := s.pool.Query(ctx, `
		WITH names AS (
		  SELECT partition_key FROM headgate_active_partition WHERE queue = $1
		  UNION SELECT partition_key FROM headgate_inflight WHERE queue = $1 AND n > 0
		  UNION SELECT partition_key FROM headgate_partition_counter
		        WHERE queue = $1 AND bucket_ms >= $2
		  ORDER BY 1 LIMIT $3
		), rates AS (
		  SELECT partition_key, sum(arrived)::bigint AS arrived,
		         sum(completed)::bigint AS completed
		  FROM headgate_partition_counter WHERE queue = $1 AND bucket_ms >= $2 GROUP BY 1
		)
		SELECT n.partition_key, COALESCE(i.n, 0)::bigint,
		       COALESCE(r.arrived, 0)::bigint, COALESCE(r.completed, 0)::bigint,
		       (SELECT j.scheduled_at_ms FROM headgate_job j
		        WHERE j.queue = $1 AND j.partition_key = n.partition_key
		          AND j.state = 'available'
		        ORDER BY j.scheduled_at_ms, j.id LIMIT 1)
		FROM names n
		LEFT JOIN headgate_inflight i ON i.queue = $1 AND i.partition_key = n.partition_key
		LEFT JOIN rates r ON r.partition_key = n.partition_key
		ORDER BY n.partition_key`, queue, nowMs/60_000*60_000-60_000, quietPartitionLimit+1)
	if err != nil {
		return headgate.QuietGroupMetrics{}, err
	}
	defer rows.Close()
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
	approx := int64(len(parts)) > quietPartitionLimit
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
		if p.oldestAt != nil && (oldestAt == nil || *p.oldestAt < *oldestAt) {
			v := *p.oldestAt
			oldestAt = &v
		}
	}
	var backlog int64
	if len(quietParts) > 0 {
		err = s.pool.QueryRow(ctx, `
			SELECT count(*)::bigint FROM (
			  SELECT 1 FROM headgate_job
			  WHERE queue = $1 AND partition_key = ANY($2)
			    AND state = ANY(ARRAY['pending','scheduled','available','running','retryable']::headgate_state[])
			  LIMIT $3
			) bounded`, queue, quietParts, sampleLimit).Scan(&backlog)
		if err != nil {
			return headgate.QuietGroupMetrics{}, err
		}
	}
	q := headgate.QuietGroupMetrics{
		ArrivalRate: float64(arrived) / 60.0, DrainRate: float64(completed) / 60.0,
		NoisyPartitions: uint32(len(noisy)), Approximate: approx || backlog >= sampleLimit,
	}
	if q.DrainRate > q.ArrivalRate && q.DrainRate > 0 {
		ttd := int64(float64(backlog) / (q.DrainRate - q.ArrivalRate) * 1000.0)
		q.TimeToDrainMs = &ttd
	}
	if oldestAt != nil {
		age := max(nowMs-*oldestAt, 0)
		q.OldestAvailableMs = &age
	}
	return q, nil
}

var _ headgate.InspectStore = (*PgxStore)(nil) // transactional API's compile-time capability check
var _ headgate.ResultInspectStore = (*PgxStore)(nil)
var _ headgate.OutputInspectStore = (*PgxStore)(nil)
var _ headgate.ProgressInspectStore = (*PgxStore)(nil)

const jobCols = `j.ulid, j.kind, j.queue, j.state::text, j.schema_version, j.priority,
	j.attempt, j.crash_attempt, j.max_attempts, j.partition_key, j.rate_class, j.sticky_worker,
	j.weight, j.fingerprint, j.enqueued_at_ms, j.scheduled_at_ms,
	j.periodic_schedule_id, j.periodic_tick_ms, j.finalized_at_ms, j.payload,
	j.errors::text, j.id, COALESCE((SELECT json_agg(t.tag ORDER BY t.tag) FROM headgate_job_tag t WHERE t.job_id=j.id),'[]')::text`

func scanJob(row pgx.Row, includePayload bool) (*headgate.JobSummary, int64, error) {
	var j headgate.JobSummary
	var schemaVersion, attempt, crash, maxAtt int32
	var payload []byte
	var internalID int64
	var tagsJSON string
	err := row.Scan(&j.ID, &j.Kind, &j.Queue, &j.State, &schemaVersion, &j.Priority,
		&attempt, &crash, &maxAtt, &j.PartitionKey, &j.RateClass, &j.StickyWorker, &j.Weight, &j.Fingerprint,
		&j.EnqueuedAtMs, &j.ScheduledAtMs, &j.PeriodicScheduleID, &j.PeriodicTickMs,
		&j.FinalizedAtMs, &payload, &j.ErrorsJSON,
		&internalID, &tagsJSON)
	if err != nil {
		return nil, 0, err
	}
	j.SchemaVersion = uint32(schemaVersion)
	j.Attempt, j.CrashAttempt, j.MaxAttempts = uint32(attempt), uint32(crash), uint32(maxAtt)
	_ = json.Unmarshal([]byte(tagsJSON), &j.Tags)
	if includePayload {
		j.Payload = payload // invariant 9: withheld unless explicitly requested
	}
	return &j, internalID, nil
}

func (s *PgxStore) GetJob(ctx context.Context, id string, includePayload bool) (*headgate.JobSummary, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM headgate_job j WHERE j.ulid = $1`, id)
	j, _, err := scanJob(row, includePayload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

func (s *PgxStore) GetJobResult(ctx context.Context, id string) (*headgate.JobResult, error) {
	var version int32
	var bytes []byte
	err := s.pool.QueryRow(ctx, `SELECT result_schema_version, result_bytes
		FROM headgate_job WHERE ulid = $1 AND result_schema_version IS NOT NULL`, id).
		Scan(&version, &bytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &headgate.JobResult{SchemaVersion: uint32(version), Bytes: bytes}, nil
}

func (s *PgxStore) GetJobOutput(ctx context.Context, id string) (*headgate.JobOutput, error) {
	var version int32
	var bytes []byte
	var fence, updatedAtMs int64
	err := s.pool.QueryRow(ctx, `SELECT output_schema_version, output_bytes, output_fence,
		output_updated_at_ms FROM headgate_job
		WHERE ulid = $1 AND output_schema_version IS NOT NULL`, id).
		Scan(&version, &bytes, &fence, &updatedAtMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &headgate.JobOutput{
		SchemaVersion: uint32(version), Bytes: bytes, Fence: uint64(fence), UpdatedAtMs: updatedAtMs,
	}, nil
}

func (s *PgxStore) GetJobProgress(ctx context.Context, id string) (*headgate.JobProgress, error) {
	var current, total, fence int64
	var message *string
	var updatedAtMs int64
	err := s.pool.QueryRow(ctx, `SELECT progress_current, progress_total, progress_message,
		progress_fence, progress_updated_at_ms FROM headgate_job
		WHERE ulid = $1 AND progress_current IS NOT NULL`, id).
		Scan(&current, &total, &message, &fence, &updatedAtMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	progress := &headgate.JobProgress{
		Current: uint64(current), Total: uint64(total), Fence: uint64(fence), UpdatedAtMs: updatedAtMs,
	}
	if message != nil {
		progress.Message = *message
	}
	return progress, nil
}

func (s *PgxStore) ListJobs(ctx context.Context, f headgate.JobFilter, cursor string, limit uint32) (headgate.JobPage, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > maxPage {
		limit = maxPage
	}
	clauses := []string{}
	args := []any{}
	bind := func(sql string, v any) {
		args = append(args, v)
		clauses = append(clauses, fmt.Sprintf(sql, len(args)))
	}
	// `!= nil`, not `!= ""`. An explicitly empty value is a filter FOR the
	// empty value — `partition_key = ''` is the default partition and the most common
	// one there is. `!= ""` answered with the whole queue instead. Mirrors Rust's
	// `if let Some(v) = filter.x` exactly.
	if f.Queue != nil {
		bind("j.queue = $%d", *f.Queue)
	}
	if f.Kind != nil {
		bind("j.kind = $%d", *f.Kind)
	}
	if f.KindPrefix != nil {
		bind("starts_with(j.kind, $%d)", *f.KindPrefix)
	}
	if f.PartitionKey != nil {
		bind("j.partition_key = $%d", *f.PartitionKey)
	}
	if f.State != nil {
		bind("j.state::text = $%d", *f.State)
	}
	if f.ID != nil {
		bind("j.ulid = $%d", *f.ID)
	}
	if f.Fingerprint != nil {
		bind("j.fingerprint = $%d", *f.Fingerprint)
	}
	if f.RateClass != nil {
		bind("j.rate_class = $%d", *f.RateClass)
	}
	if f.Priority != nil {
		bind("j.priority = $%d", *f.Priority)
	}
	if len(f.TagsAll) > 0 {
		bind("NOT EXISTS (SELECT 1 FROM unnest($%d::text[]) want(tag) WHERE NOT EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag=want.tag))", f.TagsAll)
	}
	if len(f.TagsAny) > 0 {
		bind("EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag=ANY($%d::text[]))", f.TagsAny)
	}
	// Newest first; the cursor is the last row's internal id.
	cursorID := int64(1<<63 - 1)
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &cursorID); err != nil {
			return headgate.JobPage{}, &headgate.InvalidError{Msg: "bad cursor"}
		}
	}
	bind("j.id < $%d", cursorID)
	args = append(args, int64(limit))
	sql := fmt.Sprintf(`SELECT `+jobCols+` FROM headgate_job j WHERE %s ORDER BY j.id DESC LIMIT $%d`,
		strings.Join(clauses, " AND "), len(args))
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return headgate.JobPage{}, err
	}
	defer rows.Close()
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
	if uint32(len(page.Jobs)) == limit {
		page.NextCursor = fmt.Sprintf("%d", lastInternal)
	}
	return page, rows.Err()
}

func (s *PgxStore) Counts(ctx context.Context, queue *string) (headgate.StateCounts, error) {
	// nil = every queue; a pointer to "" = the queue literally named "". The SQL below
	// already distinguishes them ($1 IS NULL vs queue = $1) — only the port could not.
	var q any
	if queue != nil {
		q = *queue
	}
	rows, err := s.pool.Query(ctx, `
		WITH sample AS (
		  SELECT state FROM headgate_job
		  WHERE ($1::text IS NULL OR queue = $1)
		  LIMIT $2
		)
		SELECT state::text, count(*)::bigint FROM sample GROUP BY 1`, q, sampleLimit)
	if err != nil {
		return headgate.StateCounts{}, err
	}
	defer rows.Close()
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

func (s *PgxStore) QueueStats(ctx context.Context) ([]headgate.QueueStatsView, error) {
	rows, err := s.pool.Query(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms),
		sample AS (SELECT queue, state FROM headgate_job LIMIT $1),
		names AS (
		  SELECT queue FROM headgate_queue_state
		  UNION SELECT queue FROM headgate_enqueue_policy
		  UNION SELECT queue FROM headgate_queue_counter, p
		        WHERE bucket_ms >= p.now_ms - 3600000
		  UNION SELECT DISTINCT queue FROM sample
		),
		by_state AS (
		  SELECT queue, state::text AS state, count(*)::bigint AS n
		  FROM sample GROUP BY 1, 2
		),
		rates AS (
		  SELECT c.queue,
		         sum(c.arrived)::float8 / 60.0 AS arrival,
		         sum(c.completed)::float8 / 60.0 AS drain
		  FROM headgate_queue_counter c, p
		  WHERE c.bucket_ms >= (p.now_ms / 60000 * 60000) - 60000
		  GROUP BY 1
		)
		SELECT n.queue,
		       p.now_ms,
		       COALESCE(qs.paused, false) AS paused,
		       COALESCE(qs.weight, 1) AS weight,
		       COALESCE(r.arrival, 0) AS arrival,
		       COALESCE(r.drain, 0) AS drain,
		       COALESCE((SELECT json_agg(json_build_array(b.state, b.n))
		                 FROM by_state b WHERE b.queue = n.queue), '[]'::json)::text AS states,
		       (SELECT count(*) FROM sample) >= $1 AS approx,
		       (SELECT j.scheduled_at_ms FROM headgate_job j
		        WHERE j.queue = n.queue AND j.state = 'available'
		        ORDER BY j.scheduled_at_ms, j.id LIMIT 1) AS oldest_available_at_ms,
		       ep.max_unfinished_jobs, COALESCE(ent.n, 0), COALESCE(ext.n, 0),
		       qsamp.memory_bytes
		FROM names n CROSS JOIN p
		LEFT JOIN headgate_queue_state qs ON qs.queue = n.queue
		LEFT JOIN rates r ON r.queue = n.queue
		LEFT JOIN headgate_enqueue_policy ep ON ep.queue = n.queue
		LEFT JOIN headgate_enqueue_counter ent
		  ON ent.queue = n.queue AND ent.counter_kind = 'entered'
		LEFT JOIN headgate_enqueue_counter ext
		  ON ext.queue = n.queue AND ext.counter_kind = 'exited'
		LEFT JOIN headgate_queue_sample qsamp ON qsamp.queue = n.queue
		ORDER BY n.queue`, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.QueueStatsView
	for rows.Next() {
		var v headgate.QueueStatsView
		var statesJSON string
		var nowMs int64
		var oldestAt *int64
		var maxUnfinished *int64
		var entered, exited int64
		if err := rows.Scan(&v.Queue, &nowMs, &v.Paused, &v.Weight, &v.ArrivalRate, &v.DrainRate,
			&statesJSON, &v.CountsApproximate, &oldestAt, &maxUnfinished, &entered, &exited, &v.MemoryBytes); err != nil {
			return nil, err
		}
		if oldestAt != nil {
			age := max(nowMs-*oldestAt, 0)
			v.OldestAvailableMs = &age
		}
		var pairs [][2]any
		_ = json.Unmarshal([]byte(statesJSON), &pairs)
		v.ByState = map[string]int64{}
		v.UnfinishedJobs = uint64(max(int64(0), entered-exited))
		if maxUnfinished != nil {
			n := uint64(*maxUnfinished)
			v.MaxUnfinishedJobs = &n
		}
		for _, p := range pairs {
			st, _ := p[0].(string)
			n, _ := p[1].(float64)
			v.ByState[st] = int64(n)
		}
		// backlog metrics time-to-drain: nil when arrival >= drain — the alert condition.
		if v.DrainRate > v.ArrivalRate && v.DrainRate > 0 {
			ttd := int64(float64(v.UnfinishedJobs) / (v.DrainRate - v.ArrivalRate) * 1000.0)
			v.TimeToDrainMs = &ttd
		}
		v.QuietGroups, err = s.quietGroupMetrics(ctx, v.Queue, nowMs)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *PgxStore) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_queue_state (queue, paused) VALUES ($1, $2)
		ON CONFLICT (queue) DO UPDATE SET paused = EXCLUDED.paused`, queue, paused)
	return err
}

func (s *PgxStore) SetQueueWeight(ctx context.Context, queue string, weight uint32) error {
	if weight == 0 {
		return &headgate.InvalidError{Msg: "weight must be >= 1"}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_queue_state (queue, weight) VALUES ($1, $2)
		ON CONFLICT (queue) DO UPDATE SET
		  dispatch_count = floor(headgate_queue_state.dispatch_count::numeric
		                         * EXCLUDED.weight / headgate_queue_state.weight)::bigint,
		  weight = EXCLUDED.weight`, queue, weight)
	return err
}

func (s *PgxStore) SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error {
	var limit any
	if maxUnfinishedJobs != nil {
		if *maxUnfinishedJobs > uint64(^uint64(0)>>1) {
			return &headgate.InvalidError{Msg: "max_unfinished_jobs is too large"}
		}
		limit = int64(*maxUnfinishedJobs)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_enqueue_policy (queue, max_unfinished_jobs) VALUES ($1, $2)
		ON CONFLICT (queue) DO UPDATE SET max_unfinished_jobs = EXCLUDED.max_unfinished_jobs`, queue, limit)
	return err
}

func (s *PgxStore) RateClasses(ctx context.Context) ([]headgate.RateClassState, error) {
	rows, err := s.pool.Query(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms)
		SELECT b.name, b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((p.now_ms - b.refilled_at_ms) * b.limit_per_window / b.window_ms))
		            ELSE b.tokens END AS avail,
		       (SELECT count(*) FROM (
		          SELECT 1 FROM headgate_job w
		          WHERE w.state = 'available' AND w.rate_class = b.name LIMIT $1
		       ) t)::bigint AS waiting
		FROM headgate_rate_bucket b, p
		ORDER BY b.name`, positionLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *PgxStore) UpsertRateClass(ctx context.Context, cfg headgate.RateClassConfig) error {
	if cfg.WindowMs < 1 {
		// boundary validation, and it divides the refill. Text matches the Rust store word-for-word
		// (the mutation diff asserts error-message parity).
		return &headgate.InvalidError{Msg: "window_ms must be >= 1"}
	}
	if cfg.Limit < 0 || cfg.Burst < 1 {
		return &headgate.InvalidError{Msg: "limit must be >= 0 and burst >= 1"}
	}
	limit, tokensInsert := cfg.Limit, cfg.Burst
	if cfg.Paused {
		limit, tokensInsert = 0, 0
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_rate_bucket
		       (name, tokens, burst, limit_per_window, window_ms, refilled_at_ms)
		SELECT $1, $2, $3, $4, $5, `+nowMS+`
		ON CONFLICT (name) DO UPDATE SET
		  burst = EXCLUDED.burst,
		  limit_per_window = EXCLUDED.limit_per_window,
		  window_ms = EXCLUDED.window_ms,
		  tokens = CASE WHEN $6 THEN 0
		                ELSE LEAST(headgate_rate_bucket.tokens, EXCLUDED.burst) END,
		  refilled_at_ms = EXCLUDED.refilled_at_ms`,
		cfg.Name, tokensInsert, cfg.Burst, limit, cfg.WindowMs, cfg.Paused)
	return err
}

func (s *PgxStore) ConcurrencyLimits(ctx context.Context) ([]headgate.ConcurrencyLimit, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, queue, max_concurrent, on_saturated
		FROM headgate_concurrency_limit ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *PgxStore) UpsertConcurrencyLimit(ctx context.Context, cfg headgate.ConcurrencyLimit) error {
	if cfg.Name == "" || cfg.Queue == "" {
		return &headgate.InvalidError{Msg: "name and queue must not be empty"}
	}
	if cfg.MaxConcurrent == 0 {
		return &headgate.InvalidError{Msg: "max_concurrent must be >= 1"}
	}
	if !cfg.OnSaturated.Valid() {
		return &headgate.InvalidError{Msg: fmt.Sprintf("unknown saturation strategy `%s`", cfg.OnSaturated)}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_concurrency_limit
		       (name, queue, max_concurrent, on_saturated)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE SET
		  queue = EXCLUDED.queue,
		  max_concurrent = EXCLUDED.max_concurrent,
		  on_saturated = EXCLUDED.on_saturated`,
		cfg.Name, cfg.Queue, cfg.MaxConcurrent, cfg.OnSaturated)
	return err
}

func (s *PgxStore) Partitions(ctx context.Context, queue string) ([]headgate.PartitionState, error) {
	rows, err := s.pool.Query(ctx, `
		WITH sample AS (
		  SELECT partition_key FROM headgate_job
		  WHERE queue = $1 AND state = 'available' LIMIT $2
		),
		waiting AS (
		  SELECT partition_key, count(*)::bigint AS n FROM sample GROUP BY 1
		)
		SELECT COALESCE(w.partition_key, d.partition_key) AS partition_key,
		       COALESCE(d.deficit, 0) AS deficit,
		       COALESCE(w.n, 0) AS waiting
		FROM waiting w
		FULL OUTER JOIN headgate_partition_deficit d
		  ON d.queue = $1 AND d.partition_key = w.partition_key
		WHERE d.queue IS NULL OR d.queue = $1
		ORDER BY 1`, queue, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *PgxStore) QuarantineList(ctx context.Context) ([]headgate.QuarantineEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT fingerprint, kind, crash_count, quarantined_at_ms,
		       COALESCE(reason, '') AS reason
		FROM headgate_quarantine ORDER BY quarantined_at_ms DESC LIMIT $1`, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.QuarantineEntry
	for rows.Next() {
		var q headgate.QuarantineEntry
		var crash int32
		if err := rows.Scan(&q.Fingerprint, &q.Kind, &crash, &q.QuarantinedAtMs, &q.Reason); err != nil {
			return nil, err
		}
		q.CrashCount = int64(crash)
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *PgxStore) QuarantineRelease(ctx context.Context, fingerprint string) (uint64, error) {
	var released, deleted int64
	err := s.pool.QueryRow(ctx, `
		WITH p AS (SELECT `+nowMS+` AS now_ms),
		rel AS ( -- quarantined + operator_release -> available (the table's row)
		  UPDATE headgate_job j SET state = 'available', scheduled_at_ms = p.now_ms,
		         finalized_at_ms = NULL
		  FROM p WHERE j.fingerprint = $1 AND j.state = 'quarantined'
		  RETURNING j.queue, j.partition_key
		),
		-- tenant fairness/adaptive admission released jobs are available again, so their partitions rejoin the
		-- gate's set — in this statement, never a follow-up one.
		active AS (
		  INSERT INTO headgate_active_partition (queue, partition_key)
		  SELECT DISTINCT queue, partition_key FROM rel
		  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
		),
		del AS (
		  DELETE FROM headgate_quarantine WHERE fingerprint = $1 RETURNING 1
		)
		SELECT (SELECT count(*) FROM rel)::bigint, (SELECT count(*) FROM del)::bigint`,
		fingerprint).Scan(&released, &deleted)
	if err != nil {
		return 0, err
	}
	if released == 0 && deleted == 0 {
		// The `not found: ` prefix is REQUIRED, not decoration: Go has no NotFoundError
		// type, so headgateapi.storeErr classifies by exactly this prefix and everything
		// without it falls through to 400. Rust reaches the identical bytes through
		// StoreError::NotFound's Display. Omitting it here made this route a 400 in Go
		// and a 404 in Rust for four rounds, uncaught because no diff covered the path.
		return 0, headgate.NotFoundf("fingerprint %s is not quarantined", fingerprint)
	}
	return uint64(released), nil
}

func (s *PgxStore) jobState(ctx context.Context, id string) (string, bool, error) {
	var st string
	err := s.pool.QueryRow(ctx, `SELECT state::text FROM headgate_job WHERE ulid = $1`, id).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return st, err == nil, err
}

func (s *PgxStore) OperatorRetry(ctx context.Context, id string) error {
	var retried int64
	err := s.pool.QueryRow(ctx, `
		WITH upd AS (
		  UPDATE headgate_job SET state = 'available', scheduled_at_ms = `+nowMS+`,
		         finalized_at_ms = NULL
		  WHERE ulid = $1 AND state = 'archived'
		  RETURNING queue, partition_key
		),
		-- tenant fairness/adaptive admission retry-now makes the row available; list its partition here.
		active AS (
		  INSERT INTO headgate_active_partition (queue, partition_key)
		  SELECT queue, partition_key FROM upd
		  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
		)
		SELECT count(*)::bigint FROM upd`, id).Scan(&retried)
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

func (s *PgxStore) OperatorCancel(ctx context.Context, id string) error {
	// adaptive admission the decrement must key off the PRE-update state, and an UPDATE's RETURNING
	// reports the NEW row — by then state is 'cancelled' and lease_id is NULL, so "was it
	// running?" is unanswerable from there. The row is picked and locked first, in its own
	// CTE, and was_running is read off THAT. Cancelling a scheduled or available job must
	// not decrement a slot it never took.
	var n int64
	err := s.pool.QueryRow(ctx, `
		WITH pick AS (
		  SELECT j.id, j.queue, j.partition_key, (j.state = 'running') AS was_running
		  FROM headgate_job j
		  WHERE j.ulid = $1 AND j.state IN ('pending', 'scheduled', 'available', 'running')
		  FOR UPDATE
		),
		upd AS (
		  UPDATE headgate_job j SET state = 'cancelled', lease_id = NULL,
		         lease_expires_at_ms = NULL, claimed_by = NULL,
		         finalized_at_ms = `+nowMS+`
		  WHERE j.id IN (SELECT id FROM pick)
		  RETURNING 1
		),
		infl AS (`+inflightDec("(SELECT queue, partition_key FROM pick WHERE was_running)")+`)
		SELECT count(*)::bigint FROM upd`, id).Scan(&n)
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

func (s *PgxStore) DeleteJob(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM headgate_job WHERE ulid = $1 AND state <> 'running'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
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

func (s *PgxStore) History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]headgate.HistoryBucket, error) {
	if bucketMs < 60_000 {
		return nil, &headgate.InvalidError{Msg: "bucket_ms must be >= 60000 (the stored granularity)"}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT (bucket_ms / $2) * $2 AS at_ms,
		       sum(arrived)::bigint AS arrived, sum(completed)::bigint AS completed
		FROM headgate_queue_counter
		WHERE queue = $1 AND bucket_ms >= $3
		GROUP BY 1 ORDER BY 1 LIMIT 10000`, queue, bucketMs, sinceMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *PgxStore) QuarantineSweep(ctx context.Context, limit int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH pick AS (
		  SELECT j.id FROM headgate_job j
		  WHERE j.state IN ('pending', 'available', 'scheduled', 'retryable')
		    AND j.fingerprint IN (SELECT fingerprint FROM headgate_quarantine)
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		)
		UPDATE headgate_job j
		SET state = 'quarantined', finalized_at_ms = `+nowMS+`
		WHERE j.id IN (SELECT id FROM pick)`, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *PgxStore) RescheduleJob(ctx context.Context, id string, atMs int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE headgate_job SET scheduled_at_ms = $2
		WHERE ulid = $1 AND state IN ('scheduled', 'retryable')`, id, atMs)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
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

func (s *PgxStore) EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE headgate_job
		SET payload = $2, schema_version = $3, fingerprint = $4
		WHERE ulid = $1 AND state <> 'running'`,
		id, payload, int32(schemaVersion), fingerprint)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
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

// ExplainAdmission replays the gate's own clause order read-only for one job — the
// admission policy/control plane endpoint only this design needs. Same query and assembly as the Rust
// adapter's explain_admission.
func (s *PgxStore) ExplainAdmission(ctx context.Context, id string) (*headgate.AdmissionExplain, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT j.state::text AS state, j.queue, j.scheduled_at_ms, j.priority,
		       j.rate_class, j.partition_key, j.fingerprint, j.id,
		       j.weight::bigint AS weight,
		       `+nowMS+` AS now_ms,
		       COALESCE(qs.paused, false) AS paused,
		       (q.fingerprint IS NOT NULL) AS quarantined,
		       b.burst, b.limit_per_window, b.window_ms,
		       CASE WHEN b.name IS NULL THEN NULL
		            WHEN b.limit_per_window > 0 AND b.window_ms > 0
		            THEN LEAST(b.burst, b.tokens +
		                 ((`+nowMS+` - b.refilled_at_ms) * b.limit_per_window / b.window_ms))
		            ELSE b.tokens END AS avail,
		       COALESCE(d.deficit, 0) AS deficit,
		       cl.max_concurrent, cl.on_saturated,
		       -- adaptive admission read the counter the GATE reads, not a fresh count of running
		       -- rows. "Why is this job not running" must answer for the gate that is
		       -- actually deciding: if headgate_inflight ever drifts, an explain that
		       -- quietly recomputed the truth would report a ceiling as clear while
		       -- admission kept refusing — the one failure this endpoint exists to make
		       -- visible. Also O(1) instead of O(running).
		       COALESCE((SELECT f.n FROM headgate_inflight f
		                 WHERE f.queue = j.queue
		                   AND f.partition_key = j.partition_key), 0) AS inflight,
		       (SELECT COALESCE(sum(t.weight), 0)::bigint FROM (
		          SELECT a.weight FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.rate_class = j.rate_class
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms, a.id) < (j.scheduled_at_ms, j.id)))
		          ORDER BY a.priority DESC, a.scheduled_at_ms, a.id
		          LIMIT $2
		       ) t) AS cost_ahead_in_class,
		       (SELECT count(*) FROM (
		          SELECT 1 FROM headgate_job a
		          WHERE a.state = 'available' AND a.queue = j.queue
		            AND a.partition_key = j.partition_key
		            AND (a.priority > j.priority
		                 OR (a.priority = j.priority
		                     AND (a.scheduled_at_ms, a.id) < (j.scheduled_at_ms, j.id)))
		          LIMIT $2
		       ) t)::bigint AS ahead_in_partition
		FROM headgate_job j
		LEFT JOIN headgate_queue_state qs ON qs.queue = j.queue
		LEFT JOIN headgate_quarantine q ON q.fingerprint = j.fingerprint
		LEFT JOIN headgate_rate_bucket b ON b.name = j.rate_class AND j.rate_class <> ''
		LEFT JOIN headgate_partition_deficit d
		       ON d.queue = j.queue AND d.partition_key = j.partition_key
		LEFT JOIN headgate_concurrency_limit cl ON cl.queue = j.queue
		WHERE j.ulid = $1`, id, positionLimit)

	var (
		state, queue, rateClass, partitionKey, fingerprint string
		scheduledAt, nowMs, deficit, inflight, weight      int64
		aheadInClass, aheadInPartition, internalID         int64
		priority                                           int32
		paused, quarantined                                bool
		burst, limitPerWindow, windowMs, avail, maxConc    *int64
		onSaturated                                        *string
	)
	err := row.Scan(&state, &queue, &scheduledAt, &priority, &rateClass, &partitionKey,
		&fingerprint, &internalID, &weight, &nowMs, &paused, &quarantined,
		&burst, &limitPerWindow, &windowMs, &avail, &deficit, &maxConc, &onSaturated,
		&inflight, &aheadInClass, &aheadInPartition)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	ex := &headgate.AdmissionExplain{State: state, Detail: map[string]string{"state": state}}
	block := func(by string, eta *int64) *headgate.AdmissionExplain {
		ex.Admissible, ex.BlockedBy, ex.EstimatedAdmissionMs = false, by, eta
		return ex
	}
	zero := int64(0)
	switch state {
	case "running":
		ex.Admissible, ex.EstimatedAdmissionMs = true, &zero
		return ex, nil
	case "scheduled", "retryable":
		ex.Detail["scheduled_at_ms"] = fmt.Sprint(scheduledAt)
		eta := max64(scheduledAt-nowMs, 0)
		return block("schedule", &eta), nil
	case "quarantined":
		return block("quarantine", nil), nil // will not clear on its own
	case "available":
	default: // terminal
		ex.Admissible = false
		return ex, nil
	}
	// Available: the gate's clauses in the gate's order.
	if paused {
		return block("queue_paused", nil), nil
	}
	if scheduledAt > nowMs {
		ex.Detail["scheduled_at_ms"] = fmt.Sprint(scheduledAt)
		eta := scheduledAt - nowMs
		return block("schedule", &eta), nil
	}
	if quarantined {
		ex.Detail["fingerprint"] = fingerprint
		return block("quarantine", nil), nil
	}
	if rateClass != "" {
		required := aheadInClass + max64(weight, 1)
		ex.Detail["rate_class"] = rateClass
		ex.Detail["weight"] = fmt.Sprint(max64(weight, 1))
		ex.Detail["tokens_ahead_in_class"] = fmt.Sprint(aheadInClass)
		if avail == nil {
			// an unconfigured class is UNLIMITED, not blocking — the gate's
			// `b.name IS NULL OR ...` fail-open arm. Still reported, because "you named a
			// rate class that does not exist" is worth seeing even when nothing stalls.
			ex.Detail["tokens_available"] = "unlimited (no such rate class)"
		} else {
			ex.Detail["tokens_available"] = fmt.Sprint(*avail)
			if *avail < required {
				var eta *int64
				if limitPerWindow != nil && *limitPerWindow > 0 && windowMs != nil {
					e := max64(required-*avail, 1) * *windowMs / *limitPerWindow
					eta = &e
				}
				return block("rate_class", eta), nil
			}
		}
	}
	if maxConc != nil {
		ex.Detail["max_concurrent"] = fmt.Sprint(*maxConc)
		ex.Detail["inflight"] = fmt.Sprint(inflight)
		strategy := string(headgate.SaturateQueue)
		if onSaturated != nil {
			strategy = *onSaturated
		}
		ex.Detail["on_saturated"] = strategy
		if inflight >= *maxConc && strategy != string(headgate.SaturateCancelRunning) {
			return block("concurrency_limit", nil), nil // clears when something finishes
		}
	}
	// Fairness never blocks outright — it is work-conserving (invariant 11).
	ex.Detail["position_in_partition"] = fmt.Sprint(aheadInPartition)
	ex.Detail["partition_deficit"] = fmt.Sprint(deficit)
	ex.Admissible, ex.EstimatedAdmissionMs = true, &zero
	return ex, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

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
	switch p {
	case headgate.MissedRunOnce:
		return "run_once"
	case headgate.MissedBackfill:
		return "backfill"
	default:
		return "skip"
	}
}

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

func (s *PgxStore) ListScheduleEvents(ctx context.Context, scheduleID string, beforeEventID uint64, limit uint32) ([]headgate.ScheduleEvent, error) {
	if limit == 0 || limit > headgate.ScheduleEventLimit {
		return nil, headgate.Invalidf("schedule event limit must be between 1 and 100")
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

// ---------- worker registry + surveyed policy behavior control channel ----------

func (s *PgxStore) HeartbeatWorker(ctx context.Context, w headgate.WorkerMeta) (string, error) {
	var cmd *string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO headgate_worker
		       (worker_id, host, pid, queues, concurrency, started_at_ms, heartbeat_at_ms,
		        inflight, polls, empty_polls)
		SELECT $1, $2, $3, $4, $5, $6, `+nowMS+`, $7, $8, $9
		ON CONFLICT (worker_id) DO UPDATE SET
		  queues = EXCLUDED.queues, concurrency = EXCLUDED.concurrency,
		  heartbeat_at_ms = EXCLUDED.heartbeat_at_ms,
		  -- ADDITIVE: the cluster view's and backlog metrics's inputs are LEVELS, so the
		  -- beat overwrites them rather than accumulating. A worker that stops beating
		  -- keeps its last reported level and ages out as stale.
		  inflight = EXCLUDED.inflight, polls = EXCLUDED.polls,
		  empty_polls = EXCLUDED.empty_polls
		RETURNING command`,
		w.WorkerID, w.Host, w.PID, w.Queues, int32(w.Concurrency), w.StartedAtMs,
		int32(w.Inflight), int64(w.Polls), int64(w.EmptyPolls)).Scan(&cmd)
	if err != nil {
		return "", err
	}
	if cmd == nil {
		return "", nil
	}
	return *cmd, nil
}

func (s *PgxStore) ListWorkers(ctx context.Context, staleAfterMs int64) ([]headgate.WorkerMeta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT worker_id, host, pid, queues, concurrency, started_at_ms, heartbeat_at_ms,
		       inflight, polls, empty_polls
		FROM headgate_worker
		WHERE heartbeat_at_ms >= `+nowMS+` - $1
		ORDER BY worker_id LIMIT 10000`, staleAfterMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.WorkerMeta
	for rows.Next() {
		var w headgate.WorkerMeta
		var conc, inflight int32
		var polls, emptyPolls int64
		if err := rows.Scan(&w.WorkerID, &w.Host, &w.PID, &w.Queues, &conc,
			&w.StartedAtMs, &w.HeartbeatAtMs, &inflight, &polls, &emptyPolls); err != nil {
			return nil, err
		}
		w.Concurrency = uint32(conc)
		w.Inflight, w.Polls, w.EmptyPolls = uint32(inflight), uint64(polls), uint64(emptyPolls)
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *PgxStore) SignalWorker(ctx context.Context, workerID, command string) error {
	var cmd any
	if command != "" {
		switch command {
		case "quiet", "resume", "restart", "terminate", "resign":
			cmd = command
		default:
			return &headgate.InvalidError{Msg: "command must be quiet, resume, restart, terminate, or resign"}
		}
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE headgate_worker SET command = $2 WHERE worker_id = $1`, workerID, cmd)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return headgate.NotFoundf("worker %s", workerID)
	}
	return nil
}

func (s *PgxStore) DistinctKinds(ctx context.Context, limit int64) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT kind FROM (
		  SELECT kind FROM headgate_job
		  WHERE state IN ('available', 'scheduled', 'retryable')
		  LIMIT $1
		) t ORDER BY kind`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func actionStates(action string) (string, bool) {
	switch action {
	case "retry":
		return "('archived')", true
	case "cancel":
		return "('scheduled', 'available', 'running')", true
	case "delete":
		return "('scheduled', 'available', 'retryable', 'completed', 'archived', 'cancelled', 'quarantined', 'undecodable')", true
	}
	return "", false
}

func selectorWhere(req headgate.BulkOp, allowed string, firstParam int) (string, []any) {
	clauses := []string{"j.state IN " + allowed}
	var args []any
	bind := func(sql string, v any) {
		args = append(args, v)
		clauses = append(clauses, fmt.Sprintf(sql, len(args)+firstParam-1))
	}
	if req.Queue != "" {
		bind("j.queue = $%d", req.Queue)
	}
	if req.State != "" {
		bind("j.state::text = $%d", req.State)
	}
	if req.Kind != "" {
		bind("j.kind = $%d", req.Kind)
	}
	if req.PartitionKey != "" {
		bind("j.partition_key = $%d", req.PartitionKey)
	}
	if req.OlderThanMs != nil {
		bind("j.enqueued_at_ms < "+nowMS+" - $%d", *req.OlderThanMs)
	}
	return strings.Join(clauses, " AND "), args
}

func (s *PgxStore) CreateOperation(ctx context.Context, req headgate.BulkOp) error {
	if req.Queue == "" && req.State == "" && req.Kind == "" && req.PartitionKey == "" &&
		req.OlderThanMs == nil {
		return &headgate.InvalidError{Msg: "empty selector is rejected"} // control API contract
	}
	allowed, ok := actionStates(req.Action)
	if !ok {
		return headgate.Invalidf("unknown action `%s`", req.Action)
	}
	where, args := selectorWhere(req, allowed, 2)
	estArgs := append([]any{sampleLimit}, args...)
	var estimated int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*)::bigint FROM (SELECT 1 FROM headgate_job j WHERE `+where+` LIMIT $1) t`,
		estArgs...).Scan(&estimated)
	if err != nil {
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
	_, err = s.pool.Exec(ctx, `
		INSERT INTO headgate_operation
		       (id, action, selector, status, total_estimated, dry_run, created_at_ms)
		VALUES ($1, $2, $3, $4, $5, $6, `+nowMS+`)`,
		req.ID, req.Action, selector, status, estimated, req.DryRun)
	return err
}

func nz(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *PgxStore) GetOperation(ctx context.Context, id string) (*headgate.OperationStatus, error) {
	var op headgate.OperationStatus
	var errText *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, status, affected, total_estimated, dry_run, error
		FROM headgate_operation WHERE id = $1`, id).
		Scan(&op.ID, &op.Status, &op.Affected, &op.TotalEstimated, &op.DryRun, &errText)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if errText != nil {
		op.Error = *errText
	}
	return &op, nil
}

func (s *PgxStore) RunPendingOperations(ctx context.Context, batch int64) (uint64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, action, selector FROM headgate_operation
		WHERE status IN ('pending', 'running')
		ORDER BY created_at_ms LIMIT 5`)
	if err != nil {
		return 0, err
	}
	type opRow struct {
		id, action string
		selector   []byte
	}
	var ops []opRow
	for rows.Next() {
		var o opRow
		if err := rows.Scan(&o.id, &o.action, &o.selector); err != nil {
			rows.Close()
			return 0, err
		}
		ops = append(ops, o)
	}
	rows.Close()
	var total uint64
	for _, o := range ops {
		var sel struct {
			Queue        string `json:"queue"`
			State        string `json:"state"`
			Kind         string `json:"kind"`
			PartitionKey string `json:"partition_key"`
			OlderThanMs  *int64 `json:"older_than_ms"`
		}
		_ = json.Unmarshal(o.selector, &sel)
		req := headgate.BulkOp{
			ID: o.id, Action: o.action, Queue: sel.Queue, State: sel.State,
			Kind: sel.Kind, PartitionKey: sel.PartitionKey, OlderThanMs: sel.OlderThanMs,
		}
		n, err := s.runOperationBatch(ctx, req, batch)
		if err != nil {
			_, _ = s.pool.Exec(ctx,
				`UPDATE headgate_operation SET status = 'failed', error = $2 WHERE id = $1`,
				o.id, err.Error())
			continue
		}
		total += uint64(n)
		status := "running"
		if n < batch {
			status = "completed"
		}
		if _, err := s.pool.Exec(ctx,
			`UPDATE headgate_operation SET status = $2, affected = affected + $3 WHERE id = $1`,
			o.id, status, n); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (s *PgxStore) PromoteJob(ctx context.Context, id string) error {
	var n int64
	err := s.pool.QueryRow(ctx, `WITH moved AS (
		UPDATE headgate_job SET state='available', scheduled_at_ms=(extract(epoch from clock_timestamp())*1000)::bigint
		WHERE ulid=$1 AND state='pending' RETURNING queue,partition_key
	), active AS (
		INSERT INTO headgate_active_partition(queue,partition_key) SELECT queue,partition_key FROM moved
		ON CONFLICT(queue,partition_key) DO UPDATE SET queue=EXCLUDED.queue
	) SELECT count(*) FROM moved`, id).Scan(&n)
	if err != nil {
		return err
	}
	if n == 0 {
		return headgate.Invalidf("operator_promote is defined only from pending")
	}
	return nil
}

func queueDeleteID(now int64, queue string) string {
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, queue)
	return fmt.Sprintf("qdel-%d-%s", now, clean)
}

func (s *PgxStore) DeleteQueue(ctx context.Context, queue string, force bool) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err = tx.Exec(ctx, `INSERT INTO headgate_enqueue_policy(queue) VALUES($1) ON CONFLICT(queue) DO NOTHING`, queue); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `SELECT queue FROM headgate_enqueue_policy WHERE queue=$1 FOR UPDATE`, queue); err != nil {
		return "", err
	}
	var depth int64
	err = tx.QueryRow(ctx, `SELECT GREATEST(0,
		COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=$1 AND counter_kind='entered'),0)-
		COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=$1 AND counter_kind='exited'),0))`, queue).Scan(&depth)
	if err != nil {
		return "", err
	}
	if depth > 0 && !force {
		return "", headgate.Invalidf("queue is not empty; retry with force=true")
	}
	if depth == 0 {
		if _, err = tx.Exec(ctx, "DELETE FROM headgate_queue_state WHERE queue=$1", queue); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, "DELETE FROM headgate_enqueue_policy WHERE queue=$1", queue); err != nil {
			return "", err
		}
		return "", tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `UPDATE headgate_enqueue_policy SET max_unfinished_jobs=0 WHERE queue=$1`, queue); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	var now int64
	if err = s.pool.QueryRow(ctx, "SELECT (extract(epoch from clock_timestamp())*1000)::bigint").Scan(&now); err != nil {
		return "", err
	}
	id := queueDeleteID(now, queue)
	err = s.CreateOperation(ctx, headgate.BulkOp{ID: id, Action: "delete", Queue: queue})
	return id, err
}

func (s *PgxStore) SampleQueueMemory(ctx context.Context, limit uint32) (uint32, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `WITH queues AS (SELECT queue FROM headgate_queue_state ORDER BY queue LIMIT 200), samples AS (
		SELECT q.queue,COALESCE(sum(pg_column_size(j.*)),0)::bigint bytes,count(*)::int n FROM queues q
		LEFT JOIN LATERAL (SELECT j FROM headgate_job j WHERE j.queue=q.queue ORDER BY j.id DESC LIMIT $1) x(j) ON TRUE GROUP BY q.queue)
		INSERT INTO headgate_queue_sample(queue,memory_bytes,sampled_jobs,sampled_at_ms)
		SELECT queue,bytes,n,(extract(epoch from clock_timestamp())*1000)::bigint FROM samples
		ON CONFLICT(queue) DO UPDATE SET memory_bytes=EXCLUDED.memory_bytes,sampled_jobs=EXCLUDED.sampled_jobs,sampled_at_ms=EXCLUDED.sampled_at_ms RETURNING queue`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n uint32
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

func (s *PgxStore) runOperationBatch(ctx context.Context, req headgate.BulkOp, batch int64) (int64, error) {
	allowed, ok := actionStates(req.Action)
	if !ok {
		return 0, headgate.Invalidf("unknown action `%s`", req.Action)
	}
	where, args := selectorWhere(req, allowed, 2)
	pick := `SELECT j.id FROM headgate_job j WHERE ` + where +
		` ORDER BY j.id LIMIT $1 FOR UPDATE SKIP LOCKED`
	// adaptive admission cancel is the only bulk action whose allowed states include 'running', so it is
	// the only one that moves the inflight counter. It carries the pre-update state out
	// of the pick, for the same reason OperatorCancel does.
	pickState := `SELECT j.id, j.queue, j.partition_key, (j.state = 'running') AS was_running
		FROM headgate_job j WHERE ` + where +
		` ORDER BY j.id LIMIT $1 FOR UPDATE SKIP LOCKED`
	var stmt string
	switch req.Action {
	case "retry":
		// tenant fairness/adaptive admission `act` is a DATA-MODIFYING CTE, so it runs unconditionally and in this
		// same statement — the partitions are listed before anything can observe the rows
		// as available. Keeping the UPDATE as the outer statement preserves the row count
		// this function returns as operation progress.
		stmt = `WITH picked AS (` + pick + `),
			act AS (
			  INSERT INTO headgate_active_partition (queue, partition_key)
			  SELECT DISTINCT j.queue, j.partition_key FROM headgate_job j
			  WHERE j.id IN (SELECT id FROM picked)
			  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
			)
			UPDATE headgate_job j SET state = 'available', scheduled_at_ms = ` + nowMS + `,
			       finalized_at_ms = NULL
			WHERE j.id IN (SELECT id FROM picked)`
	case "cancel":
		stmt = `WITH picked AS (` + pickState + `),
			infl AS (` + inflightDec("(SELECT queue, partition_key FROM picked WHERE was_running)") + `)
			UPDATE headgate_job j SET state = 'cancelled', lease_id = NULL,
			       lease_expires_at_ms = NULL, claimed_by = NULL,
			       finalized_at_ms = ` + nowMS + `
			WHERE j.id IN (SELECT id FROM picked)`
	case "delete":
		stmt = `WITH picked AS (` + pick + `)
			DELETE FROM headgate_job j WHERE j.id IN (SELECT id FROM picked)`
	}
	all := append([]any{batch}, args...)
	tag, err := s.pool.Exec(ctx, stmt, all...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
