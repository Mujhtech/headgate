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
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

const (
	sampleLimit         = int64(headgateshared.InspectionSampleLimit)
	positionLimit       = int64(headgateshared.InspectionPositionLimit)
	quietPartitionLimit = int64(headgateshared.InspectionQuietPartitionLimit)
	maxPage             = uint32(headgateshared.InspectionMaxPage)
)

type quietPartMetric struct {
	partition          string
	inflight           int64
	arrived, completed int64
	oldestAt           *int64
}

const quietPartsSQL = `
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
		ORDER BY n.partition_key`

const quietBacklogSQL = `
	SELECT count(*)::bigint FROM (
	  SELECT 1 FROM headgate_job
	  WHERE queue = $1 AND partition_key = ANY($2)
	    AND state = ANY(ARRAY['pending','scheduled','available','running','retryable']::headgate_state[])
	  LIMIT $3
	) bounded`

func summarizeQuietParts(parts []quietPartMetric, nowMs int64) (headgate.QuietGroupMetrics, []string) {
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
	q := headgate.QuietGroupMetrics{
		ArrivalRate: float64(arrived) / 60.0, DrainRate: float64(completed) / 60.0,
		NoisyPartitions: uint32(len(noisy)), Approximate: approx,
	}
	if oldestAt != nil {
		age := headgate.AgeMillis(nowMs, *oldestAt)
		q.OldestAvailableMs = &age
	}
	return q, quietParts
}

func finishQuietBacklog(q *headgate.QuietGroupMetrics, backlog int64) {
	q.Approximate = q.Approximate || backlog >= sampleLimit
	q.TimeToDrainMs = headgate.TimeToDrainMillis(backlog, q.ArrivalRate, q.DrainRate)
}

// quietGroupMetricsBatch pipelines the two bounded quiet-group query phases over one
// connection each. QueueStats therefore uses a constant number of network round trips
// instead of two additional pool acquisitions per queue.
func (s *PgxStore) quietGroupMetricsBatch(ctx context.Context, queues []string, nowMs []int64) ([]headgate.QuietGroupMetrics, error) {
	metrics := make([]headgate.QuietGroupMetrics, len(queues))
	if len(queues) == 0 {
		return metrics, nil
	}
	quietParts := make([][]string, len(queues))
	first := &pgx.Batch{}
	for i, queue := range queues {
		first.Queue(s.pool.namespace.render(quietPartsSQL), queue, nowMs[i]/60_000*60_000-60_000, quietPartitionLimit+1)
	}
	results := s.pool.raw.SendBatch(ctx, first)
	for i := range queues {
		rows, err := results.Query()
		if err != nil {
			_ = results.Close()
			return nil, err
		}
		parts := make([]quietPartMetric, 0)
		for rows.Next() {
			var part quietPartMetric
			if err := rows.Scan(&part.partition, &part.inflight, &part.arrived, &part.completed, &part.oldestAt); err != nil {
				rows.Close()
				_ = results.Close()
				return nil, err
			}
			parts = append(parts, part)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			_ = results.Close()
			return nil, err
		}
		rows.Close()
		metrics[i], quietParts[i] = summarizeQuietParts(parts, nowMs[i])
	}
	if err := results.Close(); err != nil {
		return nil, err
	}

	second := &pgx.Batch{}
	indices := make([]int, 0, len(queues))
	for i, parts := range quietParts {
		if len(parts) == 0 {
			continue
		}
		indices = append(indices, i)
		second.Queue(s.pool.namespace.render(quietBacklogSQL), queues[i], parts, sampleLimit)
	}
	if len(indices) == 0 {
		return metrics, nil
	}
	results = s.pool.raw.SendBatch(ctx, second)
	for _, index := range indices {
		var backlog int64
		if err := results.QueryRow().Scan(&backlog); err != nil {
			_ = results.Close()
			return nil, err
		}
		finishQuietBacklog(&metrics[index], backlog)
	}
	if err := results.Close(); err != nil {
		return nil, err
	}
	return metrics, nil
}

var _ headgate.InspectStore = (*PgxStore)(nil) // transactional API's compile-time capability check
var _ headgate.ResultInspectStore = (*PgxStore)(nil)
var _ headgate.OutputInspectStore = (*PgxStore)(nil)
var _ headgate.ProgressInspectStore = (*PgxStore)(nil)
var _ headgate.CheckpointInspectStore = (*PgxStore)(nil)

const jobCols = `j.ulid, j.kind, j.queue, j.state::text, j.schema_version, j.priority,
	j.attempt, j.crash_attempt, j.max_attempts, j.partition_key, j.rate_class, j.sticky_worker,
	j.weight, j.fingerprint, j.enqueued_at_ms, j.scheduled_at_ms, j.claimed_at_ms,
	j.periodic_schedule_id, j.periodic_tick_ms, j.finalized_at_ms, j.payload, j.headers,
	j.errors::text, j.id, COALESCE((SELECT json_agg(t.tag ORDER BY t.tag) FROM headgate_job_tag t WHERE t.job_id=j.id),'[]')::text`

func scanJob(row pgx.Row, includePayload bool) (*headgate.JobSummary, int64, error) {
	var j headgate.JobSummary
	var schemaVersion, attempt, crash, maxAtt int32
	var payload []byte
	var headersJSON []byte
	var internalID int64
	var tagsJSON string
	err := row.Scan(&j.ID, &j.Kind, &j.Queue, &j.State, &schemaVersion, &j.Priority,
		&attempt, &crash, &maxAtt, &j.PartitionKey, &j.RateClass, &j.StickyWorker, &j.Weight, &j.Fingerprint,
		&j.EnqueuedAtMs, &j.ScheduledAtMs, &j.ClaimedAtMs, &j.PeriodicScheduleID, &j.PeriodicTickMs,
		&j.FinalizedAtMs, &payload, &headersJSON, &j.ErrorsJSON,
		&internalID, &tagsJSON)
	if err != nil {
		return nil, 0, err
	}
	j.SchemaVersion = uint32(schemaVersion)
	j.Attempt, j.CrashAttempt, j.MaxAttempts = uint32(attempt), uint32(crash), uint32(maxAtt)
	_ = json.Unmarshal([]byte(tagsJSON), &j.Tags)
	if includePayload {
		j.Payload = payload // invariant 9: withheld unless explicitly requested
		j.Headers = headgate.DecodeHeaders(headersJSON)
	}
	return &j, internalID, nil
}

// GetJob returns a job summary, optionally including its payload and headers.
func (s *PgxStore) GetJob(ctx context.Context, id string, includePayload bool) (*headgate.JobSummary, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM headgate_job j WHERE j.ulid = $1`, id)
	j, _, err := scanJob(row, includePayload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

// GetJobResult returns a completed job's durable result, if present.
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

// GetJobOutput returns a job's latest durable output, if present.
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

// GetJobProgress returns a job's latest progress update, if present.
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

// GetJobCheckpoint returns a job's latest resumable checkpoint.
func (s *PgxStore) GetJobCheckpoint(ctx context.Context, id string) (*headgate.Checkpoint, error) {
	var raw, cursor []byte
	err := s.pool.QueryRow(ctx,
		`SELECT checkpoint, cp_cursor FROM headgate_job WHERE ulid = $1`, id).
		Scan(&raw, &cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	checkpoint := decodeCheckpoint(raw, cursor)
	return &checkpoint, nil
}

// ListJobs returns a newest-first page matching f.
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

// Counts returns bounded per-state counts for one queue or all queues.
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

// QueueStats returns bounded operational statistics for known queues.
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
		ORDER BY n.queue LIMIT 10000`, sampleLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.QueueStatsView
	var quietNow []int64
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
			age := headgate.AgeMillis(nowMs, *oldestAt)
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
		v.TimeToDrainMs = headgate.TimeToDrainMillis(
			int64(v.UnfinishedJobs), v.ArrivalRate, v.DrainRate,
		)
		out = append(out, v)
		quietNow = append(quietNow, nowMs)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Release the query's pool connection before issuing quiet-group follow-ups. Holding
	// rows open here deadlocked a one-connection pool and could exhaust larger pools when
	// several /queues or /cluster requests arrived together.
	rows.Close()
	queues := make([]string, len(out))
	for i := range out {
		queues[i] = out[i].Queue
	}
	quiet, err := s.quietGroupMetricsBatch(ctx, queues, quietNow)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].QuietGroups = quiet[i]
	}
	return out, nil
}

// SetQueuePaused changes whether admission may draw from queue.
