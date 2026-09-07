package headgatepgx

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

// ---------- control API contract async bulk operations ----------

func actionStates(action string) (string, bool) {
	states, ok := headgate.BulkActionStates(action)
	if !ok {
		return "", false
	}
	return "('" + strings.Join(states, "', '") + "')", true
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

// CreateOperation queues an asynchronous bulk operation.
func (s *PgxStore) CreateOperation(ctx context.Context, req headgate.BulkOp) error {
	if !req.HasSelector() {
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

// GetOperation returns the status of a bulk operation.
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

// RunPendingOperations executes a bounded batch of pending bulk operations.
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

// PromoteJob moves a pending job into the admission queue.
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

// SchedulePendingJob moves a pending job to a future run time.
func (s *PgxStore) SchedulePendingJob(ctx context.Context, id string, atMs int64) error {
	if atMs <= 0 {
		return headgate.Invalidf("pending schedule timestamp must be positive")
	}
	result, err := s.pool.Exec(ctx, `UPDATE headgate_job SET state='scheduled', scheduled_at_ms=$2
		WHERE ulid=$1 AND state='pending'`, id, atMs)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return headgate.Invalidf("workflow_schedule is defined only from pending")
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

// DeleteQueue removes queue configuration and, when forced, its jobs.
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

// SampleQueueMemory refreshes bounded per-queue storage estimates.
func (s *PgxStore) SampleQueueMemory(ctx context.Context, limit uint32) (uint32, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > headgateshared.InspectionMemorySampleLimit {
		limit = headgateshared.InspectionMemorySampleLimit
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
	// cancel is the only bulk action whose allowed states include 'running', so it is
	// the only one that moves the inflight counter. It carries the pre-update state out
	// of the pick, for the same reason OperatorCancel does.
	pickState := `SELECT j.id, j.queue, j.partition_key, (j.state = 'running') AS was_running
		FROM headgate_job j WHERE ` + where +
		` ORDER BY j.id LIMIT $1 FOR UPDATE SKIP LOCKED`
	var stmt string
	switch req.Action {
	case "retry":
		// `act` is a DATA-MODIFYING CTE, so it runs unconditionally and in this
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
