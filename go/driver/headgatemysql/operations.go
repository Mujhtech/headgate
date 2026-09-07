package headgatemysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

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

// CreateOperation queues an asynchronous bulk operation.
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

// GetOperation returns the status of a bulk operation.
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

// RunPendingOperations executes a bounded batch of pending bulk operations.
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

// PromoteJob moves a pending job into the admission queue.
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

// SchedulePendingJob moves a pending job to a future run time.
func (s *MysqlStore) SchedulePendingJob(ctx context.Context, id string, atMs int64) error {
	if atMs <= 0 {
		return headgate.Invalidf("pending schedule timestamp must be positive")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET state='scheduled', scheduled_at_ms=?
		WHERE ulid=? AND state='pending'`, atMs, id)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return headgate.Invalidf("workflow_schedule is defined only from pending")
	}
	return nil
}

// DeleteQueue removes queue configuration and, when forced, its jobs.
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

// SampleQueueMemory refreshes bounded per-queue storage estimates.
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
		// cancel is the only bulk action whose allowed states include 'running', so
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
	// retry makes rows available, so the partitions are listed in the SAME
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
