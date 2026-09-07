package headgatepgx

import (
	"context"

	headgate "github.com/mujhtech/headgate/go"
)

// ---------- worker registry + surveyed policy behavior control channel ----------

// HeartbeatWorker records worker liveness and returns its pending command.
func (s *PgxStore) HeartbeatWorker(ctx context.Context, w headgate.WorkerMeta) (string, error) {
	status := w.Status
	if status == "" {
		status = "running"
	}
	var cmd *string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO headgate_worker
		       (worker_id, host, pid, queues, concurrency, started_at_ms, heartbeat_at_ms,
		        inflight, polls, empty_polls, status, duties_active)
		SELECT $1, $2, $3, $4, $5, $6, `+nowMS+`, $7, $8, $9, $10, $11
		ON CONFLICT (worker_id) DO UPDATE SET
		  queues = EXCLUDED.queues, concurrency = EXCLUDED.concurrency,
		  heartbeat_at_ms = EXCLUDED.heartbeat_at_ms,
		  -- ADDITIVE: the cluster view's and backlog metrics's inputs are LEVELS, so the
		  -- beat overwrites them rather than accumulating. A worker that stops beating
		  -- keeps its last reported level and ages out as stale.
		  inflight = EXCLUDED.inflight, polls = EXCLUDED.polls,
		  empty_polls = EXCLUDED.empty_polls, status = EXCLUDED.status,
		  duties_active = EXCLUDED.duties_active
		RETURNING command`,
		w.WorkerID, w.Host, w.PID, w.Queues, int32(w.Concurrency), w.StartedAtMs,
		int32(w.Inflight), int64(w.Polls), int64(w.EmptyPolls), status, w.DutiesActive).Scan(&cmd)
	if err != nil {
		return "", err
	}
	if cmd == nil {
		return "", nil
	}
	return *cmd, nil
}

// ListWorkers returns workers whose heartbeat is newer than staleAfterMs.
func (s *PgxStore) ListWorkers(ctx context.Context, staleAfterMs int64) ([]headgate.WorkerMeta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT worker_id, host, pid, queues, concurrency, started_at_ms, heartbeat_at_ms,
		       inflight, polls, empty_polls, status, duties_active, command
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
		var command *string
		if err := rows.Scan(&w.WorkerID, &w.Host, &w.PID, &w.Queues, &conc,
			&w.StartedAtMs, &w.HeartbeatAtMs, &inflight, &polls, &emptyPolls,
			&w.Status, &w.DutiesActive, &command); err != nil {
			return nil, err
		}
		if command != nil {
			w.PendingCommand = *command
		}
		w.Concurrency = uint32(conc)
		w.Inflight, w.Polls, w.EmptyPolls = uint32(inflight), uint64(polls), uint64(emptyPolls)
		out = append(out, w)
	}
	return out, rows.Err()
}

// SignalWorker records a control command for workerID.
func (s *PgxStore) SignalWorker(ctx context.Context, workerID, command string) error {
	var cmd any
	if command != "" {
		if !headgate.ValidWorkerCommand(command) {
			return &headgate.InvalidError{Msg: "command must be quiet, resume, restart, terminate, or resign"}
		}
		cmd = command
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

// DistinctKinds returns a bounded list of known job kinds.
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
