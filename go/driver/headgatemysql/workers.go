package headgatemysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	headgate "github.com/mujhtech/headgate/go"
)

// ---------- worker registry + surveyed policy behavior control channel ----------

// HeartbeatWorker records worker liveness and returns its pending command.
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

// ListWorkers returns workers whose heartbeat is newer than staleAfterMs.
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

// SignalWorker records a control command for workerID.
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

// DistinctKinds returns a bounded list of known job kinds.
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
