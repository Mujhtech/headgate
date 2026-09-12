package headgatesqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

type sqliteJob struct {
	envelope                   headgate.Envelope
	state                      string
	leaseID                    sql.NullString
	fence                      int64
	retentionMs, scheduledAtMs int64
	checkpointJSON             sql.NullString
	checkpointCursor           []byte
	errorsJSON                 string
	rateCharge                 int64
}

const jobColumns = `id,kind,schema_version,payload,queue,partition_key,rate_class,weight,
	fingerprint,priority,attempt,crash_attempt,max_attempts,enqueued_at_ms,scheduled_at_ms,
	timeout_ms,deadline_ms,retention_ms,state,lease_id,fence,checkpoint_json,
	checkpoint_cursor,headers_json,tags_json,periodic_schedule_id,periodic_tick_ms,sticky_worker,
	errors_json,rate_charge`

func scanJob(row interface{ Scan(...any) error }) (sqliteJob, error) {
	var j sqliteJob
	var schema, weight, attempt, crashAttempt, maxAttempts int64
	var headers, tags sql.NullString
	err := row.Scan(
		&j.envelope.ID, &j.envelope.Kind, &schema, &j.envelope.Payload, &j.envelope.Queue,
		&j.envelope.PartitionKey, &j.envelope.RateClass, &weight, &j.envelope.Fingerprint,
		&j.envelope.Priority, &attempt, &crashAttempt, &maxAttempts, &j.envelope.EnqueuedAtMs,
		&j.scheduledAtMs, &j.envelope.TimeoutMs, &j.envelope.DeadlineMs, &j.retentionMs,
		&j.state, &j.leaseID, &j.fence, &j.checkpointJSON, &j.checkpointCursor, &headers, &tags,
		&j.envelope.PeriodicScheduleID, &j.envelope.PeriodicTickMs, &j.envelope.StickyWorker,
		&j.errorsJSON, &j.rateCharge,
	)
	if err != nil {
		return j, err
	}
	j.envelope.SchemaVersion = uint32(schema)
	j.envelope.Weight = uint32(weight)
	j.envelope.Attempt = uint32(attempt)
	j.envelope.CrashAttempt = uint32(crashAttempt)
	j.envelope.MaxAttempts = uint32(maxAttempts)
	j.envelope.ScheduledAtMs = j.scheduledAtMs
	j.envelope.RetentionMs = j.retentionMs
	if headers.Valid {
		j.envelope.Headers = headgate.DecodeHeaders([]byte(headers.String))
	}
	if tags.Valid {
		_ = json.Unmarshal([]byte(tags.String), &j.envelope.Tags)
	}
	return j, nil
}

func placeholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

// Admit atomically selects fair per-partition candidates and writes their leases.
func (s *SqliteStore) Admit(ctx context.Context, req headgate.AdmitRequest) ([]headgate.AdmissionUnit, error) {
	var err error
	req, leaseMs, err := headgate.NormalizeAdmitRequest(req)
	if err != nil {
		return nil, err
	}
	if len(req.Queues) == 0 || req.Capacity <= 0 {
		return nil, nil
	}
	if req.Quantum < 1 {
		req.Quantum = 1
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("beginning SQLite admission: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return nil, fmt.Errorf("reading SQLite store time: %w", err)
	}
	// Expired deadlines are terminalized before drawing candidates, so they cannot
	// remain permanently visible while never being claimable.
	if _, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state='cancelled',finalized_at_ms=?
		WHERE state='available' AND deadline_ms>0 AND deadline_ms<=?`, now, now); err != nil {
		return nil, fmt.Errorf("expiring SQLite deadlines: %w", err)
	}
	type queuePolicy struct{ weight, dispatch int64 }
	queues := make(map[string]*queuePolicy, len(req.Queues))
	for _, queue := range req.Queues {
		if _, err := tx.ExecContext(ctx, `INSERT INTO headgate_queue_state(queue) VALUES(?) ON CONFLICT(queue) DO NOTHING`, queue); err != nil {
			return nil, fmt.Errorf("seeding SQLite queue policy: %w", err)
		}
		var weight, dispatch int64
		var paused bool
		if err := tx.QueryRowContext(ctx, `SELECT weight,dispatch_count,paused FROM headgate_queue_state WHERE queue=?`, queue).Scan(&weight, &dispatch, &paused); err != nil {
			return nil, fmt.Errorf("reading SQLite queue policy: %w", err)
		}
		if !paused {
			queues[queue] = &queuePolicy{weight: weight, dispatch: dispatch}
		}
	}
	if len(queues) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("committing SQLite paused admission: %w", err)
		}
		return nil, nil
	}

	type rateBucket struct {
		tokens, burst, limit, window, refilled int64
		paused                                 bool
	}
	buckets := map[string]*rateBucket{}
	bucketRows, err := tx.QueryContext(ctx, `SELECT name,tokens,burst,limit_per_window,window_ms,refilled_at_ms,paused FROM headgate_rate_bucket`)
	if err != nil {
		return nil, fmt.Errorf("reading SQLite rate policies: %w", err)
	}
	for bucketRows.Next() {
		var name string
		b := &rateBucket{}
		if err := bucketRows.Scan(&name, &b.tokens, &b.burst, &b.limit, &b.window, &b.refilled, &b.paused); err != nil {
			_ = bucketRows.Close()
			return nil, err
		}
		if elapsed := now - b.refilled; elapsed > 0 && b.limit > 0 {
			b.tokens = min(b.burst, b.tokens+elapsed*b.limit/b.window)
		}
		b.refilled = now
		buckets[name] = b
	}
	if err := bucketRows.Close(); err != nil {
		return nil, err
	}

	type concurrencyPolicy struct {
		max      int64
		strategy string
	}
	limits := map[string]concurrencyPolicy{}
	limitRows, err := tx.QueryContext(ctx, `SELECT queue,max_concurrent,on_saturated FROM headgate_concurrency_limit`)
	if err != nil {
		return nil, fmt.Errorf("reading SQLite concurrency policies: %w", err)
	}
	for limitRows.Next() {
		var queue string
		var p concurrencyPolicy
		if err := limitRows.Scan(&queue, &p.max, &p.strategy); err != nil {
			_ = limitRows.Close()
			return nil, err
		}
		limits[queue] = p
	}
	if err := limitRows.Close(); err != nil {
		return nil, err
	}

	args := make([]any, 0, len(req.Queues)+5)
	for _, queue := range req.Queues {
		args = append(args, queue)
	}
	drawLimit := int64(req.Capacity) * int64(len(req.Queues)) * 4
	args = append(args, req.Worker, req.Worker, req.Capacity, req.Quantum, drawLimit)
	query := `SELECT ` + jobColumns + ` FROM (
		SELECT ` + jobColumns + `,
		ROW_NUMBER() OVER (PARTITION BY queue,partition_key ORDER BY priority DESC,scheduled_at_ms,id) AS rn
		FROM headgate_job WHERE state='available' AND queue IN (` + placeholders(len(req.Queues)) + `)
		AND (sticky_worker='' OR sticky_worker=? OR (?='' AND sticky_worker=''))
		AND NOT EXISTS (SELECT 1 FROM headgate_quarantine q WHERE q.fingerprint=headgate_job.fingerprint)
	) WHERE rn<=? ORDER BY CAST((rn-1)/? AS INTEGER),rn,priority DESC,scheduled_at_ms,id LIMIT ?`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("selecting SQLite admission candidates: %w", err)
	}
	jobsByQueue := map[string][]sqliteJob{}
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning SQLite candidate: %w", scanErr)
		}
		if _, enabled := queues[job.envelope.Queue]; enabled {
			jobsByQueue[job.envelope.Queue] = append(jobsByQueue[job.envelope.Queue], job)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing SQLite candidates: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating SQLite candidates: %w", err)
	}
	claims := make([]headgate.Claim, 0, req.Capacity)
	service := map[string]int64{}
	inflight := map[string]int64{}
	decisions := 0
	for decisions < req.Capacity {
		queue := ""
		for name, jobs := range jobsByQueue {
			if len(jobs) == 0 {
				continue
			}
			if queue == "" || queues[name].dispatch*queues[queue].weight < queues[queue].dispatch*queues[name].weight || (queues[name].dispatch*queues[queue].weight == queues[queue].dispatch*queues[name].weight && name < queue) {
				queue = name
			}
		}
		if queue == "" {
			break
		}
		job := jobsByQueue[queue][0]
		jobsByQueue[queue] = jobsByQueue[queue][1:]
		weight := int64(job.envelope.Weight)
		bucket, configuredRate := buckets[job.envelope.RateClass]
		if configuredRate && (bucket.paused || bucket.tokens < weight) {
			continue
		}

		key := queue + "\x00" + job.envelope.PartitionKey
		limit, configuredLimit := limits[queue]
		if configuredLimit {
			current, ok := inflight[key]
			if !ok {
				if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM headgate_job WHERE queue=? AND partition_key=? AND state='running'`, queue, job.envelope.PartitionKey).Scan(&current); err != nil {
					return nil, err
				}
				inflight[key] = current
			}
			if current >= limit.max {
				switch limit.strategy {
				case "queue":
					continue
				case "discard", "cancel_incoming":
					state := "archived"
					if limit.strategy == "cancel_incoming" {
						state = "cancelled"
					}
					if _, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state=?,finalized_at_ms=? WHERE id=? AND state='available'`, state, now, job.envelope.ID); err != nil {
						return nil, err
					}
					queues[queue].dispatch++
					service[queue]++
					decisions++
					continue
				case "cancel_running":
					var victim string
					err := tx.QueryRowContext(ctx, `SELECT id FROM headgate_job WHERE queue=? AND partition_key=? AND state='running' ORDER BY claimed_at_ms,id LIMIT 1`, queue, job.envelope.PartitionKey).Scan(&victim)
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					if err != nil {
						return nil, err
					}
					if _, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state='cancelled',finalized_at_ms=?,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL,fence=fence+1,rate_charge=0 WHERE id=? AND state='running'`, now, victim); err != nil {
						return nil, err
					}
					inflight[key]--
				}
			}
		}
		res, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state='running',lease_id=?,
			fence=fence+1,lease_expires_at_ms=?,claimed_at_ms=?,claimed_by=?,rate_charge=?
			WHERE id=? AND state='available'`, req.LeaseID, now+leaseMs, now, req.Worker,
			map[bool]int64{true: weight, false: 0}[configuredRate], job.envelope.ID)
		if err != nil {
			return nil, fmt.Errorf("claiming SQLite job: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("counting SQLite claim: %w", err)
		}
		if n != 1 {
			continue
		}
		if configuredRate {
			bucket.tokens -= weight
		}
		if configuredLimit {
			inflight[key]++
		}
		queues[queue].dispatch++
		service[queue]++
		decisions++
		cp := headgateshared.DecodeCheckpoint([]byte(job.checkpointJSON.String), job.checkpointCursor)
		claims = append(claims, headgate.Claim{
			Envelope: job.envelope, LeaseID: req.LeaseID, Fence: uint64(job.fence + 1),
			Expires: time.UnixMilli(now + leaseMs), Checkpoint: cp,
		})
	}
	for name, policy := range queues {
		if service[name] > 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE headgate_queue_state SET dispatch_count=? WHERE queue=?`, policy.dispatch, name); err != nil {
				return nil, err
			}
		}
	}
	for name, bucket := range buckets {
		if _, err := tx.ExecContext(ctx, `UPDATE headgate_rate_bucket SET tokens=?,refilled_at_ms=? WHERE name=?`, bucket.tokens, bucket.refilled, name); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing SQLite admission: %w", err)
	}
	return headgate.GroupAdmissionClaims(claims, 1), nil
}

// Ack records an attempt outcome when the caller still owns the fenced lease.
func (s *SqliteStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return s.AckAttemptWithActualWeight(ctx, lease, outcome, errMsg, delayMs, nil, nil)
}

// AckAttempt records an outcome and buffered attempt logs.
func (s *SqliteStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return s.AckAttemptWithActualWeight(ctx, lease, outcome, errMsg, delayMs, logs, nil)
}

// AckAttemptWithActualWeight applies the lifecycle transition and cost correction atomically.
func (s *SqliteStore) AckAttemptWithActualWeight(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string, actualWeight *uint32) error {
	if err := headgate.ValidateAckRequest(outcome, delayMs); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("beginning SQLite ack: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	job, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM headgate_job
		WHERE id=? AND state='running' AND lease_id=? AND fence=?`, lease.JobID, lease.LeaseID, int64(lease.Fence)))
	if errors.Is(err, sql.ErrNoRows) {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	if err != nil {
		return fmt.Errorf("reading SQLite leased job: %w", err)
	}
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return fmt.Errorf("reading SQLite store time: %w", err)
	}
	if actualWeight != nil && job.rateCharge > 0 {
		if err := reconcileActualWeightOn(ctx, tx, job, *actualWeight, now); err != nil {
			return err
		}
	}
	attempt := job.envelope.Attempt
	state := ""
	deleteRow := false
	scheduledAt := job.scheduledAtMs
	finalized := any(nil)
	record := false
	switch outcome {
	case headgate.OutcomeSuccess:
		if job.retentionMs == 0 {
			deleteRow = true
		} else {
			state, finalized = "completed", now
		}
		record = len(logs) > 0
	case headgate.OutcomeRetry:
		attempt++
		record = true
		if attempt < job.envelope.MaxAttempts {
			state = "retryable"
			delay := delayMs
			if delay <= 0 {
				delay = s.retryDelay(attempt)
			}
			scheduledAt = now + delay
		} else {
			state, finalized = "archived", now
		}
	case headgate.OutcomeSkip:
		state, finalized, record = "archived", now, errMsg != "" || len(logs) > 0
	case headgate.OutcomeRevoke:
		deleteRow = true
	case headgate.OutcomeSnooze:
		state, scheduledAt = "scheduled", now+delayMs
	case headgate.OutcomeUndecodable:
		state, finalized, record = "undecodable", now, true
	case headgate.OutcomeRateLimited:
		state = "available"
	default:
		return &headgate.InvalidError{Msg: "unknown outcome"}
	}
	if deleteRow {
		res, err := tx.ExecContext(ctx, `DELETE FROM headgate_job WHERE id=? AND state='running' AND lease_id=? AND fence=?`, lease.JobID, lease.LeaseID, int64(lease.Fence))
		if err != nil {
			return fmt.Errorf("deleting SQLite acked job: %w", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
	} else {
		errorsJSON := job.errorsJSON
		if record {
			errorsJSON = appendAttempt(errorsJSON, now, attempt, outcome.String(), errMsg, logs, 0)
		}
		charge := job.rateCharge
		if actualWeight != nil {
			charge = 0
		}
		res, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state=?,attempt=?,scheduled_at_ms=?,
			finalized_at_ms=?,errors_json=?,lease_id=NULL,lease_expires_at_ms=NULL,
			claimed_at_ms=NULL,claimed_by=NULL,rate_charge=?
			WHERE id=? AND state='running' AND lease_id=? AND fence=?`,
			state, attempt, scheduledAt, finalized, errorsJSON, charge,
			lease.JobID, lease.LeaseID, int64(lease.Fence))
		if err != nil {
			return fmt.Errorf("updating SQLite ack: %w", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return &headgate.LeaseRejectedError{JobID: lease.JobID}
		}
	}
	if outcome == headgate.OutcomeSuccess || outcome == headgate.OutcomeSkip || outcome == headgate.OutcomeRevoke || outcome == headgate.OutcomeUndecodable {
		bucket := now / 60_000 * 60_000
		if _, err := tx.ExecContext(ctx, `INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed)
			VALUES(?,?,0,1) ON CONFLICT(queue,bucket_ms) DO UPDATE SET completed=completed+1`, job.envelope.Queue, bucket); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing SQLite ack: %w", err)
	}
	return nil
}

func reconcileActualWeightOn(ctx context.Context, tx *sql.Tx, job sqliteJob, actual uint32, now int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE headgate_rate_bucket SET
		tokens=MIN(burst,MIN(burst,tokens+MAX(0,?-refilled_at_ms)*limit_per_window/window_ms)+?-?),
		refilled_at_ms=? WHERE name=?`, now, job.rateCharge, actual, now, job.envelope.RateClass)
	if err != nil {
		return fmt.Errorf("reconciling SQLite actual weight: %w", err)
	}
	return nil
}

func (s *SqliteStore) retryDelay(attempt uint32) int64 {
	shift := attempt - 1
	if shift > 20 {
		shift = 20
	}
	delay := s.options.RetryBase.Milliseconds() * (int64(1) << shift)
	if delay > s.options.RetryCap.Milliseconds() {
		return s.options.RetryCap.Milliseconds()
	}
	return delay
}

func appendAttempt(raw string, at int64, attempt uint32, outcome, errMsg string, logs []string, crash uint32) string {
	var history []map[string]any
	_ = json.Unmarshal([]byte(raw), &history)
	entry := map[string]any{"at_ms": at, "outcome": outcome}
	entry["attempt"] = attempt
	if crash > 0 {
		entry["crash_attempt"] = crash
	}
	if errMsg != "" {
		entry["error"] = errMsg
	}
	if len(logs) > 0 {
		entry["logs"] = logs
	}
	if len(history) >= 50 {
		history = history[len(history)-49:]
	}
	history = append(history, entry)
	encoded, _ := json.Marshal(history)
	return string(encoded)
}

// Renew extends every still-current lease and reports lost job IDs.
func (s *SqliteStore) Renew(ctx context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	leaseMs := lease.Milliseconds()
	if leaseMs < 1 {
		return nil, &headgate.InvalidError{Msg: "lease must be >= 1ms"}
	}
	var lost []string
	for _, item := range leases {
		res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET lease_expires_at_ms=`+nowMS+`+?
			WHERE id=? AND state='running' AND lease_id=? AND fence=?`, leaseMs, item.JobID, item.LeaseID, int64(item.Fence))
		if err != nil {
			return nil, fmt.Errorf("renewing SQLite lease: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			lost = append(lost, item.JobID)
		}
	}
	return lost, nil
}

// Checkpoint durably records resumable state under the current fence.
func (s *SqliteStore) Checkpoint(ctx context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET checkpoint_json=?,checkpoint_cursor=?
		WHERE id=? AND state='running' AND lease_id=? AND fence=?`,
		string(headgateshared.EncodeCheckpoint(cp)), cp.Cursor, lease.JobID, lease.LeaseID, int64(lease.Fence))
	if err != nil {
		return fmt.Errorf("checkpointing SQLite job: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return &headgate.LeaseRejectedError{JobID: lease.JobID}
	}
	return nil
}

// ReclaimExpired applies lease_lost using store-observed expiry.
func (s *SqliteStore) ReclaimExpired(ctx context.Context, limit int64) ([]headgate.Reclaimed, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("beginning SQLite reclaim: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return nil, fmt.Errorf("reading SQLite store time: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+jobColumns+` FROM headgate_job
		WHERE state='running' AND lease_expires_at_ms<=? ORDER BY lease_expires_at_ms,id LIMIT ?`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("selecting expired SQLite leases: %w", err)
	}
	var victims []sqliteJob
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning expired SQLite lease: %w", scanErr)
		}
		victims = append(victims, job)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing expired SQLite leases: %w", err)
	}
	var reclaimed []headgate.Reclaimed
	for _, job := range victims {
		crash := job.envelope.CrashAttempt + 1
		quarantined := int64(crash) >= s.options.CrashLimit
		state := "retryable"
		finalized := any(nil)
		scheduled := now + s.retryDelay(crash)
		if quarantined {
			state, finalized, scheduled = "quarantined", now, job.scheduledAtMs
		}
		checkpoint := headgateshared.DecodeCheckpoint([]byte(job.checkpointJSON.String), job.checkpointCursor)
		if checkpoint.InProgressStep != "" {
			if checkpoint.CrashesByStep == nil {
				checkpoint.CrashesByStep = map[string]uint32{}
			}
			checkpoint.CrashesByStep[checkpoint.InProgressStep]++
		}
		errorsJSON := appendAttempt(job.errorsJSON, now, job.envelope.Attempt, "lease_lost", "lease expired without ack", nil, crash)
		if _, err := tx.ExecContext(ctx, `UPDATE headgate_job SET state=?,crash_attempt=?,scheduled_at_ms=?,
			finalized_at_ms=?,errors_json=?,checkpoint_json=?,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL
			WHERE id=? AND state='running'`, state, crash, scheduled, finalized, errorsJSON,
			string(headgateshared.EncodeCheckpoint(checkpoint)), job.envelope.ID); err != nil {
			return nil, fmt.Errorf("reclaiming SQLite lease: %w", err)
		}
		if quarantined {
			if _, err := tx.ExecContext(ctx, `INSERT INTO headgate_quarantine(fingerprint,crash_count,quarantined_at_ms)
				VALUES(?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET crash_count=MAX(crash_count,excluded.crash_count)`,
				job.envelope.Fingerprint, crash, now); err != nil {
				return nil, fmt.Errorf("quarantining SQLite fingerprint: %w", err)
			}
		}
		reclaimed = append(reclaimed, headgate.Reclaimed{JobID: job.envelope.ID, Fingerprint: job.envelope.Fingerprint, CrashAttempt: crash, Quarantined: quarantined})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing SQLite reclaim: %w", err)
	}
	return reclaimed, nil
}

// PromoteDue makes a bounded set of due scheduled/retryable jobs available.
func (s *SqliteStore) PromoteDue(ctx context.Context, limit int64) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `UPDATE headgate_job SET state='available' WHERE id IN (
		SELECT id FROM headgate_job WHERE state IN ('scheduled','retryable') AND scheduled_at_ms<=`+nowMS+`
		ORDER BY scheduled_at_ms,id LIMIT ?)`, limit)
	if err != nil {
		return 0, fmt.Errorf("promoting due SQLite jobs: %w", err)
	}
	return res.RowsAffected()
}

// EvictRetained deletes a bounded set of lapsed terminal rows; quarantined is excluded.
func (s *SqliteStore) EvictRetained(ctx context.Context, limit int64) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM headgate_job WHERE id IN (
		SELECT id FROM headgate_job WHERE state IN ('completed','archived','cancelled','undecodable')
		AND retention_ms>0 AND finalized_at_ms+retention_ms<=`+nowMS+`
		ORDER BY finalized_at_ms,id LIMIT ?)`, limit)
	if err != nil {
		return 0, fmt.Errorf("evicting retained SQLite jobs: %w", err)
	}
	return res.RowsAffected()
}

// ClaimDuty claims or renews a singleton duty using store time.
func (s *SqliteStore) ClaimDuty(ctx context.Context, name, holder string, lease time.Duration) (bool, error) {
	leaseMs := lease.Milliseconds()
	if leaseMs < 1 {
		return false, &headgate.InvalidError{Msg: "duty lease must be >= 1ms"}
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO headgate_duty(name,holder,expires_at_ms)
		VALUES(?,?,`+nowMS+`+?) ON CONFLICT(name) DO UPDATE SET holder=excluded.holder,
		expires_at_ms=excluded.expires_at_ms WHERE headgate_duty.expires_at_ms<`+nowMS+`
		OR headgate_duty.holder=excluded.holder`, name, holder, leaseMs)
	if err != nil {
		return false, fmt.Errorf("claiming SQLite duty: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// ReleaseDuty makes a matching singleton duty immediately claimable.
func (s *SqliteStore) ReleaseDuty(ctx context.Context, name, holder string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE headgate_duty SET expires_at_ms=0 WHERE name=? AND holder=?`, name, holder)
	if err != nil {
		return fmt.Errorf("releasing SQLite duty: %w", err)
	}
	return nil
}

// Caps advertises the optional interfaces this store implements. SQLite remains
// deliberately poll-only, so Notifying is absent.
func (s *SqliteStore) Caps() headgate.Caps {
	return headgate.CapTransactional | headgate.CapInspect
}
