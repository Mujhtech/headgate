package headgatemysql

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
)

// Enqueue inserts a batch of jobs atomically.
func (s *MysqlStore) Enqueue(ctx context.Context, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	// Validate before acquiring a connection. An invalid envelope remains a caller error
	// even when MySQL is unreachable; transport state must not change the taxonomy.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	scoped := append([]headgate.Envelope(nil), batch...)
	for i := range scoped {
		scoped[i].UniqueKey = headgate.EffectiveUniqueKey(scoped[i])
	}
	batch = scoped
	// the rows and their active-partition entries must land together: a crash
	// between them would leave an available job whose partition is not listed, which is
	// starvation. (EnqueueTx already supplies the caller's own transaction.)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return headgate.WrapUnavailable(err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	if err := s.enqueueOn(ctx, tx, batch); err != nil {
		var duplicate *headgate.DuplicateError
		if errors.As(err, &duplicate) && duplicate.Replaced {
			if commitErr := tx.Commit(); commitErr != nil {
				return headgate.WrapUnavailable(commitErr)
			}
			return err
		}
		return headgate.WrapUnavailable(err)
	}
	return headgate.WrapUnavailable(tx.Commit())
}

// execer is what enqueue needs from either *sql.DB or *sql.Tx.
type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *MysqlStore) enqueueOn(ctx context.Context, c execer, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	// typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	// idempotent enqueue identity the strict caller-supplied id contract, classified BEFORE anything is written
	// so the batch stays all-or-nothing (and this whole function already runs inside a
	// transaction, plain or caller-supplied). Matching content drops out — idempotent
	// success, what makes the API's Idempotency-Key replay safe; different content
	// rejects the whole batch naming the offender. A terminal row still counts as
	// existing; reuse follows retention eviction.
	{
		ids := make([]any, len(batch))
		for i, e := range batch {
			ids[i] = e.ID
		}
		present := map[string][3]string{}
		rows, err := c.QueryContext(ctx,
			`SELECT ulid, kind, fingerprint, queue FROM headgate_job WHERE ulid IN (`+
				placeholders(len(batch))+`)`, ids...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, kind, fp, queue string
			if err := rows.Scan(&id, &kind, &fp, &queue); err != nil {
				_ = rows.Close()
				return err
			}
			present[id] = [3]string{kind, fp, queue}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		kept := make([]headgate.Envelope, 0, len(batch))
		for _, e := range batch {
			ex, exists := present[e.ID]
			switch {
			case !exists:
				kept = append(kept, e)
			case headgate.SameJobContent(e, ex[0], ex[1], ex[2]):
				// idempotent replay — do not re-write it, do not duplicate it
			default:
				return &headgate.IDConflictError{JobID: e.ID}
			}
		}
		if len(kept) == 0 {
			return nil // every row already exists, unchanged — nothing to write
		}
		batch = kept
	}
	// crash quarantine quarantined fingerprints are rejected before anything is written.
	var fps []any
	for _, e := range batch {
		if e.Fingerprint != "" {
			fps = append(fps, e.Fingerprint)
		}
	}
	if len(fps) > 0 {
		var fp string
		err := c.QueryRowContext(ctx,
			"SELECT fingerprint FROM headgate_quarantine WHERE fingerprint IN ("+
				placeholders(len(fps))+") LIMIT 1", fps...).Scan(&fp)
		if err == nil {
			return &headgate.QuarantinedError{Fingerprint: fp}
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	// Exact producer depth lives in two PK counter rows. Lock policy rows in sorted
	// order so concurrent multi-queue batches cannot overrun or deadlock. Idempotent
	// replays were removed above and therefore consume no slot.
	demand := map[string]int64{}
	for _, e := range batch {
		demand[headgate.EnqueueQueue(e)]++
	}
	demandQueues := make([]string, 0, len(demand))
	for queue := range demand {
		demandQueues = append(demandQueues, queue)
	}
	slices.Sort(demandQueues)
	policyArgs := make([]any, len(demandQueues))
	for i, queue := range demandQueues {
		policyArgs[i] = queue
	}
	if _, err := c.ExecContext(ctx,
		`INSERT INTO headgate_enqueue_policy (queue) VALUES `+
			strings.TrimSuffix(strings.Repeat("(?), ", len(demandQueues)), ", ")+`
		 AS new ON DUPLICATE KEY UPDATE queue = new.queue`, policyArgs...); err != nil {
		return err
	}
	// Materialize both counter rows before the locking read. A LEFT JOIN FOR UPDATE
	// against absent rows takes next-key gap locks; producers for different queues can
	// then each hold the same gap and deadlock when the INSERT trigger creates `entered`.
	// Sorted PK inserts make the query below take record locks instead.
	counterArgs := make([]any, 0, len(demandQueues)*2)
	for _, queue := range demandQueues {
		counterArgs = append(counterArgs, queue, queue)
	}
	if _, err := c.ExecContext(ctx,
		`INSERT IGNORE INTO headgate_enqueue_counter (queue, counter_kind, n) VALUES `+
			strings.TrimSuffix(strings.Repeat("(?, 'entered', 0), (?, 'exited', 0), ", len(demandQueues)), ", "),
		counterArgs...); err != nil {
		return err
	}
	// The sorted no-op upsert takes policy X locks immediately. The counter SELECT is
	// still a locking/current read over real records: a caller's REPEATABLE READ
	// transaction may have established its snapshot in the earlier ID pre-check.
	rows, err := c.QueryContext(ctx, enqueueBackpressureDepthSQL(len(demandQueues)), policyArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var queue string
		var limit sql.NullInt64
		var entered, exited int64
		if err := rows.Scan(&queue, &limit, &entered, &exited); err != nil {
			_ = rows.Close()
			return err
		}
		if limit.Valid {
			current := max(int64(0), entered-exited)
			incoming := demand[queue]
			if current+incoming > limit.Int64 {
				_ = rows.Close()
				return &headgate.BackpressureError{Queue: queue, Limit: uint64(limit.Int64), Current: uint64(current), Incoming: uint64(incoming)}
			}
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var now int64
	if err := c.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return err
	}
	rowSQL := `(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + nowMS + `,
	            ?, ?, ?, ?, ?, JSON_ARRAY(), ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?)`
	values := strings.TrimSuffix(strings.Repeat(rowSQL+", ", len(batch)), ", ")
	stmt := `INSERT INTO headgate_job
	           (ulid, kind, schema_version, payload, queue, partition_key, rate_class,
	            weight, fingerprint, priority, max_attempts, enqueued_at_ms,
	            scheduled_at_ms, timeout_ms, deadline_ms, retention_ms,
	            state, errors, unique_key, unique_states, unique_window_ms,
	            unique_expires_at_ms, headers, periodic_schedule_id, periodic_tick_ms,
	            sticky_worker) VALUES ` + values
	var args []any
	var candidates []any
	for _, e := range batch {
		sv := headgate.EffectiveSchemaVersion(e.SchemaVersion)
		queue := headgate.EnqueueQueue(e)
		ma := headgate.EffectiveMaxAttempts(e.MaxAttempts)
		var expires any
		if e.UniqueKey != nil && e.UniqueWindowMs > 0 {
			expires = now + e.UniqueWindowMs
		}
		scheduledAt := e.ScheduledAtMs
		if e.UniqueDebounceMs > 0 {
			scheduledAt = now + e.UniqueDebounceMs
		} else if scheduledAt == 0 {
			scheduledAt = now
		}
		state := "available"
		if e.Pending {
			state = "pending"
		} else if scheduledAt > now {
			state = "scheduled"
		}
		args = append(args,
			e.ID, e.Kind, sv, e.Payload, queue, e.PartitionKey, e.RateClass,
			headgate.EffectiveWeight(e.Weight), e.Fingerprint, e.Priority, ma,
			scheduledAt, e.TimeoutMs, e.DeadlineMs, e.RetentionMs, state,
			e.UniqueKey, e.UniqueStates, e.UniqueWindowMs, expires,
			// Opaque headers are encoded without being interpreted.
			headersJSON(e.Headers), e.PeriodicScheduleID, e.PeriodicTickMs, e.StickyWorker)
		if e.UniqueKey != nil {
			candidates = append(candidates, e.UniqueKey)
		}
	}

	// Lock maintained route rows before inserting jobs. Pruning and reconciliation take
	// route -> job; the old job -> route producer order formed an InnoDB deadlock. A
	// harmless stale route can be pruned, while an available job without one can starve.
	routes := make([]string, 0, len(batch))
	for _, e := range batch {
		queue := headgate.EnqueueQueue(e)
		routes = append(routes, queue+"\x00"+e.PartitionKey)
	}
	slices.Sort(routes)
	routes = slices.Compact(routes)
	routeArgs := make([]any, 0, len(routes)*2)
	for _, route := range routes {
		queue, partition, _ := strings.Cut(route, "\x00")
		routeArgs = append(routeArgs, queue, partition)
	}
	routeValues := strings.TrimSuffix(strings.Repeat("(?, ?), ", len(routes)), ", ")
	if _, err := c.ExecContext(ctx,
		`INSERT INTO headgate_active_partition (queue, partition_key)
		 VALUES `+routeValues+`
		 ON DUPLICATE KEY UPDATE queue = VALUES(queue)`, routeArgs...); err != nil {
		return err
	}
	inflightValues := strings.TrimSuffix(strings.Repeat("(?, ?, 0), ", len(routes)), ", ")
	if _, err := c.ExecContext(ctx,
		`INSERT INTO headgate_inflight (queue, partition_key, n)
		 VALUES `+inflightValues+`
		 AS new ON DUPLICATE KEY UPDATE n = headgate_inflight.n`, routeArgs...); err != nil {
		return err
	}

	for attempt := 0; attempt < 2; attempt++ {
		_, err := c.ExecContext(ctx, stmt, args...)
		if err == nil {
			for _, e := range batch {
				for _, tag := range headgate.CanonicalTags(e.Tags) {
					if _, err := c.ExecContext(ctx, `INSERT INTO headgate_job_tag(job_id,tag) SELECT id,? FROM headgate_job WHERE ulid=?`, tag, e.ID); err != nil {
						return err
					}
				}
			}
			// backlog metrics arrived counters, one upsert per distinct queue.
			perQueue := map[string]int64{}
			for _, e := range batch {
				queue := e.Queue
				if queue == "" {
					queue = "default"
				}
				perQueue[queue]++
			}
			for q, n := range perQueue {
				if _, err := c.ExecContext(ctx,
					`INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived)
					 VALUES (?, (`+nowMS+` DIV 60000) * 60000, ?) AS new
					 ON DUPLICATE KEY UPDATE arrived = headgate_queue_counter.arrived + new.arrived`,
					q, n); err != nil {
					return err
				}
			}
			perPartition := map[string]int64{}
			for _, e := range batch {
				queue := e.Queue
				if queue == "" {
					queue = "default"
				}
				perPartition[queue+"\x00"+e.PartitionKey]++
			}
			for route, n := range perPartition {
				queue, partition, _ := strings.Cut(route, "\x00")
				if _, err := c.ExecContext(ctx,
					`INSERT INTO headgate_partition_counter
					   (queue, partition_key, bucket_ms, arrived)
					 VALUES (?, ?, (`+nowMS+` DIV 60000) * 60000, ?) AS new
					 ON DUPLICATE KEY UPDATE arrived = headgate_partition_counter.arrived + new.arrived`,
					queue, partition, n); err != nil {
					return err
				}
			}
			return nil
		}
		if !isDupKey(err) {
			return err
		}
		// Throttle keys release LAZILY: the conflicting enqueue clears any holder
		// whose window has passed, then retries once.
		if attempt == 0 && len(candidates) > 0 {
			res, rerr := c.ExecContext(ctx,
				lazyUniqueReleaseSQL(len(candidates)), candidates...)
			if rerr == nil {
				if released, _ := res.RowsAffected(); released > 0 {
					continue
				}
			}
		}
		// job uniqueness one semantic: the duplicate is a normal result carrying the winner's
		// id — never a silent skip, never a bare constraint error.
		if len(candidates) > 0 {
			var existing string
			holderArgs := append(append([]any{}, candidates...), candidates...)
			err := c.QueryRowContext(ctx,
				uniqueHolderSQL(len(candidates)), holderArgs...).Scan(&existing)
			if err == nil {
				incoming := batch[0]
				replaced := false
				if incoming.UniqueDebounceMs > 0 {
					schemaVersion := incoming.SchemaVersion
					if schemaVersion == 0 {
						schemaVersion = 1
					}
					res, updateErr := c.ExecContext(ctx, `UPDATE headgate_job SET schema_version=?,payload=?,fingerprint=?,state='scheduled',scheduled_at_ms=`+nowMS+`+? WHERE ulid=? AND state IN ('pending','scheduled','available','retryable')`, schemaVersion, incoming.Payload, incoming.Fingerprint, incoming.UniqueDebounceMs, existing)
					if updateErr != nil {
						return updateErr
					}
					rowsAffected, _ := res.RowsAffected()
					replaced = rowsAffected > 0
					if replaced {
						if _, err := c.ExecContext(ctx, `DELETE FROM headgate_job_tag WHERE job_id=(SELECT id FROM headgate_job WHERE ulid=?)`, existing); err != nil {
							return err
						}
						for _, tag := range headgate.CanonicalTags(incoming.Tags) {
							if _, err := c.ExecContext(ctx, `INSERT INTO headgate_job_tag(job_id,tag) SELECT id,? FROM headgate_job WHERE ulid=?`, tag, existing); err != nil {
								return err
							}
						}
					}
				} else if incoming.UniqueReplace != 0 {
					schemaVersion := incoming.SchemaVersion
					if schemaVersion == 0 {
						schemaVersion = 1
					}
					maxAttempts := incoming.MaxAttempts
					if maxAttempts == 0 {
						maxAttempts = 25
					}
					res, updateErr := c.ExecContext(ctx, `UPDATE headgate_job SET
						schema_version = IF((? & ?) <> 0, ?, schema_version),
						payload = IF((? & ?) <> 0, ?, payload), fingerprint = IF((? & ?) <> 0, ?, fingerprint),
						scheduled_at_ms = IF((? & ?) <> 0 AND state = 'scheduled', IF(? = 0, `+nowMS+`, ?), scheduled_at_ms),
						priority = IF((? & ?) <> 0, ?, priority), max_attempts = IF((? & ?) <> 0, ?, max_attempts)
					  WHERE ulid = ? AND state IN ('scheduled','available','retryable')
					    AND ((? & ?) <> 0 OR (? & ?) <> 0 OR (? & ?) <> 0 OR ((? & ?) <> 0 AND state = 'scheduled'))`,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, schemaVersion,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, incoming.Payload,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, incoming.Fingerprint,
						incoming.UniqueReplace, headgate.UniqueReplaceScheduledAt, incoming.ScheduledAtMs, incoming.ScheduledAtMs,
						incoming.UniqueReplace, headgate.UniqueReplacePriority, incoming.Priority,
						incoming.UniqueReplace, headgate.UniqueReplaceMaxAttempts, maxAttempts, existing,
						incoming.UniqueReplace, headgate.UniqueReplacePayload, incoming.UniqueReplace, headgate.UniqueReplacePriority,
						incoming.UniqueReplace, headgate.UniqueReplaceMaxAttempts, incoming.UniqueReplace, headgate.UniqueReplaceScheduledAt)
					if updateErr != nil {
						return updateErr
					}
					rowsAffected, _ := res.RowsAffected()
					replaced = rowsAffected > 0
				}
				return &headgate.DuplicateError{ExistingID: existing, Replaced: replaced}
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		// Not a uniqueness index — the ulid key collided. The pre-check above classified
		// every id this call knew about, so reaching here means a CONCURRENT producer
		// inserted the row between the read and the write; idempotent enqueue identity's answer is the same
		// typed conflict, naming the id.
		ids := make([]any, len(batch))
		for i, e := range batch {
			ids[i] = e.ID
		}
		var raced string
		if rerr := c.QueryRowContext(ctx,
			`SELECT ulid FROM headgate_job WHERE ulid IN (`+placeholders(len(batch))+
				`) LIMIT 1`, ids...).Scan(&raced); rerr != nil && !errors.Is(rerr, sql.ErrNoRows) {
			return rerr
		}
		return &headgate.IDConflictError{JobID: raced}
	}
	panic("enqueue retries at most once")
}

// ReclaimExpired makes jobs with expired leases eligible for admission again.
