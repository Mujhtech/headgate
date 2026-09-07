package headgatepgx

import (
	"context"
	"errors"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- enqueue ----------

// Enqueue inserts a batch of jobs atomically.
func (s *PgxStore) Enqueue(ctx context.Context, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	scoped := append([]headgate.Envelope(nil), batch...)
	for i := range scoped {
		scoped[i].UniqueKey = headgate.EffectiveUniqueKey(scoped[i])
	}
	batch = scoped
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return headgate.WrapUnavailable(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit
	if err := s.enqueueOn(ctx, tx, batch); err != nil {
		var duplicate *headgate.DuplicateError
		if errors.As(err, &duplicate) && duplicate.Replaced {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return headgate.WrapUnavailable(commitErr)
			}
			return err
		}
		return headgate.WrapUnavailable(err)
	}
	return headgate.WrapUnavailable(tx.Commit(ctx))
}

// querier covers both the pool and a pgx.Tx, so transactional enqueue shares this path.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (s *PgxStore) enqueueOn(ctx context.Context, q querier, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	q = s.pool.scope(q)
	// typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	// idempotent enqueue identity the strict caller-supplied id contract. A batch is all-or-nothing, so the
	// whole classification happens BEFORE anything is written: an id whose row exists
	// with matching content drops out of the batch (idempotent success — this is what
	// makes the API's Idempotency-Key replay safe), and an id whose row exists with
	// DIFFERENT content rejects the entire batch naming the offender. A terminal row
	// still counts as existing; reuse follows retention eviction.
	allIDs := make([]string, len(batch))
	for i, e := range batch {
		allIDs[i] = e.ID
	}
	present := map[string][3]string{}
	rows, err := q.Query(ctx,
		`SELECT ulid, kind, fingerprint, queue FROM headgate_job WHERE ulid = ANY($1::text[])`,
		allIDs)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, kind, fp, queue string
		if err := rows.Scan(&id, &kind, &fp, &queue); err != nil {
			rows.Close()
			return err
		}
		present[id] = [3]string{kind, fp, queue}
	}
	rows.Close()
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
	n := len(batch)
	ulids := make([]string, n)
	kinds := make([]string, n)
	versions := make([]int32, n)
	payloads := make([][]byte, n)
	queues := make([]string, n)
	partitions := make([]string, n)
	rateClasses := make([]string, n)
	weights := make([]int32, n)
	fingerprints := make([]string, n)
	priorities := make([]int32, n)
	maxAttempts := make([]int32, n)
	scheduled := make([]int64, n)
	timeouts := make([]int64, n)
	deadlines := make([]int64, n)
	uniqueKeys := make([][]byte, n) // nil element = NULL
	uniqueStates := make([]int32, n)
	uniqueWindows := make([]int64, n)
	retentions := make([]int64, n)
	// Encode opaque envelope headers without interpreting them.
	headers := make([]string, n)
	periodicScheduleIDs := make([]string, n)
	periodicTicks := make([]int64, n)
	debounceWindows := make([]int64, n)
	pendingFlags := make([]bool, n)
	tagsJSON := make([]string, n)
	stickyWorkers := make([]string, n)
	for i, e := range batch {
		ulids[i], kinds[i], payloads[i] = e.ID, e.Kind, e.Payload
		versions[i] = int32(headgate.EffectiveSchemaVersion(e.SchemaVersion))
		queues[i] = headgate.EnqueueQueue(e)
		partitions[i], rateClasses[i], fingerprints[i] = e.PartitionKey, e.RateClass, e.Fingerprint
		weights[i] = int32(headgate.EffectiveWeight(e.Weight))
		priorities[i] = e.Priority
		maxAttempts[i] = int32(headgate.EffectiveMaxAttempts(e.MaxAttempts))
		scheduled[i], timeouts[i], deadlines[i] = e.ScheduledAtMs, e.TimeoutMs, e.DeadlineMs
		uniqueKeys[i] = e.UniqueKey
		uniqueStates[i] = int32(e.UniqueStates)
		uniqueWindows[i] = e.UniqueWindowMs
		retentions[i] = e.RetentionMs
		hdr := headgate.EncodeHeaders(e.Headers)
		if hdr == "" {
			hdr = "{}" // the column is NOT NULL DEFAULT '{}'
		}
		headers[i] = hdr
		periodicScheduleIDs[i] = e.PeriodicScheduleID
		periodicTicks[i] = e.PeriodicTickMs
		debounceWindows[i] = e.UniqueDebounceMs
		pendingFlags[i] = e.Pending
		canonicalTags := headgate.CanonicalTags(e.Tags)
		if canonicalTags == nil {
			canonicalTags = []string{}
		}
		tagsJSON[i] = headgateshared.EncodeStringList(canonicalTags)
		stickyWorkers[i] = e.StickyWorker
	}

	// crash quarantine quarantined fingerprints are rejected at enqueue, loudly.
	var quarantined *string
	err = q.QueryRow(ctx, `
		SELECT q.fingerprint FROM unnest($1::text[]) f(fp)
		JOIN headgate_quarantine q ON q.fingerprint = f.fp LIMIT 1`,
		fingerprints).Scan(&quarantined)
	if err == nil && quarantined != nil {
		return &headgate.QuarantinedError{Fingerprint: *quarantined}
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// Exact O(1) enqueue policy: producers serialize on one policy row per queue and
	// read only the two monotonic counter rows. Matching-id replays were removed above,
	// so they consume no capacity. This function must run in a transaction; every public
	// Store/Transactional entry point supplies one.
	demand := map[string]int64{}
	for _, e := range batch {
		demand[headgate.EnqueueQueue(e)]++
	}
	demandQueues := make([]string, 0, len(demand))
	for queue := range demand {
		demandQueues = append(demandQueues, queue)
	}
	slices.Sort(demandQueues)
	if _, err := q.Exec(ctx, `
		INSERT INTO headgate_enqueue_policy (queue)
		SELECT unnest($1::text[]) ON CONFLICT (queue) DO NOTHING`, demandQueues); err != nil {
		return err
	}
	// Lock and counter read are separate statements. A joined SELECT FOR UPDATE takes
	// its snapshot before waiting; after the wait its joined counters can remain stale
	// even though the policy tuple is refreshed (the exact over-admission trap).
	lockRows, err := q.Query(ctx, `
		SELECT queue FROM headgate_enqueue_policy
		WHERE queue = ANY($1::text[]) ORDER BY queue FOR UPDATE`, demandQueues)
	if err != nil {
		return err
	}
	for lockRows.Next() {
		var locked string
		if err := lockRows.Scan(&locked); err != nil {
			lockRows.Close()
			return err
		}
	}
	lockRows.Close()
	if err := lockRows.Err(); err != nil {
		return err
	}
	policyRows, err := q.Query(ctx, enqueueBackpressureDepthSQL, demandQueues)
	if err != nil {
		return err
	}
	defer policyRows.Close()
	for policyRows.Next() {
		var queue string
		var limit *int64
		var entered, exited int64
		if err := policyRows.Scan(&queue, &limit, &entered, &exited); err != nil {
			return err
		}
		if limit == nil {
			continue
		}
		current := max(int64(0), entered-exited)
		incoming := demand[queue]
		if current+incoming > *limit {
			return &headgate.BackpressureError{Queue: queue, Limit: uint64(*limit), Current: uint64(current), Incoming: uint64(incoming)}
		}
	}
	if err := policyRows.Err(); err != nil {
		return err
	}

	insert := `
		WITH now AS (SELECT ` + nowMS + ` AS ms),
		input AS (
		  SELECT * FROM unnest(
		    $1::text[], $2::text[], $3::int[], $4::bytea[], $5::text[], $6::text[],
		    $7::text[], $8::int[], $9::text[], $10::int[], $11::int[], $12::bigint[],
		    $13::bigint[], $14::bigint[], $15::bytea[], $16::int[], $17::bigint[],
		    $18::bigint[], $19::jsonb[], $20::text[], $21::bigint[],
		    $22::bigint[], $23::boolean[], $24::jsonb[], $25::text[]
		  ) AS t(ulid, kind, schema_version, payload, queue, partition_key,
		         rate_class, weight, fingerprint, priority, max_attempts, scheduled_at_ms,
		         timeout_ms, deadline_ms, unique_key, unique_states, unique_window_ms,
		         retention_ms, headers, periodic_schedule_id, periodic_tick_ms,
		         unique_debounce_ms, pending, tags, sticky_worker)
		),
		ins AS (
		  INSERT INTO headgate_job
		    (ulid, kind, schema_version, payload, queue, state, partition_key,
		     rate_class, weight, fingerprint, priority, max_attempts, enqueued_at_ms,
		     scheduled_at_ms, timeout_ms, deadline_ms, retention_ms,
		     unique_key, unique_states, unique_expires_at_ms, headers,
		     periodic_schedule_id, periodic_tick_ms, sticky_worker)
		  SELECT i.ulid, i.kind, i.schema_version, i.payload, i.queue,
		         CASE WHEN i.pending THEN 'pending'
		              WHEN i.unique_debounce_ms > 0 THEN 'scheduled'
		              WHEN i.scheduled_at_ms > n.ms THEN 'scheduled'
		              ELSE 'available' END::headgate_state,
		         i.partition_key, i.rate_class, i.weight, i.fingerprint, i.priority,
		         i.max_attempts, n.ms,
		         CASE WHEN i.unique_debounce_ms > 0 THEN n.ms + i.unique_debounce_ms
		              WHEN i.scheduled_at_ms = 0 THEN n.ms ELSE i.scheduled_at_ms END,
		         i.timeout_ms, i.deadline_ms, i.retention_ms,
		         i.unique_key, i.unique_states,
		         CASE WHEN i.unique_window_ms > 0 THEN n.ms + i.unique_window_ms
		              ELSE NULL END,
		         i.headers, i.periodic_schedule_id, i.periodic_tick_ms, i.sticky_worker
		  FROM input i CROSS JOIN now n
		  RETURNING id, ulid, queue, partition_key, state
		),
		tag_rows AS (
		  INSERT INTO headgate_job_tag (job_id, tag)
		  SELECT ins.id, jsonb_array_elements_text(i.tags)
		  FROM ins JOIN input i USING (ulid)
		  RETURNING 1
		),
		queue_defaults AS (
		  INSERT INTO headgate_queue_state (queue)
		  SELECT DISTINCT queue FROM ins
		  ON CONFLICT (queue) DO NOTHING
		),
		-- Seed the per-partition ceiling counter before the first possible claim. This
		-- makes the gate's FOR UPDATE lock real even when inflight is still zero.
		inflight_defaults AS (
		  INSERT INTO headgate_inflight (queue, partition_key, n)
		  SELECT DISTINCT queue, partition_key, 0 FROM ins
		  ON CONFLICT (queue, partition_key) DO NOTHING
		),
		-- the maintained active-partition set the gate reads instead of
		-- scanning. Only rows that landed 'available' count; a 'scheduled' row's
		-- partition is added by PromoteDue when it actually becomes drawable.
		-- ON CONFLICT DO UPDATE, not DO NOTHING: the no-op update takes the row lock
		-- the pruner must wait behind, which is the whole reason a producer can never
		-- lose a race to it (see the migration's comment).
		active AS (
		  INSERT INTO headgate_active_partition (queue, partition_key)
		  SELECT DISTINCT queue, partition_key FROM ins WHERE state = 'available'
		  ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
		),
		counters AS (
		  INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived)
		  SELECT queue, (SELECT ms FROM now) / 60000 * 60000, count(*)
		  FROM ins GROUP BY 1
		  ON CONFLICT (queue, bucket_ms) DO UPDATE
		    SET arrived = headgate_queue_counter.arrived + EXCLUDED.arrived
		),
		partition_counters AS (
		  INSERT INTO headgate_partition_counter
		    (queue, partition_key, bucket_ms, arrived)
		  SELECT queue, partition_key, (SELECT ms FROM now) / 60000 * 60000, count(*)
		  FROM ins GROUP BY 1, 2
		  ON CONFLICT (queue, partition_key, bucket_ms) DO UPDATE
		    SET arrived = headgate_partition_counter.arrived + EXCLUDED.arrived
		),
		wakeup AS ( -- push wakeups delivered on COMMIT; spurious wakeups cost latency only
		  SELECT pg_notify('headgate_wakeup', queue)
		  FROM (SELECT DISTINCT queue FROM ins) nq
		)
		-- wakeup MUST be referenced: an unreferenced SELECT CTE is never executed
		-- (only data-modifying CTEs run unconditionally).
		SELECT (SELECT count(*) FROM ins) AS inserted,
		       (SELECT count(*) FROM wakeup) AS notified`
	args := []any{ulids, kinds, versions, payloads, queues, partitions, rateClasses,
		weights, fingerprints, priorities, maxAttempts, scheduled, timeouts, deadlines,
		uniqueKeys, uniqueStates, uniqueWindows, retentions, headers,
		periodicScheduleIDs, periodicTicks, debounceWindows, pendingFlags, tagsJSON, stickyWorkers}
	candidates := make([][]byte, 0, n)
	for _, k := range uniqueKeys {
		if k != nil {
			candidates = append(candidates, k)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		// PostgreSQL aborts the caller transaction after any statement error. A unique
		// collision is a normal typed enqueue result, so isolate the insert and restore
		// the transaction before querying the holder. Plain enqueue now needs this too:
		// its backpressure policy lock and insert deliberately share one transaction.
		if _, err := q.Exec(ctx, "SAVEPOINT headgate_enqueue_insert_attempt"); err != nil {
			return err
		}
		var inserted, notified int64
		err := q.QueryRow(ctx, insert, args...).Scan(&inserted, &notified)
		if err == nil {
			if _, err := q.Exec(ctx, "RELEASE SAVEPOINT headgate_enqueue_insert_attempt"); err != nil {
				return err
			}
			return nil
		}
		if _, rollbackErr := q.Exec(ctx, "ROLLBACK TO SAVEPOINT headgate_enqueue_insert_attempt"); rollbackErr != nil {
			return rollbackErr
		}
		if _, releaseErr := q.Exec(ctx, "RELEASE SAVEPOINT headgate_enqueue_insert_attempt"); releaseErr != nil {
			return releaseErr
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			return err
		}
		// Throttle keys release LAZILY: clear expired holders once, retry once.
		if attempt == 0 {
			tag, rerr := q.Exec(ctx, `
				UPDATE headgate_job SET unique_expires_at_ms = NULL
				WHERE unique_key = ANY($1::bytea[])
				  AND unique_expires_at_ms IS NOT NULL
				  AND unique_expires_at_ms <= `+nowMS, candidates)
			if rerr == nil && tag.RowsAffected() > 0 {
				continue
			}
		}
		// job uniqueness one semantic: the duplicate carries the winner's id.
		var existing, holderState string
		lerr := q.QueryRow(ctx, `
			SELECT ulid, state::text FROM headgate_job
			WHERE unique_key = ANY($1::bytea[])
			  AND (unique_expires_at_ms IS NOT NULL
			       OR state = ANY(ARRAY['pending','scheduled','available','running','retryable']::headgate_state[]))
			LIMIT 1 FOR UPDATE`, candidates).Scan(&existing, &holderState)
		if lerr == nil {
			incoming := batch[0]
			replaced := false
			if incoming.UniqueDebounceMs > 0 {
				schemaVersion := incoming.SchemaVersion
				if schemaVersion == 0 {
					schemaVersion = 1
				}
				var changed int64
				updateErr := q.QueryRow(ctx, `WITH upd AS (
				  UPDATE headgate_job SET schema_version=$2, payload=$3, fingerprint=$4,
				         state='scheduled', scheduled_at_ms=`+nowMS+`+$5
				  WHERE ulid=$1 AND state=ANY(ARRAY['pending','scheduled','available','retryable']::headgate_state[])
				  RETURNING id, queue, partition_key
				), del AS (DELETE FROM headgate_job_tag WHERE job_id IN (SELECT id FROM upd)),
				ins AS (INSERT INTO headgate_job_tag(job_id,tag)
				  SELECT upd.id, unnest($6::text[]) FROM upd RETURNING 1)
				SELECT count(*)::bigint FROM upd`, existing, int32(schemaVersion), incoming.Payload,
					incoming.Fingerprint, incoming.UniqueDebounceMs, headgate.CanonicalTags(incoming.Tags)).Scan(&changed)
				if updateErr != nil {
					return updateErr
				}
				replaced = changed > 0
			} else if incoming.UniqueReplace != 0 {
				schemaVersion := incoming.SchemaVersion
				if schemaVersion == 0 {
					schemaVersion = 1
				}
				maxAttempts := incoming.MaxAttempts
				if maxAttempts == 0 {
					maxAttempts = 25
				}
				tag, updateErr := q.Exec(ctx, `UPDATE headgate_job SET
				schema_version = CASE WHEN ($2::integer & $9::integer) <> 0 THEN $3::integer ELSE schema_version END,
				payload = CASE WHEN ($2::integer & $9::integer) <> 0 THEN $4::bytea ELSE payload END,
				fingerprint = CASE WHEN ($2::integer & $9::integer) <> 0 THEN $5::text ELSE fingerprint END,
				scheduled_at_ms = CASE WHEN ($2::integer & $10::integer) <> 0 AND state = 'scheduled'
				  THEN CASE WHEN $6::bigint = 0 THEN `+nowMS+` ELSE $6::bigint END ELSE scheduled_at_ms END,
				priority = CASE WHEN ($2::integer & $11::integer) <> 0 THEN $7::integer ELSE priority END,
				max_attempts = CASE WHEN ($2::integer & $12::integer) <> 0 THEN $8::integer ELSE max_attempts END
				  WHERE ulid = $1 AND state = ANY(ARRAY['scheduled','available','retryable']::headgate_state[])
				    AND (($2::integer & ($9::integer|$11::integer|$12::integer)) <> 0 OR (($2::integer & $10::integer) <> 0 AND state = 'scheduled'))`,
					existing, int32(incoming.UniqueReplace), int32(schemaVersion), incoming.Payload,
					incoming.Fingerprint, incoming.ScheduledAtMs, incoming.Priority, int32(maxAttempts),
					int32(headgate.UniqueReplacePayload), int32(headgate.UniqueReplaceScheduledAt),
					int32(headgate.UniqueReplacePriority), int32(headgate.UniqueReplaceMaxAttempts))
				if updateErr != nil {
					return updateErr
				}
				replaced = tag.RowsAffected() > 0
			}
			_ = holderState
			return &headgate.DuplicateError{ExistingID: existing, Replaced: replaced}
		}
		// Not a uniqueness index — the ulid PK collided. The pre-check above already
		// classified every id this call knew about, so reaching here means a CONCURRENT
		// producer inserted the row between the read and the write. idempotent enqueue identity's answer is
		// the same typed conflict rather than a bare constraint error; name the offender
		// instead of guessing which id raced.
		var raced string
		if rerr := q.QueryRow(ctx,
			`SELECT ulid FROM headgate_job WHERE ulid = ANY($1::text[]) LIMIT 1`,
			ulids).Scan(&raced); rerr != nil && !errors.Is(rerr, pgx.ErrNoRows) {
			return rerr
		}
		return &headgate.IDConflictError{JobID: raced}
	}
	return errors.New("headgate: enqueue retries at most once")
}
