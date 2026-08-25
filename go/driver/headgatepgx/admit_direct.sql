-- adaptive admission DIRECT POLICY-FREE ADMISSION.
--
-- This is the semantically comparable fast shape for the common case: no rate bucket,
-- no quarantine, no concurrency ceiling on the requested queues, positive capacity, and
-- at most one active partition. With one partition there is nothing to merge and every
-- row inside LEAST(quantum*4, deficit+quantum, capacity) is admissible, so the partition
-- index scan can lock while it draws. The row is then read twice (draw+lock, update), the
-- same shape as a plain SKIP LOCKED fetch, rather than draw, ID-lock, update.
--
-- `applicable` is computed inside this statement. A visible policy row or second active
-- partition returns one sentinel (`hg_widen = true`) and makes no write; the driver then
-- runs admit.sql unchanged. A policy committed after this snapshot is also invisible to
-- the full statement at this instant, exactly the round-32e race argument. Zero active
-- partitions is a handled empty poll, not a fallback. Capacity zero falls back because
-- the reference gate accrues fairness credit without locking a row in that shape.
--
-- Under contention this path is deliberately work-conserving: SKIP LOCKED advances to
-- the next row in the same sole partition. The old draw-then-lock shape returned a short
-- batch instead. No policy-rejected row is locked because policy makes this path
-- inapplicable, and with one partition every drawn row is selected.
--
-- Parameters and output positions match admit.sql so both drivers use one decoder.
WITH params AS (
  SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint AS now_ms,
         $4::bigint AS lease_ms, $5::text AS worker,
         $6::text AS lease_id, $7::bigint AS quantum, $2::int AS capacity,
         $3::bigint AS retired_clock, $8::int AS overfetch, $9::int AS wide
),
pol AS MATERIALIZED (
  SELECT (    NOT EXISTS (SELECT 1 FROM headgate_rate_bucket)
          AND NOT EXISTS (SELECT 1 FROM headgate_quarantine)
          AND NOT EXISTS (SELECT 1 FROM headgate_concurrency_limit cl
                          WHERE cl.queue = ANY($1::text[]))
          -- Sticky rows require merging the unpinned and worker-specific route streams.
          -- Keep this compact shape compact and conservatively use the full gate.
          AND NOT EXISTS (SELECT 1 FROM headgate_job j
                          WHERE j.state = 'available'
                            AND j.sticky_worker <> ''
                            AND j.queue = ANY($1::text[]))
         ) AS free
),
requested_queues AS (
  SELECT DISTINCT unnest($1::text[]) AS queue
),
active_parts AS MATERIALIZED (
  -- Only the distinction 0 / 1 / many matters here, so LIMIT 2 is an exact bounded test.
  SELECT ap.queue, ap.partition_key, COALESCE(d.deficit, 0)::bigint AS deficit
  FROM requested_queues rq
  JOIN headgate_active_partition ap ON ap.queue = rq.queue
  LEFT JOIN headgate_queue_state qs ON qs.queue = ap.queue
  LEFT JOIN headgate_partition_deficit d
         ON d.queue = ap.queue AND d.partition_key = ap.partition_key
  WHERE COALESCE(qs.paused, false) = false
  ORDER BY ap.queue, ap.partition_key
  LIMIT 2
),
shape AS MATERIALIZED (
  SELECT (SELECT free FROM pol)
     AND p.capacity > 0
     AND (SELECT count(*) FROM active_parts) <= 1 AS applicable
  FROM params p
),
drawn AS MATERIALIZED (
  SELECT d.id
  FROM active_parts ap
  CROSS JOIN params p
  CROSS JOIN LATERAL (
    SELECT j.id
    FROM headgate_job j
    WHERE (SELECT applicable FROM shape)
      AND j.state = 'available'
      AND j.queue = ap.queue
      AND j.partition_key = ap.partition_key
      AND (j.scheduled_at_ms <= p.now_ms OR j.priority > 0)
    ORDER BY j.priority DESC, j.scheduled_at_ms, j.id
    LIMIT LEAST(p.quantum * 4, ap.deficit + p.quantum, p.capacity::bigint)
    FOR UPDATE OF j SKIP LOCKED
  ) d
),
claimed AS MATERIALIZED (
  UPDATE headgate_job j
  SET state = 'running',
      lease_id = p.lease_id,
      lease_expires_at_ms = p.now_ms + p.lease_ms,
      claimed_at_ms = p.now_ms,
      fence = j.fence + 1,
      claimed_by = p.worker,
      rate_charge = 0
  FROM params p
  WHERE j.id IN (SELECT id FROM drawn)
    AND j.state = 'available'
  RETURNING j.id, j.ulid, j.kind, j.schema_version, j.payload, j.queue,
            j.rate_class, j.partition_key, j.weight, j.fingerprint, j.priority,
            j.attempt, j.crash_attempt, j.max_attempts,
            j.scheduled_at_ms, j.timeout_ms, j.deadline_ms, j.retention_ms,
            j.checkpoint, j.cp_cursor, j.headers,
            j.periodic_schedule_id, j.periodic_tick_ms,
            j.sticky_worker,
            j.fence, j.lease_id, j.lease_expires_at_ms
),
charge AS (
  INSERT INTO headgate_partition_deficit AS d
         (queue, partition_key, deficit, updated_at_ms)
  SELECT ap.queue, ap.partition_key,
         GREATEST(0, p.quantum - (SELECT count(*) FROM claimed)), p.now_ms
  FROM active_parts ap CROSS JOIN params p
  WHERE EXISTS (SELECT 1 FROM drawn)
  ON CONFLICT (queue, partition_key) DO UPDATE
    SET deficit = LEAST((SELECT quantum * 4 FROM params), d.deficit + EXCLUDED.deficit),
        updated_at_ms = EXCLUDED.updated_at_ms
),
infl AS (
  INSERT INTO headgate_inflight AS f (queue, partition_key, n)
  SELECT queue, partition_key, count(*)::bigint FROM claimed GROUP BY 1, 2
  ON CONFLICT (queue, partition_key) DO UPDATE SET n = f.n + EXCLUDED.n
),
queue_charge AS (
  INSERT INTO headgate_queue_state AS qs (queue, dispatch_count)
  SELECT queue, count(*)::bigint FROM claimed GROUP BY queue
  ON CONFLICT (queue) DO UPDATE
    SET dispatch_count = qs.dispatch_count + EXCLUDED.dispatch_count
)
SELECT c.*, false AS hg_widen FROM claimed c
UNION ALL
SELECT 0::bigint, ''::text, ''::text, 0::int, ''::bytea, ''::text, ''::text, ''::text,
       0::int, ''::text, 0::int, 0::int, 0::int, 0::int, 0::bigint, 0::bigint, 0::bigint,
       0::bigint, '{}'::jsonb, NULL::bytea, '{}'::jsonb, ''::text, 0::bigint,
       ''::text, 0::bigint, ''::text, 0::bigint,
       true
FROM shape s WHERE NOT s.applicable;
