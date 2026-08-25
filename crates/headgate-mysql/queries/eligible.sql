-- admission policy THE ADMISSION GATE, MySQL edition — the ELIGIBILITY read of it.
--
-- The PG gate is one data-modifying CTE statement; MySQL has neither data-modifying
-- CTEs nor RETURNING, so store port boundary's "each natively" here means: the atomic unit is one
-- READ COMMITTED InnoDB transaction, and this statement is its policy step. The driver
-- then locks survivors (state re-check + FOR UPDATE SKIP LOCKED), claims, reads the
-- claimed rows, spends buckets, and charges deficits — all inside that transaction.
-- Clause order and predicates mirror queries/admit.sql line for line; the same two
-- traps carry over verbatim (see the lock statement in lib.rs for the second):
--   * time comes from the store, never the caller (?1 now_ms is read from MySQL first,
--     inside this same transaction);
--   * candidates are drawn PER PARTITION via LATERAL, never one flat window.
--
-- The rate buckets were already locked (FOR UPDATE) by this transaction before this
-- statement runs, so the avail computed here cannot move under us.
--
-- Placeholders, in order: /*QUEUE_ROWS*/ (one queue value each), ?=cap*overfetch,
--                         ?=now_ms, ?=draw_limit,
--                         ?=worker, ?=now_ms, ?=draw_limit, ?=draw_limit, ?=now_ms,
--                         ?=quantum, ?=quantum, ?=capacity,
--                         ?=capacity, ?=capacity, ?=draw_limit, ?=draw_limit, ?=quantum*4
-- (16 after the queue values.) Queue weight moved the capacity LIMIT after both policy
-- arms, because taking a global-priority prefix before queue selection would let job
-- priority erase a queue before its weight was consulted.
-- (the marker above is substituted too, because both drivers replace ALL occurrences --
-- harmless inside a comment, and replacing only the FIRST once left the real site as
-- `IN ()`. The active_parts now_ms placeholder is GONE with the scan it belonged to.)
-- Returns tagged rows: tag 'claim' / 'discard' / 'cancel_incoming' /
--                                'cancel_running' = selected incoming decision;
--                      tag 'p' = a (queue, partition_key) seen by ranking (charge these);
--                      tag 'w' = the adaptive admission adaptive-window revision ESCALATION VERDICT, id 1 = re-run this
--                                statement with draw_limit = quantum*4 and use THAT result.
--
-- adaptive admission ADAPTIVE WIDENING (adaptive-window revision), the MySQL half. The per-partition draw used to be a
-- flat `LIMIT quantum * 4`, so the gate read 4x the rows it could claim. It is now sized
-- to the work -- about capacity / active_partitions, plus one row -- and this statement
-- reports, PRECISELY, whether that narrower window could have changed the answer.
--
-- The proof is the same as queries/admit.sql's, and so is the escalation ceiling
-- (quantum * 4: beyond it the previous gate would not have admitted either). What differs
-- is that MySQL's gate is a TRANSACTION rather than one statement, and this statement is
-- its READ step -- so a widening pass has nothing to suppress. The driver simply re-runs
-- this SELECT with the wide limit inside the SAME transaction and uses the second answer;
-- no row was locked, no bucket refilled, no deficit charged in between.
--
-- The driver, not this file, computes draw_limit, because MySQL's LIMIT takes a literal or
-- a placeholder and never an expression -- so the active-partition count it divides
-- `capacity` by is read by its own small statement in this same transaction.
--
-- adaptive admission THE PARTITION SET IS MAINTAINED, NEVER DERIVED — same change as admit.sql, same
-- reason: this CTE used to be SELECT DISTINCT (queue, partition_key) over the available
-- rows, a full scan of the backlog on every admission. headgate_active_partition is
-- maintained by the write paths (the SQL twin of the Redis gate's parts:{queue}).
-- Staleness is one-directional: a listed-but-empty partition costs one LATERAL probe and
-- is pruned by promote_due; an available job whose partition is missing would be
-- starvation, so producers insert in their own transaction with ON DUPLICATE KEY UPDATE
-- (that no-op update takes the row lock the pruner must wait behind).
WITH requested_queues AS (
  /*QUEUE_ROWS*/
),
active_parts AS (
  -- Per-queue bound: one queue's partition flood cannot erase every other queue before
  -- the weighted selector runs. The correlated probe stays index-bounded.
  SELECT ap.queue, ap.partition_key
  FROM requested_queues rq
  JOIN LATERAL (
    SELECT ap0.queue, ap0.partition_key
    FROM headgate_active_partition ap0
    LEFT JOIN headgate_queue_state qs ON qs.queue = ap0.queue
    WHERE ap0.queue = rq.queue AND COALESCE(qs.paused, FALSE) = FALSE
    ORDER BY ap0.partition_key
    LIMIT ?
  ) ap ON TRUE
),

-- adaptive admission policy-free fast-path revision THE NO-POLICY PREDICATE, the MySQL twin of queries/admit.sql's `pol`.
-- True iff nothing the policy clauses read can constrain this admission; deliberately
-- SOUND rather than tight, because a false "free" is a correctness bug and a false
-- "not free" is only slow.
--
-- WHY IT IS COMPUTED HERE AND NOT IN THE DRIVER, and this is the one place where MySQL's
-- transaction-shaped gate is genuinely more dangerous than Postgres's one-statement gate:
-- InnoDB under READ COMMITTED takes a NEW consistent-read snapshot for EVERY statement.
-- A driver-side probe would therefore be a DIFFERENT snapshot from this SELECT's, and a
-- rate bucket committed in between would be missed by the fast arm while the policy arm of
-- this very statement would have seen it. Inside the statement there is no such gap: the
-- probes and the arm they choose read one snapshot.
--
-- The ceiling probe is scoped through `active_parts` rather than through a second
-- queue-list marker site, for two reasons. It keeps that marker at exactly the two
-- occurrences both drivers' ReplaceAll expects; and it is EXACT rather than equivalent —
-- `active_parts` is already filtered to $queues, already excludes paused queues, and
-- already carries the bounded-count contract partition bound, so a ceiling it cannot join provably cannot
-- reach a candidate either.
pol AS (
  SELECT (    NOT EXISTS (SELECT 1 FROM headgate_rate_bucket)
          AND NOT EXISTS (SELECT 1 FROM headgate_quarantine)
          AND NOT EXISTS (SELECT 1 FROM headgate_concurrency_limit cl
                          JOIN active_parts apc ON apc.queue = cl.queue)
         ) AS free
),
-- adaptive admission ONE WINDOW SORT, NOT TWO — same change as queries/admit.sql, same reason. `ranked`
-- used to compute BOTH ROW_NUMBER()s, and two windows with different PARTITION BY clauses
-- are two sorts of the whole candidate set (quantum*4 rows PER active partition). The
-- rank_part window is GONE rather than reordered: the LATERAL already emits each
-- partition's rows in exactly (priority DESC, scheduled_at_ms, id) order, so ROW_NUMBER()
-- applied at the LATERAL's outer level ranks an already-ordered stream.
--
-- The inner LIMIT derived table must stay its own query level. SQL evaluates window
-- functions BEFORE ORDER BY/LIMIT, so writing ROW_NUMBER() beside `LIMIT ?` would rank the
-- partition's ENTIRE available backlog and then take quantum*4 of it — undoing the bound
-- this per-partition draw exists to enforce.
--
-- rank_part is IDENTICAL to the window it replaces: headgate_active_partition's primary
-- key is (queue, partition_key) and the join to headgate_queue_state is on that table's
-- primary key, so each partition enters the LATERAL exactly once and the old window's
-- partition IS this LATERAL's output, in this order.
--
-- rank_class STAYS over the full candidate set: it is ranked BEFORE the quarantine and
-- fairness predicates, so a quarantined candidate still consumes a rate-class slot.
-- Ranking after those filters would admit different jobs.
candidates AS (
  SELECT c.id, c.queue, c.rate_class, c.partition_key, c.weight, c.fingerprint,
         c.priority, c.scheduled_at_ms, c.rank_part
  FROM active_parts ap
  JOIN LATERAL (
    SELECT m.*,
           ROW_NUMBER() OVER (ORDER BY m.priority DESC, m.scheduled_at_ms, m.id) AS rank_part
    FROM (
      SELECT routes.*
      FROM (
        (SELECT j.id, j.queue, j.rate_class, j.partition_key, j.weight, j.fingerprint,
                j.priority, j.scheduled_at_ms
         FROM headgate_job j
         WHERE j.state = 'available'
           AND j.queue = ap.queue
           AND j.partition_key = ap.partition_key
           AND j.sticky_worker = ''
           AND (j.scheduled_at_ms <= ? OR j.priority > 0)
         ORDER BY j.priority DESC, j.scheduled_at_ms, j.id
         LIMIT ?)
        UNION ALL
        (SELECT j.id, j.queue, j.rate_class, j.partition_key, j.weight, j.fingerprint,
                j.priority, j.scheduled_at_ms
         FROM headgate_job j
         WHERE j.state = 'available'
           AND j.queue = ap.queue
           AND j.partition_key = ap.partition_key
           AND j.sticky_worker = ?
           AND (j.scheduled_at_ms <= ? OR j.priority > 0)
         ORDER BY j.priority DESC, j.scheduled_at_ms, j.id
         LIMIT ?)
      ) routes
      ORDER BY routes.priority DESC, routes.scheduled_at_ms, routes.id
      LIMIT ?
    ) m
  ) c ON TRUE
),
ranked AS (
  SELECT c.*,
    -- surveyed policy behavior cost-weighted limits: the prefix consumes envelope weights, not row count.
    SUM(c.weight) OVER (
      PARTITION BY c.rate_class
      ORDER BY c.priority DESC, c.scheduled_at_ms, c.id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cost_class
  FROM candidates c
),
bucket_state AS (
  -- Same lazy-refill formula the driver used when locking; DIV mirrors PG's integer
  -- division.
  SELECT b.name,
         LEAST(b.burst,
               b.tokens + ((? - b.refilled_at_ms) * b.limit_per_window DIV b.window_ms)
         ) AS avail
  FROM headgate_rate_bucket b
),
-- adaptive admission THE INFLIGHT COUNT IS MAINTAINED, NEVER DERIVED — the same trick as
-- headgate_active_partition, applied to the gate's other full scan. This CTE used to be
--   SELECT queue, partition_key, COUNT(*) FROM headgate_job WHERE state = 'running' ..
-- which aggregates EVERY running row in the fleet on EVERY admission, and on MySQL there
-- is not even a partial index to narrow it: headgate_job_avail_partition leads with state,
-- so it degenerated to a scan of the whole running prefix.
--
-- GRANULARITY IS (queue, partition_key) AND THAT IS NOT AN ACCIDENT. admission policy states the
-- clause as "is this job's PARTITION under its ceiling?": headgate_concurrency_limit is
-- keyed per QUEUE, and the ceiling it names applies to each partition of that queue
-- separately. A per-queue counter would silently be a different, stricter policy.
--
-- The join to active_parts is the bound, not a filter: the LEFT JOIN below can only match
-- a partition that reached `ranked`, and every such partition is in active_parts. At most
-- capacity*overfetch primary-key lookups instead of a scan of the running set. The +1 is
-- the driver's post-claim upsert in the SAME transaction; the −1 rides every running → *
-- transition; reconcile_inflight (promote_due) heals drift.
inflight AS (
  SELECT f.queue, f.partition_key, f.n
  FROM headgate_inflight f
  JOIN active_parts ap2 ON ap2.queue = f.queue AND ap2.partition_key = f.partition_key
),
policy_ready AS (
  -- Earlier policies speak before saturation. The rank therefore counts only candidates
  -- that could otherwise run; a quarantined/rate-blocked head cannot consume a slot.
  SELECT r.id, r.queue, r.rate_class, r.partition_key, r.priority, r.scheduled_at_ms
       , cl.max_concurrent, cl.on_saturated, COALESCE(i.n, 0) AS inflight
       , ROW_NUMBER() OVER (
           PARTITION BY r.queue, r.partition_key
           ORDER BY r.priority DESC, r.scheduled_at_ms, r.id
         ) AS concurrency_rank
  FROM ranked r
  LEFT JOIN bucket_state b                ON b.name = r.rate_class
  LEFT JOIN headgate_quarantine q         ON q.fingerprint = r.fingerprint
  LEFT JOIN headgate_partition_deficit d  ON d.queue = r.queue AND d.partition_key = r.partition_key
  LEFT JOIN headgate_concurrency_limit cl ON cl.queue = r.queue
  LEFT JOIN inflight i                    ON i.queue = r.queue AND i.partition_key = r.partition_key
  WHERE NOT (SELECT free FROM pol)   -- adaptive admission policy-free fast-path revision: the other arm answered this call
    AND q.fingerprint IS NULL                                        -- crash quarantine not quarantined
    -- admission policy has budget. FAIL OPEN on an UNCONFIGURED class: b.name IS NULL means no bucket
    -- row exists, and a limit nobody has written is not a limit -- the semantic admit.lua
    -- has always had. Was COALESCE(b.avail, 0) (unconfigured admitted NOTHING), which made
    -- a typo'd rate_class a silent permanent stall. A CONFIGURED bucket is unchanged, so
    -- invariant 16's kill switch (limit 0 + empty bucket => avail 0) still admits nothing.
    AND (r.rate_class = '' OR b.name IS NULL OR r.cost_class <= b.avail)
    AND (r.rank_part  <= COALESCE(d.deficit, 0) + ?)                 -- tenant fairness within fair share
),
elig_policy AS (
  SELECT p.id, p.queue, p.rate_class, p.partition_key, p.priority, p.scheduled_at_ms,
         CASE
           WHEN p.max_concurrent IS NULL THEN 'claim'
           WHEN p.on_saturated = 'cancel_running'
             AND p.concurrency_rank <= p.max_concurrent THEN 'cancel_running'
           WHEN p.inflight + p.concurrency_rank <= p.max_concurrent THEN 'claim'
           WHEN p.on_saturated = 'discard' THEN 'discard'
           WHEN p.on_saturated = 'cancel_incoming' THEN 'cancel_incoming'
           ELSE 'queue'
         END AS saturation_action
  FROM policy_ready p
  WHERE p.max_concurrent IS NULL
     OR p.inflight + p.concurrency_rank <= p.max_concurrent
     OR p.on_saturated IN ('discard', 'cancel_incoming')
     OR (p.on_saturated = 'cancel_running'
         AND p.concurrency_rank <= p.max_concurrent)
),

-- adaptive admission policy-free fast-path revision THE FAST ARM. It reads `candidates` rather than `ranked`, so the
-- rate-class window is not merely unused but never computed, and it drops the quarantine,
-- bucket, ceiling and inflight joins outright — none of which can change an answer that
-- no policy row exists to change.
--
-- WHAT IT KEEPS, and why this arm is NOT a copy of Postgres's. tenant fairness's fairness clause
-- STAYS here. Postgres's fast arm can delete it because the draw itself is narrowed to
-- `LEAST(quantum*4, deficit + quantum, capacity, <round-32d adaptive window>)` PER
-- PARTITION, which is always at or below the fair share — but MySQL's LIMIT takes a
-- literal or a placeholder and never an expression, so a per-partition bound is not
-- expressible here and the draw is unchanged from adaptive-window revision. Keeping the clause is
-- therefore not a shortcut, it is the correct compensation: with the draw unchanged, the
-- fair share must still be applied, and the round-32d escalation verdict below stays sound
-- for this arm too and stays UNCONDITIONALLY ON (Postgres can switch it off when its own
-- exact bound binds; MySQL, drawing adaptive-window revision's window either way, never can).
-- The consequence, stated rather than hidden: MySQL's fast path saves the rate-class
-- window and the four policy joins but not the rows read and not the escalation, so its
-- win is smaller than Postgres's by construction.
elig_free AS (
  SELECT c.id, c.queue, c.rate_class, c.partition_key, c.priority, c.scheduled_at_ms,
         'claim' AS saturation_action
  FROM candidates c
  LEFT JOIN headgate_partition_deficit d  ON d.queue = c.queue AND d.partition_key = c.partition_key
  WHERE (SELECT free FROM pol)
    AND (c.rank_part <= COALESCE(d.deficit, 0) + ?)                  -- tenant fairness within fair share
),

-- Queue weight chooses BETWEEN queues after policy. Candidate rank is computed only
-- WITHIN a queue, so a numeric job priority can never jump across queues (queue-weight separation).
eligible_pool AS (
  SELECT * FROM elig_policy
  UNION ALL
  SELECT * FROM elig_free
),
queue_ranked AS (
  SELECT e.*,
         ROW_NUMBER() OVER (
           PARTITION BY e.queue
           ORDER BY e.priority DESC, e.scheduled_at_ms, e.id
         ) AS queue_rank
  FROM eligible_pool e
),
eligible AS (
  SELECT r.id, r.queue, r.rate_class, r.partition_key, r.priority, r.scheduled_at_ms,
         r.saturation_action
  FROM queue_ranked r
  JOIN headgate_queue_state qs ON qs.queue = r.queue
  ORDER BY (CAST(qs.dispatch_count + r.queue_rank - 1 AS DECIMAL(65,20)) / qs.weight),
           r.queue, r.queue_rank
  LIMIT ?
),

-- adaptive admission adaptive-window revision THE ESCALATION VERDICT — the MySQL twin of admit.sql's part_tail/elig_z/
-- verdict chain, clause for clause.
--
-- part_tail: how many rows each partition DREW and the sort key of the last one.
-- rank_part is a dense ROW_NUMBER over the drawn rows, so MAX(rank_part) IS the drawn
-- count and the tail is the row carrying it.
--
-- adaptive admission policy-free fast-path revision: sourced from `candidates`, not `ranked`. `ranked` is `candidates` plus a
-- rate-class rank and drops no row, so rank_part, the drawn count and the tail are
-- identical — but reading `candidates` leaves `ranked` referenced ONLY by `elig_policy`,
-- which is what lets the fast arm skip that window entirely instead of merely ignoring it.
part_tail AS (
  SELECT s.queue, s.partition_key, s.drawn, c2.priority, c2.scheduled_at_ms, c2.id
  FROM (SELECT queue, partition_key, MAX(rank_part) AS drawn
        FROM candidates GROUP BY queue, partition_key) s
  JOIN candidates c2 ON c2.queue = s.queue
                    AND c2.partition_key = s.partition_key
                    AND c2.rank_part = s.drawn
),
-- z: the row the final LIMIT stops at, and ONLY when that limit actually bound. Taken by
-- reversing all three sort keys rather than by OFFSET, which would re-scan.
elig_n AS (SELECT COUNT(*) AS n FROM eligible),
elig_z AS (
  SELECT e.priority, e.scheduled_at_ms, e.id
  FROM eligible e CROSS JOIN elig_n n
  WHERE n.n >= ?                                                   -- capacity
  ORDER BY e.priority ASC, e.scheduled_at_ms DESC, e.id DESC
  LIMIT 1
),
-- Widen iff some partition BOTH (a) actually hit the narrow limit while that limit is
-- below quantum*4, i.e. it really did lose rows the old gate would have DRAWN -- a
-- partition that merely RAN OUT of jobs has drawn < the limit and is never a reason to
-- re-draw, which is the half of the test that pays for the mechanism -- AND (b) could
-- still hide a row that belongs in the answer: either the final LIMIT never bound (no z)
-- or its last drawn row sorts BEFORE z.
--
-- (a) IS `quantum*4` AND NOT LEAST(quantum*4, deficit + quantum), for the reason spelled
-- out in queries/admit.sql and found by RUNNING the relaxed version on Postgres: a row
-- beyond the fair share is never admitted but is still a CANDIDATE, and rank_class is
-- computed over the full candidate set on purpose, so a fairness-blocked candidate
-- consumes a rate-class slot exactly as a quarantined one does. Dropping it hands that
-- slot to another partition and changes who runs.
--
-- Comparison is in the gate's own order (priority DESC, scheduled_at_ms, id), so priority
-- is negated to make all three keys ascending.
verdict AS (
  SELECT EXISTS (
    SELECT 1
    FROM part_tail t
    LEFT JOIN elig_z z ON 1 = 1
    WHERE ? > 0                                                    -- capacity
      AND t.drawn >= ?                                             -- draw_limit
      AND ? < ?                                                    -- draw_limit < quantum*4
      AND ((SELECT COUNT(DISTINCT queue) FROM active_parts) > 1
           OR z.id IS NULL
           OR (-t.priority, t.scheduled_at_ms, t.id)
            < (-z.priority, z.scheduled_at_ms, z.id))
  ) AS widen
)
SELECT e.saturation_action AS tag, e.id AS id, e.queue, e.partition_key FROM eligible e
UNION ALL
SELECT 'p', 0, rp.queue, rp.partition_key
FROM (SELECT DISTINCT queue, partition_key FROM candidates) rp
UNION ALL
SELECT 'w', (SELECT widen FROM verdict), '', ''
