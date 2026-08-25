-- admission policy THE ADMISSION GATE.
-- Policy evaluation + claim + lease + accounting, in ONE statement.
-- Jobs rejected by policy are never locked, so they never block another worker.
--
-- $1 queues text[]   $2 capacity int      $3 UNUSED (was now_ms)  $4 lease_ms bigint
-- $5 worker text     $6 lease_id text     $7 quantum bigint       $8 overfetch int
-- $9 wide int        -- adaptive admission adaptive-window revision: 0 = narrow (adaptive) pass, 1 = wide (final) pass
--
-- adaptive admission ADAPTIVE WIDENING (adaptive-window revision). The candidate window used to
-- be a flat `LIMIT quantum * 4` per active partition, so the gate READ 4x the rows it
-- could claim and throughput was very nearly inversely proportional to that window
-- (measured ablation, one partition, capacity 100: 400 rows 42.3k/s, 800 27.4k/s,
-- 1600 20.8k/s, 3200 13.7k/s, against a plain SKIP LOCKED's 75.8k/s).
--
-- The window cannot simply be truncated at `capacity`: a candidate at rank_part k is
-- still admissible when the k-1 rows ahead of it are quarantined or out of rate budget.
-- So this statement draws NARROW first and reports, PRECISELY, whether the narrow draw
-- could have changed the answer. The driver re-issues with $9 = 1 only when it did.
--
-- THE PROOF THAT THE ADMITTED SET IS UNCHANGED. Let z be the LAST row of `eligible`
-- (the capacity-th, i.e. the one the final LIMIT stops at). Every row a narrower draw
-- dropped from partition p sorts strictly AFTER p's last DRAWN row. Therefore, if every
-- truncated partition's last drawn row sorts at-or-after z, every dropped row sorts
-- after z, and:
--   * rank_class(r) for any r <= z counts only rows sorting <= r, all of which were
--     drawn -- so the rate clause decides identically. This is the ONLY clause the draw
--     size can move, and it is why the test below is against `quantum * 4` and not
--     against the fair share: rank_class is computed over the FULL candidate set on
--     purpose, so a quarantined OR FAIRNESS-BLOCKED candidate still consumes a class
--     slot, and a row that is never admissible is still load-bearing as a candidate;
--   * rank_part, the ceiling clause and quarantine are row-local and already identical;
--   * `eligible` therefore agrees with the wide draw on every row <= z, and the wide
--     draw's extra rows all sort after z, so ORDER BY .. LIMIT capacity returns THE SAME
--     capacity rows.
-- If `eligible` did NOT reach capacity there is no z, and a wider draw could add rows --
-- so any truncation at all forces the wide pass. `charge` sees the same partition set
-- either way (a partition with any available row yields at least one candidate at any
-- limit >= 1), and `spend`/`infl` count claims, which are the same rows.
--
-- TERMINATION IS STRUCTURAL, not a retry budget: with $9 = 1 the limit IS quantum * 4,
-- so `part_tail` is empty by construction (the `wide = 0` term of `chain`) and no row
-- could satisfy the verdict's `lim < quantum * 4` clause either. `verdict.widen` is false.
-- At most two passes, and the second is byte-for-byte the statement this file has always
-- been. The round-32e fast arm inherits this unchanged: on the wide pass its own draw
-- collapses to its EXACT bound, which is what makes that pass final there too.
-- The narrow pass claims NOTHING when it widens -- `locked`, `spend` and `charge` are all
-- gated on the verdict -- so the wide pass runs against exactly the state the narrow pass
-- found, and each pass remains its own atomic unit. Between passes another worker may
-- claim rows; that is the same READ COMMITTED race the gate already runs against
-- concurrent producers, and per-pass atomicity is the bar, not cross-pass snapshots.
--
-- TIME COMES FROM THE STORE, NEVER THE CALLER. `now_ms` used to be parameter $3, which
-- made every limit a function of the calling worker's clock: a worker 60s fast computes
-- 60 extra seconds of token refill and admits a second full bucket in the same real
-- second. Measured: 10 admitted against a limit of 5. It also skews lease expiry, which
-- causes early expiry (double-claim) or late expiry (stranded job).
-- Sidekiq's rate limiter documents an NTP requirement for exactly this reason; the store
-- is the one clock every worker already shares, so use it.
--
-- adaptive admission THE POLICY-FREE FAST PATH (policy-free fast-path revision). adaptive admission asks for "a fast
-- path that skips evaluation for jobs with no policy attached"; this is it, and it is one
-- extra CTE (`pol`) plus a second `eligible` arm, NOT a second statement. Detection is
-- IN-STATEMENT and therefore shares the statement's snapshot, which is what makes it
-- race-correct rather than merely cheap — see the argument on `pol` below.
WITH params AS (
  SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint AS now_ms,
         $4::bigint AS lease_ms, $5::text AS worker,
         $6::text AS lease_id, $7::bigint AS quantum, $2::int AS capacity,
         $8::int AS overfetch, $9::int AS wide
),

-- adaptive admission policy-free fast-path revision THE NO-POLICY PREDICATE. `free` is true iff NOTHING the policy clauses
-- read can constrain this admission. It is deliberately SOUND rather than tight: a false
-- "free" is a correctness bug, a false "not free" is only slow, so every probe errs toward
-- the full path.
--
-- The three probes are exactly the three policy tables `eligible` reads that the fast arm
-- would otherwise have to consult. Two are unscoped and one is queue-scoped, and that
-- asymmetry is the tables' own, not a shortcut:
--   * headgate_rate_bucket  — a bucket constrains any candidate carrying its class name,
--     and rate_class is a free-form string on the job, so ANY bucket row anywhere is a
--     reason to evaluate. Scoping this to "classes referenced by the candidates" would be
--     circular: the candidate set depends on the draw, and the draw depends on `free`.
--   * headgate_quarantine   — keyed by fingerprint, which is likewise global.
--   * headgate_concurrency_limit — keyed BY QUEUE, and `eligible` joins it on
--     `cl.queue = r.queue` where every candidate's queue is in $1. So a ceiling on some
--     OTHER queue provably cannot bind here, and scoping the probe to $1 is exact.
-- Three things the list deliberately does NOT contain, because the fast arm handles them
-- rather than skipping them:
--   * headgate_partition_deficit — FAIRNESS IS CORE SEMANTICS, NOT POLICY. It survives on
--     the fast path (see `candidates` and `charge`); a fast path that dropped the deficit
--     round-robin would change the admitted set and would simply be wrong.
--   * headgate_queue_state.paused — read by `active_parts`, which is SHARED by both arms,
--     so a paused queue is excluded identically either way and needs no probe.
--   * headgate_inflight — read ONLY by the concurrency clause, so the ceiling probe above
--     already covers it. The counter is still MAINTAINED on the fast path (`infl` below):
--     a fast path that stopped counting would leave the ceiling wrong for the first
--     admission after someone configures one.
--
-- WHY IN-STATEMENT DETECTION IS RACE-CORRECT, and why a cached flag was not needed. Every
-- CTE in a Postgres statement — these EXISTS probes included — sees ONE snapshot, taken
-- when the statement begins. So:
--   * a policy row committed BEFORE that snapshot is visible to the probe, `free` is
--     false, and the full path runs. There is no window in which it can be missed.
--   * a policy row committed AFTER that snapshot is invisible to the probe AND to every
--     policy join the full path would have run, and it cannot be locked by
--     `bucket_state`'s FOR UPDATE either (row locking re-checks rows the scan FOUND; it
--     cannot find a row the snapshot excludes). So the full path at this same snapshot
--     would have admitted exactly what the fast path admits.
-- The fast path is therefore not "the full path with a staleness window" — it is the full
-- path evaluated at the same instant, with the clauses that provably cannot fire removed.
-- A driver-side cached flag would have needed an invalidation argument for exactly the
-- window this construction does not have.
pol AS (
  SELECT (    NOT EXISTS (SELECT 1 FROM headgate_rate_bucket)
          AND NOT EXISTS (SELECT 1 FROM headgate_quarantine)
          AND NOT EXISTS (SELECT 1 FROM headgate_concurrency_limit cl
                          WHERE cl.queue = ANY($1::text[]))
         ) AS free
),

-- Lock and lazily refill the token buckets. FOR UPDATE is what makes the limit
-- fleet-wide rather than per-process: concurrent admissions serialize here.
bucket_state AS (
  SELECT b.name,
         LEAST(b.burst,
               b.tokens + ((p.now_ms - b.refilled_at_ms) * b.limit_per_window / b.window_ms)
         )::bigint AS avail
  FROM headgate_rate_bucket b CROSS JOIN params p
  FOR UPDATE OF b
),

-- tenant fairness CRITICAL: candidates are drawn PER PARTITION, never from one flat window.
-- A single ORDER BY .. LIMIT window returns only the flooding tenant's jobs, so quiet
-- tenants never enter the candidate set. Fairness then degrades to FIFO while still
-- appearing to enforce a quantum -- and throughput collapses to one partition's share.
-- Verified: with 5000 jobs in one partition and 8x overfetch, the flat version returned
-- 3 rows from one tenant where the correct answer was 9 rows across three.
--
-- adaptive admission THE PARTITION SET IS MAINTAINED, NEVER DERIVED. This CTE used to be
--   SELECT DISTINCT j.queue, j.partition_key FROM headgate_job WHERE state = 'available' ..
-- which is a full scan of the available backlog on EVERY admission: partition_key is not
-- in headgate_job_admit, Postgres has no skip scan, and the LIMIT therefore cannot help.
-- Measured: 9k claims/s at a 20k backlog against 55k for a plain SKIP LOCKED fetch, and
-- O(backlog) per call. The write paths now maintain headgate_active_partition instead --
-- exactly what the Redis gate has always done with parts:{queue}, which is why the Lua
-- gate never had this problem.
-- Staleness is one-directional and deliberate: a listed partition with no available job
-- costs one LATERAL probe (index lookup, zero rows) and is pruned by the promote_due
-- duty. The reverse -- an available job whose partition is not listed -- is starvation,
-- and is prevented by inserting into the set inside the SAME transaction that makes the
-- row available, with ON CONFLICT DO UPDATE so the producer takes the row lock the
-- pruner must wait behind.
--
-- adaptive admission policy-free fast-path revision: the partition's DEFICIT is carried out of here as well. It is one probe
-- of headgate_partition_deficit's primary key per listed partition, and the fast arm needs
-- it to size its draw (below) — the fair share `deficit + quantum` IS the bound that makes
-- a policy-free draw provably sufficient. The join is 1:1 on that table's primary key, so
-- it changes neither the cardinality nor which partitions the bounded-count contract LIMIT keeps.
requested_queues AS (
  SELECT DISTINCT unnest($1::text[]) AS queue
),
active_parts AS (
  -- The bound is PER QUEUE. A fleet with many partitions in the lexically first queue
  -- must not fill one flat partition window and erase every other queue before the
  -- weighted selector gets to choose between them.
  SELECT ap.queue, ap.partition_key, COALESCE(pd.deficit, 0) AS deficit
  FROM requested_queues rq
  CROSS JOIN params p
  CROSS JOIN LATERAL (
    SELECT ap0.queue, ap0.partition_key
    FROM headgate_active_partition ap0
    LEFT JOIN headgate_queue_state qs ON qs.queue = ap0.queue
    WHERE ap0.queue = rq.queue
      AND COALESCE(qs.paused, false) = false
    ORDER BY ap0.partition_key
    LIMIT (p.capacity * p.overfetch)
  ) ap
  LEFT JOIN headgate_partition_deficit pd
         ON pd.queue = ap.queue AND pd.partition_key = ap.partition_key
),

-- queue-weight separation weighted queue state is locked before selection, so concurrent workers cannot
-- both spend the same virtual service position. A legacy/direct-SQL queue with no row
-- reads as (weight=1, dispatch_count=0); `queue_charge` creates it after a real claim.
queue_state_locked AS MATERIALIZED (
  SELECT qs.queue, qs.weight, qs.dispatch_count
  FROM headgate_queue_state qs
  JOIN (SELECT DISTINCT queue FROM active_parts) a USING (queue)
  ORDER BY qs.queue
  FOR UPDATE OF qs
),
queue_policy AS (
  SELECT a.queue, COALESCE(q.weight, 1)::bigint AS weight,
         COALESCE(q.dispatch_count, 0)::bigint AS dispatch_count
  FROM (SELECT DISTINCT queue FROM active_parts) a
  LEFT JOIN queue_state_locked q USING (queue)
),

-- adaptive admission adaptive-window revision THE CANDIDATE WINDOW, now sized to the work instead of to the worst case.
-- One admit can never return more than `capacity` rows, so a partition's useful share of
-- ONE draw is about capacity / active_partitions. The narrow limit is that share PLUS ONE
-- row, and the extra row is not slack -- it is what makes the verdict below decidable: a
-- partition whose last drawn row sorts after z proves nothing beyond z was dropped.
-- The wide pass ($9 = 1) restores `quantum * 4` exactly, which is both the old behavior
-- and the escalation ceiling: beyond it the previous gate would not have admitted either,
-- because a partition's fair share is deficit + quantum and deficit is capped at 4 x
-- quantum by `charge`.
draw AS (
  SELECT CASE
           WHEN p.wide <> 0 THEN p.quantum * 4
           ELSE LEAST(p.quantum * 4, ((p.capacity + n.c - 1) / n.c)::bigint + 1)
         END AS lim
  FROM params p
  CROSS JOIN (SELECT GREATEST(count(*), 1)::bigint AS c FROM active_parts) n
),

-- adaptive admission policy-free fast-path revision DOES THE POLICY-FREE ARM STILL NEED THE ROUND-32d ESCALATION?
--
-- The fast arm's own exact bound is `E = LEAST(quantum*4, deficit + quantum, capacity)`
-- per partition (proved at `candidates`). adaptive-window revision's adaptive window is a SCALAR,
-- `ceil(capacity / active_partitions) + 1`, and neither dominates the other:
--   * one partition, capacity 100, quantum 200 — E = 100, adaptive-window revision's window = 101. E is
--     tighter AND exact, so the escalation machinery is pure overhead.
--   * ten partitions, capacity 100, quantum 20 — deficit walks to `4 x quantum` = 80, so
--     E = 80 and adaptive-window revision's window is 11. Drawing E is 800 rows to claim 100, i.e. the
--     WIDE window; the adaptive one is 7x narrower. Measured, no policy, 10 partitions:
--     drawing E cost 13% against adaptive-window revision on this laptop, which is the whole reason this
--     paragraph exists — the first version of this arm did exactly that.
-- So the draw is the MINIMUM of the two, and the escalation chain is switched on exactly
-- when the round-32d window is the binding one, i.e. when the fast arm actually gave up
-- rows its own exact bound would have drawn. When E binds, nothing was dropped, the answer
-- is provably complete, and the chain stays off — which is the case adaptive admission's fast path is
-- about and the case where the saving is largest.
--
-- On the WIDE pass `draw.lim` IS `quantum * 4` and E <= quantum * 4, so the minimum is E:
-- the second pass draws the exact bound, the verdict is false by construction, and
-- termination stays structural rather than becoming a retry budget.
fast_esc AS (
  SELECT (SELECT free FROM pol)
     AND EXISTS (
       SELECT 1
       FROM active_parts ap CROSS JOIN params p CROSS JOIN draw w
       WHERE w.lim < LEAST(p.quantum * 4,
                           ap.deficit + p.quantum,
                           GREATEST(p.capacity, 1)::bigint)
     ) AS esc
),
-- One scalar, read by the three CTEs of the escalation chain. On the policy path it is
-- adaptive-window revision's `wide = 0` unchanged.
chain AS (
  SELECT (SELECT wide FROM params) = 0
     AND (NOT (SELECT free FROM pol) OR (SELECT esc FROM fast_esc)) AS needed
),
-- adaptive admission ONE WINDOW SORT, NOT TWO. `ranked` used to compute BOTH row_number()s here, and
-- two windows with different PARTITION BY clauses are two sorts of the whole candidate
-- set: at the bench's quantum 200 x 10 partitions that was 8,000 rows sorted twice, 38 ms
-- of a 50 ms admit (Sort 19.4 ms + Sort 15.4 ms + two WindowAggs). The rank_part window
-- is now GONE, not merely reordered: the LATERAL already emits each partition's rows in
-- exactly (priority DESC, scheduled_at_ms, id) order -- that is what
-- headgate_job_avail_partition is for -- so row_number() applied at the LATERAL's outer
-- level is a WindowAgg over an ALREADY-SORTED input and plans with no Sort node at all.
--
-- The inner LIMIT subquery is load-bearing and must stay a separate query level. SQL
-- evaluates window functions BEFORE ORDER BY/LIMIT, so writing row_number() alongside the
-- `LIMIT quantum * 4` would rank the partition's ENTIRE available backlog first and then
-- take 800 of it -- undoing the bound this draw exists to enforce. A subquery with LIMIT
-- is never flattened by the planner, so the two levels stay two levels.
--
-- rank_part is IDENTICAL to the window it replaces: active_parts is keyed by the primary
-- key of headgate_active_partition and its LEFT JOIN to headgate_queue_state is on that
-- table's primary key, so each (queue, partition_key) enters the LATERAL exactly once and
-- the old window's partition IS this LATERAL's output, in this order.
--
-- rank_class STAYS where it is, over the full candidate set, and that is deliberate: it
-- is ranked BEFORE the quarantine and fairness predicates, so a quarantined candidate
-- still consumes a rate-class slot. Computing it after those filters would admit
-- different jobs (class avail 1, top candidate quarantined: today nothing is admitted for
-- the class, filtered-first would admit the runner-up). One sort, same answer.
candidates AS (
  SELECT c.id, c.queue, c.rate_class, c.partition_key, c.weight, c.fingerprint,
         c.priority, c.scheduled_at_ms, c.rank_part
  FROM active_parts ap
  CROSS JOIN params p
  CROSS JOIN LATERAL (
    SELECT CASE WHEN (SELECT free FROM pol)
                THEN LEAST(p.quantum * 4,
                           ap.deficit + p.quantum,
                           GREATEST(p.capacity, 1)::bigint,
                           (SELECT lim FROM draw))
                ELSE (SELECT lim FROM draw)
           END AS lim
  ) route_draw
  CROSS JOIN LATERAL (
    SELECT m.*,
           row_number() OVER (ORDER BY m.priority DESC, m.scheduled_at_ms, m.id) AS rank_part
    FROM (
      -- Sticky routing is filtered BEFORE ranking. Filtering later would let a high
      -- priority prefix pinned to other workers fill the bounded draw and hide runnable
      -- work. Two route streams are bounded independently through
      -- headgate_job_avail_sticky, merged, and bounded again; the top N of their union
      -- cannot contain a row below rank N in either input.
      SELECT routes.*
      FROM (
        (SELECT j.id, j.queue, j.rate_class, j.partition_key, j.weight, j.fingerprint,
                j.priority, j.scheduled_at_ms
         FROM headgate_job j
         WHERE j.state = 'available'
           AND j.queue = ap.queue
           AND j.partition_key = ap.partition_key
           AND j.sticky_worker = ''
           AND (j.scheduled_at_ms <= p.now_ms OR j.priority > 0)
         ORDER BY j.priority DESC, j.scheduled_at_ms, j.id
         LIMIT route_draw.lim)
        UNION ALL
        (SELECT j.id, j.queue, j.rate_class, j.partition_key, j.weight, j.fingerprint,
                j.priority, j.scheduled_at_ms
         FROM headgate_job j
         WHERE j.state = 'available'
           AND j.queue = ap.queue
           AND j.partition_key = ap.partition_key
           AND p.worker <> ''
           AND j.sticky_worker = p.worker
           AND (j.scheduled_at_ms <= p.now_ms OR j.priority > 0)
         ORDER BY j.priority DESC, j.scheduled_at_ms, j.id
         LIMIT route_draw.lim)
      ) routes
      ORDER BY routes.priority DESC, routes.scheduled_at_ms, routes.id
      LIMIT route_draw.lim
    ) m
  ) c
),

ranked AS (
  SELECT c.*,
    -- surveyed policy behavior cost-weighted limits: the class prefix is measured in TOKENS, not rows.
    -- ROWS is explicit so equal sort keys cannot collapse into one RANGE peer group.
    sum(c.weight::bigint) OVER (
      PARTITION BY c.rate_class
      ORDER BY c.priority DESC, c.scheduled_at_ms, c.id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cost_class
  FROM candidates c
),

-- A configured ceiling is serialized at the same granularity the policy is written.
-- The inflight rows below are seeded by enqueue and locked per partition; together these
-- locks make the read/terminalize-or-claim/counter update one fleet-wide decision.
limit_state AS MATERIALIZED (
  SELECT cl.queue, cl.max_concurrent, cl.on_saturated
  FROM headgate_concurrency_limit cl
  WHERE EXISTS (SELECT 1 FROM active_parts ap WHERE ap.queue = cl.queue)
  ORDER BY cl.queue
  FOR UPDATE OF cl
),

-- adaptive admission THE INFLIGHT COUNT IS MAINTAINED, NEVER DERIVED -- the same trick as
-- headgate_active_partition, applied to the other full scan. This CTE used to be
--   SELECT queue, partition_key, count(*) FROM headgate_job WHERE state = 'running' ..
-- which aggregates EVERY running row in the fleet on EVERY admission. Measured on the
-- adaptive admission bench: 0.09 ms at 200 running, 2.5 ms at 10k, 4.3 ms at 20k, 11.5 ms at 50k -- the
-- whole of the gate's remaining sensitivity to how much work is in flight, paid even when
-- no queue has a concurrency ceiling configured at all.
--
-- GRANULARITY IS (queue, partition_key) AND THAT IS NOT AN ACCIDENT. admission policy states the
-- clause as "is this job's PARTITION under its ceiling?": headgate_concurrency_limit is
-- keyed per QUEUE, and the ceiling it names applies to each partition of that queue
-- separately. The counter therefore has to be per (queue, partition_key), matching the
-- old GROUP BY exactly -- a per-queue counter would enforce a different, stricter policy.
--
-- The join to active_parts is not a filter, it is the bound: the LEFT JOIN below can only
-- match a partition that reached `ranked`, and every such partition is in active_parts.
-- So this reads at most `capacity * overfetch` rows by primary key instead of scanning
-- the running set. Rows are +1'd by the `infl` arm at the bottom of this statement and
-- -1'd by every running -> * transition; `reconcile_inflight` (promote_due) heals drift.
--
-- MATERIALIZED is not decoration. A single-reference CTE is inlined in PG12+, and the
-- planner then re-executed this lookup once per RANKED ROW inside a nested loop — 2,000
-- primary-key probes, 2.0 ms, no better than the scan it replaced. Materialized it runs
-- ONCE, at most `capacity * overfetch` rows, and the join above becomes a hash probe.
inflight AS MATERIALIZED (
  SELECT f.queue, f.partition_key, f.n
  FROM headgate_inflight f
  JOIN active_parts ap ON ap.queue = f.queue AND ap.partition_key = f.partition_key
  JOIN limit_state cl ON cl.queue = f.queue
  ORDER BY f.queue, f.partition_key
  FOR UPDATE OF f
),

-- adaptive admission policy-free fast-path revision THE FAST ARM. No WHERE clause, because the draw above can only ever be
-- AT OR BELOW the fair share `deficit + quantum`, which is the only bound that exists when
-- `free` — so every drawn row is admissible and there is nothing left to filter. (Below
-- the fair share is the adaptive-window case, and that is what `fast_esc` re-arms the
-- escalation for.) What it does NOT skip is as important as what it does:
--   * the per-partition LATERAL draw survives (trap #2: one flat window is not fairness);
--   * `active_parts`, and with it the paused-queue filter, is shared;
--   * `charge` still charges the deficit and `infl` still counts the claim;
--   * `locked` still re-checks `state = 'available'` under FOR UPDATE SKIP LOCKED
--     (trap #1: SKIP LOCKED alone double-claims).
-- What it skips is `ranked`'s rate-class window, all five policy joins and the maintained
-- inflight read — none of which can change an answer that no policy row exists to change —
-- plus the round-32d escalation chain whenever the draw was exact.
--
-- Both arms carry a one-time filter on `pol.free`, which is a scalar with no Vars, so
-- Postgres evaluates it ONCE per node and leaves the losing arm's whole subtree unexecuted
-- (the same mechanism adaptive-window revision's `wide = 0` filters already rely on). The arm not taken
-- costs a plan node, not a row.
elig_free AS (
  SELECT c.id, c.queue, c.rate_class, c.partition_key,
         c.priority, c.scheduled_at_ms, 'claim'::text AS saturation_action
  FROM candidates c
  WHERE (SELECT free FROM pol)
),

-- Policy through fairness, before the ceiling decides what happens to a saturated row.
-- `concurrency_rank` counts only rows that survived the earlier clauses, so a quarantined
-- or rate-blocked head cannot consume a concurrency slot it will never run in.
policy_ready AS (
  SELECT r.id, r.queue, r.rate_class, r.partition_key,
         r.priority, r.scheduled_at_ms,
         cl.max_concurrent, cl.on_saturated, COALESCE(i.n, 0) AS inflight,
         row_number() OVER (
           PARTITION BY r.queue, r.partition_key
           ORDER BY r.priority DESC, r.scheduled_at_ms, r.id
         )::bigint AS concurrency_rank
  FROM ranked r
  CROSS JOIN params p
  LEFT JOIN bucket_state b               ON b.name = r.rate_class
  LEFT JOIN headgate_quarantine q        ON q.fingerprint = r.fingerprint
  LEFT JOIN headgate_partition_deficit d ON d.queue = r.queue AND d.partition_key = r.partition_key
  LEFT JOIN limit_state cl                ON cl.queue = r.queue
  LEFT JOIN inflight i                   ON i.queue = r.queue AND i.partition_key = r.partition_key
  WHERE NOT (SELECT free FROM pol)   -- adaptive admission policy-free fast-path revision: the other arm answered this call
    AND q.fingerprint IS NULL                                        -- crash quarantine not quarantined
    -- admission policy has budget. FAIL OPEN on an UNCONFIGURED class: b.name IS NULL means no bucket
    -- row exists, and a limit nobody has written is not a limit -- the same semantic
    -- admit.lua's bucket_avail has always had (`if not b[1] then return math.huge end`).
    -- This used to be COALESCE(b.avail, 0), i.e. an unconfigured class admitted NOTHING,
    -- which made a typo'd rate_class a silent permanent stall. A CONFIGURED bucket is
    -- unchanged and exact, so invariant 16's kill switch (paused class = limit 0 + empty
    -- bucket => avail 0) still admits nothing.
    AND (r.rate_class = '' OR b.name IS NULL OR r.cost_class <= b.avail)
    AND (r.rank_part  <= COALESCE(d.deficit, 0) + p.quantum)         -- tenant fairness within fair share
),

-- surveyed policy behavior saturation is a store decision, never worker cleanup. Rows under the ceiling
-- claim normally. `queue` omits overflow so it remains visible and unleased. `discard`
-- and `cancel_incoming` carry a terminal action into the SAME atomic statement.
-- `cancel_running` may select at most max_concurrent incoming rows; after weighted
-- selection the gate displaces exactly the oldest running rows needed for those selected
-- replacements, never healthy siblings beyond that number.
elig_policy AS (
  SELECT p.id, p.queue, p.rate_class, p.partition_key,
         p.priority, p.scheduled_at_ms,
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

-- Exactly one arm ever has rows. Queue weight chooses BETWEEN these queues only after
-- every row-local policy has spoken; priority has already ordered candidates WITHIN each
-- queue and is never compared across queues (queue-weight separation/invariant 12).
eligible_pool AS (
  SELECT * FROM elig_free
  UNION ALL
  SELECT * FROM elig_policy
),
queue_ranked AS (
  SELECT e.*,
         row_number() OVER (
           PARTITION BY e.queue
           ORDER BY e.priority DESC, e.scheduled_at_ms, e.id
         )::bigint AS queue_rank
  FROM eligible_pool e
),
eligible AS (
  SELECT r.id, r.queue, r.rate_class, r.partition_key,
         r.priority, r.scheduled_at_ms, r.saturation_action
  FROM queue_ranked r
  JOIN queue_policy q USING (queue)
  -- Exact rational virtual position. Numeric avoids floating drift; queue is the stable
  -- tie break shared with MySQL and Redis.
  ORDER BY ((q.dispatch_count + r.queue_rank - 1)::numeric / q.weight::numeric),
           r.queue, r.queue_rank
  LIMIT (SELECT capacity FROM params)
),

-- adaptive admission adaptive-window revision THE ESCALATION VERDICT. Three CTEs, all switched off together by the
-- `chain` scalar: on the wide pass (adaptive-window revision) and on a policy-free admission whose draw
-- was its own EXACT bound rather than the adaptive window (policy-free fast-path revision, see `fast_esc`).
--
-- part_stats/part_tail: how many rows each partition DREW, and the sort key of the last
-- one. rank_part is a dense row_number over the drawn rows, so `max(rank_part)` IS the
-- drawn count and the tail is the row carrying it.
--
-- adaptive admission policy-free fast-path revision: these read `candidates`, not `ranked`. rank_part and all three sort keys
-- live in `candidates` and `ranked` adds only the rate-class rank, so the numbers are
-- identical — but the fast arm can ALSO escalate now, and sourcing the chain from `ranked`
-- would have dragged the rate-class window back into a path that has no rate class to
-- rank. It also leaves `ranked` with a SINGLE reference (`elig_policy`), so Postgres
-- inlines it: the policy path materializes `candidates` instead of `ranked` rather than in
-- addition to it.
part_stats AS (
  SELECT c.queue, c.partition_key, max(c.rank_part) AS drawn
  FROM candidates c
  WHERE (SELECT needed FROM chain)
  GROUP BY 1, 2
),
-- The filter is repeated here rather than inherited from `part_stats`, and it is not
-- redundant: with it only on `part_stats` the planner put the (empty) aggregate on the
-- HASH side and scanned the candidate set on the OUTER side, executing it for nothing.
-- Measured on this laptop, back when this chain read `ranked`: 0.17 ms of a 3.6 ms
-- no-policy statement. Filtering the JOIN itself leaves the whole subtree unexecuted.
part_tail AS (
  SELECT s.queue, s.partition_key, s.drawn, c.priority, c.scheduled_at_ms, c.id
  FROM part_stats s
  JOIN candidates c ON c.queue = s.queue
                   AND c.partition_key = s.partition_key
                   AND c.rank_part = s.drawn
  WHERE (SELECT needed FROM chain)
),

-- z: the row the final LIMIT stops at, and ONLY when the limit actually bound. `eligible`
-- is ordered (priority DESC, scheduled_at_ms, id), so z is its last row -- taken by
-- reversing all three keys rather than by OFFSET, which would re-scan.
elig_z AS (
  SELECT e.priority, e.scheduled_at_ms, e.id
  FROM eligible e
  WHERE (SELECT needed FROM chain)
    AND (SELECT count(*) FROM eligible) >= (SELECT capacity FROM params)
  ORDER BY e.priority ASC, e.scheduled_at_ms DESC, e.id DESC
  LIMIT 1
),

-- The verdict is a PROOF OBLIGATION, not a heuristic: widen iff some partition both
--   (a) actually hit the narrow limit while that limit is below `quantum * 4`, i.e. this
--       partition really did lose rows the old gate would have DRAWN. A partition that
--       simply RAN OUT of jobs has drawn < lim and is never a reason to re-draw -- that
--       is the half of the test that pays for the whole mechanism; and
--   (b) could still have hidden a row that belongs in the answer -- either the final
--       LIMIT never bound (no z, so more eligible rows would have changed the result) or
--       the partition's last drawn row sorts BEFORE z, which is exactly the case where a
--       dropped row could have landed inside the top `capacity`.
--
-- (a) IS DELIBERATELY `quantum * 4` AND NOT `LEAST(quantum * 4, deficit + quantum)`, and
-- that was found by running it, not by reading it. Relaxing it to the fair share looks
-- safe -- a row beyond `deficit + quantum` is never ADMITTED -- and is wrong, because such
-- a row is still a CANDIDATE, and `rank_class` is computed over the full candidate set on
-- purpose: a fairness-blocked candidate consumes a rate-class slot exactly as a
-- quarantined one does. Measured on the relaxed version: 3 partitions x 4 jobs, one class
-- with 4 tokens, quantum 1, capacity 6 -- the old gate admits {A1}, the relaxed narrow
-- gate admitted {A1, B1}, because dropping A's ranks 2-4 handed B1 a class slot A owned.
-- Anything inside the window the old gate drew is load-bearing, admissible or not.
--
-- Row-wise comparison in the gate's own order: (priority DESC, scheduled_at_ms, id), so
-- priority is negated (cast first -- `::` binds tighter than unary minus) to make all
-- three keys ascending.
verdict AS (
  SELECT EXISTS (
    SELECT 1
    FROM part_tail t
    CROSS JOIN params p
    CROSS JOIN draw w
    LEFT JOIN elig_z z ON true
    WHERE p.capacity > 0
      AND t.drawn >= w.lim
      AND w.lim < p.quantum * 4
      AND ((SELECT count(*) FROM queue_policy) > 1
           OR z.id IS NULL
           OR (-t.priority::bigint, t.scheduled_at_ms, t.id)
            < (-z.priority::bigint, z.scheduled_at_ms, z.id))
  ) AS widen
),

-- Only selected incoming rows are locked. Rejected `queue` rows never reached eligible;
-- policy-rejected rows are therefore still never locked (invariant 2). ORDER BY id gives
-- a stable lock order across claim and terminal saturation decisions.
locked AS (
  SELECT j.id, e.queue, e.partition_key, e.priority, e.scheduled_at_ms,
         e.saturation_action
  FROM headgate_job j
  JOIN eligible e ON e.id = j.id
  WHERE true
    -- adaptive admission adaptive-window revision: a widening narrow pass claims NOTHING, so the wide pass that follows
    -- sees exactly the state this one found. Gating here (rather than not running the
    -- statement) keeps the decision inside the same snapshot that drew the candidates.
    AND NOT (SELECT widen FROM verdict)
    -- REQUIRED. Under READ COMMITTED, SKIP LOCKED only skips rows locked *right now*.
    -- A row another worker claimed and COMMITTED mid-statement is unlocked and would
    -- pass straight through. Re-checking state here makes EvalPlanQual re-evaluate
    -- against the updated row and drop it. Without this line the gate double-claims
    -- under concurrency, and the duplicate is invisible to every CHECK constraint.
    AND j.state = 'available'
  ORDER BY j.id
  FOR UPDATE OF j SKIP LOCKED
),

-- Newest-wins replacement. All selected incoming rows for this strategy are counted,
-- including those that would have fit before their selected siblings. This is what makes
-- max=2, inflight=1, selected=2 cancel exactly one old job rather than overfilling to 3.
replace_selected AS (
  SELECT queue, partition_key, count(*)::bigint AS n
  FROM locked WHERE saturation_action = 'cancel_running'
  GROUP BY 1, 2
),
victim_need AS (
  SELECT s.queue, s.partition_key,
         GREATEST(0, COALESCE(i.n, 0) + s.n - l.max_concurrent)::bigint AS n
  FROM replace_selected s
  JOIN limit_state l USING (queue)
  LEFT JOIN inflight i USING (queue, partition_key)
),
victim_ids AS (
  SELECT v.id
  FROM victim_need n
  CROSS JOIN LATERAL (
    SELECT j.id
    FROM headgate_job j
    WHERE j.state = 'running'
      AND j.queue = n.queue AND j.partition_key = n.partition_key
    ORDER BY j.claimed_at_ms, j.id
    LIMIT n.n
    -- Do not deadlock an ack that already owns a victim row. If a row is skipped, the
    -- replacement capacity below admits only the room the victims actually freed.
    FOR UPDATE SKIP LOCKED
  ) v
),
displaced AS (
  UPDATE headgate_job j
  SET state = 'cancelled', finalized_at_ms = p.now_ms,
      lease_id = NULL, lease_expires_at_ms = NULL, claimed_at_ms = NULL,
      claimed_by = NULL, rate_charge = 0, fence = j.fence + 1
  FROM params p
  WHERE j.id IN (SELECT id FROM victim_ids) AND j.state = 'running'
  RETURNING j.queue, j.partition_key
),
replacement_capacity AS (
  SELECT s.queue, s.partition_key,
         GREATEST(0, l.max_concurrent - COALESCE(i.n, 0) + COALESCE(d.n, 0))::bigint AS n
  FROM replace_selected s
  JOIN limit_state l USING (queue)
  LEFT JOIN inflight i USING (queue, partition_key)
  LEFT JOIN (
    SELECT queue, partition_key, count(*)::bigint AS n FROM displaced GROUP BY 1, 2
  ) d USING (queue, partition_key)
),
replace_ranked AS (
  SELECT l.*,
         row_number() OVER (
           PARTITION BY l.queue, l.partition_key
           ORDER BY l.priority DESC, l.scheduled_at_ms, l.id
         )::bigint AS replacement_rank
  FROM locked l WHERE l.saturation_action = 'cancel_running'
),
claimable AS (
  SELECT id FROM locked WHERE saturation_action = 'claim'
  UNION ALL
  SELECT r.id FROM replace_ranked r
  JOIN replacement_capacity c USING (queue, partition_key)
  WHERE r.replacement_rank <= c.n
),

-- Incoming terminal strategies never acquire a lease. Their state and timestamp are
-- written in this statement, so `discard` cannot mean silently absent and cancellation
-- remains inspectable until the normal retention sweep evicts it.
terminalized AS (
  UPDATE headgate_job j
  SET state = CASE l.saturation_action
                WHEN 'discard' THEN 'archived'::headgate_state
                ELSE 'cancelled'::headgate_state
              END,
      finalized_at_ms = p.now_ms, rate_charge = 0,
      lease_id = NULL, lease_expires_at_ms = NULL, claimed_at_ms = NULL, claimed_by = NULL
  FROM locked l CROSS JOIN params p
  WHERE j.id = l.id
    AND l.saturation_action IN ('discard', 'cancel_incoming')
    AND j.state = 'available'
  RETURNING j.id, j.queue, j.partition_key
),

-- lease fencing the lease is written HERE, by the same statement that claims. Never separately.
claimed AS (
  UPDATE headgate_job j
  SET state = 'running',
      lease_id = p.lease_id,
      lease_expires_at_ms = p.now_ms + p.lease_ms,
      claimed_at_ms = p.now_ms,
      fence = j.fence + 1,
      claimed_by = p.worker,
      -- Zero is load-bearing: a fail-open/unconfigured class must not become chargeable
      -- merely because an operator creates it while this attempt is running.
      rate_charge = CASE
        WHEN j.rate_class <> ''
         AND EXISTS (SELECT 1 FROM bucket_state bs WHERE bs.name = j.rate_class)
        THEN j.weight ELSE 0 END
  FROM params p
  WHERE j.id IN (SELECT id FROM claimable)
    AND j.state = 'available'
  RETURNING j.id, j.ulid, j.kind, j.schema_version, j.payload, j.queue,
            j.rate_class, j.partition_key, j.weight, j.fingerprint, j.priority,
            j.attempt, j.crash_attempt, j.max_attempts,
            j.scheduled_at_ms, j.timeout_ms, j.deadline_ms, j.retention_ms,
            j.checkpoint, j.cp_cursor,
            -- telemetry and trace context regression revision, ADDITIVE: the envelope's opaque headers ride the claim so
            -- the runtime can read the RESERVED `traceparent` at dispatch. A new
            -- RETURNING column only; no CTE, predicate, or pass above is touched, and
            -- both drivers read columns by NAME, so the position is not load-bearing.
            j.headers,
            j.periodic_schedule_id, j.periodic_tick_ms,
            j.sticky_worker,
            j.fence, j.lease_id, j.lease_expires_at_ms
),

-- A terminalized incoming row consumes queue/fairness service just like a claim: the
-- gate made a final visible decision for it. Displaced running victims do not; they are
-- the cost of newest-wins, not newly selected queue work.
decisions AS (
  SELECT id, queue, partition_key FROM claimed
  UNION ALL
  SELECT id, queue, partition_key FROM terminalized
),

-- Refill and spend are ONE update: Postgres will not update the same row twice
-- in a single statement, and splitting them would let the refill be lost.
spend AS (
  UPDATE headgate_rate_bucket b
  SET tokens = bs.avail - COALESCE(cc.n, 0),
      refilled_at_ms = (SELECT now_ms FROM params)
  FROM bucket_state bs
  LEFT JOIN (SELECT rate_class, sum(weight)::bigint AS n FROM claimed GROUP BY 1) cc
         ON cc.rate_class = bs.name
  -- adaptive admission adaptive-window revision: a widening pass must not even REFILL, or the wide pass that follows
  -- would recompute `avail` off a moved refilled_at_ms and could lose an integer-division
  -- remainder. Nothing about a narrow probe is allowed to be observable.
  WHERE b.name = bs.name AND NOT (SELECT widen FROM verdict)
  RETURNING b.name
),

-- tenant fairness deficit round-robin. A partition that had work but did not get to run
-- accrues credit; one that ran spends it. EXCLUDED.deficit is (quantum - claimed),
-- so the conflict branch reduces to d.deficit + EXCLUDED.deficit.
charge AS (
  INSERT INTO headgate_partition_deficit AS d (queue, partition_key, deficit, updated_at_ms)
  SELECT r.queue, r.partition_key,
         GREATEST(0, p.quantum - COALESCE(cl.n, 0)),
         p.now_ms
  -- adaptive admission policy-free fast-path revision: reads `candidates`, not `ranked`. `ranked` is `candidates` plus a
  -- rate-class rank and drops no row, so the partition set is identical — but sourcing it
  -- here means the fast arm never touches `ranked` at all, and its window is skipped
  -- rather than merely unused. The set is the partitions that produced a CANDIDATE, which
  -- is the tenant fairness rule: a partition that had work and was not served accrues credit.
  FROM (SELECT DISTINCT queue, partition_key FROM candidates) r
  CROSS JOIN params p
  LEFT JOIN (SELECT queue, partition_key, count(*)::bigint AS n FROM decisions GROUP BY 1, 2) cl
         ON cl.queue = r.queue AND cl.partition_key = r.partition_key
  -- adaptive admission adaptive-window revision: charging a deficit on a pass that claimed nothing would change the very
  -- fair share the wide pass is about to read, so the narrow probe stays invisible here too.
  WHERE NOT (SELECT widen FROM verdict)
  ON CONFLICT (queue, partition_key) DO UPDATE
    SET deficit = LEAST((SELECT quantum * 4 FROM params), d.deficit + EXCLUDED.deficit),
        updated_at_ms = EXCLUDED.updated_at_ms
),

-- adaptive admission the net (+claims − displaced victims) maintained inflight change, in the SAME
-- statement as both state transitions. The correlated delta in the conflict arm keeps a
-- pre-existing counter exact; the insert arm also heals a legacy missing row.
-- for the same reason the lease is (lease fencing): a counter written by a second statement can be
-- lost to a crash between them, and a ceiling that leaks slots downward stalls a queue
-- permanently. ONE upsert per partition, not per row -- the count is aggregated first.
-- It reads `claimed`, so it counts exactly the rows this statement actually claimed, not
-- the ones it merely considered. The `inflight` read above sees the pre-statement value
-- (every CTE shares one snapshot), which is precisely what the old aggregate returned.
infl AS (
  INSERT INTO headgate_inflight AS f (queue, partition_key, n)
  SELECT d.queue, d.partition_key, GREATEST(0, sum(d.delta))::bigint
  FROM (
    SELECT queue, partition_key, 1::bigint AS delta FROM claimed
    UNION ALL
    SELECT queue, partition_key, -1::bigint AS delta FROM displaced
  ) d GROUP BY 1, 2
  ON CONFLICT (queue, partition_key) DO UPDATE
    SET n = GREATEST(0, f.n + (
      SELECT sum(d2.delta)::bigint FROM (
        SELECT queue, partition_key, 1::bigint AS delta FROM claimed
        UNION ALL
        SELECT queue, partition_key, -1::bigint AS delta FROM displaced
      ) d2
      WHERE d2.queue = EXCLUDED.queue AND d2.partition_key = EXCLUDED.partition_key
    ))
)

-- queue-weight separation advance only queues for which the gate made a claim or incoming-terminal decision.
-- default policy row for legacy/direct-SQL fixtures without racing a separate seed write.
, queue_charge AS (
  INSERT INTO headgate_queue_state AS qs (queue, dispatch_count)
  SELECT queue, count(*)::bigint FROM decisions GROUP BY queue
  ON CONFLICT (queue) DO UPDATE
    SET dispatch_count = qs.dispatch_count + EXCLUDED.dispatch_count
)

-- adaptive admission adaptive-window revision THE ESCALATION SIGNAL, `hg_widen`, appended as the LAST column so every
-- existing column position is byte-untouched (the Go driver scans positionally). It is
-- false on every claimed row; when the narrow pass widens there are no claimed rows at
-- all, so the verdict rides one sentinel row of dummy-but-NON-NULL values -- NULLs there
-- would force both drivers to re-type all 24 claim columns as nullable to read a flag.
-- A driver that ignores the column entirely still behaves as the gate always did on the
-- wide pass, which is what makes $9 = 1 the safe default for any future caller.
SELECT c.*, false AS hg_widen FROM claimed c
UNION ALL
SELECT 0::bigint, ''::text, ''::text, 0::int, ''::bytea, ''::text, ''::text, ''::text,
       0::int, ''::text, 0::int, 0::int, 0::int, 0::int, 0::bigint, 0::bigint, 0::bigint,
       0::bigint, '{}'::jsonb, NULL::bytea, '{}'::jsonb, ''::text, 0::bigint,
       ''::text, 0::bigint, ''::text, 0::bigint,
       true
FROM verdict v WHERE v.widen;
