-- Exact producer backpressure without an O(queue depth) count.
--
-- `entered - exited` is the unfinished depth. Producers serialize only on the policy
-- row; terminal transitions append to the independent `exited` counter, so a handler
-- finishing while a producer enqueues cannot form a policy-row/job-row lock cycle.
-- Running this migration requires producers stopped: the baseline count and trigger
-- installation must be one logical cut-over.

CREATE TABLE headgate_enqueue_policy (
  queue               text PRIMARY KEY,
  max_unfinished_jobs bigint NULL CHECK (max_unfinished_jobs >= 0)
);

CREATE TABLE headgate_enqueue_counter (
  queue        text NOT NULL,
  counter_kind text NOT NULL CHECK (counter_kind IN ('entered', 'exited')),
  n            bigint NOT NULL DEFAULT 0 CHECK (n >= 0),
  PRIMARY KEY (queue, counter_kind)
);

INSERT INTO headgate_enqueue_policy (queue, max_unfinished_jobs)
SELECT queue, NULL
FROM (
  SELECT queue FROM headgate_queue_state
  UNION
  SELECT queue FROM headgate_job
) q;

INSERT INTO headgate_enqueue_counter (queue, counter_kind, n)
SELECT q.queue, k.counter_kind,
       CASE WHEN k.counter_kind = 'entered' THEN COALESCE(j.unfinished, 0) ELSE 0 END
FROM headgate_enqueue_policy q
CROSS JOIN (VALUES ('entered'), ('exited')) AS k(counter_kind)
LEFT JOIN (
  SELECT queue, count(*)::bigint AS unfinished
  FROM headgate_job
  WHERE state IN ('scheduled', 'available', 'running', 'retryable')
  GROUP BY queue
) j USING (queue);

CREATE OR REPLACE FUNCTION headgate_track_enqueue_depth()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  old_live boolean := false;
  new_live boolean := false;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_live := OLD.state IN ('scheduled', 'available', 'running', 'retryable');
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_live := NEW.state IN ('scheduled', 'available', 'running', 'retryable');
  END IF;

  IF TG_OP = 'INSERT' THEN
    IF new_live THEN
      EXECUTE format(
        'INSERT INTO %I.headgate_enqueue_counter AS counter (queue, counter_kind, n)
         VALUES ($1, $2, 1)
         ON CONFLICT (queue, counter_kind) DO UPDATE
           SET n = counter.n + 1',
        TG_TABLE_SCHEMA
      ) USING NEW.queue, 'entered'::text;
    END IF;
    RETURN NEW;
  END IF;

  IF TG_OP = 'DELETE' THEN
    IF old_live THEN
      EXECUTE format(
        'INSERT INTO %I.headgate_enqueue_counter AS counter (queue, counter_kind, n)
         VALUES ($1, $2, 1)
         ON CONFLICT (queue, counter_kind) DO UPDATE
           SET n = counter.n + 1',
        TG_TABLE_SCHEMA
      ) USING OLD.queue, 'exited'::text;
    END IF;
    RETURN OLD;
  END IF;

  IF old_live AND (NOT new_live OR OLD.queue <> NEW.queue) THEN
    EXECUTE format(
      'INSERT INTO %I.headgate_enqueue_counter AS counter (queue, counter_kind, n)
       VALUES ($1, $2, 1)
       ON CONFLICT (queue, counter_kind) DO UPDATE
         SET n = counter.n + 1',
      TG_TABLE_SCHEMA
    ) USING OLD.queue, 'exited'::text;
  END IF;
  IF new_live AND (NOT old_live OR OLD.queue <> NEW.queue) THEN
    EXECUTE format(
      'INSERT INTO %I.headgate_enqueue_counter AS counter (queue, counter_kind, n)
       VALUES ($1, $2, 1)
       ON CONFLICT (queue, counter_kind) DO UPDATE
         SET n = counter.n + 1',
      TG_TABLE_SCHEMA
    ) USING NEW.queue, 'entered'::text;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER headgate_enqueue_depth_insert
AFTER INSERT ON headgate_job
FOR EACH ROW EXECUTE FUNCTION headgate_track_enqueue_depth();

CREATE TRIGGER headgate_enqueue_depth_update
AFTER UPDATE OF state, queue ON headgate_job
FOR EACH ROW EXECUTE FUNCTION headgate_track_enqueue_depth();

CREATE TRIGGER headgate_enqueue_depth_delete
AFTER DELETE ON headgate_job
FOR EACH ROW EXECUTE FUNCTION headgate_track_enqueue_depth();
