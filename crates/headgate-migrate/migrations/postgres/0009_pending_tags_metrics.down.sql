DROP TABLE headgate_queue_sample;
DROP TABLE headgate_job_tag;
DROP INDEX headgate_job_unique;
CREATE UNIQUE INDEX headgate_job_unique ON headgate_job (unique_key)
  WHERE unique_key IS NOT NULL AND unique_expires_at_ms IS NULL
    AND state = ANY(ARRAY['scheduled','available','running','retryable']::headgate_state[]);

CREATE OR REPLACE FUNCTION headgate_track_enqueue_depth()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE old_live boolean := false; new_live boolean := false;
BEGIN
  IF TG_OP <> 'INSERT' THEN old_live := OLD.state IN ('scheduled','available','running','retryable'); END IF;
  IF TG_OP <> 'DELETE' THEN new_live := NEW.state IN ('scheduled','available','running','retryable'); END IF;
  IF TG_OP = 'INSERT' THEN IF new_live THEN EXECUTE format('INSERT INTO %I.headgate_enqueue_counter AS counter (queue,counter_kind,n) VALUES ($1,$2,1) ON CONFLICT (queue,counter_kind) DO UPDATE SET n=counter.n+1',TG_TABLE_SCHEMA) USING NEW.queue,'entered'::text; END IF; RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN IF old_live THEN EXECUTE format('INSERT INTO %I.headgate_enqueue_counter AS counter (queue,counter_kind,n) VALUES ($1,$2,1) ON CONFLICT (queue,counter_kind) DO UPDATE SET n=counter.n+1',TG_TABLE_SCHEMA) USING OLD.queue,'exited'::text; END IF; RETURN OLD; END IF;
  IF old_live AND (NOT new_live OR OLD.queue<>NEW.queue) THEN EXECUTE format('INSERT INTO %I.headgate_enqueue_counter AS counter (queue,counter_kind,n) VALUES ($1,$2,1) ON CONFLICT (queue,counter_kind) DO UPDATE SET n=counter.n+1',TG_TABLE_SCHEMA) USING OLD.queue,'exited'::text; END IF;
  IF new_live AND (NOT old_live OR OLD.queue<>NEW.queue) THEN EXECUTE format('INSERT INTO %I.headgate_enqueue_counter AS counter (queue,counter_kind,n) VALUES ($1,$2,1) ON CONFLICT (queue,counter_kind) DO UPDATE SET n=counter.n+1',TG_TABLE_SCHEMA) USING NEW.queue,'entered'::text; END IF;
  RETURN NEW;
END; $$;
