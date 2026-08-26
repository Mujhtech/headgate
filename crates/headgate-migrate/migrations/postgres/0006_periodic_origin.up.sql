ALTER TABLE headgate_job
  ADD COLUMN periodic_schedule_id text NOT NULL DEFAULT '',
  ADD COLUMN periodic_tick_ms bigint NOT NULL DEFAULT 0,
  ADD CONSTRAINT periodic_origin_pair CHECK (
    (periodic_schedule_id = '') = (periodic_tick_ms = 0)
  );
