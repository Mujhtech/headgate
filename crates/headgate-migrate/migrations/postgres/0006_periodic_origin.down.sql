ALTER TABLE headgate_job DROP CONSTRAINT periodic_origin_pair;
ALTER TABLE headgate_job DROP COLUMN periodic_tick_ms;
ALTER TABLE headgate_job DROP COLUMN periodic_schedule_id;
