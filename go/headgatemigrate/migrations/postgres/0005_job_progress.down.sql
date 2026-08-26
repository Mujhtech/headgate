ALTER TABLE headgate_job DROP CONSTRAINT headgate_job_progress_tuple;
ALTER TABLE headgate_job DROP COLUMN progress_updated_at_ms;
ALTER TABLE headgate_job DROP COLUMN progress_fence;
ALTER TABLE headgate_job DROP COLUMN progress_message;
ALTER TABLE headgate_job DROP COLUMN progress_total;
ALTER TABLE headgate_job DROP COLUMN progress_current;
