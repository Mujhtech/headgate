ALTER TABLE headgate_job DROP CONSTRAINT headgate_job_output_tuple;
ALTER TABLE headgate_job DROP COLUMN output_updated_at_ms;
ALTER TABLE headgate_job DROP COLUMN output_fence;
ALTER TABLE headgate_job DROP COLUMN output_bytes;
ALTER TABLE headgate_job DROP COLUMN output_schema_version;
