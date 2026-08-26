ALTER TABLE headgate_job DROP CONSTRAINT headgate_job_result_pair;
ALTER TABLE headgate_job DROP COLUMN result_bytes;
ALTER TABLE headgate_job DROP COLUMN result_schema_version;
