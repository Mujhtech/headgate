ALTER TABLE headgate_job
  ADD COLUMN result_schema_version INT UNSIGNED NULL,
  ADD COLUMN result_bytes LONGBLOB NULL,
  ADD CONSTRAINT headgate_job_result_pair CHECK (
    (result_schema_version IS NULL AND result_bytes IS NULL)
    OR (result_schema_version > 0 AND result_bytes IS NOT NULL)
  );
