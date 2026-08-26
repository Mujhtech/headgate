ALTER TABLE headgate_job
  ADD COLUMN output_schema_version INT UNSIGNED NULL,
  ADD COLUMN output_bytes LONGBLOB NULL,
  ADD COLUMN output_fence BIGINT UNSIGNED NULL,
  ADD COLUMN output_updated_at_ms BIGINT NULL,
  ADD CONSTRAINT headgate_job_output_tuple CHECK (
    (output_schema_version IS NULL
      AND output_bytes IS NULL
      AND output_fence IS NULL
      AND output_updated_at_ms IS NULL)
    OR (output_schema_version > 0
      AND output_bytes IS NOT NULL
      AND output_fence > 0
      AND output_updated_at_ms > 0)
  );
