ALTER TABLE headgate_job
  ADD COLUMN output_schema_version integer,
  ADD COLUMN output_bytes bytea,
  ADD COLUMN output_fence bigint,
  ADD COLUMN output_updated_at_ms bigint,
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
