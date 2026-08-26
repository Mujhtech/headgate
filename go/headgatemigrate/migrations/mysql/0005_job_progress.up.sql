ALTER TABLE headgate_job
  ADD COLUMN progress_current BIGINT UNSIGNED NULL,
  ADD COLUMN progress_total BIGINT UNSIGNED NULL,
  ADD COLUMN progress_message VARCHAR(512) NULL,
  ADD COLUMN progress_fence BIGINT UNSIGNED NULL,
  ADD COLUMN progress_updated_at_ms BIGINT NULL,
  ADD CONSTRAINT headgate_job_progress_tuple CHECK (
    (progress_current IS NULL
      AND progress_total IS NULL
      AND progress_message IS NULL
      AND progress_fence IS NULL
      AND progress_updated_at_ms IS NULL)
    OR (progress_current <= progress_total
      AND progress_total > 0
      AND progress_total <= 9007199254740991
      AND (progress_message IS NULL OR OCTET_LENGTH(progress_message) <= 512)
      AND progress_fence > 0
      AND progress_updated_at_ms > 0)
  );
