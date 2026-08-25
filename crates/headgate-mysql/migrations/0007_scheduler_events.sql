CREATE TABLE headgate_schedule_event (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  schedule_id    VARCHAR(255) NOT NULL,
  tick_ms        BIGINT NOT NULL,
  job_id         VARCHAR(255) NOT NULL,
  outcome        ENUM('enqueued','deduplicated','failed','skipped') NOT NULL,
  reason         VARCHAR(64) NOT NULL,
  recorded_at_ms BIGINT NOT NULL,
  KEY headgate_schedule_event_recent (schedule_id, id DESC)
);
