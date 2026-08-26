CREATE TABLE headgate_schedule_event (
  id             bigserial PRIMARY KEY,
  schedule_id    text   NOT NULL,
  tick_ms        bigint NOT NULL,
  job_id         text   NOT NULL,
  outcome        text   NOT NULL CHECK (outcome IN ('enqueued','deduplicated','failed','skipped')),
  reason         text   NOT NULL CHECK (length(reason) <= 64),
  recorded_at_ms bigint NOT NULL
);

CREATE INDEX headgate_schedule_event_recent
  ON headgate_schedule_event (schedule_id, id DESC);
