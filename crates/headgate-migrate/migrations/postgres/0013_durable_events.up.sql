CREATE TABLE headgate_durable_event_scope (
  scope text PRIMARY KEY
);

CREATE TABLE headgate_durable_event (
  id              bigserial PRIMARY KEY,
  scope           text  NOT NULL REFERENCES headgate_durable_event_scope(scope) ON DELETE CASCADE,
  topic           text  NOT NULL,
  idempotency_key text  NOT NULL,
  payload         bytea NOT NULL,
  source          bytea NOT NULL,
  recorded_at_ms  bigint NOT NULL,
  UNIQUE (scope, idempotency_key)
);

CREATE INDEX headgate_durable_event_recent
  ON headgate_durable_event (scope, id DESC);
