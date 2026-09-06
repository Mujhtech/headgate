CREATE TABLE headgate_durable_event_scope (
  scope VARCHAR(512) NOT NULL PRIMARY KEY
);

CREATE TABLE headgate_durable_event (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  scope           VARCHAR(512) NOT NULL,
  topic           VARCHAR(255) NOT NULL,
  idempotency_key VARCHAR(255) NOT NULL,
  payload         LONGBLOB NOT NULL,
  source          LONGBLOB NOT NULL,
  recorded_at_ms  BIGINT NOT NULL,
  UNIQUE KEY headgate_durable_event_idempotency (scope, idempotency_key),
  KEY headgate_durable_event_recent (scope, id DESC),
  CONSTRAINT headgate_durable_event_scope_fk FOREIGN KEY (scope)
    REFERENCES headgate_durable_event_scope(scope) ON DELETE CASCADE
);
