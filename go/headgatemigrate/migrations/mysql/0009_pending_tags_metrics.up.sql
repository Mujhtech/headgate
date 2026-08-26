-- v9: explicit pending state, indexed tags, and persisted bounded queue metrics.
ALTER TABLE headgate_job
  DROP INDEX headgate_job_unique,
  DROP COLUMN unique_active,
  MODIFY COLUMN state ENUM('pending','available','scheduled','retryable','running','completed',
                           'archived','cancelled','undecodable','quarantined') NOT NULL,
  ADD COLUMN unique_active VARBINARY(255) GENERATED ALWAYS AS (
    CASE WHEN unique_key IS NOT NULL AND unique_expires_at_ms IS NULL
          AND state IN ('pending','scheduled','available','running','retryable')
         THEN unique_key ELSE NULL END) STORED,
  ADD UNIQUE KEY headgate_job_unique (unique_active);

CREATE TABLE IF NOT EXISTS headgate_job_tag (
  job_id BIGINT NOT NULL,
  tag VARCHAR(64) NOT NULL,
  PRIMARY KEY (job_id, tag),
  KEY headgate_job_tag_lookup (tag, job_id),
  CONSTRAINT headgate_job_tag_job_fk FOREIGN KEY (job_id)
    REFERENCES headgate_job(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS headgate_queue_sample (
  queue VARCHAR(255) NOT NULL PRIMARY KEY,
  memory_bytes BIGINT UNSIGNED NULL,
  sampled_jobs INT UNSIGNED NOT NULL DEFAULT 0,
  sampled_at_ms BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB;

DROP TRIGGER IF EXISTS headgate_enqueue_depth_insert;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_update_exit;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_update_enter;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_delete;
CREATE TRIGGER headgate_enqueue_depth_insert AFTER INSERT ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(NEW.queue,'entered',IF(NEW.state IN ('pending','scheduled','available','running','retryable'),1,0))
ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
CREATE TRIGGER headgate_enqueue_depth_update_exit AFTER UPDATE ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(OLD.queue,'exited',IF(OLD.state IN ('pending','scheduled','available','running','retryable') AND (NEW.state NOT IN ('pending','scheduled','available','running','retryable') OR OLD.queue<>NEW.queue),1,0))
ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
CREATE TRIGGER headgate_enqueue_depth_update_enter AFTER UPDATE ON headgate_job FOR EACH ROW FOLLOWS headgate_enqueue_depth_update_exit
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(NEW.queue,'entered',IF(NEW.state IN ('pending','scheduled','available','running','retryable') AND (OLD.state NOT IN ('pending','scheduled','available','running','retryable') OR OLD.queue<>NEW.queue),1,0))
ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
CREATE TRIGGER headgate_enqueue_depth_delete AFTER DELETE ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(OLD.queue,'exited',IF(OLD.state IN ('pending','scheduled','available','running','retryable'),1,0))
ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
