DROP TABLE IF EXISTS headgate_queue_sample;
DROP TABLE IF EXISTS headgate_job_tag;

ALTER TABLE headgate_job
  DROP INDEX headgate_job_unique,
  DROP COLUMN unique_active,
  MODIFY COLUMN state ENUM('available','scheduled','retryable','running','completed',
                           'archived','cancelled','undecodable','quarantined') NOT NULL,
  ADD COLUMN unique_active VARBINARY(255) GENERATED ALWAYS AS (
    CASE WHEN unique_key IS NOT NULL AND unique_expires_at_ms IS NULL
          AND state IN ('scheduled','available','running','retryable')
         THEN unique_key ELSE NULL END) STORED,
  ADD UNIQUE KEY headgate_job_unique (unique_active);

DROP TRIGGER IF EXISTS headgate_enqueue_depth_insert;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_update_exit;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_update_enter;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_delete;
CREATE TRIGGER headgate_enqueue_depth_insert AFTER INSERT ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(NEW.queue,'entered',IF(NEW.state IN ('scheduled','available','running','retryable'),1,0)) ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
CREATE TRIGGER headgate_enqueue_depth_update_exit AFTER UPDATE ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(OLD.queue,'exited',IF(OLD.state IN ('scheduled','available','running','retryable') AND (NEW.state NOT IN ('scheduled','available','running','retryable') OR OLD.queue<>NEW.queue),1,0)) ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
CREATE TRIGGER headgate_enqueue_depth_update_enter AFTER UPDATE ON headgate_job FOR EACH ROW FOLLOWS headgate_enqueue_depth_update_exit
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(NEW.queue,'entered',IF(NEW.state IN ('scheduled','available','running','retryable') AND (OLD.state NOT IN ('scheduled','available','running','retryable') OR OLD.queue<>NEW.queue),1,0)) ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
CREATE TRIGGER headgate_enqueue_depth_delete AFTER DELETE ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter(queue,counter_kind,n) VALUES(OLD.queue,'exited',IF(OLD.state IN ('scheduled','available','running','retryable'),1,0)) ON DUPLICATE KEY UPDATE n=headgate_enqueue_counter.n+VALUES(n);
