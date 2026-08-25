-- Exact producer backpressure without an O(queue depth) count. This migration is an
-- offline cut-over: baseline the four unfinished states, then let triggers maintain
-- the two monotonic counters. Producers serialize on policy rows; terminal transitions
-- touch only the independent `exited` counter, avoiding producer/handler lock cycles.

CREATE TABLE IF NOT EXISTS headgate_enqueue_policy (
  queue               VARCHAR(255) NOT NULL PRIMARY KEY,
  max_unfinished_jobs BIGINT UNSIGNED NULL
);

CREATE TABLE IF NOT EXISTS headgate_enqueue_counter (
  queue        VARCHAR(255) NOT NULL,
  counter_kind ENUM('entered', 'exited') NOT NULL,
  n            BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, counter_kind)
);

INSERT IGNORE INTO headgate_enqueue_policy (queue, max_unfinished_jobs)
SELECT queue, NULL FROM headgate_queue_state
UNION
SELECT queue, NULL FROM headgate_job;

INSERT INTO headgate_enqueue_counter (queue, counter_kind, n)
SELECT p.queue, 'entered', COALESCE(j.unfinished, 0)
FROM headgate_enqueue_policy p
LEFT JOIN (
  SELECT queue, COUNT(*) AS unfinished
  FROM headgate_job
  WHERE state IN ('scheduled', 'available', 'running', 'retryable')
  GROUP BY queue
) j ON j.queue = p.queue
ON DUPLICATE KEY UPDATE n = VALUES(n);

INSERT IGNORE INTO headgate_enqueue_counter (queue, counter_kind, n)
SELECT queue, 'exited', 0 FROM headgate_enqueue_policy;

CREATE TRIGGER IF NOT EXISTS headgate_enqueue_depth_insert
AFTER INSERT ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter (queue, counter_kind, n)
VALUES (NEW.queue, 'entered', IF(NEW.state IN ('scheduled', 'available', 'running', 'retryable'), 1, 0))
ON DUPLICATE KEY UPDATE n = headgate_enqueue_counter.n + VALUES(n);

CREATE TRIGGER IF NOT EXISTS headgate_enqueue_depth_update_exit
AFTER UPDATE ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter (queue, counter_kind, n)
VALUES (OLD.queue, 'exited', IF(OLD.state IN ('scheduled', 'available', 'running', 'retryable') AND (NEW.state NOT IN ('scheduled', 'available', 'running', 'retryable') OR OLD.queue <> NEW.queue), 1, 0))
ON DUPLICATE KEY UPDATE n = headgate_enqueue_counter.n + VALUES(n);

CREATE TRIGGER IF NOT EXISTS headgate_enqueue_depth_update_enter
AFTER UPDATE ON headgate_job FOR EACH ROW FOLLOWS headgate_enqueue_depth_update_exit
INSERT INTO headgate_enqueue_counter (queue, counter_kind, n)
VALUES (NEW.queue, 'entered', IF(NEW.state IN ('scheduled', 'available', 'running', 'retryable') AND (OLD.state NOT IN ('scheduled', 'available', 'running', 'retryable') OR OLD.queue <> NEW.queue), 1, 0))
ON DUPLICATE KEY UPDATE n = headgate_enqueue_counter.n + VALUES(n);

CREATE TRIGGER IF NOT EXISTS headgate_enqueue_depth_delete
AFTER DELETE ON headgate_job FOR EACH ROW
INSERT INTO headgate_enqueue_counter (queue, counter_kind, n)
VALUES (OLD.queue, 'exited', IF(OLD.state IN ('scheduled', 'available', 'running', 'retryable'), 1, 0))
ON DUPLICATE KEY UPDATE n = headgate_enqueue_counter.n + VALUES(n);
