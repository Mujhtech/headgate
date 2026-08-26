-- v10: strict sticky worker routing, enforced inside admission.
ALTER TABLE headgate_job
  ADD COLUMN sticky_worker text NOT NULL DEFAULT '';

-- Each admission merges at most two bounded, already ordered streams: unpinned jobs and
-- jobs pinned to the calling worker. Keeping sticky_worker before priority prevents a
-- fleet of jobs pinned to other workers from turning a draw into an O(queue depth) scan.
CREATE INDEX headgate_job_avail_sticky
  ON headgate_job (queue, partition_key, sticky_worker, priority DESC, scheduled_at_ms, id)
  WHERE state = 'available';

-- The compact single-partition gate falls back when any sticky route is present. This
-- partial probe makes that conservative decision independent of queue depth.
CREATE INDEX headgate_job_sticky_available
  ON headgate_job (queue)
  WHERE state = 'available' AND sticky_worker <> '';
