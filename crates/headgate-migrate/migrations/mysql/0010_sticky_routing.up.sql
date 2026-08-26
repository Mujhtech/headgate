-- v10: strict sticky worker routing, enforced inside admission.
ALTER TABLE headgate_job
  ADD COLUMN sticky_worker VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  ADD KEY headgate_job_avail_sticky
    (state, queue, partition_key, sticky_worker, priority DESC, scheduled_at_ms, id);
