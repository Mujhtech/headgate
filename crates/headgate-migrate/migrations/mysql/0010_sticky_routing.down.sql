ALTER TABLE headgate_job
  DROP INDEX headgate_job_avail_sticky,
  DROP COLUMN sticky_worker;

