DROP INDEX headgate_job_sticky_available;
DROP INDEX headgate_job_avail_sticky;
ALTER TABLE headgate_job DROP COLUMN sticky_worker;
