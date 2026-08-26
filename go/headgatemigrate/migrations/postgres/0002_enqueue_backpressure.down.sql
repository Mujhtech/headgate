DROP TRIGGER IF EXISTS headgate_enqueue_depth_delete ON headgate_job;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_update ON headgate_job;
DROP TRIGGER IF EXISTS headgate_enqueue_depth_insert ON headgate_job;
DROP FUNCTION IF EXISTS headgate_track_enqueue_depth();
DROP TABLE IF EXISTS headgate_enqueue_counter;
DROP TABLE IF EXISTS headgate_enqueue_policy;
