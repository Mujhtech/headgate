//! Minimum current-schema manifest used by `validate` and by the one-time `adopt` path.
//!
//! Extra application objects are harmless and allowed. Every object headgate itself
//! reads or writes is required, including the columns added in ; this is what
//! makes adoption fail on a partially upgraded hand-installed schema instead of blessing
//! it as "version 1".

#[derive(Clone, Copy)]
pub(crate) struct RequiredColumn {
    pub table: &'static str,
    pub column: &'static str,
}

macro_rules! required_columns {
    ($name:ident { $($table:literal => [$($column:literal),+ $(,)?]),+ $(,)? }) => {
        pub(crate) const $name: &[RequiredColumn] = &[
            $($(RequiredColumn { table: $table, column: $column },)+)+
        ];
    };
}

pub(crate) const TABLES: &[&str] = &[
    "headgate_job",
    "headgate_rate_bucket",
    "headgate_quarantine",
    "headgate_partition_deficit",
    "headgate_active_partition",
    "headgate_inflight",
    "headgate_concurrency_limit",
    "headgate_queue_counter",
    "headgate_partition_counter",
    "headgate_queue_state",
    "headgate_enqueue_policy",
    "headgate_enqueue_counter",
    "headgate_duty",
    "headgate_schedule",
    "headgate_schedule_event",
    "headgate_worker",
    "headgate_effect",
    "headgate_operation",
    "headgate_job_tag",
    "headgate_queue_sample",
    "headgate_archive_policy",
    "headgate_job_archive",
];

required_columns!(POSTGRES_COLUMNS {
    "headgate_job" => [
        "id", "ulid", "kind", "schema_version", "payload", "queue", "state",
        "partition_key", "rate_class", "weight", "rate_charge", "fingerprint",
        "priority", "attempt", "crash_attempt", "max_attempts", "enqueued_at_ms",
        "scheduled_at_ms", "timeout_ms", "deadline_ms", "retention_ms",
        "finalized_at_ms", "lease_id", "lease_expires_at_ms", "claimed_at_ms",
        "fence", "claimed_by", "unique_key", "unique_states",
        "unique_expires_at_ms", "headers", "errors", "checkpoint", "cp_cursor",
        "result_schema_version", "result_bytes", "output_schema_version", "output_bytes",
        "output_fence", "output_updated_at_ms", "progress_current", "progress_total",
        "progress_message", "progress_fence", "progress_updated_at_ms",
        "periodic_schedule_id", "periodic_tick_ms", "sticky_worker"
    ],
    "headgate_rate_bucket" => [
        "name", "tokens", "burst", "limit_per_window", "window_ms", "refilled_at_ms"
    ],
    "headgate_quarantine" => [
        "fingerprint", "kind", "crash_count", "quarantined_at_ms", "sample_payload", "reason"
    ],
    "headgate_partition_deficit" => [
        "queue", "partition_key", "deficit", "updated_at_ms"
    ],
    "headgate_active_partition" => ["queue", "partition_key"],
    "headgate_inflight" => [
        "queue", "partition_key", "n", "reconciled_at_ms"
    ],
    "headgate_concurrency_limit" => [
        "name", "queue", "max_concurrent", "on_saturated"
    ],
    "headgate_queue_counter" => [
        "queue", "bucket_ms", "arrived", "completed"
    ],
    "headgate_partition_counter" => [
        "queue", "partition_key", "bucket_ms", "arrived", "completed"
    ],
    "headgate_queue_state" => [
        "queue", "paused", "weight", "dispatch_count"
    ],
    "headgate_enqueue_policy" => ["queue", "max_unfinished_jobs"],
    "headgate_enqueue_counter" => ["queue", "counter_kind", "n"],
    "headgate_duty" => ["name", "holder", "expires_at_ms"],
    "headgate_schedule" => [
        "id", "kind", "payload", "queue", "partition_key", "rate_class", "priority",
        "max_attempts", "retention_ms", "spec", "next_run_ms", "last_enqueued_ms",
        "on_missed", "backfill_limit", "paused", "updated_at_ms"
    ],
    "headgate_schedule_event" => [
        "id", "schedule_id", "tick_ms", "job_id", "outcome", "reason", "recorded_at_ms"
    ],
    "headgate_worker" => [
        "worker_id", "host", "pid", "queues", "concurrency", "started_at_ms",
        "heartbeat_at_ms", "command", "inflight", "polls", "empty_polls"
    ],
    "headgate_effect" => ["key", "at_ms"],
    "headgate_operation" => [
        "id", "action", "selector", "status", "affected", "total_estimated",
        "dry_run", "error", "created_at_ms"
    ],
    "headgate_job_tag" => ["job_id", "tag"],
    "headgate_queue_sample" => ["queue", "memory_bytes", "sampled_jobs", "sampled_at_ms"],
    "headgate_archive_policy" => ["queue", "archive_retention_ms"],
    "headgate_job_archive" => [
        "evicted_at_ms", "finalized_at_ms", "ulid", "kind", "queue", "state",
        "fingerprint", "attempt", "crash_attempt", "payload", "errors",
        "archive_retention_ms"
    ],
});

required_columns!(MYSQL_COLUMNS {
    "headgate_job" => [
        "id", "ulid", "kind", "schema_version", "payload", "queue", "state",
        "partition_key", "rate_class", "weight", "rate_charge", "fingerprint",
        "priority", "attempt", "crash_attempt", "max_attempts", "enqueued_at_ms",
        "scheduled_at_ms", "timeout_ms", "deadline_ms", "retention_ms", "finalized_at_ms",
        "lease_id", "lease_expires_at_ms", "claimed_at_ms", "fence", "claimed_by",
        "unique_key", "unique_states", "unique_window_ms", "unique_expires_at_ms",
        "unique_active", "unique_throttle", "headers", "errors", "checkpoint", "cp_cursor",
        "result_schema_version", "result_bytes", "output_schema_version", "output_bytes",
        "output_fence", "output_updated_at_ms", "progress_current", "progress_total",
        "progress_message", "progress_fence", "progress_updated_at_ms",
        "periodic_schedule_id", "periodic_tick_ms", "sticky_worker"
    ],
    "headgate_rate_bucket" => [
        "name", "tokens", "burst", "limit_per_window", "window_ms", "refilled_at_ms"
    ],
    "headgate_quarantine" => [
        "fingerprint", "kind", "crash_count", "quarantined_at_ms", "sample_payload", "reason"
    ],
    "headgate_partition_deficit" => [
        "queue", "partition_key", "deficit", "updated_at_ms"
    ],
    "headgate_active_partition" => ["queue", "partition_key"],
    "headgate_inflight" => [
        "queue", "partition_key", "n", "reconciled_at_ms"
    ],
    "headgate_concurrency_limit" => [
        "name", "queue", "max_concurrent", "on_saturated"
    ],
    "headgate_queue_counter" => [
        "queue", "bucket_ms", "arrived", "completed"
    ],
    "headgate_partition_counter" => [
        "queue", "partition_key", "bucket_ms", "arrived", "completed"
    ],
    "headgate_queue_state" => [
        "queue", "paused", "weight", "dispatch_count"
    ],
    "headgate_enqueue_policy" => ["queue", "max_unfinished_jobs"],
    "headgate_enqueue_counter" => ["queue", "counter_kind", "n"],
    "headgate_duty" => ["name", "holder", "expires_at_ms"],
    "headgate_schedule" => [
        "id", "kind", "payload", "queue", "partition_key", "rate_class", "priority",
        "max_attempts", "retention_ms", "spec", "next_run_ms", "last_enqueued_ms",
        "on_missed", "backfill_limit", "paused", "updated_at_ms"
    ],
    "headgate_schedule_event" => [
        "id", "schedule_id", "tick_ms", "job_id", "outcome", "reason", "recorded_at_ms"
    ],
    "headgate_worker" => [
        "worker_id", "host", "pid", "queues", "concurrency", "started_at_ms",
        "heartbeat_at_ms", "command", "inflight", "polls", "empty_polls"
    ],
    "headgate_effect" => ["effect_key", "job_ulid", "claimed_at_ms"],
    "headgate_operation" => [
        "id", "action", "selector", "status", "affected", "total_estimated",
        "dry_run", "error", "created_at_ms"
    ],
    "headgate_job_tag" => ["job_id", "tag"],
    "headgate_queue_sample" => ["queue", "memory_bytes", "sampled_jobs", "sampled_at_ms"],
    "headgate_archive_policy" => ["queue", "archive_retention_ms"],
    "headgate_job_archive" => [
        "evicted_at_ms", "finalized_at_ms", "ulid", "kind", "queue", "state",
        "fingerprint", "attempt", "crash_attempt", "payload", "errors",
        "archive_retention_ms"
    ],
});

pub(crate) const POSTGRES_INDEXES: &[&str] = &[
    "headgate_job_admit",
    "headgate_job_lease",
    "headgate_job_running_partition",
    "headgate_job_running_oldest",
    "headgate_job_avail_partition",
    "headgate_job_avail_sticky",
    "headgate_job_sticky_available",
    "headgate_job_oldest_available",
    "headgate_job_oldest_available_partition",
    "headgate_job_live_partition_metric",
    "headgate_job_fp_waiting",
    "headgate_job_retention",
    "headgate_job_unique",
    "headgate_job_unique_throttle",
    "headgate_job_tag_lookup",
    "headgate_inflight_stale",
    "headgate_partition_counter_recent",
    "headgate_schedule_due",
    "headgate_schedule_event_recent",
    "headgate_job_archive_queue_time",
];

pub(crate) const MYSQL_INDEXES: &[&str] = &[
    "headgate_job_ulid",
    "headgate_job_unique",
    "headgate_job_unique_throttle",
    "headgate_job_tag_lookup",
    "headgate_job_admit",
    "headgate_job_avail_partition",
    "headgate_job_avail_sticky",
    "headgate_job_oldest_available",
    "headgate_job_oldest_available_partition",
    "headgate_job_lease",
    "headgate_job_running_oldest",
    "headgate_job_fp",
    "headgate_job_retention",
    "headgate_inflight_stale",
    "headgate_concurrency_limit_queue",
    "headgate_partition_counter_recent",
    "headgate_schedule_due",
    "headgate_schedule_event_recent",
    "headgate_job_archive_queue_time",
];

pub(crate) const POSTGRES_TRIGGERS: &[&str] = &[
    "headgate_enqueue_depth_insert",
    "headgate_enqueue_depth_update",
    "headgate_enqueue_depth_delete",
];

pub(crate) const MYSQL_TRIGGERS: &[&str] = &[
    "headgate_enqueue_depth_insert",
    "headgate_enqueue_depth_update_exit",
    "headgate_enqueue_depth_update_enter",
    "headgate_enqueue_depth_delete",
];

pub(crate) const STATES: &[&str] = &[
    "pending",
    "scheduled",
    "available",
    "running",
    "retryable",
    "completed",
    "archived",
    "cancelled",
    "quarantined",
    "undecodable",
];
