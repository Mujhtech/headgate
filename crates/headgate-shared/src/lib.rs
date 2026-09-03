//! Dependency-light data types and utilities shared across headgate crates.
//!
//! This crate contains no store driver, network client, runtime, or exporter dependency,
//! keeping it safe for core, adapters, and optional integrations to use as a leaf.

use std::collections::BTreeMap;
use std::time::Duration;

pub mod log;

/// Portable lifecycle result written by every worker runtime and store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Retry,
    Skip,
    Revoke,
    Snooze,
    LeaseLost,
    Undecodable,
    RateLimited,
}

impl Outcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Retry => "retry",
            Self::Skip => "skip",
            Self::Revoke => "revoke",
            Self::Snooze => "snooze",
            Self::LeaseLost => "lease_lost",
            Self::Undecodable => "undecodable",
            Self::RateLimited => "rate_limited",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "retry" => Some(Self::Retry),
            "skip" => Some(Self::Skip),
            "revoke" => Some(Self::Revoke),
            "snooze" => Some(Self::Snooze),
            "lease_lost" => Some(Self::LeaseLost),
            "undecodable" => Some(Self::Undecodable),
            "rate_limited" => Some(Self::RateLimited),
            _ => None,
        }
    }
}

/// What happens to periodic runs missed during downtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissedPolicy {
    Skip,
    RunOnce,
    Backfill,
}

impl MissedPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::RunOnce => "run_once",
            Self::Backfill => "backfill",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "skip" => Some(Self::Skip),
            "run_once" => Some(Self::RunOnce),
            "backfill" => Some(Self::Backfill),
            _ => None,
        }
    }
}

pub const DEFAULT_QUEUE: &str = "default";
pub const DEFAULT_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 25;
pub const DEFAULT_WEIGHT: u32 = 1;
pub const MAX_OPAQUE_SCHEMA_VERSION: u32 = i32::MAX as u32;

pub fn normalize_queues(mut queues: Vec<String>) -> Vec<String> {
    queues.sort();
    queues.dedup();
    queues
}

/// Validate the millisecond wire boundary without truncating a positive
/// sub-millisecond duration or overflowing the signed store representation.
pub fn duration_millis(duration: Duration) -> Option<i64> {
    i64::try_from(duration.as_millis())
        .ok()
        .filter(|millis| *millis > 0)
}

pub fn effective_queue(queue: &str) -> &str {
    if queue.is_empty() {
        DEFAULT_QUEUE
    } else {
        queue
    }
}

pub const fn effective_schema_version(version: u32) -> u32 {
    if version == 0 {
        DEFAULT_SCHEMA_VERSION
    } else {
        version
    }
}

pub const fn effective_max_attempts(max_attempts: u32) -> u32 {
    if max_attempts == 0 {
        DEFAULT_MAX_ATTEMPTS
    } else {
        max_attempts
    }
}

pub const fn effective_weight(weight: u32) -> u32 {
    if weight == 0 { DEFAULT_WEIGHT } else { weight }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckValidation {
    Valid,
    LeaseLost,
    SnoozeDelayRequired,
}

pub const fn validate_ack(outcome: Outcome, delay_ms: Option<i64>) -> AckValidation {
    if matches!(outcome, Outcome::LeaseLost) {
        AckValidation::LeaseLost
    } else if matches!(outcome, Outcome::Snooze) && !matches!(delay_ms, Some(delay) if delay > 0) {
        AckValidation::SnoozeDelayRequired
    } else {
        AckValidation::Valid
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpaqueSchemaValidation {
    Valid,
    Zero,
    TooLarge,
}

pub const fn validate_opaque_schema(version: u32) -> OpaqueSchemaValidation {
    if version == 0 {
        OpaqueSchemaValidation::Zero
    } else if version > MAX_OPAQUE_SCHEMA_VERSION {
        OpaqueSchemaValidation::TooLarge
    } else {
        OpaqueSchemaValidation::Valid
    }
}

pub fn bulk_action_states(action: &str) -> Option<&'static [&'static str]> {
    match action {
        "retry" => Some(&["archived"]),
        "cancel" => Some(&["scheduled", "available", "running"]),
        "delete" => Some(&[
            "scheduled",
            "available",
            "retryable",
            "completed",
            "archived",
            "cancelled",
            "quarantined",
            "undecodable",
        ]),
        _ => None,
    }
}

pub fn valid_worker_command(command: &str) -> bool {
    matches!(
        command,
        "" | "quiet" | "resume" | "restart" | "terminate" | "resign"
    )
}

pub fn format_generated_id(now_ms: u64, process_id: u32, sequence: u64) -> String {
    format!(
        "hg{now_ms:012x}{:05x}{:04x}",
        process_id & 0xfffff,
        sequence & 0xffff
    )
}

#[derive(Clone, Debug, Default)]
pub struct AdmissionFacts {
    pub state: String,
    pub now_ms: i64,
    pub scheduled_at_ms: i64,
    pub queue_paused: bool,
    pub quarantined: bool,
    pub fingerprint: String,
    pub rate_class: String,
    pub weight: i64,
    pub tokens_available: Option<i64>,
    pub tokens_ahead: i64,
    pub limit_per_window: i64,
    pub window_ms: i64,
    pub max_concurrent: Option<i64>,
    pub inflight: i64,
    pub saturation: String,
    pub position: i64,
    pub deficit: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionEvaluation {
    pub admissible: bool,
    pub blocked_by: Option<&'static str>,
    pub detail: Vec<(String, String)>,
    pub estimated_admission_ms: Option<i64>,
}

pub fn evaluate_admission(f: &AdmissionFacts) -> AdmissionEvaluation {
    let mut result = AdmissionEvaluation {
        admissible: false,
        blocked_by: None,
        detail: vec![("state".into(), f.state.clone())],
        estimated_admission_ms: None,
    };
    let block = |mut value: AdmissionEvaluation, by, eta| {
        value.blocked_by = Some(by);
        value.estimated_admission_ms = eta;
        value
    };
    match f.state.as_str() {
        "running" => {
            result.admissible = true;
            result.estimated_admission_ms = Some(0);
            return result;
        }
        "scheduled" | "retryable" => {
            result
                .detail
                .push(("scheduled_at_ms".into(), f.scheduled_at_ms.to_string()));
            return block(
                result,
                "schedule",
                Some((f.scheduled_at_ms - f.now_ms).max(0)),
            );
        }
        "quarantined" => return block(result, "quarantine", None),
        "available" => {}
        _ => return result,
    }
    if f.queue_paused {
        return block(result, "queue_paused", None);
    }
    if f.scheduled_at_ms > f.now_ms {
        result
            .detail
            .push(("scheduled_at_ms".into(), f.scheduled_at_ms.to_string()));
        return block(result, "schedule", Some(f.scheduled_at_ms - f.now_ms));
    }
    if f.quarantined {
        result
            .detail
            .push(("fingerprint".into(), f.fingerprint.clone()));
        return block(result, "quarantine", None);
    }
    if !f.rate_class.is_empty() {
        let weight = f.weight.max(1);
        let required = f.tokens_ahead + weight;
        result.detail.extend([
            ("rate_class".into(), f.rate_class.clone()),
            ("weight".into(), weight.to_string()),
            ("tokens_ahead_in_class".into(), f.tokens_ahead.to_string()),
        ]);
        if let Some(available) = f.tokens_available {
            result
                .detail
                .push(("tokens_available".into(), available.to_string()));
            if available < required {
                let eta = (f.limit_per_window > 0)
                    .then(|| (required - available).max(1) * f.window_ms / f.limit_per_window);
                return block(result, "rate_class", eta);
            }
        } else {
            result.detail.push((
                "tokens_available".into(),
                "unlimited (no such rate class)".into(),
            ));
        }
    }
    if let Some(max_concurrent) = f.max_concurrent {
        let strategy = if f.saturation.is_empty() {
            "queue"
        } else {
            &f.saturation
        };
        result.detail.extend([
            ("max_concurrent".into(), max_concurrent.to_string()),
            ("inflight".into(), f.inflight.to_string()),
            ("on_saturated".into(), strategy.into()),
        ]);
        if f.inflight >= max_concurrent && strategy != "cancel_running" {
            return block(result, "concurrency_limit", None);
        }
    }
    result.detail.extend([
        ("position_in_partition".into(), f.position.to_string()),
        ("partition_deficit".into(), f.deficit.to_string()),
    ]);
    result.admissible = true;
    result.estimated_admission_ms = Some(0);
    result
}

/// Durable progress within a resumable job.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Checkpoint {
    pub last_completed_step: Option<String>,
    /// Completed steps in execution order. Replay compares them positionally.
    pub completed_steps: Vec<String>,
    /// The step recorded before its side effects began.
    pub in_progress_step: Option<String>,
    pub cursor_step: Option<String>,
    /// Stored outside checkpoint JSON to avoid base64 encoding native binary data.
    pub cursor: Option<Vec<u8>>,
    pub schema_version: u32,
    pub step_set_hash: String,
    pub crashes_by_step: Vec<(String, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    Continue,
    Remapped,
    Undecodable,
}

impl Checkpoint {
    /// Decide whether a checkpoint can continue against the current task definition.
    pub fn resumability(&self, current_version: u32, current_step_set_hash: &str) -> Resume {
        if self.step_set_hash.is_empty() || self.step_set_hash == current_step_set_hash {
            Resume::Continue
        } else if self.schema_version != current_version {
            Resume::Remapped
        } else {
            Resume::Undecodable
        }
    }
}

pub mod inspection {
    /// Largest row sample used by an aggregate inspection query.
    pub const SAMPLE_LIMIT: i64 = 50_000;
    /// Largest sample used to estimate a job's queue position.
    pub const POSITION_LIMIT: i64 = 1_000;
    /// Largest quiet-partition set inspected in one request.
    pub const QUIET_PARTITION_LIMIT: i64 = 1_000;
    /// Largest list page exposed by an inspection adapter.
    pub const MAX_PAGE: u32 = 200;
    /// Largest per-queue sample used for memory estimates.
    pub const MEMORY_SAMPLE_LIMIT: u32 = 1_000;

    pub const fn age_ms(now_ms: i64, at_ms: i64) -> i64 {
        let age = now_ms - at_ms;
        if age > 0 { age } else { 0 }
    }

    pub fn time_to_drain_ms(backlog: i64, arrival_rate: f64, drain_rate: f64) -> Option<i64> {
        (drain_rate > arrival_rate && drain_rate > 0.0)
            .then(|| (backlog as f64 / (drain_rate - arrival_rate) * 1000.0) as i64)
    }
}

pub mod codec {
    use super::{BTreeMap, Checkpoint};

    pub fn encode_string_list(values: &[String]) -> String {
        serde_json::to_string(values).unwrap_or_else(|_| "[]".into())
    }

    pub fn decode_string_list(encoded: &str) -> Vec<String> {
        serde_json::from_str(encoded).unwrap_or_default()
    }

    pub fn encode_checkpoint_value(checkpoint: &Checkpoint) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        if !checkpoint.completed_steps.is_empty() {
            object.insert(
                "completed".into(),
                checkpoint.completed_steps.clone().into(),
            );
        }
        if let Some(step) = &checkpoint.in_progress_step {
            object.insert("in_progress".into(), step.clone().into());
        }
        if let Some(step) = &checkpoint.cursor_step {
            object.insert("cursor_step".into(), step.clone().into());
        }
        if checkpoint.schema_version != 0 {
            object.insert("version".into(), checkpoint.schema_version.into());
        }
        if !checkpoint.step_set_hash.is_empty() {
            object.insert("hash".into(), checkpoint.step_set_hash.clone().into());
        }
        if !checkpoint.crashes_by_step.is_empty() {
            let crashes = checkpoint
                .crashes_by_step
                .iter()
                .map(|(step, count)| (step.clone(), (*count).into()))
                .collect();
            object.insert("crashes".into(), serde_json::Value::Object(crashes));
        }
        serde_json::Value::Object(object)
    }

    pub fn encode_checkpoint_json(checkpoint: &Checkpoint) -> String {
        encode_checkpoint_value(checkpoint).to_string()
    }

    pub fn decode_checkpoint_value(
        value: Option<serde_json::Value>,
        cursor: Option<Vec<u8>>,
    ) -> Checkpoint {
        let mut checkpoint = Checkpoint {
            cursor,
            ..Default::default()
        };
        let Some(serde_json::Value::Object(object)) = value else {
            return checkpoint;
        };
        if let Some(serde_json::Value::Array(completed)) = object.get("completed") {
            checkpoint.completed_steps = completed
                .iter()
                .filter_map(|step| step.as_str().map(String::from))
                .collect();
            checkpoint.last_completed_step = checkpoint.completed_steps.last().cloned();
        }
        checkpoint.in_progress_step = object
            .get("in_progress")
            .and_then(|step| step.as_str())
            .map(String::from);
        checkpoint.cursor_step = object
            .get("cursor_step")
            .and_then(|step| step.as_str())
            .map(String::from);
        checkpoint.schema_version = object
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32;
        checkpoint.step_set_hash = object
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        if let Some(serde_json::Value::Object(crashes)) = object.get("crashes") {
            checkpoint.crashes_by_step = crashes
                .iter()
                .map(|(step, count)| (step.clone(), count.as_u64().unwrap_or(0) as u32))
                .collect();
        }
        checkpoint
    }

    pub fn decode_checkpoint_bytes(json: Option<&[u8]>, cursor: Option<Vec<u8>>) -> Checkpoint {
        let value = json.and_then(|bytes| serde_json::from_slice(bytes).ok());
        decode_checkpoint_value(value, cursor)
    }

    pub fn decode_checkpoint_str(json: Option<&str>, cursor: Option<Vec<u8>>) -> Checkpoint {
        decode_checkpoint_bytes(json.map(str::as_bytes), cursor)
    }

    pub fn encode_headers_value(headers: &BTreeMap<String, String>) -> serde_json::Value {
        serde_json::Value::Object(
            headers
                .iter()
                .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
                .collect(),
        )
    }

    pub fn encode_headers_json(headers: &BTreeMap<String, String>, omit_empty: bool) -> String {
        if omit_empty && headers.is_empty() {
            String::new()
        } else {
            encode_headers_value(headers).to_string()
        }
    }

    pub fn decode_headers_value(value: Option<serde_json::Value>) -> BTreeMap<String, String> {
        let Some(serde_json::Value::Object(object)) = value else {
            return BTreeMap::new();
        };
        object
            .into_iter()
            .filter_map(|(key, value)| match value {
                serde_json::Value::String(text) => Some((key, text)),
                _ => None,
            })
            .collect()
    }

    pub fn decode_headers_bytes(json: Option<&[u8]>) -> BTreeMap<String, String> {
        let value = json.and_then(|bytes| serde_json::from_slice(bytes).ok());
        decode_headers_value(value)
    }

    pub fn decode_headers_str(json: Option<&str>) -> BTreeMap<String, String> {
        decode_headers_bytes(json.map(str::as_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::{AdmissionFacts, Checkpoint, Outcome, Resume, codec};

    #[test]
    fn checkpoint_codec_has_a_stable_wire_shape() {
        let checkpoint = Checkpoint {
            completed_steps: vec!["fetch".into(), "transform".into()],
            in_progress_step: Some("publish".into()),
            cursor_step: Some("transform".into()),
            cursor: Some(b"opaque".to_vec()),
            schema_version: 2,
            step_set_hash: "steps-v2".into(),
            crashes_by_step: vec![("publish".into(), 1)],
            ..Default::default()
        };
        let encoded = codec::encode_checkpoint_json(&checkpoint);
        assert_eq!(
            encoded,
            r#"{"completed":["fetch","transform"],"crashes":{"publish":1},"cursor_step":"transform","hash":"steps-v2","in_progress":"publish","version":2}"#
        );
        let mut expected = checkpoint.clone();
        expected.last_completed_step = Some("transform".into());
        assert_eq!(
            codec::decode_checkpoint_str(Some(&encoded), checkpoint.cursor.clone()),
            expected
        );
    }

    #[test]
    fn malformed_checkpoint_preserves_cursor() {
        let checkpoint = codec::decode_checkpoint_str(Some("{"), Some(b"cursor".to_vec()));
        assert_eq!(checkpoint.cursor.as_deref(), Some(b"cursor".as_slice()));
        assert!(checkpoint.completed_steps.is_empty());
    }

    #[test]
    fn resumability_remains_conservative() {
        let checkpoint = Checkpoint {
            schema_version: 1,
            step_set_hash: "old".into(),
            ..Default::default()
        };
        assert_eq!(checkpoint.resumability(1, "old"), Resume::Continue);
        assert_eq!(checkpoint.resumability(2, "new"), Resume::Remapped);
        assert_eq!(checkpoint.resumability(1, "new"), Resume::Undecodable);
    }

    #[test]
    fn policy_and_admission_rules_are_shared() {
        for raw in [
            "success",
            "retry",
            "skip",
            "revoke",
            "snooze",
            "lease_lost",
            "undecodable",
            "rate_limited",
        ] {
            let outcome = Outcome::parse(raw).expect("known outcome");
            assert_eq!(outcome.as_str(), raw);
        }
        assert_eq!(
            super::bulk_action_states("cancel"),
            Some(["scheduled", "available", "running"].as_slice())
        );
        let evaluation = super::evaluate_admission(&AdmissionFacts {
            state: "available".into(),
            rate_class: "api".into(),
            weight: 3,
            tokens_available: Some(2),
            limit_per_window: 1,
            window_ms: 1_000,
            ..Default::default()
        });
        assert_eq!(evaluation.blocked_by, Some("rate_class"));
        assert_eq!(evaluation.estimated_admission_ms, Some(1_000));
        assert_eq!(
            super::inspection::time_to_drain_ms(10, 2.0, 4.0),
            Some(5_000)
        );
    }
}
