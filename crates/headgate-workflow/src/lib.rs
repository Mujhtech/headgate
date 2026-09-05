//! Durable DAG dependencies layered on headgate's ordinary pending jobs.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use futures_util::{StreamExt, TryStreamExt, stream};
use headgate::{CodecError, Control, Envelope, JobCtx, JobError, Registry, Task};
use headgate_core::{
    DurableEvent, Inspect, JobFilter, MAX_ENQUEUE_BATCH_SIZE, MAX_JOB_IDENTIFIER_LEN,
};
use serde::{Deserialize, Serialize};

const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const MAX_WORKFLOW_NODES: usize = MAX_ENQUEUE_BATCH_SIZE - 1;
const MAX_WORKFLOW_EDGES: usize = 10_000;
const MAX_WORKFLOW_EVENTS: usize = 256;
const WORKFLOW_CONCURRENCY: usize = 16;
const MAX_SIGNAL_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_SIGNAL_SOURCE_BYTES: usize = 16 * 1024;

#[derive(Debug)]
pub struct WorkflowError(String);

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for WorkflowError {}

#[derive(Clone)]
struct DraftNode {
    name: String,
    kind: DraftNodeKind,
    deps: Vec<String>,
}

#[derive(Clone)]
enum DraftNodeKind {
    Task(Box<Envelope>),
    Signal { signal: String },
    TimerAt { wake_at_ms: i64 },
    TimerAfter { delay_ms: i64 },
    ChildWorkflow { workflow_id: String },
    Condition { expression: String },
}

/// A validated DAG builder. `prepare` returns one atomic enqueue batch containing the
/// durable coordinator plus every child in `pending` state.
pub struct Workflow {
    id: String,
    nodes: Vec<DraftNode>,
    coordinator_queue: String,
    retention_ms: i64,
    failed_subgraph_retry: bool,
    retry_policy: Option<WorkflowRetryPolicy>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRetryPolicy {
    pub max_generations: u32,
    pub backoff_ms: i64,
}

/// A revision-checked set of tasks to graft onto a running workflow.
///
/// Enqueue the returned batch atomically. The receipt and its pending tasks then either
/// all exist or none do; the coordinator accepts only the receipt for its next revision.
pub struct WorkflowGraft {
    workflow_id: String,
    expected_revision: u64,
    nodes: Vec<DraftNode>,
    queue: String,
    retention_ms: i64,
}

/// Prepare parent and child workflows as one atomic store enqueue. Every child link in
/// the bundle must name another member, which makes the complete cross-workflow graph
/// available for cycle detection before any row is written.
pub fn prepare_bundle(workflows: Vec<Workflow>) -> Result<Vec<Envelope>, WorkflowError> {
    if workflows.is_empty() {
        return Err(WorkflowError(
            "workflow bundle must contain at least one workflow".into(),
        ));
    }
    let ids: HashSet<&str> = workflows
        .iter()
        .map(|workflow| workflow.id.as_str())
        .collect();
    if ids.len() != workflows.len() || ids.contains("") {
        return Err(WorkflowError(
            "workflow bundle ids must be non-empty and unique".into(),
        ));
    }
    let mut indegree: HashMap<&str, usize> = ids.iter().map(|id| (*id, 0)).collect();
    let mut outgoing: HashMap<&str, Vec<&str>> = HashMap::new();
    for workflow in &workflows {
        let mut children = HashSet::new();
        for child in workflow.nodes.iter().filter_map(|node| match &node.kind {
            DraftNodeKind::ChildWorkflow { workflow_id } => Some(workflow_id.as_str()),
            _ => None,
        }) {
            if !ids.contains(child) {
                return Err(WorkflowError(format!(
                    "atomic workflow bundle is missing child `{child}`"
                )));
            }
            if children.insert(child) {
                *indegree.get_mut(child).expect("bundle child exists") += 1;
                outgoing
                    .entry(workflow.id.as_str())
                    .or_default()
                    .push(child);
            }
        }
    }
    let mut ready: VecDeque<&str> = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
        .collect();
    let mut visited = 0;
    while let Some(id) = ready.pop_front() {
        visited += 1;
        for child in outgoing.get(id).into_iter().flatten() {
            let degree = indegree.get_mut(child).expect("bundle child exists");
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(child);
            }
        }
    }
    if visited != workflows.len() {
        return Err(WorkflowError(
            "cross-workflow child graph contains a cycle".into(),
        ));
    }
    let mut batch = Vec::new();
    for workflow in workflows {
        batch.extend(workflow.prepare()?);
        if batch.len() > MAX_ENQUEUE_BATCH_SIZE {
            return Err(WorkflowError(format!(
                "workflow bundle must contain at most {MAX_ENQUEUE_BATCH_SIZE} jobs"
            )));
        }
    }
    Ok(batch)
}

impl WorkflowGraft {
    pub fn new(workflow_id: impl Into<String>, expected_revision: u64) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            expected_revision,
            nodes: Vec::new(),
            queue: "headgate-workflow".into(),
            retention_ms: DEFAULT_RETENTION_MS,
        }
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    pub fn retention(mut self, duration: Duration) -> Result<Self, WorkflowError> {
        let ms = i64::try_from(duration.as_millis())
            .map_err(|_| WorkflowError("workflow graft retention is too large".into()))?;
        if ms <= 0 {
            return Err(WorkflowError(
                "workflow graft retention must be at least 1ms".into(),
            ));
        }
        self.retention_ms = ms;
        Ok(self)
    }

    pub fn add(
        mut self,
        name: impl Into<String>,
        envelope: Envelope,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.nodes.push(DraftNode {
            name: name.into(),
            kind: DraftNodeKind::Task(Box::new(envelope)),
            deps: deps.into_iter().map(Into::into).collect(),
        });
        self
    }

    pub fn prepare(self) -> Result<Vec<Envelope>, WorkflowError> {
        if self.workflow_id.is_empty() {
            return Err(WorkflowError("workflow id must not be empty".into()));
        }
        if self.expected_revision == 0 {
            return Err(WorkflowError(
                "workflow graft expected revision must be at least 1".into(),
            ));
        }
        if self.nodes.is_empty() || self.nodes.len() > MAX_WORKFLOW_NODES {
            return Err(WorkflowError(format!(
                "workflow graft must contain 1-{MAX_WORKFLOW_NODES} tasks"
            )));
        }
        let next_revision = self
            .expected_revision
            .checked_add(1)
            .ok_or_else(|| WorkflowError("workflow graft revision would overflow".into()))?;
        let mut names = HashSet::with_capacity(self.nodes.len());
        let mut specs = Vec::with_capacity(self.nodes.len());
        let mut children = Vec::with_capacity(self.nodes.len());
        for node in self.nodes {
            if node.name.is_empty() || node.name.len() > 128 || !names.insert(node.name.clone()) {
                return Err(WorkflowError(
                    "workflow graft task names must be non-empty and unique".into(),
                ));
            }
            let DraftNodeKind::Task(envelope) = node.kind else {
                return Err(WorkflowError(
                    "workflow graft currently accepts ordinary tasks only".into(),
                ));
            };
            let mut envelope = *envelope;
            if envelope.id.is_empty() {
                envelope.id = format!("{}:g{}:{}", self.workflow_id, next_revision, node.name);
            }
            envelope.pending = true;
            envelope.scheduled_at_ms = 0;
            if envelope.retention_ms < self.retention_ms {
                envelope.retention_ms = self.retention_ms;
            }
            envelope = headgate::prepare_envelope(envelope)
                .map_err(|error| WorkflowError(error.to_string()))?;
            specs.push(NodeSpec {
                name: node.name,
                job_id: envelope.id.clone(),
                deps: node.deps,
                kind: NodeType::Task,
                signal: None,
                wake_at_ms: None,
                delay_ms: None,
                child_workflow_id: None,
                condition: None,
            });
            children.push(envelope);
        }
        validate_graft_nodes(&specs)?;
        let graft = GraftTask {
            workflow_id: self.workflow_id.clone(),
            expected_revision: self.expected_revision,
            nodes: specs,
        };
        let payload = graft
            .encode()
            .map_err(|error| WorkflowError(error.to_string()))?;
        let receipt = headgate::prepare_envelope(Envelope {
            id: graft_receipt_id(&self.workflow_id, next_revision),
            kind: GraftTask::TYPE.into(),
            schema_version: GraftTask::VERSION,
            fingerprint: headgate::fingerprint(GraftTask::TYPE, &payload),
            payload,
            queue: self.queue,
            pending: true,
            retention_ms: self.retention_ms,
            ..Default::default()
        })
        .map_err(|error| WorkflowError(error.to_string()))?;
        let mut batch = Vec::with_capacity(children.len() + 1);
        batch.push(receipt);
        batch.extend(children);
        Ok(batch)
    }
}

fn validate_graft_nodes(nodes: &[NodeSpec]) -> Result<(), WorkflowError> {
    let names: HashSet<&str> = nodes.iter().map(|node| node.name.as_str()).collect();
    let mut indegree: HashMap<&str, usize> =
        nodes.iter().map(|node| (node.name.as_str(), 0)).collect();
    let mut outgoing: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut edges = 0usize;
    for node in nodes {
        let mut unique = HashSet::new();
        for dep in &node.deps {
            edges = edges.saturating_add(1);
            if !unique.insert(dep.as_str()) {
                return Err(WorkflowError(format!(
                    "workflow graft task `{}` repeats dependency `{dep}`",
                    node.name
                )));
            }
            if dep == &node.name {
                return Err(WorkflowError(format!(
                    "workflow graft task `{}` depends on itself",
                    node.name
                )));
            }
            if names.contains(dep.as_str()) {
                *indegree.get_mut(node.name.as_str()).expect("known node") += 1;
                outgoing.entry(dep).or_default().push(&node.name);
            }
        }
    }
    if edges > MAX_WORKFLOW_EDGES {
        return Err(WorkflowError(format!(
            "workflow graft must contain at most {MAX_WORKFLOW_EDGES} dependency edges"
        )));
    }
    let mut ready: VecDeque<&str> = indegree
        .iter()
        .filter_map(|(name, degree)| (*degree == 0).then_some(*name))
        .collect();
    let mut visited = 0;
    while let Some(name) = ready.pop_front() {
        visited += 1;
        for child in outgoing.get(name).into_iter().flatten() {
            let degree = indegree.get_mut(child).expect("known child");
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(child);
            }
        }
    }
    if visited != nodes.len() {
        return Err(WorkflowError(
            "workflow graft dependency graph contains a cycle".into(),
        ));
    }
    Ok(())
}

impl Workflow {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            nodes: Vec::new(),
            coordinator_queue: "headgate-workflow".into(),
            retention_ms: DEFAULT_RETENTION_MS,
            failed_subgraph_retry: false,
            retry_policy: None,
        }
    }

    pub fn coordinator_queue(mut self, queue: impl Into<String>) -> Self {
        self.coordinator_queue = queue.into();
        self
    }

    pub fn retention(mut self, duration: Duration) -> Result<Self, WorkflowError> {
        let ms = i64::try_from(duration.as_millis())
            .map_err(|_| WorkflowError("workflow retention is too large".into()))?;
        if ms <= 0 {
            return Err(WorkflowError(
                "workflow retention must be at least 1ms".into(),
            ));
        }
        self.retention_ms = ms;
        Ok(self)
    }

    /// Retain dependency-blocked pending jobs so a failed generation can be retried
    /// without rerunning successful ancestors.
    pub fn failed_subgraph_retry(mut self) -> Self {
        self.failed_subgraph_retry = true;
        self
    }

    /// Automatically retry the failed subgraph after a store-timed snooze. The limit
    /// includes the initial generation, so `max_generations = 3` permits two retries.
    pub fn automatic_retry(
        mut self,
        max_generations: u32,
        backoff: Duration,
    ) -> Result<Self, WorkflowError> {
        let backoff_ms = i64::try_from(backoff.as_millis())
            .map_err(|_| WorkflowError("workflow retry backoff is too large".into()))?;
        if max_generations < 2 || backoff_ms <= 0 {
            return Err(WorkflowError(
                "automatic workflow retry requires at least 2 generations and 1ms backoff".into(),
            ));
        }
        self.failed_subgraph_retry = true;
        self.retry_policy = Some(WorkflowRetryPolicy {
            max_generations,
            backoff_ms,
        });
        Ok(self)
    }

    pub fn add(
        mut self,
        name: impl Into<String>,
        envelope: Envelope,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.nodes.push(DraftNode {
            name: name.into(),
            kind: DraftNodeKind::Task(Box::new(envelope)),
            deps: deps.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Add a durable workflow signal. Emission may happen before its dependencies
    /// complete; the coordinator buffers that fact and consumes it once the node is
    /// eligible.
    pub fn add_signal(
        mut self,
        name: impl Into<String>,
        signal: impl Into<String>,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.nodes.push(DraftNode {
            name: name.into(),
            kind: DraftNodeKind::Signal {
                signal: signal.into(),
            },
            deps: deps.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Add an absolute store-time timer. The ordinary scheduled-job promoter supplies
    /// the clock, so worker clock skew cannot fire the timer early or late.
    pub fn add_timer_at(
        mut self,
        name: impl Into<String>,
        wake_at_ms: i64,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.nodes.push(DraftNode {
            name: name.into(),
            kind: DraftNodeKind::TimerAt { wake_at_ms },
            deps: deps.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Add a relative timer anchored to the latest dependency finalization timestamp.
    /// The coordinator durably records that store timestamp before scheduling the timer.
    pub fn add_timer_after(
        mut self,
        name: impl Into<String>,
        delay: Duration,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, WorkflowError> {
        let delay_ms = i64::try_from(delay.as_millis())
            .map_err(|_| WorkflowError("workflow timer delay is too large".into()))?;
        if delay_ms <= 0 {
            return Err(WorkflowError(
                "workflow timer delay must be at least 1ms".into(),
            ));
        }
        self.nodes.push(DraftNode {
            name: name.into(),
            kind: DraftNodeKind::TimerAfter { delay_ms },
            deps: deps.into_iter().map(Into::into).collect(),
        });
        Ok(self)
    }

    /// Add an explicit child-workflow link. The child workflow is enqueued separately;
    /// this node mirrors its coordinator's terminal state into the parent.
    pub fn add_child(
        mut self,
        name: impl Into<String>,
        workflow_id: impl Into<String>,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.nodes.push(DraftNode {
            name: name.into(),
            kind: DraftNodeKind::ChildWorkflow {
                workflow_id: workflow_id.into(),
            },
            deps: deps.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Wait until a CEL expression over `revision`, `generation`, `completed`, and
    /// `states` evaluates to true.
    pub fn add_condition(
        mut self,
        name: impl Into<String>,
        expression: impl Into<String>,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.nodes.push(DraftNode {
            name: name.into(),
            kind: DraftNodeKind::Condition {
                expression: expression.into(),
            },
            deps: deps.into_iter().map(Into::into).collect(),
        });
        self
    }

    pub fn prepare(self) -> Result<Vec<Envelope>, WorkflowError> {
        if self.id.is_empty() {
            return Err(WorkflowError("workflow id must not be empty".into()));
        }
        if self.nodes.is_empty() {
            return Err(WorkflowError(
                "workflow must contain at least one task".into(),
            ));
        }
        validate_graph(&self.nodes)?;

        let mut specs = Vec::with_capacity(self.nodes.len());
        let mut children = Vec::with_capacity(self.nodes.len());
        for node in self.nodes {
            let (mut envelope, kind, signal, wake_at_ms, delay_ms, child_workflow_id, condition) =
                match node.kind {
                    DraftNodeKind::Task(envelope) => {
                        (*envelope, NodeType::Task, None, None, None, None, None)
                    }
                    DraftNodeKind::Signal { signal } => {
                        let task = SignalTask {
                            workflow_id: self.id.clone(),
                            signal: signal.clone(),
                        };
                        let payload = task
                            .encode()
                            .map_err(|error| WorkflowError(error.to_string()))?;
                        (
                            Envelope {
                                kind: SignalTask::TYPE.into(),
                                schema_version: SignalTask::VERSION,
                                fingerprint: headgate::fingerprint(SignalTask::TYPE, &payload),
                                payload,
                                queue: self.coordinator_queue.clone(),
                                ..Default::default()
                            },
                            NodeType::Signal,
                            Some(signal),
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                    DraftNodeKind::TimerAt { wake_at_ms } => {
                        let task = TimerTask {
                            workflow_id: self.id.clone(),
                            wake_at_ms: Some(wake_at_ms),
                            delay_ms: None,
                        };
                        let payload = task
                            .encode()
                            .map_err(|error| WorkflowError(error.to_string()))?;
                        (
                            Envelope {
                                kind: TimerTask::TYPE.into(),
                                schema_version: TimerTask::VERSION,
                                fingerprint: headgate::fingerprint(TimerTask::TYPE, &payload),
                                payload,
                                queue: self.coordinator_queue.clone(),
                                scheduled_at_ms: wake_at_ms,
                                ..Default::default()
                            },
                            NodeType::Timer,
                            None,
                            Some(wake_at_ms),
                            None,
                            None,
                            None,
                        )
                    }
                    DraftNodeKind::TimerAfter { delay_ms } => {
                        let task = TimerTask {
                            workflow_id: self.id.clone(),
                            wake_at_ms: None,
                            delay_ms: Some(delay_ms),
                        };
                        let payload = task
                            .encode()
                            .map_err(|error| WorkflowError(error.to_string()))?;
                        (
                            Envelope {
                                kind: TimerTask::TYPE.into(),
                                schema_version: TimerTask::VERSION,
                                fingerprint: headgate::fingerprint(TimerTask::TYPE, &payload),
                                payload,
                                queue: self.coordinator_queue.clone(),
                                ..Default::default()
                            },
                            NodeType::Timer,
                            None,
                            None,
                            Some(delay_ms),
                            None,
                            None,
                        )
                    }
                    DraftNodeKind::ChildWorkflow { workflow_id } => {
                        if workflow_id == self.id {
                            return Err(WorkflowError(
                                "workflow cannot contain itself as a child".into(),
                            ));
                        }
                        let task = ChildWorkflowTask {
                            parent_workflow_id: self.id.clone(),
                            child_workflow_id: workflow_id.clone(),
                        };
                        let payload = task
                            .encode()
                            .map_err(|error| WorkflowError(error.to_string()))?;
                        (
                            Envelope {
                                kind: ChildWorkflowTask::TYPE.into(),
                                schema_version: ChildWorkflowTask::VERSION,
                                fingerprint: headgate::fingerprint(
                                    ChildWorkflowTask::TYPE,
                                    &payload,
                                ),
                                payload,
                                queue: self.coordinator_queue.clone(),
                                ..Default::default()
                            },
                            NodeType::ChildWorkflow,
                            None,
                            None,
                            None,
                            Some(workflow_id),
                            None,
                        )
                    }
                    DraftNodeKind::Condition { expression } => {
                        let task = ConditionTask {
                            workflow_id: self.id.clone(),
                            expression: expression.clone(),
                        };
                        let payload = task
                            .encode()
                            .map_err(|error| WorkflowError(error.to_string()))?;
                        (
                            Envelope {
                                kind: ConditionTask::TYPE.into(),
                                schema_version: ConditionTask::VERSION,
                                fingerprint: headgate::fingerprint(ConditionTask::TYPE, &payload),
                                payload,
                                queue: self.coordinator_queue.clone(),
                                ..Default::default()
                            },
                            NodeType::Condition,
                            None,
                            None,
                            None,
                            None,
                            Some(expression),
                        )
                    }
                };
            if envelope.id.is_empty() {
                envelope.id = format!("{}:{}", self.id, node.name);
            }
            if envelope.retention_ms < self.retention_ms {
                envelope.retention_ms = self.retention_ms;
            }
            if kind == NodeType::Timer && wake_at_ms.is_some() {
                envelope.pending = false;
            } else {
                envelope.pending = true;
                envelope.scheduled_at_ms = 0;
            }
            envelope =
                headgate::prepare_envelope(envelope).map_err(|e| WorkflowError(e.to_string()))?;
            specs.push(NodeSpec {
                name: node.name,
                job_id: envelope.id.clone(),
                deps: node.deps,
                kind,
                signal,
                wake_at_ms,
                delay_ms,
                child_workflow_id,
                condition,
            });
            children.push(envelope);
        }

        let task = CoordinatorTask {
            workflow_id: self.id.clone(),
            nodes: specs,
            failed_subgraph_retry: self.failed_subgraph_retry,
            retry_policy: self.retry_policy,
        };
        let payload = task.encode().map_err(|e| WorkflowError(e.to_string()))?;
        let coordinator = headgate::prepare_envelope(Envelope {
            id: format!("{}:coordinator", self.id),
            kind: CoordinatorTask::TYPE.into(),
            schema_version: CoordinatorTask::VERSION,
            fingerprint: headgate::fingerprint(CoordinatorTask::TYPE, &payload),
            payload,
            queue: self.coordinator_queue,
            retention_ms: self.retention_ms,
            ..Default::default()
        })
        .map_err(|e| WorkflowError(e.to_string()))?;
        let mut batch = Vec::with_capacity(children.len() + 1);
        batch.push(coordinator);
        batch.extend(children);
        Ok(batch)
    }
}

fn validate_graph(nodes: &[DraftNode]) -> Result<(), WorkflowError> {
    if nodes.len() > MAX_WORKFLOW_NODES {
        return Err(WorkflowError(format!(
            "workflow must contain at most {MAX_WORKFLOW_NODES} tasks"
        )));
    }
    let names: HashSet<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
    if names.len() != nodes.len() || names.contains("") {
        return Err(WorkflowError(
            "workflow task names must be non-empty and unique".into(),
        ));
    }
    let mut indegree: HashMap<&str, usize> = nodes.iter().map(|n| (n.name.as_str(), 0)).collect();
    let mut outgoing: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut edges = 0usize;
    for node in nodes {
        if node.name.len() > 128 {
            return Err(WorkflowError(format!(
                "workflow task name `{}` exceeds 128 bytes",
                node.name
            )));
        }
        if matches!(&node.kind, DraftNodeKind::Signal { signal } if signal.is_empty()) {
            return Err(WorkflowError(format!(
                "workflow signal node `{}` has an empty signal",
                node.name
            )));
        }
        if matches!(&node.kind, DraftNodeKind::TimerAt { wake_at_ms } if *wake_at_ms <= 0) {
            return Err(WorkflowError(format!(
                "workflow timer node `{}` must have a positive absolute wake time",
                node.name
            )));
        }
        if matches!(node.kind, DraftNodeKind::TimerAfter { .. }) && node.deps.is_empty() {
            return Err(WorkflowError(format!(
                "relative workflow timer `{}` requires at least one dependency",
                node.name
            )));
        }
        if matches!(&node.kind, DraftNodeKind::ChildWorkflow { workflow_id } if workflow_id.is_empty())
        {
            return Err(WorkflowError(format!(
                "workflow child node `{}` has an empty workflow id",
                node.name
            )));
        }
        if let DraftNodeKind::Condition { expression } = &node.kind {
            validate_condition(expression)?;
        }
        edges = edges.saturating_add(node.deps.len());
        let mut unique = HashSet::new();
        for dep in &node.deps {
            if !names.contains(dep.as_str()) {
                return Err(WorkflowError(format!(
                    "task `{}` depends on missing task `{dep}`",
                    node.name
                )));
            }
            if !unique.insert(dep.as_str()) {
                return Err(WorkflowError(format!(
                    "task `{}` repeats dependency `{dep}`",
                    node.name
                )));
            }
            *indegree.get_mut(node.name.as_str()).unwrap() += 1;
            outgoing.entry(dep).or_default().push(&node.name);
        }
    }
    if edges > MAX_WORKFLOW_EDGES {
        return Err(WorkflowError(format!(
            "workflow must contain at most {MAX_WORKFLOW_EDGES} dependency edges"
        )));
    }
    let mut ready: VecDeque<&str> = indegree
        .iter()
        .filter_map(|(n, d)| (*d == 0).then_some(*n))
        .collect();
    let mut visited = 0;
    while let Some(name) = ready.pop_front() {
        visited += 1;
        for child in outgoing.get(name).into_iter().flatten() {
            let degree = indegree.get_mut(child).unwrap();
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(child);
            }
        }
    }
    if visited != nodes.len() {
        return Err(WorkflowError(
            "workflow dependency graph contains a cycle".into(),
        ));
    }
    Ok(())
}

fn validate_condition(expression: &str) -> Result<(), WorkflowError> {
    if expression.is_empty() || expression.len() > 1_024 {
        return Err(WorkflowError(
            "workflow CEL condition must contain 1-1024 bytes".into(),
        ));
    }
    cel::Program::compile(expression)
        .map(|_| ())
        .map_err(|error| WorkflowError(format!("invalid workflow CEL condition: {error}")))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NodeSpec {
    name: String,
    job_id: String,
    deps: Vec<String>,
    #[serde(default)]
    kind: NodeType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    wake_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delay_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    child_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    condition: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NodeType {
    #[default]
    Task,
    Signal,
    Timer,
    ChildWorkflow,
    Condition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoordinatorTask {
    pub workflow_id: String,
    nodes: Vec<NodeSpec>,
    #[serde(default)]
    failed_subgraph_retry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy: Option<WorkflowRetryPolicy>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignalTask {
    pub workflow_id: String,
    pub signal: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerTask {
    pub workflow_id: String,
    pub wake_at_ms: Option<i64>,
    pub delay_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildWorkflowTask {
    pub parent_workflow_id: String,
    pub child_workflow_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConditionTask {
    pub workflow_id: String,
    pub expression: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraftTask {
    pub workflow_id: String,
    pub expected_revision: u64,
    nodes: Vec<NodeSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryTask {
    pub workflow_id: String,
    pub expected_revision: u64,
}

impl Task for SignalTask {
    const TYPE: &'static str = "headgate:workflow-signal";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|error| CodecError::Malformed(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes).map_err(|error| CodecError::Malformed(error.to_string()))
    }
}

impl Task for TimerTask {
    const TYPE: &'static str = "headgate:workflow-timer";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|error| CodecError::Malformed(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes).map_err(|error| CodecError::Malformed(error.to_string()))
    }
}

impl Task for ChildWorkflowTask {
    const TYPE: &'static str = "headgate:workflow-child";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|error| CodecError::Malformed(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes).map_err(|error| CodecError::Malformed(error.to_string()))
    }
}

impl Task for ConditionTask {
    const TYPE: &'static str = "headgate:workflow-condition";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|error| CodecError::Malformed(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes).map_err(|error| CodecError::Malformed(error.to_string()))
    }
}

impl Task for GraftTask {
    const TYPE: &'static str = "headgate:workflow-graft";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|error| CodecError::Malformed(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes).map_err(|error| CodecError::Malformed(error.to_string()))
    }
}

impl Task for RetryTask {
    const TYPE: &'static str = "headgate:workflow-retry";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|error| CodecError::Malformed(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes).map_err(|error| CodecError::Malformed(error.to_string()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignalReceipt {
    pub matched: usize,
    pub promoted: usize,
    pub inserted: bool,
    pub emission: WorkflowSignal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignalEmission {
    pub signal: String,
    pub idempotency_key: String,
    pub payload: serde_json::Value,
    pub source: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowSignal {
    pub id: u64,
    pub signal: String,
    pub idempotency_key: String,
    pub payload: serde_json::Value,
    pub source: serde_json::Value,
    pub recorded_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryReceipt {
    pub revision: u64,
    pub generation: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRecovery {
    pub node: String,
    pub payload: Option<Vec<u8>>,
    pub schema_version: Option<u32>,
    pub release_quarantine: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CancelReceipt {
    pub workflows: usize,
    pub jobs: usize,
}

/// Cancel a workflow and, by default, every linked child workflow. Traversal is
/// iterative and bounded by the same node cap as creation; running jobs lose their
/// lease through the ordinary operator-cancel path.
pub async fn cancel_workflow(
    inspect: &dyn Inspect,
    workflow_id: &str,
    propagate_children: bool,
) -> Result<CancelReceipt, WorkflowError> {
    if workflow_id.is_empty() {
        return Err(WorkflowError("workflow id must not be empty".into()));
    }
    let mut pending = VecDeque::from([workflow_id.to_string()]);
    let mut visited = HashSet::new();
    let mut jobs = 0;
    while let Some(current) = pending.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if visited.len() > MAX_WORKFLOW_NODES {
            return Err(WorkflowError(
                "workflow cancellation exceeds the bounded nested-workflow limit".into(),
            ));
        }
        let coordinator_id = format!("{current}:coordinator");
        let coordinator = inspect
            .get_job(&coordinator_id, true)
            .await
            .map_err(|error| WorkflowError(error.to_string()))?
            .ok_or_else(|| WorkflowError(format!("workflow `{current}` was not found")))?;
        let payload = coordinator
            .payload
            .as_deref()
            .ok_or_else(|| WorkflowError("workflow coordinator payload was not returned".into()))?;
        let task = CoordinatorTask::decode(payload)
            .map_err(|error| WorkflowError(format!("invalid workflow coordinator: {error}")))?;
        if propagate_children {
            pending.extend(
                task.nodes
                    .iter()
                    .filter_map(|node| node.child_workflow_id.clone()),
            );
        }
        for job_id in task
            .nodes
            .iter()
            .map(|node| node.job_id.as_str())
            .chain(std::iter::once(coordinator_id.as_str()))
        {
            let Some(job) = inspect
                .get_job(job_id, false)
                .await
                .map_err(|error| WorkflowError(error.to_string()))?
            else {
                continue;
            };
            if matches!(
                job.state.as_str(),
                "pending" | "scheduled" | "available" | "running" | "retryable"
            ) {
                inspect
                    .operator_cancel(job_id)
                    .await
                    .map_err(|error| WorkflowError(error.to_string()))?;
                jobs += 1;
            }
        }
    }
    Ok(CancelReceipt {
        workflows: visited.len(),
        jobs,
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEvent {
    pub sequence: u64,
    pub event: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    pub revision: u64,
    pub generation: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_ms: Option<i64>,
}

/// The durable role a node plays in a workflow graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowNodeKind {
    Task,
    Signal,
    Timer,
    ChildWorkflow,
    Condition,
}

/// One node in an inspected workflow graph. Dependencies and dependents contain node
/// names, while `job_id` is the underlying Headgate job to inspect or control.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub name: String,
    pub job_id: String,
    pub kind: WorkflowNodeKind,
    pub job_kind: String,
    pub state: String,
    pub dependencies: Vec<String>,
    pub dependents: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<i64>,
}

/// A bounded point-in-time view of a workflow and its complete accepted graph,
/// including additive grafts accepted in later revisions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowSnapshot {
    pub workflow_id: String,
    pub coordinator_job_id: String,
    pub coordinator_state: String,
    pub revision: u64,
    pub generation: u32,
    pub failed: bool,
    pub failed_subgraph_retry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<WorkflowRetryPolicy>,
    pub nodes: Vec<WorkflowNode>,
}

/// One coordinator entry returned by [`list_workflows`]. Fetch its graph with
/// [`inspect_workflow`] only when node-level detail is needed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowSummary {
    pub workflow_id: String,
    pub coordinator_job_id: String,
    pub state: String,
    pub enqueued_at_ms: i64,
    pub scheduled_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowPage {
    pub workflows: Vec<WorkflowSummary>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

impl WorkflowSnapshot {
    pub fn node(&self, name: &str) -> Option<&WorkflowNode> {
        self.nodes.iter().find(|node| node.name == name)
    }

    pub fn dependencies(&self, name: &str) -> Option<Vec<&WorkflowNode>> {
        let node = self.node(name)?;
        Some(
            node.dependencies
                .iter()
                .filter_map(|dependency| self.node(dependency))
                .collect(),
        )
    }

    pub fn dependents(&self, name: &str) -> Option<Vec<&WorkflowNode>> {
        let node = self.node(name)?;
        Some(
            node.dependents
                .iter()
                .filter_map(|dependent| self.node(dependent))
                .collect(),
        )
    }
}

/// List workflow coordinators without loading every graph. The page is capped at 200;
/// use [`inspect_workflow`] for a selected workflow.
pub async fn list_workflows(
    inspect: &dyn Inspect,
    cursor: Option<&str>,
    limit: u32,
) -> Result<WorkflowPage, WorkflowError> {
    if !(1..=200).contains(&limit) {
        return Err(WorkflowError(
            "workflow list limit must be between 1 and 200".into(),
        ));
    }
    let page = inspect
        .list_jobs(
            &JobFilter {
                kind: Some(CoordinatorTask::TYPE.into()),
                ..Default::default()
            },
            cursor,
            limit,
        )
        .await
        .map_err(|error| WorkflowError(error.to_string()))?;
    Ok(WorkflowPage {
        workflows: page
            .jobs
            .into_iter()
            .map(|job| WorkflowSummary {
                workflow_id: job
                    .id
                    .strip_suffix(":coordinator")
                    .unwrap_or(&job.id)
                    .to_string(),
                coordinator_job_id: job.id,
                state: job.state,
                enqueued_at_ms: job.enqueued_at_ms,
                scheduled_at_ms: job.scheduled_at_ms,
                finalized_at_ms: job.finalized_at_ms,
            })
            .collect(),
        next_cursor: page.next_cursor,
    })
}

/// Inspect the accepted graph and live execution state without exposing task payloads.
pub async fn inspect_workflow(
    inspect: &dyn Inspect,
    workflow_id: &str,
) -> Result<WorkflowSnapshot, WorkflowError> {
    if workflow_id.is_empty() {
        return Err(WorkflowError("workflow id must not be empty".into()));
    }
    let coordinator_job_id = format!("{workflow_id}:coordinator");
    let coordinator = inspect
        .get_job(&coordinator_job_id, true)
        .await
        .map_err(|error| WorkflowError(error.to_string()))?
        .ok_or_else(|| WorkflowError(format!("workflow `{workflow_id}` was not found")))?;
    let payload = coordinator
        .payload
        .as_deref()
        .ok_or_else(|| WorkflowError("workflow coordinator payload was not returned".into()))?;
    let base = CoordinatorTask::decode(payload)
        .map_err(|error| WorkflowError(format!("invalid workflow coordinator: {error}")))?;
    let cursor = load_workflow_cursor(inspect, &coordinator_job_id, &coordinator.state).await?;
    let effective = effective_workflow(&base, &cursor);
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    for node in &effective.nodes {
        for dependency in &node.deps {
            dependents
                .entry(dependency.clone())
                .or_default()
                .push(node.name.clone());
        }
    }
    let completed: HashSet<&str> = cursor.completed.iter().map(String::as_str).collect();
    let mut indexed_nodes = stream::iter(effective.nodes.into_iter().enumerate().map(
        |(index, node)| {
            let recorded_completion = completed.contains(node.name.as_str());
            let completed_at_ms = cursor.completed_at_ms.get(&node.name).copied();
            let node_dependents = dependents.get(&node.name).cloned().unwrap_or_default();
            async move {
                let job = inspect
                    .get_job(&node.job_id, false)
                    .await
                    .map_err(|error| WorkflowError(error.to_string()))?;
                let (state, job_kind) = job.map_or_else(
                    || {
                        (
                            if recorded_completion {
                                "completed"
                            } else {
                                "missing"
                            }
                            .to_string(),
                            String::new(),
                        )
                    },
                    |job| (job.state, job.kind),
                );
                Ok::<_, WorkflowError>((
                    index,
                    WorkflowNode {
                        name: node.name,
                        job_id: node.job_id,
                        kind: match node.kind {
                            NodeType::Task => WorkflowNodeKind::Task,
                            NodeType::Signal => WorkflowNodeKind::Signal,
                            NodeType::Timer => WorkflowNodeKind::Timer,
                            NodeType::ChildWorkflow => WorkflowNodeKind::ChildWorkflow,
                            NodeType::Condition => WorkflowNodeKind::Condition,
                        },
                        job_kind,
                        state,
                        dependencies: node.deps,
                        dependents: node_dependents,
                        signal: node.signal,
                        wake_at_ms: node.wake_at_ms,
                        delay_ms: node.delay_ms,
                        child_workflow_id: node.child_workflow_id,
                        condition: node.condition,
                        completed_at_ms,
                    },
                ))
            }
        },
    ))
    .buffer_unordered(WORKFLOW_CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;
    indexed_nodes.sort_unstable_by_key(|(index, _)| *index);
    let nodes = indexed_nodes.into_iter().map(|(_, node)| node).collect();
    Ok(WorkflowSnapshot {
        workflow_id: workflow_id.to_string(),
        coordinator_job_id,
        coordinator_state: coordinator.state,
        revision: cursor.revision,
        generation: cursor.generation,
        failed: cursor.failed,
        failed_subgraph_retry: base.failed_subgraph_retry,
        retry_policy: base.retry_policy,
        nodes,
    })
}

pub async fn workflow_node(
    inspect: &dyn Inspect,
    workflow_id: &str,
    node: &str,
) -> Result<WorkflowNode, WorkflowError> {
    inspect_workflow(inspect, workflow_id)
        .await?
        .node(node)
        .cloned()
        .ok_or_else(|| WorkflowError(format!("workflow node `{node}` was not found")))
}

pub async fn workflow_dependencies(
    inspect: &dyn Inspect,
    workflow_id: &str,
    node: &str,
) -> Result<Vec<WorkflowNode>, WorkflowError> {
    let snapshot = inspect_workflow(inspect, workflow_id).await?;
    snapshot
        .dependencies(node)
        .map(|nodes| nodes.into_iter().cloned().collect())
        .ok_or_else(|| WorkflowError(format!("workflow node `{node}` was not found")))
}

pub async fn workflow_dependents(
    inspect: &dyn Inspect,
    workflow_id: &str,
    node: &str,
) -> Result<Vec<WorkflowNode>, WorkflowError> {
    let snapshot = inspect_workflow(inspect, workflow_id).await?;
    snapshot
        .dependents(node)
        .map(|nodes| nodes.into_iter().cloned().collect())
        .ok_or_else(|| WorkflowError(format!("workflow node `{node}` was not found")))
}

async fn load_workflow_cursor(
    inspect: &dyn Inspect,
    coordinator_job_id: &str,
    coordinator_state: &str,
) -> Result<WorkflowCursor, WorkflowError> {
    let checkpoint_inspect = inspect.as_checkpoint_inspect().ok_or_else(|| {
        WorkflowError("workflow inspection requires checkpoint inspection support".into())
    })?;
    let Some(checkpoint) = checkpoint_inspect
        .get_job_checkpoint(coordinator_job_id)
        .await
        .map_err(|error| WorkflowError(error.to_string()))?
    else {
        return Ok(WorkflowCursor::default());
    };
    if checkpoint
        .cursor_step
        .as_deref()
        .is_some_and(|step| step != "headgate:workflow-state")
    {
        return Err(WorkflowError(
            "workflow coordinator has no workflow-state checkpoint".into(),
        ));
    }
    let bytes = if let Some(cursor) = checkpoint.cursor {
        Some(cursor)
    } else if let Some(outputs) = inspect.as_output_inspect() {
        outputs
            .get_job_output(coordinator_job_id)
            .await
            .map_err(|error| WorkflowError(error.to_string()))?
            .map(|output| output.bytes)
    } else {
        None
    };
    if bytes.is_none()
        && matches!(
            coordinator_state,
            "completed" | "archived" | "cancelled" | "quarantined" | "undecodable"
        )
    {
        return Err(WorkflowError(
            "terminal workflow has no durable coordinator output".into(),
        ));
    }
    bytes.map_or_else(
        || Ok(WorkflowCursor::default()),
        |bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|error| WorkflowError(format!("invalid workflow cursor: {error}")))
        },
    )
}

/// Read the bounded durable event history kept in the fenced coordinator checkpoint.
pub async fn workflow_events(
    inspect: &dyn Inspect,
    workflow_id: &str,
) -> Result<Vec<WorkflowEvent>, WorkflowError> {
    let checkpoint_inspect = inspect.as_checkpoint_inspect().ok_or_else(|| {
        WorkflowError("workflow history requires checkpoint inspection support".into())
    })?;
    let checkpoint = checkpoint_inspect
        .get_job_checkpoint(&format!("{workflow_id}:coordinator"))
        .await
        .map_err(|error| WorkflowError(error.to_string()))?
        .ok_or_else(|| WorkflowError(format!("workflow `{workflow_id}` was not found")))?;
    if checkpoint
        .cursor_step
        .as_deref()
        .is_some_and(|step| step != "headgate:workflow-state")
    {
        return Err(WorkflowError(
            "workflow coordinator has no workflow-state checkpoint".into(),
        ));
    }
    let bytes = if let Some(cursor) = checkpoint.cursor {
        cursor
    } else {
        inspect
            .as_output_inspect()
            .ok_or_else(|| {
                WorkflowError("workflow history requires output inspection support".into())
            })?
            .get_job_output(&format!("{workflow_id}:coordinator"))
            .await
            .map_err(|error| WorkflowError(error.to_string()))?
            .ok_or_else(|| WorkflowError("workflow has no durable history".into()))?
            .bytes
    };
    let cursor = serde_json::from_slice::<WorkflowCursor>(&bytes)
        .map_err(|error| WorkflowError(format!("invalid workflow cursor: {error}")))?;
    Ok(cursor.events)
}

/// Request retry of only the failed and dependency-blocked portion of a retry-enabled
/// workflow. The receipt enqueue happens before the archived coordinator is reopened,
/// so interruption can be retried without losing the request.
pub async fn request_failed_subgraph_retry(
    inspect: &dyn Inspect,
    workflow_id: &str,
    expected_revision: u64,
) -> Result<RetryReceipt, WorkflowError> {
    request_failed_subgraph_retry_with_recovery(inspect, workflow_id, expected_revision, &[]).await
}

pub async fn request_failed_subgraph_retry_with_recovery(
    inspect: &dyn Inspect,
    workflow_id: &str,
    expected_revision: u64,
    recoveries: &[WorkflowRecovery],
) -> Result<RetryReceipt, WorkflowError> {
    if workflow_id.is_empty() || expected_revision == 0 {
        return Err(WorkflowError(
            "workflow id and expected revision must be set".into(),
        ));
    }
    let coordinator_id = format!("{workflow_id}:coordinator");
    let coordinator = inspect
        .get_job(&coordinator_id, true)
        .await
        .map_err(|error| WorkflowError(error.to_string()))?
        .ok_or_else(|| WorkflowError(format!("workflow `{workflow_id}` was not found")))?;
    let payload = coordinator
        .payload
        .ok_or_else(|| WorkflowError("workflow coordinator payload was not returned".into()))?;
    let task = CoordinatorTask::decode(&payload)
        .map_err(|error| WorkflowError(format!("invalid workflow coordinator: {error}")))?;
    if !task.failed_subgraph_retry {
        return Err(WorkflowError(
            "workflow was not created with failed-subgraph retry enabled".into(),
        ));
    }
    if coordinator.state != "archived" {
        return Err(WorkflowError(format!(
            "workflow retry requires an archived coordinator, found `{}`",
            coordinator.state
        )));
    }
    let nodes: HashMap<&str, &NodeSpec> = task
        .nodes
        .iter()
        .map(|node| (node.name.as_str(), node))
        .collect();
    let mut recovered = HashSet::new();
    for recovery in recoveries {
        if !recovered.insert(recovery.node.as_str()) {
            return Err(WorkflowError(format!(
                "workflow recovery repeats node `{}`",
                recovery.node
            )));
        }
        let node = nodes.get(recovery.node.as_str()).ok_or_else(|| {
            WorkflowError(format!(
                "workflow recovery names unknown node `{}`",
                recovery.node
            ))
        })?;
        let job = inspect
            .get_job(&node.job_id, true)
            .await
            .map_err(|error| WorkflowError(error.to_string()))?
            .ok_or_else(|| WorkflowError(format!("workflow node `{}` is missing", node.job_id)))?;
        match job.state.as_str() {
            "quarantined" if recovery.release_quarantine => {
                inspect
                    .quarantine_release(&job.fingerprint)
                    .await
                    .map_err(|error| WorkflowError(error.to_string()))?;
            }
            "quarantined" => {
                return Err(WorkflowError(format!(
                    "workflow node `{}` requires explicit quarantine release",
                    recovery.node
                )));
            }
            "undecodable" => {
                let replacement = recovery.payload.as_deref().ok_or_else(|| {
                    WorkflowError(format!(
                        "undecodable workflow node `{}` requires replacement payload",
                        recovery.node
                    ))
                })?;
                let version = recovery.schema_version.ok_or_else(|| {
                    WorkflowError(format!(
                        "undecodable workflow node `{}` requires schema_version",
                        recovery.node
                    ))
                })?;
                inspect
                    .edit_payload(
                        &node.job_id,
                        replacement,
                        version,
                        &headgate::fingerprint(&job.kind, replacement),
                    )
                    .await
                    .map_err(|error| WorkflowError(error.to_string()))?;
                inspect
                    .operator_retry(&node.job_id)
                    .await
                    .map_err(|error| WorkflowError(error.to_string()))?;
            }
            "archived" | "cancelled" => {}
            // A retry request may be replayed after its recovery mutation completed but
            // before the coordinator was reopened. Treat that boundary as idempotent.
            "available" => {}
            state => {
                return Err(WorkflowError(format!(
                    "workflow node `{}` does not require recovery from `{state}`",
                    recovery.node
                )));
            }
        }
    }
    for node in &task.nodes {
        let job = inspect
            .get_job(&node.job_id, false)
            .await
            .map_err(|error| WorkflowError(error.to_string()))?;
        if let Some(job) = job
            && matches!(job.state.as_str(), "quarantined" | "undecodable")
        {
            return Err(WorkflowError(format!(
                "workflow node `{}` requires recovery from `{}`",
                node.name, job.state
            )));
        }
    }
    let checkpoint_inspect = inspect.as_checkpoint_inspect().ok_or_else(|| {
        WorkflowError("workflow retry requires checkpoint inspection support".into())
    })?;
    let checkpoint = checkpoint_inspect
        .get_job_checkpoint(&coordinator_id)
        .await
        .map_err(|error| WorkflowError(error.to_string()))?
        .ok_or_else(|| WorkflowError("workflow coordinator checkpoint is missing".into()))?;
    if checkpoint.cursor_step.as_deref() != Some("headgate:workflow-state") {
        return Err(WorkflowError(
            "workflow coordinator has no workflow-state checkpoint".into(),
        ));
    }
    let bytes = checkpoint
        .cursor
        .ok_or_else(|| WorkflowError("workflow coordinator cursor is missing".into()))?;
    let cursor: WorkflowCursor = serde_json::from_slice(&bytes)
        .map_err(|error| WorkflowError(format!("invalid workflow cursor: {error}")))?;
    if !cursor.failed || cursor.revision != expected_revision {
        return Err(WorkflowError(format!(
            "workflow retry revision conflict: expected {expected_revision}, current {}",
            cursor.revision
        )));
    }
    let next_revision = expected_revision
        .checked_add(1)
        .ok_or_else(|| WorkflowError("workflow retry revision would overflow".into()))?;
    let generation = cursor
        .generation
        .checked_add(1)
        .ok_or_else(|| WorkflowError("workflow generation would overflow".into()))?;
    let retry = RetryTask {
        workflow_id: workflow_id.into(),
        expected_revision,
    };
    let payload = retry
        .encode()
        .map_err(|error| WorkflowError(error.to_string()))?;
    let receipt = headgate::prepare_envelope(Envelope {
        id: retry_receipt_id(workflow_id, next_revision),
        kind: RetryTask::TYPE.into(),
        schema_version: RetryTask::VERSION,
        fingerprint: headgate::fingerprint(RetryTask::TYPE, &payload),
        payload,
        queue: coordinator.queue,
        pending: true,
        retention_ms: DEFAULT_RETENTION_MS,
        ..Default::default()
    })
    .map_err(|error| WorkflowError(error.to_string()))?;
    inspect
        .enqueue(&[receipt])
        .await
        .map_err(|error| WorkflowError(error.to_string()))?;
    if let Err(error) = inspect.operator_retry(&coordinator_id).await {
        let current = inspect
            .get_job(&coordinator_id, false)
            .await
            .map_err(|read_error| WorkflowError(read_error.to_string()))?;
        if !current
            .as_ref()
            .is_some_and(|job| matches!(job.state.as_str(), "available" | "running"))
        {
            return Err(WorkflowError(error.to_string()));
        }
    }
    Ok(RetryReceipt {
        revision: next_revision,
        generation,
    })
}

/// Durably emit a named signal for an existing workflow. Repeating an emission after
/// its signal jobs become available, running, or completed is an idempotent success.
pub async fn emit_signal(
    inspect: &dyn Inspect,
    workflow_id: &str,
    signal: &str,
) -> Result<SignalReceipt, WorkflowError> {
    emit_signal_with(
        inspect,
        workflow_id,
        SignalEmission {
            signal: signal.into(),
            idempotency_key: format!("legacy:{signal}"),
            payload: serde_json::Value::Null,
            source: serde_json::json!({}),
        },
    )
    .await
}

/// Record payload and emitter metadata before releasing matching signal nodes. Replays
/// return the original record and may safely retry promotion.
pub async fn emit_signal_with(
    inspect: &dyn Inspect,
    workflow_id: &str,
    emission: SignalEmission,
) -> Result<SignalReceipt, WorkflowError> {
    let signal = emission.signal.as_str();
    if workflow_id.is_empty() || signal.is_empty() {
        return Err(WorkflowError(
            "workflow id and signal must not be empty".into(),
        ));
    }
    let coordinator_id = format!("{workflow_id}:coordinator");
    let coordinator = inspect
        .get_job(&coordinator_id, true)
        .await
        .map_err(|error| WorkflowError(error.to_string()))?
        .ok_or_else(|| WorkflowError(format!("workflow `{workflow_id}` was not found")))?;
    let payload = coordinator
        .payload
        .ok_or_else(|| WorkflowError("workflow coordinator payload was not returned".into()))?;
    let task = CoordinatorTask::decode(&payload)
        .map_err(|error| WorkflowError(format!("invalid workflow coordinator: {error}")))?;
    let jobs: Vec<&str> = task
        .nodes
        .iter()
        .filter(|node| node.kind == NodeType::Signal && node.signal.as_deref() == Some(signal))
        .map(|node| node.job_id.as_str())
        .collect();
    if jobs.is_empty() {
        return Err(WorkflowError(format!(
            "workflow `{workflow_id}` has no signal `{signal}`"
        )));
    }
    if emission.idempotency_key.is_empty() {
        return Err(WorkflowError(
            "signal idempotency key must not be empty".into(),
        ));
    }
    let payload =
        serde_json::to_vec(&emission.payload).map_err(|e| WorkflowError(e.to_string()))?;
    let source = serde_json::to_vec(&emission.source).map_err(|e| WorkflowError(e.to_string()))?;
    if payload.len() > MAX_SIGNAL_PAYLOAD_BYTES {
        return Err(WorkflowError(
            "signal payload must be at most 65536 bytes".into(),
        ));
    }
    if source.len() > MAX_SIGNAL_SOURCE_BYTES {
        return Err(WorkflowError(
            "signal source must be at most 16384 bytes".into(),
        ));
    }
    let (stored, inserted) = inspect
        .append_durable_event(&DurableEvent {
            event_id: 0,
            scope: workflow_signal_scope(workflow_id),
            topic: signal.into(),
            idempotency_key: emission.idempotency_key,
            payload,
            source,
            recorded_at_ms: 0,
        })
        .await
        .map_err(|e| WorkflowError(e.to_string()))?;
    let mut promoted = 0;
    for job_id in &jobs {
        let job = inspect
            .get_job(job_id, false)
            .await
            .map_err(|error| WorkflowError(error.to_string()))?
            .ok_or_else(|| WorkflowError(format!("signal job `{job_id}` was not found")))?;
        match job.state.as_str() {
            "pending" => match inspect.promote_job(job_id).await {
                Ok(()) => promoted += 1,
                Err(error) => {
                    let current = inspect
                        .get_job(job_id, false)
                        .await
                        .map_err(|read_error| WorkflowError(read_error.to_string()))?;
                    if !current.as_ref().is_some_and(|job| {
                        matches!(job.state.as_str(), "available" | "running" | "completed")
                    }) {
                        return Err(WorkflowError(error.to_string()));
                    }
                }
            },
            "available" | "running" | "completed" => {}
            state => {
                return Err(WorkflowError(format!(
                    "signal job `{job_id}` cannot be emitted from state `{state}`"
                )));
            }
        }
    }
    Ok(SignalReceipt {
        matched: jobs.len(),
        promoted,
        inserted,
        emission: workflow_signal(stored)?,
    })
}

pub async fn list_signals(
    inspect: &dyn Inspect,
    workflow_id: &str,
    before_id: Option<u64>,
    limit: u32,
) -> Result<Vec<WorkflowSignal>, WorkflowError> {
    if workflow_id.is_empty() {
        return Err(WorkflowError("workflow id must not be empty".into()));
    }
    inspect
        .list_durable_events(&workflow_signal_scope(workflow_id), before_id, limit)
        .await
        .map_err(|e| WorkflowError(e.to_string()))?
        .into_iter()
        .map(workflow_signal)
        .collect()
}

fn workflow_signal_scope(workflow_id: &str) -> String {
    format!("workflow:{workflow_id}:signals")
}

fn workflow_signal(event: DurableEvent) -> Result<WorkflowSignal, WorkflowError> {
    Ok(WorkflowSignal {
        id: event.event_id,
        signal: event.topic,
        idempotency_key: event.idempotency_key,
        payload: serde_json::from_slice(&event.payload)
            .map_err(|e| WorkflowError(format!("invalid signal payload: {e}")))?,
        source: serde_json::from_slice(&event.source)
            .map_err(|e| WorkflowError(format!("invalid signal source: {e}")))?,
        recorded_at_ms: event.recorded_at_ms,
    })
}

#[derive(Serialize, Deserialize)]
struct WorkflowCursor {
    #[serde(default = "initial_workflow_revision")]
    revision: u64,
    #[serde(default)]
    completed: Vec<String>,
    #[serde(default)]
    completed_at_ms: HashMap<String, i64>,
    #[serde(default)]
    grafts: Vec<NodeSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_graft_receipt: Option<String>,
    #[serde(default = "initial_workflow_generation")]
    generation: u32,
    #[serde(default)]
    failed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_retry_receipt: Option<String>,
    #[serde(default)]
    automatic_retry_pending: bool,
    #[serde(default)]
    events: Vec<WorkflowEvent>,
}

impl Default for WorkflowCursor {
    fn default() -> Self {
        Self {
            revision: initial_workflow_revision(),
            completed: Vec::new(),
            completed_at_ms: HashMap::new(),
            grafts: Vec::new(),
            pending_graft_receipt: None,
            generation: initial_workflow_generation(),
            failed: false,
            pending_retry_receipt: None,
            automatic_retry_pending: false,
            events: Vec::new(),
        }
    }
}

const fn initial_workflow_revision() -> u64 {
    1
}

const fn initial_workflow_generation() -> u32 {
    1
}

fn graft_receipt_id(workflow_id: &str, revision: u64) -> String {
    format!("{workflow_id}:graft:{revision}")
}

fn retry_receipt_id(workflow_id: &str, revision: u64) -> String {
    format!("{workflow_id}:retry:{revision}")
}

fn record_event(
    cursor: &mut WorkflowCursor,
    event: &str,
    node: Option<String>,
    at_ms: Option<i64>,
) -> Result<(), WorkflowError> {
    let sequence = cursor
        .events
        .last()
        .map_or(1, |entry| entry.sequence.saturating_add(1));
    if sequence == u64::MAX && cursor.events.last().is_some() {
        return Err(WorkflowError("workflow event sequence overflow".into()));
    }
    cursor.events.push(WorkflowEvent {
        sequence,
        event: event.into(),
        node,
        revision: cursor.revision,
        generation: cursor.generation,
        at_ms,
    });
    if cursor.events.len() > MAX_WORKFLOW_EVENTS {
        let excess = cursor.events.len() - MAX_WORKFLOW_EVENTS;
        cursor.events.drain(..excess);
    }
    Ok(())
}

impl Task for CoordinatorTask {
    const TYPE: &'static str = "headgate:workflow";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|e| CodecError::Malformed(e.to_string()))
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes).map_err(|e| CodecError::Malformed(e.to_string()))
    }
}

/// Install the durable coordinator handler. Every tick uses bounded point reads; the
/// graph size is bounded by the coordinator payload and never scans queue depth.
pub fn register_coordinator(
    registry: &mut Registry,
    inspect: Arc<dyn Inspect>,
    poll_interval: Duration,
) -> Result<(), String> {
    if poll_interval.as_millis() == 0 {
        return Err("workflow poll interval must be at least 1ms".into());
    }
    register_virtual_handlers(registry)?;
    let child_inspect = inspect.clone();
    registry.register::<ChildWorkflowTask, _, _>(
        move |_ctx: JobCtx, task: ChildWorkflowTask| {
            let inspect = child_inspect.clone();
            async move {
                if task.child_workflow_id.is_empty()
                    || task.child_workflow_id == task.parent_workflow_id
                {
                    return Err::<(), JobError>(Box::new(WorkflowError(
                        "invalid child workflow link".into(),
                    )));
                }
                let child_id = format!("{}:coordinator", task.child_workflow_id);
                let child = inspect.get_job(&child_id, false).await?.ok_or_else(|| {
                    WorkflowError(format!("child workflow `{child_id}` was not found"))
                })?;
                match child.state.as_str() {
                    "completed" => Ok(()),
                    "archived" | "cancelled" | "quarantined" | "undecodable" => {
                        Err(Control::Skip.into())
                    }
                    _ => Err(Control::Snooze(poll_interval).into()),
                }
            }
        },
    )?;
    registry.register::<CoordinatorTask, _, _>(move |ctx: JobCtx, task: CoordinatorTask| {
        let inspect = inspect.clone();
        async move {
            let cursor_ctx = ctx.clone();
            ctx.step_cursor("headgate:workflow-state", move |cursor| async move {
                let mut cursor = cursor
                    .map(|bytes| serde_json::from_slice::<WorkflowCursor>(&bytes))
                    .transpose()
                    .map_err(|error| -> JobError { Box::new(error) })?
                    .unwrap_or_default();
                if cursor.events.is_empty() {
                    record_event(&mut cursor, "workflow_started", None, None)?;
                    persist_workflow_cursor(&cursor_ctx, &cursor).await?;
                }
                if cursor.automatic_retry_pending {
                    enqueue_automatic_retry(
                        inspect.as_ref(),
                        &task,
                        &mut cursor,
                        &cursor_ctx,
                        cursor_ctx.queue(),
                    )
                    .await?;
                }
                if let Some(result) =
                    reconcile_retry(inspect.as_ref(), &task, &mut cursor, &cursor_ctx).await?
                {
                    return match result {
                        Tick::Waiting => Err(Control::Snooze(poll_interval).into()),
                        Tick::Succeeded => Ok(()),
                        Tick::Failed => Err(Control::Skip.into()),
                    };
                }
                if let Some(result) =
                    reconcile_graft(inspect.as_ref(), &task, &mut cursor, &cursor_ctx).await?
                {
                    return match result {
                        Tick::Waiting => Err(Control::Snooze(poll_interval).into()),
                        Tick::Succeeded => Ok(()),
                        Tick::Failed => Err(Control::Skip.into()),
                    };
                }
                let effective = effective_workflow(&task, &cursor);
                match tick_with_evidence(
                    inspect.as_ref(),
                    &effective,
                    &mut cursor,
                    Some(&cursor_ctx),
                )
                .await?
                {
                    Tick::Waiting => Err(Control::Snooze(poll_interval).into()),
                    Tick::Succeeded => {
                        record_event(&mut cursor, "workflow_succeeded", None, None)?;
                        persist_workflow_cursor(&cursor_ctx, &cursor).await?;
                        Ok(())
                    }
                    Tick::Failed => {
                        if task.failed_subgraph_retry {
                            cursor.failed = true;
                            if task
                                .retry_policy
                                .is_some_and(|policy| cursor.generation < policy.max_generations)
                            {
                                cursor.automatic_retry_pending = true;
                                record_event(&mut cursor, "automatic_retry_scheduled", None, None)?;
                            } else {
                                record_event(&mut cursor, "workflow_failed", None, None)?;
                            }
                            persist_workflow_cursor(&cursor_ctx, &cursor).await?;
                        } else {
                            record_event(&mut cursor, "workflow_failed", None, None)?;
                            persist_workflow_cursor(&cursor_ctx, &cursor).await?;
                        }
                        if cursor.automatic_retry_pending {
                            let policy = task.retry_policy.expect("automatic retry policy");
                            return Err(Control::Snooze(Duration::from_millis(
                                u64::try_from(policy.backoff_ms)
                                    .map_err(|_| "invalid workflow retry backoff")?,
                            ))
                            .into());
                        }
                        Err(Control::Skip.into())
                    }
                }
            })
            .await
        }
    })
}

fn register_virtual_handlers(registry: &mut Registry) -> Result<(), String> {
    registry.register::<SignalTask, _, _>(|_ctx: JobCtx, _task: SignalTask| async move {
        Ok::<(), JobError>(())
    })?;
    registry.register::<TimerTask, _, _>(|_ctx: JobCtx, _task: TimerTask| async move {
        Ok::<(), JobError>(())
    })?;
    registry.register::<ConditionTask, _, _>(|_ctx: JobCtx, _task: ConditionTask| async move {
        Ok::<(), JobError>(())
    })?;
    registry.register::<GraftTask, _, _>(|_ctx: JobCtx, _task: GraftTask| async move {
        Ok::<(), JobError>(())
    })?;
    registry.register::<RetryTask, _, _>(|_ctx: JobCtx, _task: RetryTask| async move {
        Ok::<(), JobError>(())
    })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Tick {
    Waiting,
    Succeeded,
    Failed,
}

fn effective_workflow(base: &CoordinatorTask, cursor: &WorkflowCursor) -> CoordinatorTask {
    let mut nodes = Vec::with_capacity(base.nodes.len() + cursor.grafts.len());
    nodes.extend(base.nodes.iter().cloned());
    nodes.extend(cursor.grafts.iter().cloned());
    CoordinatorTask {
        workflow_id: base.workflow_id.clone(),
        nodes,
        failed_subgraph_retry: base.failed_subgraph_retry,
        retry_policy: base.retry_policy,
    }
}

async fn enqueue_automatic_retry(
    inspect: &dyn Inspect,
    base: &CoordinatorTask,
    cursor: &mut WorkflowCursor,
    ctx: &JobCtx,
    queue: &str,
) -> Result<(), JobError> {
    if !cursor.failed {
        return Err(Box::new(WorkflowError(
            "automatic retry is pending for a non-failed workflow".into(),
        )));
    }
    let next_revision = cursor
        .revision
        .checked_add(1)
        .ok_or_else(|| WorkflowError("workflow retry revision would overflow".into()))?;
    let retry = RetryTask {
        workflow_id: base.workflow_id.clone(),
        expected_revision: cursor.revision,
    };
    let payload = retry.encode()?;
    let receipt = headgate::prepare_envelope(Envelope {
        id: retry_receipt_id(&base.workflow_id, next_revision),
        kind: RetryTask::TYPE.into(),
        schema_version: RetryTask::VERSION,
        fingerprint: headgate::fingerprint(RetryTask::TYPE, &payload),
        payload,
        queue: queue.into(),
        pending: true,
        retention_ms: DEFAULT_RETENTION_MS,
        ..Default::default()
    })?;
    inspect.enqueue(&[receipt]).await?;
    cursor.automatic_retry_pending = false;
    persist_workflow_cursor(ctx, cursor).await
}

async fn persist_workflow_cursor(ctx: &JobCtx, cursor: &WorkflowCursor) -> Result<(), JobError> {
    let bytes = serde_json::to_vec(cursor).map_err(|error| -> JobError { Box::new(error) })?;
    ctx.set_cursor(bytes.clone()).await?;
    ctx.persist_output(1, bytes).await?;
    Ok(())
}

async fn reject_graft(
    inspect: &dyn Inspect,
    receipt_id: &str,
    nodes: &[NodeSpec],
) -> Result<(), JobError> {
    for job_id in nodes
        .iter()
        .map(|node| node.job_id.as_str())
        .chain(std::iter::once(receipt_id))
    {
        let Some(job) = inspect.get_job(job_id, false).await? else {
            continue;
        };
        if matches!(
            job.state.as_str(),
            "pending" | "scheduled" | "available" | "retryable"
        ) {
            inspect.delete_job(job_id).await?;
        } else {
            return Err(Box::new(WorkflowError(format!(
                "rejected workflow graft job `{job_id}` is already `{}`",
                job.state
            ))));
        }
    }
    Ok(())
}

async fn failed_nodes_to_retry(
    inspect: &dyn Inspect,
    workflow: &CoordinatorTask,
) -> Result<Vec<String>, JobError> {
    let mut retry = Vec::new();
    for node in &workflow.nodes {
        let job = inspect.get_job(&node.job_id, false).await?.ok_or_else(|| {
            WorkflowError(format!(
                "retry-enabled workflow node `{}` is missing",
                node.job_id
            ))
        })?;
        match job.state.as_str() {
            "archived" | "cancelled" => retry.push(node.job_id.clone()),
            "pending" | "scheduled" | "retryable" | "available" | "running" | "completed" => {}
            state => {
                return Err(Box::new(WorkflowError(format!(
                    "workflow node `{}` cannot be retried from `{state}`",
                    node.job_id
                ))));
            }
        }
    }
    Ok(retry)
}

async fn retry_failed_children(
    inspect: &dyn Inspect,
    workflow: &CoordinatorTask,
) -> Result<(), JobError> {
    let checkpoint_inspect = inspect.as_checkpoint_inspect().ok_or_else(|| {
        WorkflowError("child retry propagation requires checkpoint inspection support".into())
    })?;
    for node in workflow
        .nodes
        .iter()
        .filter(|node| node.kind == NodeType::ChildWorkflow)
    {
        let Some(child_workflow_id) = node.child_workflow_id.as_deref() else {
            continue;
        };
        let Some(link) = inspect.get_job(&node.job_id, false).await? else {
            continue;
        };
        if !matches!(link.state.as_str(), "archived" | "cancelled") {
            continue;
        }
        let child_id = format!("{child_workflow_id}:coordinator");
        let Some(child) = inspect.get_job(&child_id, false).await? else {
            return Err(Box::new(WorkflowError(format!(
                "child workflow `{child_workflow_id}` is missing"
            ))));
        };
        if child.state != "archived" {
            continue;
        }
        let checkpoint = checkpoint_inspect
            .get_job_checkpoint(&child_id)
            .await?
            .ok_or_else(|| {
                WorkflowError(format!(
                    "child workflow `{child_workflow_id}` has no checkpoint"
                ))
            })?;
        let cursor: WorkflowCursor = serde_json::from_slice(
            checkpoint
                .cursor
                .as_deref()
                .ok_or_else(|| WorkflowError("child workflow cursor is missing".into()))?,
        )?;
        request_failed_subgraph_retry(inspect, child_workflow_id, cursor.revision).await?;
    }
    Ok(())
}

async fn reopen_failed_nodes(inspect: &dyn Inspect, jobs: &[String]) -> Result<(), JobError> {
    for job_id in jobs {
        inspect.operator_retry(job_id).await?;
    }
    Ok(())
}

async fn reconcile_retry(
    inspect: &dyn Inspect,
    base: &CoordinatorTask,
    cursor: &mut WorkflowCursor,
    ctx: &JobCtx,
) -> Result<Option<Tick>, JobError> {
    if let Some(receipt_id) = cursor.pending_retry_receipt.clone() {
        let receipt = inspect.get_job(&receipt_id, false).await?.ok_or_else(|| {
            WorkflowError(format!(
                "accepted workflow retry receipt `{receipt_id}` is missing"
            ))
        })?;
        let workflow = effective_workflow(base, cursor);
        match receipt.state.as_str() {
            "pending" => {
                let jobs = failed_nodes_to_retry(inspect, &workflow).await?;
                reopen_failed_nodes(inspect, &jobs).await?;
                inspect.promote_job(&receipt_id).await?;
                return Ok(Some(Tick::Waiting));
            }
            "available" | "running" => return Ok(Some(Tick::Waiting)),
            "completed" => {
                cursor.pending_retry_receipt = None;
                persist_workflow_cursor(ctx, cursor).await?;
            }
            state => {
                return Err(Box::new(WorkflowError(format!(
                    "accepted workflow retry receipt `{receipt_id}` entered `{state}`"
                ))));
            }
        }
    }

    let next_revision = cursor
        .revision
        .checked_add(1)
        .ok_or_else(|| WorkflowError("workflow revision would overflow".into()))?;
    let receipt_id = retry_receipt_id(&base.workflow_id, next_revision);
    let Some(receipt) = inspect.get_job(&receipt_id, true).await? else {
        return Ok(None);
    };
    if receipt.state != "pending" {
        return Err(Box::new(WorkflowError(format!(
            "unaccepted workflow retry receipt `{receipt_id}` entered `{}`",
            receipt.state
        ))));
    }
    let payload = receipt.payload.ok_or_else(|| {
        WorkflowError(format!(
            "workflow retry receipt `{receipt_id}` did not return its payload"
        ))
    })?;
    let retry = match RetryTask::decode(&payload) {
        Ok(retry) => retry,
        Err(_) => {
            reject_graft(inspect, &receipt_id, &[]).await?;
            return Ok(Some(Tick::Failed));
        }
    };
    if !base.failed_subgraph_retry
        || !cursor.failed
        || retry.workflow_id != base.workflow_id
        || retry.expected_revision != cursor.revision
    {
        reject_graft(inspect, &receipt_id, &[]).await?;
        return Ok(Some(if cursor.failed {
            Tick::Failed
        } else {
            Tick::Waiting
        }));
    }
    let competing_graft_id = graft_receipt_id(&base.workflow_id, next_revision);
    if let Some(competing) = inspect.get_job(&competing_graft_id, true).await? {
        if competing.state != "pending" {
            return Err(Box::new(WorkflowError(format!(
                "competing workflow graft receipt `{competing_graft_id}` entered `{}`",
                competing.state
            ))));
        }
        let nodes = competing
            .payload
            .as_deref()
            .and_then(|payload| GraftTask::decode(payload).ok())
            .map(|graft| graft.nodes)
            .unwrap_or_default();
        reject_graft(inspect, &competing_graft_id, &nodes).await?;
    }
    let workflow = effective_workflow(base, cursor);
    retry_failed_children(inspect, &workflow).await?;
    let jobs = match failed_nodes_to_retry(inspect, &workflow).await {
        Ok(jobs) => jobs,
        Err(_) => {
            reject_graft(inspect, &receipt_id, &[]).await?;
            return Ok(Some(Tick::Failed));
        }
    };
    cursor.generation = cursor
        .generation
        .checked_add(1)
        .ok_or_else(|| WorkflowError("workflow generation would overflow".into()))?;
    cursor.revision = next_revision;
    cursor.failed = false;
    record_event(cursor, "workflow_retry_accepted", None, None)?;
    cursor.pending_retry_receipt = Some(receipt_id.clone());
    persist_workflow_cursor(ctx, cursor).await?;
    reopen_failed_nodes(inspect, &jobs).await?;
    inspect.promote_job(&receipt_id).await?;
    Ok(Some(Tick::Waiting))
}

/// Reconcile at most one revision per tick. The cursor is persisted before the receipt
/// is promoted, so a crash can only replay an accepted revision, never lose it.
async fn reconcile_graft(
    inspect: &dyn Inspect,
    base: &CoordinatorTask,
    cursor: &mut WorkflowCursor,
    ctx: &JobCtx,
) -> Result<Option<Tick>, JobError> {
    if cursor.revision == 0 {
        return Err(Box::new(WorkflowError(
            "workflow cursor contains revision zero".into(),
        )));
    }
    if let Some(receipt_id) = cursor.pending_graft_receipt.clone() {
        let receipt = inspect.get_job(&receipt_id, false).await?.ok_or_else(|| {
            WorkflowError(format!(
                "accepted workflow graft receipt `{receipt_id}` is missing"
            ))
        })?;
        match receipt.state.as_str() {
            "pending" => {
                inspect.promote_job(&receipt_id).await?;
                return Ok(Some(Tick::Waiting));
            }
            "available" | "running" => return Ok(Some(Tick::Waiting)),
            "completed" => {
                cursor.pending_graft_receipt = None;
                persist_workflow_cursor(ctx, cursor).await?;
            }
            state => {
                return Err(Box::new(WorkflowError(format!(
                    "accepted workflow graft receipt `{receipt_id}` entered `{state}`"
                ))));
            }
        }
    }

    let next_revision = cursor
        .revision
        .checked_add(1)
        .ok_or_else(|| WorkflowError("workflow revision would overflow".into()))?;
    let receipt_id = graft_receipt_id(&base.workflow_id, next_revision);
    let Some(receipt) = inspect.get_job(&receipt_id, true).await? else {
        return Ok(None);
    };
    if receipt.state != "pending" {
        return Err(Box::new(WorkflowError(format!(
            "unaccepted workflow graft receipt `{receipt_id}` entered `{}`",
            receipt.state
        ))));
    }
    let payload = receipt.payload.ok_or_else(|| {
        WorkflowError(format!(
            "workflow graft receipt `{receipt_id}` did not return its payload"
        ))
    })?;
    let graft = match GraftTask::decode(&payload) {
        Ok(graft) => graft,
        Err(_) => {
            reject_graft(inspect, &receipt_id, &[]).await?;
            return Ok(Some(Tick::Waiting));
        }
    };
    if graft.workflow_id != base.workflow_id
        || graft.expected_revision != cursor.revision
        || graft.nodes.is_empty()
        || cursor.failed
    {
        reject_graft(inspect, &receipt_id, &graft.nodes).await?;
        return Ok(Some(Tick::Waiting));
    }
    let mut candidate = effective_workflow(base, cursor);
    candidate.nodes.extend(graft.nodes.iter().cloned());
    if validate_coordinator(&candidate).is_err() {
        reject_graft(inspect, &receipt_id, &graft.nodes).await?;
        return Ok(Some(Tick::Waiting));
    }

    cursor.revision = next_revision;
    cursor.grafts.extend(graft.nodes);
    record_event(cursor, "workflow_graft_accepted", None, None)?;
    cursor.pending_graft_receipt = Some(receipt_id.clone());
    persist_workflow_cursor(ctx, cursor).await?;
    inspect.promote_job(&receipt_id).await?;
    Ok(Some(Tick::Waiting))
}

async fn tick_with_evidence(
    inspect: &dyn Inspect,
    workflow: &CoordinatorTask,
    cursor: &mut WorkflowCursor,
    persist_ctx: Option<&JobCtx>,
) -> Result<Tick, JobError> {
    validate_coordinator(workflow).map_err(|error| -> JobError { Box::new(error) })?;
    let mut completed = completed_set(workflow, &cursor.completed);
    let reads: Vec<(String, String)> = workflow
        .nodes
        .iter()
        .map(|node| (node.name.clone(), node.job_id.clone()))
        .collect();
    let entries: Vec<(String, Option<headgate_core::JobSummary>)> = stream::iter(reads)
        .map(|(name, job_id)| async move {
            inspect.get_job(&job_id, false).await.map(|job| (name, job))
        })
        .buffer_unordered(WORKFLOW_CONCURRENCY)
        .try_collect()
        .await?;
    let state: HashMap<String, Option<headgate_core::JobSummary>> = entries.into_iter().collect();
    let before = completed.clone();
    let changed = record_completion_evidence(
        workflow,
        &state,
        &mut completed,
        &mut cursor.completed_at_ms,
    );
    for node in &workflow.nodes {
        if !before.contains(node.name.as_str()) && completed.contains(node.name.as_str()) {
            record_event(
                cursor,
                "node_completed",
                Some(node.name.clone()),
                cursor.completed_at_ms.get(&node.name).copied(),
            )?;
        }
    }
    if changed && let Some(ctx) = persist_ctx {
        cursor.completed = completed_names(workflow, &completed);
        persist_workflow_cursor(ctx, cursor).await?;
    }
    let failed_nodes = failed_set(workflow, &state, &completed);
    let mut mutations = Vec::new();
    for node in &workflow.nodes {
        let current = effective_state(
            node,
            state.get(node.name.as_str()).and_then(Option::as_ref),
            &completed,
        );
        let dep_failed = failed_nodes.contains(node.name.as_str());
        if dep_failed {
            if !workflow.failed_subgraph_retry
                && matches!(
                    current,
                    Some("pending" | "scheduled" | "available" | "retryable")
                )
            {
                mutations.push((node.job_id.clone(), true));
            }
            continue;
        }
        if matches!(node.kind, NodeType::Task | NodeType::ChildWorkflow)
            && current == Some("pending")
            && dependencies_complete(workflow, node, &state, &completed)
        {
            mutations.push((node.job_id.clone(), false));
        }
        if node.kind == NodeType::Condition
            && current == Some("pending")
            && dependencies_complete(workflow, node, &state, &completed)
            && evaluate_condition(node, cursor, workflow, &state, &completed)?
        {
            mutations.push((node.job_id.clone(), false));
        }
        if node.kind == NodeType::Timer
            && node.delay_ms.is_some()
            && current == Some("pending")
            && dependencies_complete(workflow, node, &state, &completed)
        {
            let anchor = dependency_completion_anchor(node, &cursor.completed_at_ms)?;
            let wake_at_ms = anchor
                .checked_add(node.delay_ms.unwrap_or_default())
                .ok_or_else(|| {
                    WorkflowError(format!("workflow timer `{}` deadline overflow", node.name))
                })?;
            inspect
                .schedule_pending_job(&node.job_id, wake_at_ms)
                .await?;
            return Ok(Tick::Waiting);
        }
    }
    if !mutations.is_empty() {
        stream::iter(mutations)
            .map(|(job_id, delete)| async move {
                if delete {
                    inspect.delete_job(&job_id).await
                } else {
                    inspect.promote_job(&job_id).await
                }
            })
            .buffer_unordered(WORKFLOW_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;
        return Ok(Tick::Waiting);
    }
    let mut failed = false;
    for node in &workflow.nodes {
        if failed_nodes.contains(node.name.as_str()) {
            failed = true;
            continue;
        }
        match effective_state(
            node,
            state.get(node.name.as_str()).and_then(Option::as_ref),
            &completed,
        ) {
            Some("completed") => {}
            None | Some("archived" | "cancelled" | "quarantined" | "undecodable") => failed = true,
            _ => return Ok(Tick::Waiting),
        }
    }
    Ok(if failed {
        Tick::Failed
    } else {
        Tick::Succeeded
    })
}

fn record_completion_evidence(
    workflow: &CoordinatorTask,
    state: &HashMap<String, Option<headgate_core::JobSummary>>,
    completed: &mut HashSet<String>,
    completed_at_ms: &mut HashMap<String, i64>,
) -> bool {
    let mut changed = false;
    for node in &workflow.nodes {
        if matches!(node.kind, NodeType::Task | NodeType::ChildWorkflow)
            && let Some(job) = state
                .get(node.name.as_str())
                .and_then(Option::as_ref)
                .filter(|job| job.state == "completed")
        {
            changed |= completed.insert(node.name.clone());
            if let Some(at_ms) = job.finalized_at_ms {
                changed |= completed_at_ms.insert(node.name.clone(), at_ms) != Some(at_ms);
            }
        }
    }
    loop {
        let eligible: Vec<String> = workflow
            .nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.kind,
                    NodeType::Signal | NodeType::Timer | NodeType::Condition
                )
            })
            .filter(|node| !completed.contains(node.name.as_str()))
            .filter(|node| node.deps.iter().all(|dep| completed.contains(dep)))
            .filter(|node| {
                state
                    .get(node.name.as_str())
                    .and_then(Option::as_ref)
                    .is_some_and(|job| job.state == "completed")
            })
            .map(|node| node.name.clone())
            .collect();
        if eligible.is_empty() {
            break;
        }
        changed = true;
        for name in &eligible {
            if let Some(at_ms) = state
                .get(name.as_str())
                .and_then(Option::as_ref)
                .and_then(|job| job.finalized_at_ms)
            {
                completed_at_ms.insert(name.clone(), at_ms);
            }
        }
        completed.extend(eligible);
    }
    changed
}

fn dependency_completion_anchor(
    node: &NodeSpec,
    completed_at_ms: &HashMap<String, i64>,
) -> Result<i64, JobError> {
    let mut anchor = None;
    for dependency in &node.deps {
        let completed_at = completed_at_ms.get(dependency).copied().ok_or_else(|| {
            Box::new(WorkflowError(format!(
                "workflow timer `{}` has no durable completion timestamp for `{dependency}`",
                node.name
            ))) as JobError
        })?;
        anchor = Some(anchor.map_or(completed_at, |current: i64| current.max(completed_at)));
    }
    anchor.ok_or_else(|| {
        Box::new(WorkflowError(format!(
            "relative workflow timer `{}` requires at least one dependency",
            node.name
        ))) as JobError
    })
}

fn evaluate_condition(
    node: &NodeSpec,
    cursor: &WorkflowCursor,
    workflow: &CoordinatorTask,
    state: &HashMap<String, Option<headgate_core::JobSummary>>,
    completed: &HashSet<String>,
) -> Result<bool, JobError> {
    let expression = node.condition.as_deref().ok_or_else(|| {
        Box::new(WorkflowError(format!(
            "workflow condition `{}` has no expression",
            node.name
        ))) as JobError
    })?;
    let program = cel::Program::compile(expression).map_err(|error| {
        Box::new(WorkflowError(format!(
            "invalid workflow CEL condition `{}`: {error}",
            node.name
        ))) as JobError
    })?;
    let states: HashMap<String, String> = workflow
        .nodes
        .iter()
        .map(|candidate| {
            let state = effective_state(
                candidate,
                state.get(candidate.name.as_str()).and_then(Option::as_ref),
                completed,
            )
            .unwrap_or("missing")
            .to_string();
            (candidate.name.clone(), state)
        })
        .collect();
    let completion: HashMap<String, bool> = workflow
        .nodes
        .iter()
        .map(|candidate| {
            (
                candidate.name.clone(),
                completed.contains(candidate.name.as_str()),
            )
        })
        .collect();
    let mut context = cel::Context::default();
    context.add_variable_from_value("revision", cursor.revision);
    context.add_variable_from_value("generation", u64::from(cursor.generation));
    context.add_variable_from_value("states", states);
    context.add_variable_from_value("completed", completion);
    match program.execute(&context).map_err(|error| {
        Box::new(WorkflowError(format!(
            "workflow CEL condition `{}` failed: {error}",
            node.name
        ))) as JobError
    })? {
        cel::Value::Bool(value) => Ok(value),
        _ => Err(Box::new(WorkflowError(format!(
            "workflow CEL condition `{}` must return bool",
            node.name
        )))),
    }
}

fn dependencies_complete(
    workflow: &CoordinatorTask,
    node: &NodeSpec,
    state: &HashMap<String, Option<headgate_core::JobSummary>>,
    completed: &HashSet<String>,
) -> bool {
    node.deps.iter().all(|dep| {
        let upstream = workflow
            .nodes
            .iter()
            .find(|node| node.name == *dep)
            .expect("validated dependency");
        effective_state(
            upstream,
            state.get(dep.as_str()).and_then(Option::as_ref),
            completed,
        ) == Some("completed")
    })
}

#[cfg(test)]
fn dependency_failed(
    workflow: &CoordinatorTask,
    node: &NodeSpec,
    state: &HashMap<String, Option<headgate_core::JobSummary>>,
    completed: &HashSet<String>,
) -> bool {
    node.deps
        .iter()
        .any(|dep| failed_set(workflow, state, completed).contains(dep))
}

fn failed_set(
    workflow: &CoordinatorTask,
    state: &HashMap<String, Option<headgate_core::JobSummary>>,
    completed: &HashSet<String>,
) -> HashSet<String> {
    let mut failed: HashSet<String> = workflow
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                effective_state(
                    node,
                    state.get(node.name.as_str()).and_then(Option::as_ref),
                    completed,
                ),
                None | Some("archived" | "cancelled" | "quarantined" | "undecodable")
            )
        })
        .map(|node| node.name.clone())
        .collect();
    loop {
        let before = failed.len();
        for node in &workflow.nodes {
            if node.deps.iter().any(|dep| failed.contains(dep)) {
                failed.insert(node.name.clone());
            }
        }
        if failed.len() == before {
            return failed;
        }
    }
}

fn completed_set(workflow: &CoordinatorTask, names: &[String]) -> HashSet<String> {
    let valid: HashSet<&str> = workflow
        .nodes
        .iter()
        .map(|node| node.name.as_str())
        .collect();
    names
        .iter()
        .filter(|name| valid.contains(name.as_str()))
        .cloned()
        .collect()
}

fn completed_names(workflow: &CoordinatorTask, completed: &HashSet<String>) -> Vec<String> {
    workflow
        .nodes
        .iter()
        .filter(|node| completed.contains(node.name.as_str()))
        .map(|node| node.name.clone())
        .collect()
}

fn effective_state<'a>(
    node: &NodeSpec,
    job: Option<&'a headgate_core::JobSummary>,
    completed: &HashSet<String>,
) -> Option<&'a str> {
    if completed.contains(node.name.as_str()) {
        return Some("completed");
    }
    match (node.kind, job.map(|job| job.state.as_str())) {
        // An early signal is durable evidence, but it is not consumed until all of the
        // signal node's dependencies have completed.
        (NodeType::Signal | NodeType::Timer | NodeType::Condition, Some("completed")) => {
            Some("pending")
        }
        (_, state) => state,
    }
}

fn validate_coordinator(workflow: &CoordinatorTask) -> Result<(), WorkflowError> {
    if workflow.workflow_id.is_empty() {
        return Err(WorkflowError(
            "workflow coordinator id must not be empty".into(),
        ));
    }
    if workflow.nodes.is_empty() || workflow.nodes.len() > MAX_WORKFLOW_NODES {
        return Err(WorkflowError(format!(
            "workflow coordinator must contain 1-{MAX_WORKFLOW_NODES} tasks"
        )));
    }
    if workflow.retry_policy.is_some_and(|policy| {
        policy.max_generations < 2 || policy.backoff_ms <= 0 || !workflow.failed_subgraph_retry
    }) {
        return Err(WorkflowError(
            "workflow coordinator contains an invalid retry policy".into(),
        ));
    }
    let mut names = HashSet::with_capacity(workflow.nodes.len());
    let mut edges = 0usize;
    for node in &workflow.nodes {
        if node.name.is_empty()
            || node.name.len() > 128
            || node.job_id.is_empty()
            || node.job_id.len() > MAX_JOB_IDENTIFIER_LEN
            || !names.insert(node.name.as_str())
            || (node.kind == NodeType::Signal && node.signal.as_deref().is_none_or(str::is_empty))
            || (node.kind == NodeType::Timer
                && match (node.wake_at_ms, node.delay_ms) {
                    (Some(at), None) => at <= 0,
                    (None, Some(delay)) => delay <= 0 || node.deps.is_empty(),
                    _ => true,
                })
            || (node.kind != NodeType::Signal && node.signal.is_some())
            || (node.kind != NodeType::Timer
                && (node.wake_at_ms.is_some() || node.delay_ms.is_some()))
            || (node.kind == NodeType::ChildWorkflow
                && node.child_workflow_id.as_deref().is_none_or(str::is_empty))
            || (node.kind != NodeType::ChildWorkflow && node.child_workflow_id.is_some())
            || (node.kind == NodeType::Condition
                && node
                    .condition
                    .as_deref()
                    .is_none_or(|condition| validate_condition(condition).is_err()))
            || (node.kind != NodeType::Condition && node.condition.is_some())
        {
            return Err(WorkflowError(
                "workflow coordinator contains an invalid task".into(),
            ));
        }
        edges = edges.saturating_add(node.deps.len());
    }
    if edges > MAX_WORKFLOW_EDGES {
        return Err(WorkflowError(format!(
            "workflow coordinator must contain at most {MAX_WORKFLOW_EDGES} dependency edges"
        )));
    }
    let mut indegree: HashMap<&str, usize> = workflow
        .nodes
        .iter()
        .map(|node| (node.name.as_str(), 0))
        .collect();
    let mut outgoing: HashMap<&str, Vec<&str>> = HashMap::new();
    for node in &workflow.nodes {
        let mut unique = HashSet::new();
        for dep in &node.deps {
            if !names.contains(dep.as_str()) {
                return Err(WorkflowError(
                    "workflow coordinator contains a missing dependency".into(),
                ));
            }
            if !unique.insert(dep.as_str()) {
                return Err(WorkflowError(
                    "workflow coordinator repeats a dependency".into(),
                ));
            }
            *indegree.get_mut(node.name.as_str()).expect("known node") += 1;
            outgoing.entry(dep).or_default().push(&node.name);
        }
    }
    let mut ready: VecDeque<&str> = indegree
        .iter()
        .filter_map(|(name, degree)| (*degree == 0).then_some(*name))
        .collect();
    let mut visited = 0;
    while let Some(name) = ready.pop_front() {
        visited += 1;
        for child in outgoing.get(name).into_iter().flatten() {
            let degree = indegree.get_mut(child).expect("known child");
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(child);
            }
        }
    }
    if visited != workflow.nodes.len() {
        return Err(WorkflowError(
            "workflow coordinator dependency graph contains a cycle".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct GraftStep(String);

    impl Task for GraftStep {
        const TYPE: &'static str = "workflow:test-graft-step";

        fn encode(&self) -> Result<Vec<u8>, CodecError> {
            Ok(self.0.as_bytes().to_vec())
        }

        fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
            Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
        }
    }

    fn graft_env(value: &str) -> Envelope {
        let task = GraftStep(value.into());
        Envelope {
            kind: GraftStep::TYPE.into(),
            payload: task.encode().unwrap(),
            queue: "headgate-workflow".into(),
            ..Default::default()
        }
    }

    #[test]
    fn workflow_snapshot_answers_topology_queries() {
        let snapshot = WorkflowSnapshot {
            workflow_id: "wf".into(),
            coordinator_job_id: "wf:coordinator".into(),
            coordinator_state: "running".into(),
            revision: 2,
            generation: 1,
            failed: false,
            failed_subgraph_retry: true,
            retry_policy: None,
            nodes: vec![
                WorkflowNode {
                    name: "prepare".into(),
                    job_id: "wf:prepare".into(),
                    kind: WorkflowNodeKind::Task,
                    job_kind: "task:prepare".into(),
                    state: "completed".into(),
                    dependencies: vec![],
                    dependents: vec!["publish".into()],
                    signal: None,
                    wake_at_ms: None,
                    delay_ms: None,
                    child_workflow_id: None,
                    condition: None,
                    completed_at_ms: Some(42),
                },
                WorkflowNode {
                    name: "publish".into(),
                    job_id: "wf:publish".into(),
                    kind: WorkflowNodeKind::Task,
                    job_kind: "task:publish".into(),
                    state: "pending".into(),
                    dependencies: vec!["prepare".into()],
                    dependents: vec![],
                    signal: None,
                    wake_at_ms: None,
                    delay_ms: None,
                    child_workflow_id: None,
                    condition: None,
                    completed_at_ms: None,
                },
            ],
        };
        assert_eq!(snapshot.node("prepare").unwrap().state, "completed");
        assert_eq!(snapshot.dependencies("publish").unwrap()[0].name, "prepare");
        assert_eq!(snapshot.dependents("prepare").unwrap()[0].name, "publish");
        assert!(snapshot.dependencies("missing").is_none());
    }

    fn env(kind: &str) -> Envelope {
        Envelope {
            kind: kind.into(),
            payload: b"{}".to_vec(),
            queue: "default".into(),
            ..Default::default()
        }
    }

    fn summary(id: &str, state: &str) -> headgate_core::JobSummary {
        headgate_core::JobSummary {
            id: id.into(),
            kind: "test".into(),
            queue: "default".into(),
            state: state.into(),
            schema_version: 1,
            priority: 0,
            attempt: 0,
            crash_attempt: 0,
            max_attempts: 1,
            partition_key: String::new(),
            rate_class: String::new(),
            sticky_worker: String::new(),
            weight: 1,
            fingerprint: "fp".into(),
            enqueued_at_ms: 0,
            scheduled_at_ms: 0,
            claimed_at_ms: None,
            periodic_schedule_id: String::new(),
            periodic_tick_ms: 0,
            finalized_at_ms: None,
            payload: None,
            headers: Default::default(),
            errors_json: "[]".into(),
            tags: Vec::new(),
        }
    }

    #[test]
    fn prepare_builds_one_coordinator_and_pending_fan_out_fan_in() {
        let batch = Workflow::new("wf1")
            .add("extract", env("task:extract"), Vec::<String>::new())
            .add("left", env("task:left"), ["extract"])
            .add("right", env("task:right"), ["extract"])
            .add("join", env("task:join"), ["left", "right"])
            .prepare()
            .unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(batch[0].kind, CoordinatorTask::TYPE);
        assert!(!batch[0].pending);
        assert!(batch[1..].iter().all(|e| e.pending && e.retention_ms > 0));
        let task = CoordinatorTask::decode(&batch[0].payload).unwrap();
        assert_eq!(task.nodes[3].deps, ["left", "right"]);
    }

    #[test]
    fn prepare_raises_short_child_retention_to_workflow_retention() {
        let mut short = env("task:short-retention");
        short.retention_ms = 1;
        let batch = Workflow::new("wf-retention")
            .retention(Duration::from_secs(3 * 60 * 60))
            .unwrap()
            .add("task", short, Vec::<String>::new())
            .prepare()
            .unwrap();
        assert_eq!(batch[1].retention_ms, 3 * 60 * 60 * 1000);
    }

    #[test]
    fn signal_is_pending_work_and_early_completion_waits_for_dependencies() {
        let batch = Workflow::new("wf-signals")
            .add("prepare", env("task:prepare"), Vec::<String>::new())
            .add_signal("approval", "approved", ["prepare"])
            .add("publish", env("task:publish"), ["approval"])
            .prepare()
            .unwrap();
        assert_eq!(batch[2].kind, SignalTask::TYPE);
        assert!(batch[2].pending);
        let task = CoordinatorTask::decode(&batch[0].payload).unwrap();
        assert_eq!(task.nodes[1].kind, NodeType::Signal);
        assert_eq!(task.nodes[1].signal.as_deref(), Some("approved"));

        let mut state = HashMap::from([
            ("prepare".into(), Some(summary("prepare", "available"))),
            ("approval".into(), Some(summary("approval", "completed"))),
            ("publish".into(), Some(summary("publish", "pending"))),
        ]);
        let mut completed = HashSet::new();
        let mut completed_at_ms = HashMap::new();
        assert!(!record_completion_evidence(
            &task,
            &state,
            &mut completed,
            &mut completed_at_ms
        ));
        assert!(!completed.contains("approval"));

        state.insert("prepare".into(), Some(summary("prepare", "completed")));
        assert!(record_completion_evidence(
            &task,
            &state,
            &mut completed,
            &mut completed_at_ms
        ));
        assert!(completed.contains("prepare"));
        assert!(completed.contains("approval"));
    }

    #[test]
    fn timer_uses_absolute_schedule_and_buffers_until_dependencies_complete() {
        let batch = Workflow::new("wf-timer")
            .add("prepare", env("task:prepare"), Vec::<String>::new())
            .add_timer_at("release", 1_500, ["prepare"])
            .add("publish", env("task:publish"), ["release"])
            .prepare()
            .unwrap();
        assert_eq!(batch[2].kind, TimerTask::TYPE);
        assert!(!batch[2].pending);
        assert_eq!(batch[2].scheduled_at_ms, 1_500);
        let task = CoordinatorTask::decode(&batch[0].payload).unwrap();
        assert_eq!(task.nodes[1].kind, NodeType::Timer);
        assert_eq!(task.nodes[1].wake_at_ms, Some(1_500));

        let mut state = HashMap::from([
            ("prepare".into(), Some(summary("prepare", "available"))),
            ("release".into(), Some(summary("release", "completed"))),
            ("publish".into(), Some(summary("publish", "pending"))),
        ]);
        let mut completed = HashSet::new();
        let mut completed_at_ms = HashMap::new();
        assert!(!record_completion_evidence(
            &task,
            &state,
            &mut completed,
            &mut completed_at_ms
        ));
        assert!(!completed.contains("release"));

        state.insert("prepare".into(), Some(summary("prepare", "completed")));
        assert!(record_completion_evidence(
            &task,
            &state,
            &mut completed,
            &mut completed_at_ms
        ));
        assert!(completed.contains("release"));

        completed.clear();
        state.insert("prepare".into(), Some(summary("prepare", "archived")));
        assert!(dependency_failed(&task, &task.nodes[1], &state, &completed));
    }

    #[test]
    fn relative_timer_anchors_to_dependency_completion() {
        let workflow = Workflow::new("wf-relative")
            .add("prepare", env("task:prepare"), Vec::<String>::new())
            .add_timer_after("wait", Duration::from_millis(250), ["prepare"])
            .unwrap();
        let batch = workflow.prepare().unwrap();
        let task = CoordinatorTask::decode(&batch[0].payload).unwrap();
        let timer = &task.nodes[1];
        assert_eq!(
            dependency_completion_anchor(timer, &HashMap::from([("prepare".into(), 1_000)]))
                .unwrap()
                + timer.delay_ms.unwrap(),
            1_250
        );
    }

    #[test]
    fn child_workflow_is_an_explicit_pending_node() {
        let batch = Workflow::new("parent")
            .add_child("billing", "billing-child", Vec::<String>::new())
            .add("finish", env("task:finish"), ["billing"])
            .prepare()
            .unwrap();
        assert_eq!(batch[1].kind, ChildWorkflowTask::TYPE);
        assert!(batch[1].pending);
        let child = ChildWorkflowTask::decode(&batch[1].payload).unwrap();
        assert_eq!(child.parent_workflow_id, "parent");
        assert_eq!(child.child_workflow_id, "billing-child");
        let coordinator = CoordinatorTask::decode(&batch[0].payload).unwrap();
        assert_eq!(coordinator.nodes[0].kind, NodeType::ChildWorkflow);
        assert_eq!(
            coordinator.nodes[0].child_workflow_id.as_deref(),
            Some("billing-child")
        );
    }

    #[test]
    fn retained_completion_survives_a_missing_job_row() {
        let task = CoordinatorTask {
            workflow_id: "wf".into(),
            nodes: vec![NodeSpec {
                name: "prepare".into(),
                job_id: "prepare".into(),
                deps: Vec::new(),
                kind: NodeType::Task,
                signal: None,
                wake_at_ms: None,
                delay_ms: None,
                child_workflow_id: None,
                condition: None,
            }],
            failed_subgraph_retry: false,
            retry_policy: None,
        };
        let completed = completed_set(&task, &["prepare".into()]);
        let node = &task.nodes[0];
        assert_eq!(effective_state(node, None, &completed), Some("completed"));
        assert_eq!(effective_state(node, None, &HashSet::new()), None);
    }

    #[test]
    fn prepare_rejects_missing_dependencies_and_cycles() {
        let missing = Workflow::new("wf")
            .add("a", env("task:a"), ["missing"])
            .prepare()
            .unwrap_err();
        assert!(missing.to_string().contains("missing task"));
        let cycle = Workflow::new("wf")
            .add("a", env("task:a"), ["b"])
            .add("b", env("task:b"), ["a"])
            .prepare()
            .unwrap_err();
        assert!(cycle.to_string().contains("cycle"));
    }

    #[test]
    fn revisioned_graft_prepares_one_atomic_receipt_and_pending_tasks() {
        let graft = WorkflowGraft::new("wf-graft", 1)
            .add("after", graft_env("after"), ["root"])
            .prepare()
            .unwrap();
        assert_eq!(graft[0].id, "wf-graft:graft:2");
        assert_eq!(graft[1].id, "wf-graft:g2:after");
        assert!(graft.iter().all(|job| job.pending));
        let receipt = GraftTask::decode(&graft[0].payload).unwrap();
        assert_eq!(receipt.workflow_id, "wf-graft");
        assert_eq!(receipt.expected_revision, 1);
        assert_eq!(receipt.nodes[0].deps, ["root"]);

        let cycle = WorkflowGraft::new("wf-graft", 1)
            .add("a", graft_env("a"), ["b"])
            .add("b", graft_env("b"), ["a"])
            .prepare()
            .unwrap_err();
        assert!(cycle.to_string().contains("cycle"));
    }

    #[test]
    fn failed_subgraph_retry_is_explicit_in_the_coordinator_payload() {
        let batch = Workflow::new("wf-retry")
            .failed_subgraph_retry()
            .add("prepare", graft_env("prepare"), Vec::<String>::new())
            .add("finish", graft_env("finish"), ["prepare"])
            .prepare()
            .unwrap();
        let coordinator = CoordinatorTask::decode(&batch[0].payload).unwrap();
        assert!(coordinator.failed_subgraph_retry);
        assert!(batch[1..].iter().all(|job| job.pending));
    }

    #[test]
    fn automatic_retry_policy_and_cel_condition_are_validated() {
        let batch = Workflow::new("wf-auto")
            .automatic_retry(3, Duration::from_millis(25))
            .unwrap()
            .add("prepare", env("task:prepare"), Vec::<String>::new())
            .add_condition(
                "ready",
                "completed.prepare && states.prepare == 'completed' && generation == 1u",
                ["prepare"],
            )
            .prepare()
            .unwrap();
        let coordinator = CoordinatorTask::decode(&batch[0].payload).unwrap();
        assert_eq!(
            coordinator.retry_policy,
            Some(WorkflowRetryPolicy {
                max_generations: 3,
                backoff_ms: 25,
            })
        );
        let mut cursor = WorkflowCursor::default();
        cursor.completed.push("prepare".into());
        let completed = completed_set(&coordinator, &cursor.completed);
        let states = HashMap::from([(
            "prepare".into(),
            Some(summary("wf-auto:prepare", "completed")),
        )]);
        assert!(
            evaluate_condition(
                &coordinator.nodes[1],
                &cursor,
                &coordinator,
                &states,
                &completed,
            )
            .unwrap()
        );

        assert!(
            Workflow::new("bad-cel")
                .add_condition("ready", "completed[", Vec::<String>::new())
                .prepare()
                .is_err()
        );
    }

    #[test]
    fn atomic_bundle_rejects_cross_workflow_cycles() {
        let parent = Workflow::new("parent").add_child("child", "child", Vec::<String>::new());
        let child = Workflow::new("child").add("work", env("task:child"), Vec::<String>::new());
        let batch = prepare_bundle(vec![parent, child]).unwrap();
        assert_eq!(batch.len(), 4);

        let left = Workflow::new("left").add_child("right", "right", Vec::<String>::new());
        let right = Workflow::new("right").add_child("left", "left", Vec::<String>::new());
        assert!(
            prepare_bundle(vec![left, right])
                .unwrap_err()
                .to_string()
                .contains("cycle")
        );
    }

    #[test]
    fn workflow_history_is_bounded_and_monotonic() {
        let mut cursor = WorkflowCursor::default();
        for index in 0..(MAX_WORKFLOW_EVENTS + 7) {
            record_event(&mut cursor, "tick", Some(index.to_string()), None).unwrap();
        }
        assert_eq!(cursor.events.len(), MAX_WORKFLOW_EVENTS);
        assert_eq!(cursor.events.first().unwrap().sequence, 8);
        assert_eq!(cursor.events.last().unwrap().sequence, 263);
    }

    #[test]
    fn workflow_and_coordinator_resource_bounds_are_enforced() {
        let mut workflow = Workflow::new("too-large");
        for index in 0..=MAX_WORKFLOW_NODES {
            workflow = workflow.add(
                format!("node-{index}"),
                env("task:node"),
                Vec::<String>::new(),
            );
        }
        assert!(
            workflow
                .prepare()
                .unwrap_err()
                .to_string()
                .contains("at most")
        );

        let forged = CoordinatorTask {
            workflow_id: "forged".into(),
            nodes: (0..=MAX_WORKFLOW_NODES)
                .map(|index| NodeSpec {
                    name: format!("node-{index}"),
                    job_id: format!("job-{index}"),
                    deps: Vec::new(),
                    kind: NodeType::Task,
                    signal: None,
                    wake_at_ms: None,
                    delay_ms: None,
                    child_workflow_id: None,
                    condition: None,
                })
                .collect(),
            failed_subgraph_retry: false,
            retry_policy: None,
        };
        assert!(validate_coordinator(&forged).is_err());
    }
}
