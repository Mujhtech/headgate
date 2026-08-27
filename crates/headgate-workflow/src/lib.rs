//! Durable DAG dependencies layered on headgate's ordinary pending jobs.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use headgate::{CodecError, Control, Envelope, JobCtx, JobError, Registry, Task};
use headgate_core::Inspect;
use serde::{Deserialize, Serialize};

const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

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
    envelope: Envelope,
    deps: Vec<String>,
}

/// A validated DAG builder. `prepare` returns one atomic enqueue batch containing the
/// durable coordinator plus every child in `pending` state.
pub struct Workflow {
    id: String,
    nodes: Vec<DraftNode>,
    coordinator_queue: String,
    retention_ms: i64,
}

impl Workflow {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            nodes: Vec::new(),
            coordinator_queue: "headgate-workflow".into(),
            retention_ms: DEFAULT_RETENTION_MS,
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

    pub fn add(
        mut self,
        name: impl Into<String>,
        envelope: Envelope,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.nodes.push(DraftNode {
            name: name.into(),
            envelope,
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
            let mut envelope = node.envelope;
            if envelope.id.is_empty() {
                envelope.id = format!("{}:{}", self.id, node.name);
            }
            if envelope.retention_ms == 0 {
                envelope.retention_ms = self.retention_ms;
            }
            envelope.pending = true;
            envelope.scheduled_at_ms = 0;
            envelope =
                headgate::prepare_envelope(envelope).map_err(|e| WorkflowError(e.to_string()))?;
            specs.push(NodeSpec {
                name: node.name,
                job_id: envelope.id.clone(),
                deps: node.deps,
            });
            children.push(envelope);
        }

        let task = CoordinatorTask {
            workflow_id: self.id.clone(),
            nodes: specs,
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
    let names: HashSet<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
    if names.len() != nodes.len() || names.contains("") {
        return Err(WorkflowError(
            "workflow task names must be non-empty and unique".into(),
        ));
    }
    let mut indegree: HashMap<&str, usize> = nodes.iter().map(|n| (n.name.as_str(), 0)).collect();
    let mut outgoing: HashMap<&str, Vec<&str>> = HashMap::new();
    for node in nodes {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NodeSpec {
    name: String,
    job_id: String,
    deps: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoordinatorTask {
    pub workflow_id: String,
    nodes: Vec<NodeSpec>,
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
    registry.register::<CoordinatorTask, _, _>(move |_ctx: JobCtx, task: CoordinatorTask| {
        let inspect = inspect.clone();
        async move {
            match tick(inspect.as_ref(), &task).await? {
                Tick::Waiting => Err(Control::Snooze(poll_interval).into()),
                Tick::Succeeded => Ok(()),
                Tick::Failed => Err(Control::Skip.into()),
            }
        }
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Tick {
    Waiting,
    Succeeded,
    Failed,
}

async fn tick(inspect: &dyn Inspect, workflow: &CoordinatorTask) -> Result<Tick, JobError> {
    let mut state = HashMap::with_capacity(workflow.nodes.len());
    for node in &workflow.nodes {
        state.insert(
            node.name.as_str(),
            inspect.get_job(&node.job_id, false).await?,
        );
    }
    let mut changed = false;
    for node in &workflow.nodes {
        let Some(job) = state.get(node.name.as_str()).and_then(Option::as_ref) else {
            continue;
        };
        if job.state != "pending" {
            continue;
        }
        let dep_failed =
            node.deps.iter().any(
                |dep| match state.get(dep.as_str()).and_then(Option::as_ref) {
                    None => true,
                    Some(j) => matches!(
                        j.state.as_str(),
                        "archived" | "cancelled" | "quarantined" | "undecodable"
                    ),
                },
            );
        if dep_failed {
            inspect.delete_job(&node.job_id).await?;
            changed = true;
            continue;
        }
        let deps_complete = node.deps.iter().all(|dep| {
            state
                .get(dep.as_str())
                .and_then(Option::as_ref)
                .is_some_and(|j| j.state == "completed")
        });
        if deps_complete {
            inspect.promote_job(&node.job_id).await?;
            changed = true;
        }
    }
    if changed {
        return Ok(Tick::Waiting);
    }
    let mut failed = false;
    for node in &workflow.nodes {
        match state
            .get(node.name.as_str())
            .and_then(Option::as_ref)
            .map(|j| j.state.as_str())
        {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn env(kind: &str) -> Envelope {
        Envelope {
            kind: kind.into(),
            payload: b"{}".to_vec(),
            queue: "default".into(),
            ..Default::default()
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
}
