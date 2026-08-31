//! Experimental workflow semantics. This reducer is intentionally store-agnostic: it
//! settles behavior before durable adapters and control APIs make the contract permanent.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeKind {
    Task,
    Signal { signal: String },
    Timer { wake_at_ms: i64 },
    ChildWorkflow { workflow_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub name: String,
    #[serde(default)]
    pub deps: Vec<String>,
    pub kind: NodeKind,
}

impl NodeSpec {
    pub fn task(
        name: impl Into<String>,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            deps: deps.into_iter().map(Into::into).collect(),
            kind: NodeKind::Task,
        }
    }

    pub fn signal(
        name: impl Into<String>,
        signal: impl Into<String>,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            deps: deps.into_iter().map(Into::into).collect(),
            kind: NodeKind::Signal {
                signal: signal.into(),
            },
        }
    }

    pub fn timer(
        name: impl Into<String>,
        wake_at_ms: i64,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            deps: deps.into_iter().map(Into::into).collect(),
            kind: NodeKind::Timer { wake_at_ms },
        }
    }

    pub fn child(
        name: impl Into<String>,
        workflow_id: impl Into<String>,
        deps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            deps: deps.into_iter().map(Into::into).collect(),
            kind: NodeKind::ChildWorkflow {
                workflow_id: workflow_id.into(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    Waiting,
    Active,
    Succeeded,
    Failed,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeNode {
    pub spec: NodeSpec,
    pub state: NodeState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub revision: u64,
    pub generation: u32,
    pub status: RunStatus,
    pub store_now_ms: i64,
    pub nodes: BTreeMap<String, RuntimeNode>,
    pub signals: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Signal {
        signal: String,
    },
    AdvanceStoreTime {
        now_ms: i64,
    },
    SucceedNode {
        name: String,
    },
    FailNode {
        name: String,
    },
    Graft {
        expected_revision: u64,
        nodes: Vec<NodeSpec>,
    },
    RetryFailedSubgraph {
        expected_revision: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    DispatchTask {
        name: String,
        generation: u32,
    },
    WaitForSignal {
        name: String,
        signal: String,
    },
    ArmTimer {
        name: String,
        wake_at_ms: i64,
    },
    StartChildWorkflow {
        name: String,
        workflow_id: String,
        generation: u32,
    },
    WorkflowSucceeded {
        generation: u32,
    },
    WorkflowFailed {
        name: String,
        generation: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentError(pub String);

impl std::fmt::Display for ExperimentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ExperimentError {}

impl Run {
    pub fn new(
        nodes: Vec<NodeSpec>,
        store_now_ms: i64,
    ) -> Result<(Self, Vec<Action>), ExperimentError> {
        validate_graph(&nodes)?;
        let mut run = Self {
            revision: 1,
            generation: 1,
            status: RunStatus::Running,
            store_now_ms,
            nodes: nodes
                .into_iter()
                .map(|spec| {
                    (
                        spec.name.clone(),
                        RuntimeNode {
                            spec,
                            state: NodeState::Waiting,
                        },
                    )
                })
                .collect(),
            signals: BTreeSet::new(),
        };
        let actions = run.reconcile();
        Ok((run, actions))
    }

    pub fn apply(&mut self, command: Command) -> Result<Vec<Action>, ExperimentError> {
        let mut actions = Vec::new();
        match command {
            Command::Signal { signal } => {
                if signal.is_empty() {
                    return Err(ExperimentError("signal name must not be empty".into()));
                }
                if !self.nodes.values().any(
                    |node| matches!(&node.spec.kind, NodeKind::Signal { signal: expected } if expected == &signal),
                ) {
                    return Err(ExperimentError(format!("unknown signal `{signal}`")));
                }
                self.signals.insert(signal.clone());
                for node in self.nodes.values_mut() {
                    if node.state == NodeState::Active
                        && matches!(&node.spec.kind, NodeKind::Signal { signal: expected } if expected == &signal)
                    {
                        node.state = NodeState::Succeeded;
                    }
                }
            }
            Command::AdvanceStoreTime { now_ms } => {
                if now_ms < self.store_now_ms {
                    return Err(ExperimentError("store time must not move backwards".into()));
                }
                self.store_now_ms = now_ms;
                for node in self.nodes.values_mut() {
                    if node.state == NodeState::Active
                        && matches!(node.spec.kind, NodeKind::Timer { wake_at_ms } if wake_at_ms <= now_ms)
                    {
                        node.state = NodeState::Succeeded;
                    }
                }
            }
            Command::SucceedNode { name } => self.settle_node(&name, true)?,
            Command::FailNode { name } => {
                self.settle_node(&name, false)?;
                self.block_descendants(&name);
                self.status = RunStatus::Failed;
                actions.push(Action::WorkflowFailed {
                    name,
                    generation: self.generation,
                });
            }
            Command::Graft {
                expected_revision,
                nodes,
            } => {
                self.require_revision(expected_revision)?;
                if self.status != RunStatus::Running {
                    return Err(ExperimentError(
                        "nodes may only be grafted onto a running workflow".into(),
                    ));
                }
                if nodes.is_empty() {
                    return Err(ExperimentError(
                        "graft must contain at least one node".into(),
                    ));
                }
                let mut combined: Vec<NodeSpec> =
                    self.nodes.values().map(|node| node.spec.clone()).collect();
                for node in &nodes {
                    if self.nodes.contains_key(&node.name) {
                        return Err(ExperimentError(format!(
                            "graft repeats existing node `{}`",
                            node.name
                        )));
                    }
                    combined.push(node.clone());
                }
                validate_graph(&combined)?;
                for spec in nodes {
                    self.nodes.insert(
                        spec.name.clone(),
                        RuntimeNode {
                            spec,
                            state: NodeState::Waiting,
                        },
                    );
                }
                self.revision += 1;
            }
            Command::RetryFailedSubgraph { expected_revision } => {
                self.require_revision(expected_revision)?;
                if self.status != RunStatus::Failed {
                    return Err(ExperimentError(
                        "only a failed workflow may be retried".into(),
                    ));
                }
                for node in self.nodes.values_mut() {
                    if matches!(node.state, NodeState::Failed | NodeState::Blocked) {
                        node.state = NodeState::Waiting;
                    }
                }
                self.generation = self
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| ExperimentError("workflow generation overflow".into()))?;
                self.revision += 1;
                self.status = RunStatus::Running;
            }
        }
        actions.extend(self.reconcile());
        Ok(actions)
    }

    fn require_revision(&self, expected: u64) -> Result<(), ExperimentError> {
        if expected != self.revision {
            return Err(ExperimentError(format!(
                "revision conflict: expected {expected}, current {}",
                self.revision
            )));
        }
        Ok(())
    }

    fn settle_node(&mut self, name: &str, succeeded: bool) -> Result<(), ExperimentError> {
        let node = self
            .nodes
            .get_mut(name)
            .ok_or_else(|| ExperimentError(format!("unknown node `{name}`")))?;
        if node.state != NodeState::Active {
            return Err(ExperimentError(format!("node `{name}` is not active")));
        }
        if !matches!(
            node.spec.kind,
            NodeKind::Task | NodeKind::ChildWorkflow { .. }
        ) {
            return Err(ExperimentError(format!(
                "node `{name}` is settled by its signal or timer"
            )));
        }
        node.state = if succeeded {
            NodeState::Succeeded
        } else {
            NodeState::Failed
        };
        Ok(())
    }

    fn block_descendants(&mut self, failed: &str) {
        let mut queue = VecDeque::from([failed.to_string()]);
        while let Some(parent) = queue.pop_front() {
            let children: Vec<String> = self
                .nodes
                .values()
                .filter(|node| node.spec.deps.iter().any(|dep| dep == &parent))
                .map(|node| node.spec.name.clone())
                .collect();
            for child in children {
                if let Some(node) = self.nodes.get_mut(&child) {
                    if matches!(node.state, NodeState::Waiting | NodeState::Active) {
                        node.state = NodeState::Blocked;
                        queue.push_back(child);
                    }
                }
            }
        }
    }

    fn reconcile(&mut self) -> Vec<Action> {
        if self.status != RunStatus::Running {
            return Vec::new();
        }
        let mut actions = Vec::new();
        loop {
            let ready: Vec<String> = self
                .nodes
                .values()
                .filter(|node| node.state == NodeState::Waiting)
                .filter(|node| {
                    node.spec.deps.iter().all(|dep| {
                        self.nodes
                            .get(dep)
                            .is_some_and(|upstream| upstream.state == NodeState::Succeeded)
                    })
                })
                .map(|node| node.spec.name.clone())
                .collect();
            if ready.is_empty() {
                break;
            }
            let mut completed_virtual = false;
            for name in ready {
                let node = self.nodes.get_mut(&name).expect("ready node exists");
                match &node.spec.kind {
                    NodeKind::Task => {
                        node.state = NodeState::Active;
                        actions.push(Action::DispatchTask {
                            name,
                            generation: self.generation,
                        });
                    }
                    NodeKind::Signal { signal } if self.signals.contains(signal) => {
                        node.state = NodeState::Succeeded;
                        completed_virtual = true;
                    }
                    NodeKind::Signal { signal } => {
                        node.state = NodeState::Active;
                        actions.push(Action::WaitForSignal {
                            name,
                            signal: signal.clone(),
                        });
                    }
                    NodeKind::Timer { wake_at_ms } if *wake_at_ms <= self.store_now_ms => {
                        node.state = NodeState::Succeeded;
                        completed_virtual = true;
                    }
                    NodeKind::Timer { wake_at_ms } => {
                        node.state = NodeState::Active;
                        actions.push(Action::ArmTimer {
                            name,
                            wake_at_ms: *wake_at_ms,
                        });
                    }
                    NodeKind::ChildWorkflow { workflow_id } => {
                        node.state = NodeState::Active;
                        actions.push(Action::StartChildWorkflow {
                            name,
                            workflow_id: workflow_id.clone(),
                            generation: self.generation,
                        });
                    }
                }
            }
            if !completed_virtual {
                break;
            }
        }
        if self
            .nodes
            .values()
            .all(|node| node.state == NodeState::Succeeded)
        {
            self.status = RunStatus::Succeeded;
            actions.push(Action::WorkflowSucceeded {
                generation: self.generation,
            });
        }
        actions
    }
}

fn validate_graph(nodes: &[NodeSpec]) -> Result<(), ExperimentError> {
    if nodes.is_empty() {
        return Err(ExperimentError(
            "workflow must contain at least one node".into(),
        ));
    }
    let mut names = BTreeSet::new();
    for node in nodes {
        if node.name.is_empty() || !names.insert(node.name.as_str()) {
            return Err(ExperimentError(
                "node names must be non-empty and unique".into(),
            ));
        }
        if matches!(&node.kind, NodeKind::Signal { signal } if signal.is_empty()) {
            return Err(ExperimentError(format!(
                "signal node `{}` has an empty signal",
                node.name
            )));
        }
        if matches!(&node.kind, NodeKind::ChildWorkflow { workflow_id } if workflow_id.is_empty()) {
            return Err(ExperimentError(format!(
                "child node `{}` has an empty workflow id",
                node.name
            )));
        }
    }
    let mut degree: BTreeMap<&str, usize> =
        nodes.iter().map(|node| (node.name.as_str(), 0)).collect();
    let mut outgoing: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for node in nodes {
        let mut unique = BTreeSet::new();
        for dep in &node.deps {
            if !names.contains(dep.as_str()) {
                return Err(ExperimentError(format!(
                    "node `{}` depends on missing node `{dep}`",
                    node.name
                )));
            }
            if !unique.insert(dep.as_str()) {
                return Err(ExperimentError(format!(
                    "node `{}` repeats dependency `{dep}`",
                    node.name
                )));
            }
            *degree
                .get_mut(node.name.as_str())
                .expect("node degree exists") += 1;
            outgoing.entry(dep).or_default().push(&node.name);
        }
    }
    let mut queue: VecDeque<&str> = degree
        .iter()
        .filter_map(|(name, count)| (*count == 0).then_some(*name))
        .collect();
    let mut visited = 0;
    while let Some(name) = queue.pop_front() {
        visited += 1;
        for child in outgoing.get(name).into_iter().flatten() {
            let count = degree.get_mut(child).expect("child degree exists");
            *count -= 1;
            if *count == 0 {
                queue.push_back(child);
            }
        }
    }
    if visited != nodes.len() {
        return Err(ExperimentError("workflow graph contains a cycle".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(actions: &[Action]) -> Vec<&str> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::DispatchTask { name, .. } | Action::StartChildWorkflow { name, .. } => {
                    Some(name.as_str())
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn signals_and_store_time_timers_unlock_in_dependency_order() {
        let (mut run, first) = Run::new(
            vec![
                NodeSpec::task("prepare", Vec::<String>::new()),
                NodeSpec::signal("approval", "approved", ["prepare"]),
                NodeSpec::timer("release", 1_500, ["approval"]),
                NodeSpec::task("publish", ["release"]),
            ],
            1_000,
        )
        .unwrap();
        assert_eq!(names(&first), ["prepare"]);
        let unknown = run
            .apply(Command::Signal {
                signal: "typo".into(),
            })
            .unwrap_err();
        assert!(unknown.0.contains("unknown signal"));
        assert!(
            run.apply(Command::Signal {
                signal: "approved".into()
            })
            .unwrap()
            .is_empty()
        );
        let wait = run
            .apply(Command::SucceedNode {
                name: "prepare".into(),
            })
            .unwrap();
        assert!(wait.iter().any(
            |a| matches!(a, Action::ArmTimer { name, wake_at_ms: 1_500 } if name == "release")
        ));
        assert!(
            run.apply(Command::AdvanceStoreTime { now_ms: 1_499 })
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            names(
                &run.apply(Command::AdvanceStoreTime { now_ms: 1_500 })
                    .unwrap()
            ),
            ["publish"]
        );
    }

    #[test]
    fn graft_is_additive_revision_checked_and_cycle_safe() {
        let (mut run, _) = Run::new(vec![NodeSpec::task("root", Vec::<String>::new())], 0).unwrap();
        let actions = run
            .apply(Command::Graft {
                expected_revision: 1,
                nodes: vec![NodeSpec::task("grafted", ["root"])],
            })
            .unwrap();
        assert!(actions.is_empty());
        assert_eq!(run.revision, 2);
        let stale = run
            .apply(Command::Graft {
                expected_revision: 1,
                nodes: vec![NodeSpec::task("stale", ["root"])],
            })
            .unwrap_err();
        assert!(stale.0.contains("revision conflict"));
        let cycle = run
            .apply(Command::Graft {
                expected_revision: 2,
                nodes: vec![NodeSpec::task("a", ["b"]), NodeSpec::task("b", ["a"])],
            })
            .unwrap_err();
        assert!(cycle.0.contains("cycle"));
    }

    #[test]
    fn nested_failure_retries_only_failed_subgraph() {
        let (mut run, first) = Run::new(
            vec![
                NodeSpec::task("extract", Vec::<String>::new()),
                NodeSpec::child("child", "child-workflow", ["extract"]),
                NodeSpec::task("finish", ["child"]),
            ],
            0,
        )
        .unwrap();
        assert_eq!(names(&first), ["extract"]);
        let child = run
            .apply(Command::SucceedNode {
                name: "extract".into(),
            })
            .unwrap();
        assert_eq!(names(&child), ["child"]);
        let failed = run
            .apply(Command::FailNode {
                name: "child".into(),
            })
            .unwrap();
        assert!(failed.iter().any(
            |a| matches!(a, Action::WorkflowFailed { name, generation: 1 } if name == "child")
        ));
        assert_eq!(run.nodes["extract"].state, NodeState::Succeeded);
        assert_eq!(run.nodes["finish"].state, NodeState::Blocked);
        let retried = run
            .apply(Command::RetryFailedSubgraph {
                expected_revision: 1,
            })
            .unwrap();
        assert_eq!(run.generation, 2);
        assert_eq!(run.nodes["extract"].state, NodeState::Succeeded);
        assert_eq!(names(&retried), ["child"]);
        assert!(
            retried
                .iter()
                .any(|a| matches!(a, Action::StartChildWorkflow { generation: 2, .. }))
        );
    }
}
