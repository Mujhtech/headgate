import { formatDate, formatDuration } from "@/lib/format";

export interface WorkflowNode {
  deps: string[];
  job_id: string;
  name: string;
}

export type WorkflowNodeKind =
  | "task"
  | "signal"
  | "timer"
  | "child_workflow"
  | "condition";

export interface WorkflowSnapshotNode {
  child_workflow_id?: string;
  completed_at_ms?: number;
  condition?: string;
  delay_ms?: number;
  dependencies: string[];
  dependents: string[];
  job_id: string;
  job_kind: string;
  kind: WorkflowNodeKind;
  name: string;
  signal?: string;
  state: string;
  wake_at_ms?: number;
}

export interface WorkflowSnapshot {
  coordinator_job_id: string;
  coordinator_state: string;
  failed: boolean;
  failed_subgraph_retry: boolean;
  generation: number;
  nodes: WorkflowSnapshotNode[];
  retry_policy?: { backoff_ms: number; max_generations: number };
  revision: number;
  workflow_id: string;
}

export interface WorkflowSnapshotResponse
  extends Omit<WorkflowSnapshot, "nodes"> {
  nodes?: Array<
    Omit<WorkflowSnapshotNode, "dependencies" | "dependents"> & {
      dependencies?: string[] | null;
      dependents?: string[] | null;
    }
  > | null;
}

export interface WorkflowEvent {
  at_ms?: number;
  event: string;
  generation: number;
  node?: string;
  revision: number;
  sequence: number;
}

export interface WorkflowEventsResponse {
  events?: WorkflowEvent[] | null;
}

export interface WorkflowSignal {
  id: number;
  idempotency_key: string;
  payload: unknown;
  recorded_at_ms: number;
  signal: string;
  source: unknown;
}

export interface WorkflowSignalsResponse {
  next_cursor?: number | null;
  signals?: WorkflowSignal[] | null;
}

export function normalizeWorkflowSnapshot(
  snapshot: WorkflowSnapshotResponse
): WorkflowSnapshot {
  return {
    ...snapshot,
    nodes: (snapshot.nodes ?? []).map((node) => ({
      ...node,
      dependencies: node.dependencies ?? [],
      dependents: node.dependents ?? [],
    })),
  };
}

export function workflowSignalNamesForNode(
  nodes: WorkflowSnapshotNode[],
  node: WorkflowSnapshotNode
): string[] {
  const candidates = [
    node,
    ...nodes.filter((candidate) => node.dependencies.includes(candidate.name)),
  ];
  return candidates
    .filter((candidate) => candidate.kind === "signal")
    .map((candidate) => candidate.signal ?? candidate.name);
}

export function workflowNodeWait(node: WorkflowSnapshotNode) {
  const resolved = node.state === "completed";
  if (node.kind === "signal") {
    return {
      detail: resolved
        ? `Signal “${node.signal ?? node.name}” was received.`
        : `Waiting for signal “${node.signal ?? node.name}”.`,
      label: resolved ? "Signal received" : "Waiting for signal",
    };
  }
  if (node.kind === "timer" && node.delay_ms != null) {
    const delay = formatDuration(node.delay_ms);
    return {
      detail: resolved
        ? `The ${delay} delay after dependency completion elapsed.`
        : `Runs ${delay} after all dependencies complete.`,
      label: resolved ? "Timer elapsed" : "Waiting for timer",
    };
  }
  if (node.kind === "timer" && node.wake_at_ms != null) {
    return {
      detail: resolved
        ? `The timer scheduled for ${formatDate(node.wake_at_ms)} elapsed.`
        : `Waiting until ${formatDate(node.wake_at_ms)}.`,
      label: resolved ? "Timer elapsed" : "Waiting for timer",
    };
  }
  if (node.kind === "condition") {
    return {
      detail: node.condition ?? "No condition expression was recorded.",
      label: resolved ? "Condition satisfied" : "Evaluating condition",
    };
  }
  if (node.kind === "child_workflow") {
    return {
      detail: node.child_workflow_id
        ? `Linked workflow: ${node.child_workflow_id}`
        : "No child workflow ID was recorded.",
      label: resolved
        ? "Child workflow completed"
        : "Waiting for child workflow",
    };
  }
  return null;
}

export interface CoordinatorPayload {
  failed_subgraph_retry: boolean;
  nodes: WorkflowNode[];
  workflow_id: string;
}

export interface WorkflowCursorProjection {
  completed: Set<string>;
  failed: boolean;
  generation: number;
  grafts: WorkflowNode[];
  pending_graft_receipt?: string;
  pending_retry_receipt?: string;
  revision: number;
}

function decodeBase64JSON(value: string): unknown {
  const bytes = Uint8Array.from(atob(value), (character) =>
    character.charCodeAt(0)
  );
  return JSON.parse(new TextDecoder().decode(bytes));
}

function workflowNode(value: unknown, index: number): WorkflowNode {
  if (!value || typeof value !== "object") {
    throw new Error(`node ${index} is not an object`);
  }

  const node = value as Record<string, unknown>;
  if (typeof node.name !== "string" || typeof node.job_id !== "string") {
    throw new Error(`node ${index} is missing name or job_id`);
  }
  if (
    node.deps != null &&
    (!Array.isArray(node.deps) ||
      node.deps.some((dependency) => typeof dependency !== "string"))
  ) {
    throw new Error(`node ${index} has invalid dependencies`);
  }

  return {
    // Go encodes a nil dependency slice as null. Root tasks have no
    // dependencies, so normalize null and omitted values at this boundary.
    deps: node.deps == null ? [] : (node.deps as string[]),
    job_id: node.job_id,
    name: node.name,
  };
}

export function decodeWorkflowPayload(
  payload: string | undefined
): CoordinatorPayload {
  if (!payload) {
    throw new Error("The coordinator payload was withheld by the control API.");
  }
  try {
    const value = decodeBase64JSON(payload) as Record<string, unknown>;
    if (typeof value.workflow_id !== "string" || !Array.isArray(value.nodes)) {
      throw new Error("missing workflow fields");
    }
    return {
      failed_subgraph_retry: value.failed_subgraph_retry === true,
      nodes: value.nodes.map(workflowNode),
      workflow_id: value.workflow_id,
    };
  } catch (reason) {
    throw new Error(
      `The coordinator payload is not a readable workflow graph: ${reason instanceof Error ? reason.message : String(reason)}`,
      { cause: reason }
    );
  }
}

export function decodeWorkflowCursor(
  cursor: string | null | undefined
): WorkflowCursorProjection {
  if (!cursor) {
    return {
      completed: new Set(),
      failed: false,
      generation: 1,
      grafts: [],
      revision: 1,
    };
  }
  try {
    const value = decodeBase64JSON(cursor) as Record<string, unknown>;
    if (
      !Array.isArray(value.completed) ||
      value.completed.some((name) => typeof name !== "string")
    ) {
      throw new Error("missing completed task names");
    }
    if (
      value.grafts != null &&
      (!Array.isArray(value.grafts) ||
        value.grafts.some((node) => !node || typeof node !== "object"))
    ) {
      throw new Error("invalid graft nodes");
    }
    const revision = value.revision == null ? 1 : value.revision;
    const generation = value.generation == null ? 1 : value.generation;
    if (
      typeof revision !== "number" ||
      !Number.isSafeInteger(revision) ||
      revision < 1 ||
      typeof generation !== "number" ||
      !Number.isSafeInteger(generation) ||
      generation < 1
    ) {
      throw new Error("invalid revision or generation");
    }
    return {
      completed: new Set(value.completed as string[]),
      failed: value.failed === true,
      generation,
      grafts: Array.isArray(value.grafts) ? value.grafts.map(workflowNode) : [],
      pending_graft_receipt:
        typeof value.pending_graft_receipt === "string"
          ? value.pending_graft_receipt
          : undefined,
      pending_retry_receipt:
        typeof value.pending_retry_receipt === "string"
          ? value.pending_retry_receipt
          : undefined,
      revision,
    };
  } catch (reason) {
    throw new Error(
      `The workflow completion checkpoint is not readable: ${reason instanceof Error ? reason.message : String(reason)}`,
      { cause: reason }
    );
  }
}

export function decodeWorkflowCompletionCursor(
  cursor: string | null | undefined
): Set<string> {
  return decodeWorkflowCursor(cursor).completed;
}
