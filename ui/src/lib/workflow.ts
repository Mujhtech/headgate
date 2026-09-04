export interface WorkflowNode {
  deps: string[];
  job_id: string;
  name: string;
}

export interface CoordinatorPayload {
  nodes: WorkflowNode[];
  workflow_id: string;
}

interface WorkflowCursor {
  completed: string[];
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

export function decodeWorkflowCompletionCursor(
  cursor: string | null | undefined
): Set<string> {
  if (!cursor) {
    return new Set();
  }
  try {
    const value = decodeBase64JSON(cursor) as Partial<WorkflowCursor>;
    if (
      !Array.isArray(value.completed) ||
      value.completed.some((name) => typeof name !== "string")
    ) {
      throw new Error("missing completed task names");
    }
    return new Set(value.completed);
  } catch (reason) {
    throw new Error(
      `The workflow completion checkpoint is not readable: ${reason instanceof Error ? reason.message : String(reason)}`,
      { cause: reason }
    );
  }
}
