export interface WorkflowNode {
  name: string
  job_id: string
  deps: string[]
}

export interface CoordinatorPayload {
  workflow_id: string
  nodes: WorkflowNode[]
}

function workflowNode(value: unknown, index: number): WorkflowNode {
  if (!value || typeof value !== "object") throw new Error(`node ${index} is not an object`)

  const node = value as Record<string, unknown>
  if (typeof node.name !== "string" || typeof node.job_id !== "string") {
    throw new Error(`node ${index} is missing name or job_id`)
  }
  if (node.deps != null && (!Array.isArray(node.deps) || node.deps.some((dependency) => typeof dependency !== "string"))) {
    throw new Error(`node ${index} has invalid dependencies`)
  }

  return {
    name: node.name,
    job_id: node.job_id,
    // Go encodes a nil dependency slice as null. Root tasks have no
    // dependencies, so normalize null and omitted values at this boundary.
    deps: node.deps == null ? [] : node.deps as string[],
  }
}

export function decodeWorkflowPayload(payload: string | undefined): CoordinatorPayload {
  if (!payload) throw new Error("The coordinator payload was withheld by the control API.")
  try {
    const bytes = Uint8Array.from(atob(payload), (character) => character.charCodeAt(0))
    const value = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>
    if (!value || typeof value !== "object" || typeof value.workflow_id !== "string" || !Array.isArray(value.nodes)) {
      throw new Error("missing workflow fields")
    }
    return {
      workflow_id: value.workflow_id,
      nodes: value.nodes.map(workflowNode),
    }
  } catch (reason) {
    throw new Error(`The coordinator payload is not a readable workflow graph: ${reason instanceof Error ? reason.message : String(reason)}`)
  }
}
