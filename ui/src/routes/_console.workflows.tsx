import { createFileRoute } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { WorkflowsView } from "@/views/workflows"

export interface WorkflowsSearch {
  cursor?: string
}

export const Route = createFileRoute("/_console/workflows")({
  validateSearch: (search: Record<string, unknown>): WorkflowsSearch => ({
    cursor: typeof search.cursor === "string" && search.cursor ? search.cursor : undefined,
  }),
  component: WorkflowsRoute,
})

function WorkflowsRoute() {
  return <WorkflowsView {...useConsole()} />
}
