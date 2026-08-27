import { createFileRoute } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { WorkflowDetailView } from "@/views/workflows"

export const Route = createFileRoute("/_console/workflows_/$workflowId")({
  component: WorkflowDetailRoute,
})

function WorkflowDetailRoute() {
  const { workflowId } = Route.useParams()
  return <WorkflowDetailView workflowId={workflowId} {...useConsole()} />
}
