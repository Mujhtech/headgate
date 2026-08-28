import { createFileRoute } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { JobDrawer } from "@/views/jobs"

export const Route = createFileRoute("/_console/jobs/$jobId")({ component: JobDetailRoute })

function JobDetailRoute() {
  const { jobId } = Route.useParams()
  const navigate = Route.useNavigate()
  const console = useConsole()
  return (
    <JobDrawer
      id={jobId}
      open
      setOpen={(open) => {
        if (!open) void navigate({ to: "/jobs", search: (previous) => previous, replace: true })
      }}
      notify={console.notify}
    />
  )
}
