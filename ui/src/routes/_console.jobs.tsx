import { createFileRoute, Outlet } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { JobsView } from "@/views/jobs"

export interface JobsSearch {
  q?: string
  queue?: string
  state?: string
  cursor?: string
}

export const Route = createFileRoute("/_console/jobs")({
  validateSearch: (search: Record<string, unknown>): JobsSearch => ({
    q: typeof search.q === "string" && search.q ? search.q : undefined,
    queue: typeof search.queue === "string" && search.queue ? search.queue : undefined,
    state: typeof search.state === "string" && search.state ? search.state : undefined,
    cursor: typeof search.cursor === "string" && search.cursor ? search.cursor : undefined,
  }),
  component: JobsRoute,
})

function JobsRoute() {
  const search = Route.useSearch()
  return (
    <>
      <JobsView key={`${search.q ?? ""}:${search.queue ?? ""}:${search.state ?? ""}:${search.cursor ?? ""}`} {...useConsole()} />
      <Outlet />
    </>
  )
}
