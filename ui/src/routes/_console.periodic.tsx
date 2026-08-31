import { createFileRoute } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { SchedulesView } from "@/views/schedules"

export interface PeriodicSearch {
  events?: string
}

export const Route = createFileRoute("/_console/periodic")({
  validateSearch: (search: Record<string, unknown>): PeriodicSearch => ({
    events: typeof search.events === "string" && search.events ? search.events : undefined,
  }),
  component: PeriodicRoute,
})

function PeriodicRoute() {
  return <SchedulesView {...useConsole()} />
}
