import { createFileRoute } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { SchedulesView } from "@/views/schedules"

export const Route = createFileRoute("/_console/periodic")({ component: PeriodicRoute })

function PeriodicRoute() {
  return <SchedulesView {...useConsole()} />
}
