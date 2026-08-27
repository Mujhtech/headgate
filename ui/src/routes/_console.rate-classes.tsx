import { createFileRoute } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { RatesView } from "@/views/rates"

export const Route = createFileRoute("/_console/rate-classes")({ component: RateClassesRoute })

function RateClassesRoute() {
  return <RatesView {...useConsole()} />
}
