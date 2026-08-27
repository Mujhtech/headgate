import { createFileRoute } from "@tanstack/react-router"

import { useConsole } from "@/console"
import { QuarantineView } from "@/views/quarantine"

export const Route = createFileRoute("/_console/quarantine")({ component: QuarantineRoute })

function QuarantineRoute() {
  return <QuarantineView {...useConsole()} />
}
