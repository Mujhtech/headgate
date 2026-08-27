import { createFileRoute } from "@tanstack/react-router"

import { ConsoleLayout } from "@/console"

export const Route = createFileRoute("/_console")({ component: ConsoleLayout })
