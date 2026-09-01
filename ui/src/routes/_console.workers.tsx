import { createFileRoute } from "@tanstack/react-router";

import { useConsole } from "@/console";
import { WorkersView } from "@/views/workers";

export const Route = createFileRoute("/_console/workers")({
  component: WorkersRoute,
});

function WorkersRoute() {
  return <WorkersView {...useConsole()} />;
}
