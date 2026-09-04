import { createFileRoute } from "@tanstack/react-router";

import { useConsole } from "@/console";
import { QueuesView } from "@/views/queues";

export const Route = createFileRoute("/_console/queues")({
  component: QueuesRoute,
});

function QueuesRoute() {
  return <QueuesView {...useConsole()} />;
}
