import { createFileRoute } from "@tanstack/react-router";

import { useConsole } from "@/console";
import { JobDrawer } from "@/views/jobs";
import { WorkflowDetailView } from "@/views/workflows";

interface WorkflowDetailSearch {
  selected?: string;
}

export const Route = createFileRoute("/_console/workflows_/$workflowId")({
  validateSearch: (search: Record<string, unknown>): WorkflowDetailSearch => ({
    selected:
      typeof search.selected === "string" && search.selected
        ? search.selected
        : undefined,
  }),
  component: WorkflowDetailRoute,
});

function WorkflowDetailRoute() {
  const { workflowId } = Route.useParams();
  const { selected } = Route.useSearch();
  const navigate = Route.useNavigate();
  const console = useConsole();
  return (
    <>
      <WorkflowDetailView
        selectedJobID={selected}
        workflowId={workflowId}
        {...console}
      />
      <JobDrawer
        id={selected ?? null}
        notify={console.notify}
        open={Boolean(selected)}
        setOpen={(open) => {
          if (!open) {
            void navigate({
              search: (previous) => ({ ...previous, selected: undefined }),
              replace: true,
            });
          }
        }}
        workflowId={workflowId}
      />
    </>
  );
}
