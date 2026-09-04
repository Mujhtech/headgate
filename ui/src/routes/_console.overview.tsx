import { createFileRoute } from "@tanstack/react-router";

import { type OverviewRange, OverviewView } from "@/views/overview";

export interface OverviewSearch {
  queue?: string;
  range?: OverviewRange;
}

const ranges = new Set<OverviewRange>(["1h", "6h", "24h", "7d", "30d"]);

export const Route = createFileRoute("/_console/overview")({
  validateSearch: (search: Record<string, unknown>): OverviewSearch => ({
    queue:
      typeof search.queue === "string" && search.queue
        ? search.queue
        : undefined,
    range:
      typeof search.range === "string" &&
      ranges.has(search.range as OverviewRange)
        ? (search.range as OverviewRange)
        : undefined,
  }),
  component: OverviewRoute,
});

function OverviewRoute() {
  const search = Route.useSearch();
  const navigate = Route.useNavigate();
  return (
    <OverviewView
      queue={search.queue}
      range={search.range ?? "6h"}
      setSearch={(next) =>
        void navigate({
          search: (previous) => ({ ...previous, ...next }),
          replace: true,
        })
      }
    />
  );
}
