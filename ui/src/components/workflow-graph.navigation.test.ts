// @vitest-environment jsdom
import { createMemoryHistory } from "@tanstack/react-router";
import { describe, expect, it } from "vitest";

import { workflowNodeLinkOptions } from "@/components/workflow-graph";
import { getRouter } from "@/router";

describe("workflow graph navigation", () => {
  it("builds node links under the console mount path", () => {
    const router = getRouter();
    router.update({
      basepath: "/admin/headgate",
      history: createMemoryHistory({
        initialEntries: ["/admin/headgate/workflows/wf%3Aparent"],
      }),
    });

    const location = router.buildLocation(
      workflowNodeLinkOptions("wf:parent", "job:child")
    );

    expect(location.href).toBe(
      "/admin/headgate/workflows/wf%3Aparent?selected=job%3Achild"
    );
  });
});
