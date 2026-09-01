import { describe, expect, it } from "vitest";

import { workerActionDisabledReason } from "@/lib/worker-control";

const worker = { duties_active: true, status: "running" as const };

describe("worker controls", () => {
  it("makes quiet and resume mutually exclusive", () => {
    expect(workerActionDisabledReason(worker, "quiet")).toBeNull();
    expect(workerActionDisabledReason(worker, "resume")).toContain(
      "only while"
    );
    expect(
      workerActionDisabledReason({ ...worker, status: "quiet" }, "quiet")
    ).toContain("already");
    expect(
      workerActionDisabledReason({ ...worker, status: "quiet" }, "resume")
    ).toBeNull();
  });

  it("blocks conflicting commands while one is pending or the worker is draining", () => {
    expect(
      workerActionDisabledReason(
        { ...worker, pending_command: "restart" },
        "terminate"
      )
    ).toContain("acknowledge");
    expect(
      workerActionDisabledReason({ ...worker, status: "restarting" }, "quiet")
    ).toContain("restarting");
    expect(
      workerActionDisabledReason(
        { ...worker, status: "terminating" },
        "restart"
      )
    ).toContain("terminating");
  });

  it("disables resign after singleton duties have been released", () => {
    expect(
      workerActionDisabledReason({ ...worker, duties_active: false }, "resign")
    ).toContain("already resigned");
  });
});
