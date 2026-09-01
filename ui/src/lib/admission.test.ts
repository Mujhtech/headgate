import { describe, expect, it } from "vitest";

import { admissionPresentation } from "@/lib/admission";

describe("admission presentation", () => {
  it.each(["completed", "archived", "cancelled", "undecodable"])(
    "does not describe a %s job as policy-blocked",
    (state) => {
      expect(admissionPresentation(state, { admissible: false }).title).toBe(
        "Admission no longer applies"
      );
    }
  );

  it("explains pending workflow work and running work separately", () => {
    expect(admissionPresentation("pending", { admissible: false }).title).toBe(
      "Waiting for promotion"
    );
    expect(admissionPresentation("running", { admissible: true }).title).toBe(
      "Already admitted"
    );
  });

  it("explains that quarantined work requires operator release", () => {
    expect(
      admissionPresentation("quarantined", {
        admissible: false,
        blocked_by: "quarantine",
      }).title
    ).toBe("Blocked by quarantine");
  });

  it("names actual admission-policy blockers", () => {
    expect(
      admissionPresentation("available", {
        admissible: false,
        blocked_by: "queue_paused",
      }).title
    ).toBe("Queue is paused");
    expect(
      admissionPresentation("available", {
        admissible: false,
        blocked_by: "concurrency_limit",
      }).description
    ).toContain("releases capacity");
    expect(
      admissionPresentation("scheduled", {
        admissible: false,
        blocked_by: "schedule",
        estimated_admission_ms: 2500,
      }).description
    ).toContain("3s");
  });

  it("keeps a malformed response explicit without inventing a policy", () => {
    expect(
      admissionPresentation("available", { admissible: false }).title
    ).toBe("Admission decision unavailable");
  });
});
