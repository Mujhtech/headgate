import { describe, expect, it } from "vitest";

import { consolePublicAssetUrl, mountPathFromModuleUrl } from "./mount-path";

describe("embedded console mount path", () => {
  it.each([
    ["https://example.test/assets/index.js", "/"],
    ["https://example.test/admin/headgate/assets/index.js", "/admin/headgate"],
    [
      "https://example.test/assets/headgate/assets/index.js",
      "/assets/headgate",
    ],
  ])("derives the base path from %s", (moduleUrl, expected) => {
    expect(mountPathFromModuleUrl(moduleUrl)).toBe(expected);
  });

  it("falls back to the site root when the module is not in assets", () => {
    expect(mountPathFromModuleUrl("https://example.test/index.js")).toBe("/");
  });

  it("resolves public files at the console mount root", () => {
    expect(
      consolePublicAssetUrl(
        "favicon.svg",
        "https://example.test/admin/headgate/assets/index.js"
      )
    ).toBe("https://example.test/admin/headgate/favicon.svg");
  });
});
