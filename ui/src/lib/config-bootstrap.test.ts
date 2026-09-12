import { describe, expect, it } from "vitest";

import { configBootstrapScript } from "./config-bootstrap";

describe("embedded console configuration bootstrap", () => {
  it("serializes the default configuration in the embedded handler format", () => {
    expect(configBootstrapScript(undefined)).toBe(
      `window.HEADGATE = {"apiBase":"/api/v1","readOnly":false};`
    );
  });

  it("matches injected configuration without allowing a script breakout", () => {
    expect(
      configBootstrapScript({
        apiBase: "</script><script>alert(1)</script>&\u2028\u2029",
        readOnly: true,
      })
    ).toBe(
      `window.HEADGATE = {"apiBase":"\\u003c/script\\u003e\\u003cscript\\u003ealert(1)\\u003c/script\\u003e\\u0026\\u2028\\u2029","readOnly":true};`
    );
  });
});
