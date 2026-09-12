// @vitest-environment jsdom
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { usePayloadReveal } from "@/lib/payload-reveal";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ephemeral payload reveal", () => {
  it("fetches only on demand and clears plaintext on close and job navigation", async () => {
    const fetchMock = vi.fn(
      async () =>
        new Response(
          JSON.stringify({ plaintext: btoa('{"secret":"visible"}') }),
          {
            headers: { "content-type": "application/json" },
            status: 200,
          }
        )
    );
    vi.stubGlobal("fetch", fetchMock);

    const { result, rerender, unmount } = renderHook(
      ({ id, open }) => usePayloadReveal(id, open),
      { initialProps: { id: "job-a", open: true } }
    );
    expect(fetchMock).not.toHaveBeenCalled();
    await act(async () => result.current.reveal());
    await waitFor(() => expect(result.current.revealed?.format).toBe("JSON"));
    expect(result.current.revealed?.content).toContain('"secret": "visible"');

    rerender({ id: "job-a", open: false });
    await waitFor(() => expect(result.current.revealed).toBeNull());
    rerender({ id: "job-b", open: true });
    expect(result.current.revealed).toBeNull();
    unmount();
  });

  it("does not retain a late response after the drawer closes", async () => {
    let resolveResponse: ((response: Response) => void) | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(
        () =>
          new Promise<Response>((resolve) => {
            resolveResponse = resolve;
          })
      )
    );
    const { result, rerender } = renderHook(
      ({ open }) => usePayloadReveal("job-a", open),
      { initialProps: { open: true } }
    );
    let request: Promise<void> | undefined;
    act(() => {
      request = result.current.reveal();
    });
    rerender({ open: false });
    resolveResponse?.(
      new Response(JSON.stringify({ plaintext: btoa("late secret") }), {
        status: 200,
      })
    );
    await act(async () => request);
    expect(result.current.revealed).toBeNull();
  });
});
