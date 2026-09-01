// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest"
import { createRefreshScheduler } from "./console"

describe("live refresh scheduling", () => {
  afterEach(() => vi.useRealTimers())

  it("refreshes within the fixed ceiling during a continuous event stream", async () => {
    vi.useFakeTimers()
    const refresh = vi.fn(async () => undefined)
    const scheduler = createRefreshScheduler(refresh, () => false)
    for (let elapsed = 0; elapsed < 2_000; elapsed += 100) {
      scheduler.schedule()
      await vi.advanceTimersByTimeAsync(100)
    }
    expect(refresh).toHaveBeenCalledTimes(1)
    scheduler.dispose()
  })

  it("still coalesces a quiet burst on the trailing delay", async () => {
    vi.useFakeTimers()
    const refresh = vi.fn(async () => undefined)
    const scheduler = createRefreshScheduler(refresh, () => false)
    scheduler.schedule()
    scheduler.schedule()
    await vi.advanceTimersByTimeAsync(699)
    expect(refresh).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1)
    expect(refresh).toHaveBeenCalledTimes(1)
    scheduler.dispose()
  })
})
