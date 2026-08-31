import { useSyncExternalStore } from "react"

const listeners = new Set<() => void>()
let interval: number | null = null
let now = Date.now()

function subscribe(listener: () => void) {
  listeners.add(listener)
  if (interval == null) {
    now = Date.now()
    interval = window.setInterval(() => {
      now = Date.now()
      listeners.forEach((notify) => notify())
    }, 1_000)
  }
  return () => {
    listeners.delete(listener)
    if (listeners.size === 0 && interval != null) {
      window.clearInterval(interval)
      interval = null
    }
  }
}

export function useNow() {
  return useSyncExternalStore(subscribe, () => now, () => now)
}
