import { useSyncExternalStore } from "react";

const listeners = new Set<() => void>();
let interval: number | null = null;
let now = Date.now();
const relativeListeners = new Set<() => void>();
let relativeInterval: number | null = null;
let relativeNow = now;

function subscribe(listener: () => void) {
  listeners.add(listener);
  if (interval == null) {
    now = Date.now();
    interval = window.setInterval(() => {
      now = Date.now();
      for (const notify of listeners) {
        notify();
      }
    }, 1000);
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0 && interval != null) {
      window.clearInterval(interval);
      interval = null;
    }
  };
}

export function useNow() {
  return useSyncExternalStore(
    subscribe,
    () => now,
    () => now
  );
}

function subscribeRelative(listener: () => void) {
  relativeListeners.add(listener);
  if (relativeInterval == null) {
    relativeNow = Date.now();
    relativeInterval = window.setInterval(() => {
      relativeNow = Date.now();
      for (const notify of relativeListeners) {
        notify();
      }
    }, 30_000);
  }
  return () => {
    relativeListeners.delete(listener);
    if (relativeListeners.size === 0 && relativeInterval != null) {
      window.clearInterval(relativeInterval);
      relativeInterval = null;
    }
  };
}

/** A shared low-frequency clock for timestamp tables; no timer is created per cell. */
export function useRelativeNow() {
  return useSyncExternalStore(
    subscribeRelative,
    () => relativeNow,
    () => relativeNow
  );
}
