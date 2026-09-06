export const DEFAULT_POLLING_INTERVAL_MS = 15_000;
export const POLLING_INTERVAL_STORAGE_KEY =
  "headgate.console.polling-interval-ms";

export const pollingIntervalOptions = [
  { label: "Every 5 seconds", value: 5000 },
  { label: "Every 15 seconds", value: 15_000 },
  { label: "Every 30 seconds", value: 30_000 },
  { label: "Every minute", value: 60_000 },
] as const;

const pollingIntervals = new Set<number>(
  pollingIntervalOptions.map((option) => option.value)
);
const listeners = new Set<() => void>();
let memoryPollingInterval = DEFAULT_POLLING_INTERVAL_MS;
let listeningForStorage = false;

export function parsePollingInterval(value: string | null) {
  if (value === null) {
    return DEFAULT_POLLING_INTERVAL_MS;
  }
  const interval = Number(value);
  return pollingIntervals.has(interval)
    ? interval
    : DEFAULT_POLLING_INTERVAL_MS;
}

export function readPollingInterval(storage?: Pick<Storage, "getItem">) {
  if (!storage) {
    return DEFAULT_POLLING_INTERVAL_MS;
  }
  try {
    return parsePollingInterval(storage.getItem(POLLING_INTERVAL_STORAGE_KEY));
  } catch {
    return DEFAULT_POLLING_INTERVAL_MS;
  }
}

export function writePollingInterval(
  storage: Pick<Storage, "setItem">,
  interval: number
) {
  const value = pollingIntervals.has(interval)
    ? interval
    : DEFAULT_POLLING_INTERVAL_MS;
  storage.setItem(POLLING_INTERVAL_STORAGE_KEY, String(value));
  return value;
}

export function getPollingIntervalSnapshot() {
  if (typeof window === "undefined") {
    return memoryPollingInterval;
  }
  try {
    memoryPollingInterval = parsePollingInterval(
      window.localStorage.getItem(POLLING_INTERVAL_STORAGE_KEY)
    );
  } catch {
    // Browser privacy settings may deny access; retain the in-memory choice.
  }
  return memoryPollingInterval;
}

function notifyPollingIntervalListeners() {
  for (const listener of listeners) {
    listener();
  }
}

function handleStorage(event: StorageEvent) {
  if (event.key !== POLLING_INTERVAL_STORAGE_KEY) {
    return;
  }
  memoryPollingInterval = parsePollingInterval(event.newValue);
  notifyPollingIntervalListeners();
}

export function subscribePollingInterval(listener: () => void) {
  listeners.add(listener);
  if (!listeningForStorage && typeof window !== "undefined") {
    window.addEventListener("storage", handleStorage);
    listeningForStorage = true;
  }
  return () => {
    listeners.delete(listener);
    if (listeningForStorage && listeners.size === 0) {
      window.removeEventListener("storage", handleStorage);
      listeningForStorage = false;
    }
  };
}

export function setPollingInterval(interval: number) {
  const next = pollingIntervals.has(interval)
    ? interval
    : DEFAULT_POLLING_INTERVAL_MS;
  memoryPollingInterval = next;
  try {
    writePollingInterval(window.localStorage, next);
  } catch {
    // The setting still applies for this session when storage is unavailable.
  }
  notifyPollingIntervalListeners();
  return next;
}
