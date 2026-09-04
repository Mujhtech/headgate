export type ConsoleTheme = "system" | "light" | "dark";

export const DEFAULT_THEME: ConsoleTheme = "system";
export const THEME_STORAGE_KEY = "headgate.console.theme";

export const themeOptions: Array<{ label: string; value: ConsoleTheme }> = [
  { label: "System", value: "system" },
  { label: "Light", value: "light" },
  { label: "Dark", value: "dark" },
];

const listeners = new Set<() => void>();
let memoryTheme: ConsoleTheme = DEFAULT_THEME;
let listening = false;
let mediaQuery: MediaQueryList | null = null;

export function parseTheme(value: string | null): ConsoleTheme {
  return value === "light" || value === "dark" ? value : DEFAULT_THEME;
}

export function readTheme(storage?: Pick<Storage, "getItem">) {
  if (!storage) {
    return DEFAULT_THEME;
  }
  try {
    return parseTheme(storage.getItem(THEME_STORAGE_KEY));
  } catch {
    return DEFAULT_THEME;
  }
}

export function resolveDarkTheme(theme: ConsoleTheme, prefersDark: boolean) {
  return theme === "dark" || (theme === "system" && prefersDark);
}

function applyTheme(theme: ConsoleTheme) {
  if (typeof document === "undefined" || typeof window === "undefined") {
    return;
  }
  const dark = resolveDarkTheme(
    theme,
    typeof window.matchMedia === "function" &&
      window.matchMedia("(prefers-color-scheme: dark)").matches
  );
  document.documentElement.classList.toggle("dark", dark);
  document.documentElement.style.colorScheme = dark ? "dark" : "light";
  const themeColor = document.querySelector<HTMLMetaElement>(
    'meta[name="theme-color"]'
  );
  if (themeColor) {
    themeColor.content = dark ? "#191b20" : "#fafafa";
  }
}

function notifyThemeListeners() {
  for (const listener of listeners) {
    listener();
  }
}

function handleStorage(event: StorageEvent) {
  if (event.key !== THEME_STORAGE_KEY) {
    return;
  }
  memoryTheme = parseTheme(event.newValue);
  applyTheme(memoryTheme);
  notifyThemeListeners();
}

function handleSystemThemeChange() {
  if (memoryTheme === "system") {
    applyTheme(memoryTheme);
  }
}

export function getThemeSnapshot() {
  if (typeof window === "undefined") {
    return memoryTheme;
  }
  try {
    memoryTheme = parseTheme(window.localStorage.getItem(THEME_STORAGE_KEY));
  } catch {
    // Browser privacy settings may deny access; retain the in-memory choice.
  }
  return memoryTheme;
}

export function subscribeTheme(listener: () => void) {
  listeners.add(listener);
  if (!listening && typeof window !== "undefined") {
    window.addEventListener("storage", handleStorage);
    if (typeof window.matchMedia === "function") {
      mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
      mediaQuery.addEventListener("change", handleSystemThemeChange);
    }
    listening = true;
    applyTheme(getThemeSnapshot());
  }
  return () => {
    listeners.delete(listener);
    if (listening && listeners.size === 0) {
      window.removeEventListener("storage", handleStorage);
      mediaQuery?.removeEventListener("change", handleSystemThemeChange);
      mediaQuery = null;
      listening = false;
    }
  };
}

export function setTheme(theme: ConsoleTheme) {
  memoryTheme = parseTheme(theme);
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, memoryTheme);
  } catch {
    // The setting still applies for this session when storage is unavailable.
  }
  applyTheme(memoryTheme);
  notifyThemeListeners();
  return memoryTheme;
}

// This runs in <head> before the console paints, preventing a theme flash.
export const themeBootstrapScript = `(()=>{let t;try{t=localStorage.getItem("${THEME_STORAGE_KEY}")}catch{}try{const p=typeof matchMedia==="function"&&matchMedia("(prefers-color-scheme: dark)").matches;const d=t==="dark"||(t!=="light"&&p);document.documentElement.classList.toggle("dark",d);document.documentElement.style.colorScheme=d?"dark":"light";const m=document.querySelector('meta[name="theme-color"]');if(m)m.content=d?"#191b20":"#fafafa"}catch{}})();`;
