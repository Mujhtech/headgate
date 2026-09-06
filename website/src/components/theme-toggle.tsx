import { Moon, Sun } from "lucide-react";
import { useCallback, useEffect, useState } from "react";

const storageKey = "headgate-website-theme";
type Theme = "light" | "dark";

export function ThemeToggle() {
  const [theme, setTheme] = useState<Theme | null>(null);
  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    function sync() {
      let saved: string | null = null;
      try {
        saved = localStorage.getItem(storageKey);
      } catch {
        /* Storage may be disabled. */
      }
      const systemTheme = media.matches ? "dark" : "light";
      const resolved =
        saved === "light" || saved === "dark" ? saved : systemTheme;
      document.documentElement.dataset.theme = resolved;
      setTheme(resolved);
    }
    sync();
    media.addEventListener("change", sync);
    window.addEventListener("storage", sync);
    return () => {
      media.removeEventListener("change", sync);
      window.removeEventListener("storage", sync);
    };
  }, []);
  const toggle = useCallback(() => {
    const next =
      document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    setTheme(next);
    try {
      localStorage.setItem(storageKey, next);
    } catch {
      /* Keep the in-memory choice usable. */
    }
  }, []);
  return (
    <button
      aria-label={
        theme
          ? `Switch to ${theme === "dark" ? "light" : "dark"} mode`
          : "Switch color theme"
      }
      className="theme-toggle"
      onClick={toggle}
      title="Switch color theme"
      type="button"
    >
      <Sun aria-hidden="true" className="theme-icon-sun" size={17} />
      <Moon aria-hidden="true" className="theme-icon-moon" size={17} />
    </button>
  );
}
