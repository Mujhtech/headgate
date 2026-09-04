// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_THEME,
  parseTheme,
  readTheme,
  resolveDarkTheme,
  setTheme,
  THEME_STORAGE_KEY,
} from "./theme";

describe("console theme", () => {
  afterEach(() => {
    document.documentElement.classList.remove("dark");
    document.documentElement.style.colorScheme = "";
    window.localStorage.clear();
  });

  it("parses persisted choices and treats unknown values as system", () => {
    expect(parseTheme("light")).toBe("light");
    expect(parseTheme("dark")).toBe("dark");
    expect(parseTheme("unknown")).toBe(DEFAULT_THEME);
    expect(parseTheme(null)).toBe(DEFAULT_THEME);
  });

  it("resolves system preference without overriding explicit choices", () => {
    expect(resolveDarkTheme("system", true)).toBe(true);
    expect(resolveDarkTheme("system", false)).toBe(false);
    expect(resolveDarkTheme("dark", false)).toBe(true);
    expect(resolveDarkTheme("light", true)).toBe(false);
  });

  it("reads the stable theme key and tolerates unavailable storage", () => {
    const getItem = vi.fn(() => "dark");
    expect(readTheme({ getItem })).toBe("dark");
    expect(getItem).toHaveBeenCalledWith(THEME_STORAGE_KEY);
    expect(
      readTheme({
        getItem: () => {
          throw new Error("storage denied");
        },
      })
    ).toBe(DEFAULT_THEME);
  });

  it("applies and persists explicit light and dark choices", () => {
    expect(setTheme("dark")).toBe("dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.style.colorScheme).toBe("dark");
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe("dark");

    expect(setTheme("light")).toBe("light");
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    expect(document.documentElement.style.colorScheme).toBe("light");
  });
});
