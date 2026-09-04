// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { AttemptLogs } from "@/components/attempt-logs";
import { LOG_PREFIX } from "@/lib/attempt-log";

afterEach(cleanup);

it("shows legacy and structured logs, filters levels, and exposes fields", () => {
  render(
    <AttemptLogs
      logs={[
        "Legacy message",
        `${LOG_PREFIX}${JSON.stringify({ fields: { file_id: "abc" }, level: "error", message: "Download failed", truncated: true })}`,
      ]}
    />
  );
  expect(screen.getByText("Legacy message")).toBeTruthy();
  expect(screen.getByText("Download failed")).toBeTruthy();
  expect(screen.getByText("Fields")).toBeTruthy();
  expect(screen.getByText("Truncated")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "ERROR" }));
  expect(screen.queryByText("Legacy message")).toBeNull();
  expect(screen.getByText("Download failed")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "DEBUG" }));
  expect(screen.getByText("No debug logs in this attempt.")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "All logs" }));
  expect(screen.getByText("Legacy message")).toBeTruthy();
});
