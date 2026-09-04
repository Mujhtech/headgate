import {
  AlertTriangleIcon,
  BugIcon,
  CircleAlertIcon,
  InfoIcon,
} from "lucide-react";
import { useState } from "react";
import { RelativeTime } from "@/components/relative-time";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { type LogLevel, logLevels, parseAttemptLog } from "@/lib/attempt-log";

const icons = {
  debug: BugIcon,
  error: CircleAlertIcon,
  info: InfoIcon,
  warn: AlertTriangleIcon,
};
const colors = {
  debug: "text-muted-foreground",
  error: "text-destructive",
  info: "text-blue-600 dark:text-blue-400",
  warn: "text-amber-700 dark:text-amber-400",
};

export function AttemptLogs({ logs }: { logs: string[] }) {
  const [level, setLevel] = useState<LogLevel | "all">("all");
  const entries = logs.map((line, index) => ({
    ...parseAttemptLog(line),
    id: index,
  }));
  const visible = entries.filter(
    (entry) => level === "all" || entry.level === level
  );
  return (
    <section aria-label="Attempt logs" className="mt-3 space-y-2">
      <fieldset
        aria-label="Filter logs by level"
        className="flex flex-wrap items-center gap-1"
      >
        {(["all", ...logLevels] as const).map((item) => (
          <Button
            aria-pressed={level === item}
            key={item}
            onClick={() => setLevel(item)}
            size="sm"
            variant={level === item ? "secondary" : "ghost"}
          >
            {item === "all" ? "All logs" : item.toUpperCase()}
          </Button>
        ))}
        <span className="text-muted-foreground text-xs">
          {visible.length} / {entries.length}
        </span>
      </fieldset>
      <ol className="max-h-80 space-y-2 overflow-auto rounded-lg bg-muted/50 p-3">
        {visible.map((entry) => {
          const Icon = icons[entry.level];
          return (
            <li className="space-y-1 text-xs" key={entry.id}>
              <div className="flex flex-wrap items-center gap-2">
                <Badge className={colors[entry.level]} variant="outline">
                  <Icon aria-hidden="true" className="size-3" />
                  {entry.level.toUpperCase()}
                </Badge>
                {entry.at_ms === undefined ? null : (
                  <RelativeTime
                    className="text-muted-foreground"
                    value={entry.at_ms}
                  />
                )}
                {entry.truncated ? (
                  <span className="text-muted-foreground">Truncated</span>
                ) : null}
              </div>
              <p className="whitespace-pre-wrap break-words font-mono">
                {entry.message}
              </p>
              {entry.fields && Object.keys(entry.fields).length > 0 ? (
                <details>
                  <summary className="cursor-pointer text-muted-foreground">
                    Fields
                  </summary>
                  <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-words">
                    {JSON.stringify(entry.fields, null, 2)}
                  </pre>
                </details>
              ) : null}
            </li>
          );
        })}
      </ol>
      {visible.length === 0 ? (
        <p className="text-muted-foreground text-xs">
          No {level} logs in this attempt.
        </p>
      ) : null}
    </section>
  );
}
