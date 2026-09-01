import {
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { Outlet, useRouterState } from "@tanstack/react-router";
import { AlertTriangleIcon, RefreshCwIcon } from "lucide-react";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { AppSidebar } from "@/components/app-sidebar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
} from "@/components/ui/sidebar";
import { api } from "@/lib/api";
import { config } from "@/lib/config";

export interface ViewProps {
  notify: (message: string, tone?: "normal" | "error") => void;
  refresh: () => Promise<void>;
}

interface ConsoleValue extends ViewProps {
  livePaused: boolean;
}

const ConsoleContext = createContext<ConsoleValue | null>(null);

export function useConsole() {
  const value = useContext(ConsoleContext);
  if (!value) {
    throw new Error("useConsole must be used inside ConsoleLayout");
  }
  return value;
}

export function useConsoleQuery<T>(
  queryKey: readonly unknown[],
  queryFn: (signal: AbortSignal) => Promise<T>,
  enabled = true,
  refetchInterval = 15_000
) {
  const context = useContext(ConsoleContext);
  return useQuery({
    enabled,
    queryFn: ({ signal }) => queryFn(signal),
    queryKey,
    refetchInterval: context?.livePaused ? false : refetchInterval,
  });
}

export function useConsoleQueries<T>(
  queries: Array<{
    queryKey: readonly unknown[];
    queryFn: (signal: AbortSignal) => Promise<T>;
    enabled?: boolean;
  }>
) {
  const context = useContext(ConsoleContext);
  return useQueries({
    queries: queries.map((query) => ({
      enabled: query.enabled ?? true,
      queryFn: ({ signal }: { signal: AbortSignal }) => query.queryFn(signal),
      queryKey: query.queryKey,
      refetchInterval: context?.livePaused ? false : 15_000,
    })),
  });
}

export function useApiResource<T>(path: string | null) {
  const query = useConsoleQuery(
    ["api", path],
    (signal) =>
      path === null
        ? Promise.reject(new Error("API path is required."))
        : api<T>(path, { signal }),
    Boolean(path)
  );
  return {
    data: query.data ?? null,
    error: query.error
      ? query.error instanceof Error
        ? query.error.message
        : String(query.error)
      : null,
    loading: query.isPending,
  };
}

interface ApiMutationRequest {
  body?: BodyInit | object | null;
  method?: string;
  path: string;
}

export function useApiMutation<T = unknown>() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ path, method = "POST", body }: ApiMutationRequest) =>
      api<T>(path, { body, method }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["api"] }),
  });
}

export function Empty({ children }: { children: React.ReactNode }) {
  return (
    <p className="py-8 text-center text-muted-foreground text-sm">{children}</p>
  );
}

export function Loading() {
  return (
    <div
      className="flex min-h-48 items-center justify-center text-muted-foreground"
      role="status"
    >
      <RefreshCwIcon className="mr-2 size-4 animate-spin motion-reduce:animate-none" />
      Loading…
    </div>
  );
}

export function Failure({ message }: { message: string }) {
  return (
    <div
      className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 text-destructive text-sm"
      role="alert"
    >
      <AlertTriangleIcon className="mr-2 inline size-4" />
      {message}
    </div>
  );
}

function useLiveRefresh(refresh: () => Promise<void>) {
  const [connected, setConnected] = useState(false);
  const [paused, setPausedState] = useState(false);
  const pausedRef = useRef(false);
  const debounce = useRef<number | null>(null);
  const maxWait = useRef<number | null>(null);

  const setPaused = useCallback((next: boolean) => {
    pausedRef.current = next;
    setPausedState(next);
  }, []);
  useEffect(() => {
    const source = new EventSource(`${config.apiBase}/events`);
    const scheduler = createRefreshScheduler(
      refresh,
      () => pausedRef.current,
      debounce,
      maxWait
    );
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = scheduler.schedule;
    source.addEventListener("queue_activity", scheduler.schedule);
    return () => {
      source.close();
      scheduler.dispose();
    };
  }, [refresh]);
  return { connected, paused, setPaused };
}

export function createRefreshScheduler(
  refresh: () => Promise<void>,
  isPaused: () => boolean,
  debounce = { current: null as number | null },
  maxWait = { current: null as number | null }
) {
  const run = () => {
    if (debounce.current !== null) {
      window.clearTimeout(debounce.current);
    }
    if (maxWait.current !== null) {
      window.clearTimeout(maxWait.current);
    }
    debounce.current = null;
    maxWait.current = null;
    void refresh();
  };
  return {
    dispose: () => {
      if (debounce.current !== null) {
        window.clearTimeout(debounce.current);
      }
      if (maxWait.current !== null) {
        window.clearTimeout(maxWait.current);
      }
      debounce.current = null;
      maxWait.current = null;
    },
    schedule: () => {
      if (isPaused()) {
        return;
      }
      if (debounce.current !== null) {
        window.clearTimeout(debounce.current);
      }
      debounce.current = window.setTimeout(run, 700);
      // The trailing debounce coalesces bursts; this fixed ceiling prevents a stream
      // that never goes quiet from postponing refresh forever.
      if (maxWait.current === null) {
        maxWait.current = window.setTimeout(run, 2000);
      }
    },
  };
}

const titles: Record<string, string> = {
  jobs: "Jobs",
  overview: "Overview",
  periodic: "Periodic schedules",
  quarantine: "Quarantine",
  queues: "Queues",
  "rate-classes": "Rate classes",
  workers: "Workers",
  workflows: "Workflows",
};

export function ConsoleLayout() {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const [notice, setNotice] = useState<{
    message: string;
    tone: "normal" | "error";
  } | null>(null);
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const noticeTimeout = useRef<number | null>(null);
  const queryClient = useQueryClient();
  const refresh = useCallback(
    () => queryClient.refetchQueries({ queryKey: ["api"], type: "active" }),
    [queryClient]
  );
  const live = useLiveRefresh(refresh);
  const section = pathname.split("/").find(Boolean) ?? "queues";

  const notify = useCallback(
    (message: string, tone: "normal" | "error" = "normal") => {
      if (noticeTimeout.current !== null) {
        window.clearTimeout(noticeTimeout.current);
      }
      setNotice({ message, tone });
      noticeTimeout.current = window.setTimeout(() => setNotice(null), 3500);
    },
    []
  );
  const manualRefresh = useCallback(async () => {
    if (manualRefreshing) {
      return;
    }
    setManualRefreshing(true);
    try {
      await Promise.all([
        refresh(),
        new Promise((resolve) => window.setTimeout(resolve, 400)),
      ]);
      notify("Data refreshed");
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    } finally {
      setManualRefreshing(false);
    }
  }, [manualRefreshing, notify, refresh]);
  const context = useMemo(
    () => ({ livePaused: live.paused, notify, refresh }),
    [refresh, notify, live.paused]
  );

  return (
    <ConsoleContext.Provider value={context}>
      <a
        className="sr-only fixed top-3 left-3 z-100 rounded-md bg-background px-3 py-2 focus:not-sr-only"
        href="#main-content"
      >
        Skip to content
      </a>
      <SidebarProvider>
        <AppSidebar />
        <SidebarInset>
          <header className="sticky top-0 z-30 flex h-14 shrink-0 items-center gap-3 border-b bg-background/95 px-4 backdrop-blur">
            <SidebarTrigger aria-label="Toggle navigation" />
            <p className="min-w-0 flex-1 truncate font-medium text-sm">
              {titles[section] ?? "headgate"}
            </p>
            {config.readOnly && <Badge variant="outline">read-only</Badge>}
            <Button
              aria-pressed={live.paused}
              onClick={() => live.setPaused(!live.paused)}
              size="sm"
              variant="ghost"
            >
              <span
                className={`size-2 rounded-full ${live.connected && !live.paused ? "bg-success" : "bg-muted-foreground"}`}
              />
              {live.paused
                ? "Resume updates"
                : live.connected
                  ? "Live"
                  : "Polling"}
            </Button>
            <Button
              aria-busy={manualRefreshing || undefined}
              aria-label={manualRefreshing ? "Refreshing data" : "Refresh data"}
              disabled={manualRefreshing}
              onClick={() => void manualRefresh()}
              size="icon"
              title={manualRefreshing ? "Refreshing…" : "Refresh data"}
              variant="ghost"
            >
              <RefreshCwIcon
                className={
                  manualRefreshing
                    ? "animate-spin motion-reduce:animate-none"
                    : ""
                }
              />
            </Button>
          </header>
          <main className="flex-1 scroll-mt-16 p-4 lg:p-6" id="main-content">
            <Outlet />
          </main>
        </SidebarInset>
      </SidebarProvider>
      <div
        aria-live="polite"
        className={`pointer-events-none fixed bottom-4 left-1/2 z-70 -translate-x-1/2 rounded-lg px-4 py-2 text-sm shadow-lg transition-[opacity,transform] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] motion-reduce:transition-none ${notice?.tone === "error" ? "bg-destructive text-white" : "bg-foreground text-background"} ${notice ? "translate-y-0 opacity-100" : "translate-y-2 opacity-0"}`}
      >
        {notice?.message}
      </div>
    </ConsoleContext.Provider>
  );
}
