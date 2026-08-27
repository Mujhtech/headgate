import { Outlet, useRouterState } from "@tanstack/react-router"
import { AlertTriangleIcon, RefreshCwIcon } from "lucide-react"
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react"

import { AppSidebar } from "@/components/app-sidebar"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar"
import { api } from "@/lib/api"
import { config } from "@/lib/config"

export interface ViewProps {
  refreshKey: number
  refresh: () => void
  notify: (message: string, tone?: "normal" | "error") => void
}

const ConsoleContext = createContext<ViewProps | null>(null)

export function useConsole() {
  const value = useContext(ConsoleContext)
  if (!value) throw new Error("useConsole must be used inside ConsoleLayout")
  return value
}

export function useApiResource<T>(path: string | null, refreshKey: number) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!path) {
      setData(null)
      setError(null)
      setLoading(false)
      return
    }
    const controller = new AbortController()
    setLoading(true)
    api<T>(path, { signal: controller.signal })
      .then((value) => {
        setData(value)
        setError(null)
      })
      .catch((reason: unknown) => {
        if (reason instanceof DOMException && reason.name === "AbortError") return
        setError(reason instanceof Error ? reason.message : String(reason))
      })
      .finally(() => setLoading(false))
    return () => controller.abort()
  }, [path, refreshKey])

  return { data, error, loading }
}

export async function mutate(
  path: string,
  init: Omit<RequestInit, "body"> & { body?: object | null } = { method: "POST" },
) {
  return api(path, { method: "POST", ...init })
}

export function Empty({ children }: { children: React.ReactNode }) {
  return <p className="py-8 text-center text-sm text-muted-foreground">{children}</p>
}

export function Loading() {
  return <div className="flex min-h-48 items-center justify-center text-muted-foreground" role="status"><RefreshCwIcon className="mr-2 size-4 animate-spin motion-reduce:animate-none" />Loading…</div>
}

export function Failure({ message }: { message: string }) {
  return <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 text-sm text-destructive" role="alert"><AlertTriangleIcon className="mr-2 inline size-4" />{message}</div>
}

function useLiveRefresh(refresh: () => void) {
  const [connected, setConnected] = useState(false)
  const [paused, setPaused] = useState(false)
  const pausedRef = useRef(false)
  const debounce = useRef<number | null>(null)

  useEffect(() => { pausedRef.current = paused }, [paused])
  useEffect(() => {
    const source = new EventSource(`${config.apiBase}/events`)
    const schedule = () => {
      if (pausedRef.current) return
      if (debounce.current) window.clearTimeout(debounce.current)
      debounce.current = window.setTimeout(refresh, 700)
    }
    source.onopen = () => setConnected(true)
    source.onerror = () => setConnected(false)
    source.onmessage = schedule
    source.addEventListener("queue_activity", schedule)
    return () => {
      source.close()
      if (debounce.current) window.clearTimeout(debounce.current)
    }
  }, [refresh])
  return { connected, paused, setPaused }
}

const titles: Record<string, string> = {
  jobs: "Jobs",
  workflows: "Workflows",
  queues: "Queues",
  "rate-classes": "Rate classes",
  quarantine: "Quarantine",
  periodic: "Periodic schedules",
  workers: "Workers",
}

export function ConsoleLayout() {
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const [refreshKey, setRefreshKey] = useState(0)
  const [notice, setNotice] = useState<{ message: string; tone: "normal" | "error" } | null>(null)
  const refresh = useCallback(() => setRefreshKey((value) => value + 1), [])
  const live = useLiveRefresh(refresh)
  const section = pathname.split("/").filter(Boolean)[0] ?? "queues"

  useEffect(() => {
    const timer = window.setInterval(() => { if (!live.paused) refresh() }, 15_000)
    return () => window.clearInterval(timer)
  }, [live.paused, refresh])

  const notify = useCallback((message: string, tone: "normal" | "error" = "normal") => {
    setNotice({ message, tone })
    window.setTimeout(() => setNotice(null), 3_500)
  }, [])
  const context = useMemo(() => ({ refreshKey, refresh, notify }), [refreshKey, refresh, notify])

  return (
    <ConsoleContext.Provider value={context}>
      <a href="#main-content" className="sr-only fixed left-3 top-3 z-[100] rounded-md bg-background px-3 py-2 focus:not-sr-only">Skip to content</a>
      <SidebarProvider>
        <AppSidebar />
        <SidebarInset>
          <header className="sticky top-0 z-30 flex h-14 items-center gap-3 border-b bg-background/95 px-4 backdrop-blur">
            <SidebarTrigger aria-label="Toggle navigation" />
            <p className="min-w-0 flex-1 truncate text-sm font-medium">{titles[section] ?? "headgate"}</p>
            {config.readOnly && <Badge variant="outline">read-only</Badge>}
            <Button variant="ghost" size="sm" onClick={() => live.setPaused(!live.paused)} aria-pressed={live.paused}>
              <span className={`size-2 rounded-full ${live.connected && !live.paused ? "bg-success" : "bg-muted-foreground"}`} />
              {live.paused ? "Resume updates" : live.connected ? "Live" : "Polling"}
            </Button>
            <Button variant="ghost" size="icon" onClick={refresh} aria-label="Refresh data"><RefreshCwIcon /></Button>
          </header>
          <main id="main-content" className="flex-1 scroll-mt-16 p-4 lg:p-6"><Outlet /></main>
        </SidebarInset>
      </SidebarProvider>
      <div aria-live="polite" className={`fixed bottom-4 left-1/2 z-[70] -translate-x-1/2 rounded-lg px-4 py-2 text-sm shadow-lg ${notice?.tone === "error" ? "bg-destructive text-white" : "bg-foreground text-background"} ${notice ? "block" : "hidden"}`}>{notice?.message}</div>
    </ConsoleContext.Provider>
  )
}
