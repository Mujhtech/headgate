import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Empty, Failure, Loading, mutate, useApiResource, type ViewProps } from "@/console"
import { config } from "@/lib/config"
import { formatDate } from "@/lib/format"

interface QuarantineEntry { fingerprint: string; kind: string; crash_count: number; quarantined_at_ms: number; reason: string }

export function QuarantineView({ refreshKey, refresh, notify }: ViewProps) {
  const { data, error, loading } = useApiResource<{ quarantine?: QuarantineEntry[] } | QuarantineEntry[]>("/quarantine", refreshKey)
  const entries = Array.isArray(data) ? data : data?.quarantine ?? []
  const release = async (entry: QuarantineEntry) => {
    if (!window.confirm(`Release ${entry.fingerprint}? It will be quarantined again if the crash threshold is reached.`)) return
    try { await mutate(`/quarantine/${encodeURIComponent(entry.fingerprint)}`, { method: "DELETE" }); notify("Fingerprint released"); refresh() } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") }
  }
  if (loading && !data) return <Loading />
  if (error) return <Failure message={error} />
  return <><div className="mb-4"><h1 className="text-lg font-semibold">Quarantine</h1><p className="text-sm text-muted-foreground">Poison-pill fingerprints blocked after repeated worker crashes.</p></div><Card><CardHeader><CardTitle>Quarantined fingerprints</CardTitle></CardHeader><CardContent>{entries.length ? <Table><TableHeader><TableRow><TableHead>Fingerprint</TableHead><TableHead>Kind</TableHead><TableHead>Crashes</TableHead><TableHead>Since</TableHead><TableHead>Reason</TableHead><TableHead><span className="sr-only">Actions</span></TableHead></TableRow></TableHeader><TableBody>{entries.map((entry) => <TableRow key={entry.fingerprint}><TableCell className="max-w-64 break-all font-mono text-xs">{entry.fingerprint}</TableCell><TableCell>{entry.kind}</TableCell><TableCell><Badge variant="destructive">{entry.crash_count}</Badge></TableCell><TableCell>{formatDate(entry.quarantined_at_ms)}</TableCell><TableCell>{entry.reason}</TableCell><TableCell><Button variant="outline" size="sm" disabled={config.readOnly} onClick={() => void release(entry)}>Release</Button></TableCell></TableRow>)}</TableBody></Table> : <Empty>Nothing is quarantined.</Empty>}</CardContent></Card></>
}

