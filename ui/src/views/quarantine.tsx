import { RelativeTime } from "@/components/relative-time";
import { ActionButton } from "@/components/ui/action-button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Empty,
  Failure,
  Loading,
  useApiMutation,
  useApiResource,
  type ViewProps,
} from "@/console";
import { config } from "@/lib/config";

interface QuarantineEntry {
  crash_count: number;
  fingerprint: string;
  kind: string;
  quarantined_at_ms: number;
  reason: string;
}

export function QuarantineView({ notify }: ViewProps) {
  const releaseMutation = useApiMutation();
  const { data, error, loading } = useApiResource<
    { quarantine?: QuarantineEntry[] } | QuarantineEntry[]
  >("/quarantine");
  const entries = Array.isArray(data) ? data : (data?.quarantine ?? []);
  const release = async (entry: QuarantineEntry) => {
    if (
      !window.confirm(
        `Release ${entry.fingerprint}? It will be quarantined again if the crash threshold is reached.`
      )
    ) {
      return;
    }
    try {
      await releaseMutation.mutateAsync({
        method: "DELETE",
        path: `/quarantine/${encodeURIComponent(entry.fingerprint)}`,
      });
      notify("Fingerprint released");
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    }
  };
  if (loading && !data) {
    return <Loading />;
  }
  if (error) {
    return <Failure message={error} />;
  }
  return (
    <>
      <div className="mb-4">
        <h1 className="font-semibold text-lg">Quarantine</h1>
        <p className="text-muted-foreground text-sm">
          Poison-pill fingerprints blocked after repeated worker crashes.
        </p>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Quarantined fingerprints</CardTitle>
        </CardHeader>
        <CardContent>
          {entries.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Fingerprint</TableHead>
                  <TableHead>Kind</TableHead>
                  <TableHead>Crashes</TableHead>
                  <TableHead>Since</TableHead>
                  <TableHead>Reason</TableHead>
                  <TableHead>
                    <span className="sr-only">Actions</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {entries.map((entry) => (
                  <TableRow key={entry.fingerprint}>
                    <TableCell className="max-w-64 break-all font-mono text-xs">
                      {entry.fingerprint}
                    </TableCell>
                    <TableCell>{entry.kind}</TableCell>
                    <TableCell>
                      <Badge variant="destructive">{entry.crash_count}</Badge>
                    </TableCell>
                    <TableCell>
                      <RelativeTime value={entry.quarantined_at_ms} />
                    </TableCell>
                    <TableCell>{entry.reason}</TableCell>
                    <TableCell>
                      <ActionButton
                        disabled={config.readOnly || releaseMutation.isPending}
                        onClick={() => void release(entry)}
                        pending={
                          releaseMutation.isPending &&
                          releaseMutation.variables?.path ===
                            `/quarantine/${encodeURIComponent(entry.fingerprint)}`
                        }
                        pendingLabel="Releasing…"
                        size="sm"
                        variant="outline"
                      >
                        Release
                      </ActionButton>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Empty>Nothing is quarantined.</Empty>
          )}
        </CardContent>
      </Card>
    </>
  );
}
