import { type FormEvent, useState } from "react";
import { ActionButton } from "@/components/ui/action-button";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
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
import { formatDuration } from "@/lib/format";

interface RateClass {
  burst: number;
  jobs_waiting: number;
  limit_per_window: number;
  name: string;
  paused: boolean;
  tokens_available: number;
  window_ms: number;
}

export function RatesView({ notify }: ViewProps) {
  const { data, error, loading } = useApiResource<
    { rate_classes?: RateClass[] } | RateClass[]
  >("/rate-classes");
  const [editing, setEditing] = useState<RateClass | null | undefined>(
    undefined
  );
  const rates = Array.isArray(data) ? data : (data?.rate_classes ?? []);
  if (loading && !data) {
    return <Loading />;
  }
  if (error) {
    return <Failure message={error} />;
  }
  return (
    <>
      <div className="mb-4">
        <h1 className="font-semibold text-lg">Rate classes</h1>
        <p className="text-muted-foreground text-sm">
          Fleet-wide token budgets are enforced atomically by the store.
        </p>
      </div>
      <Card>
        <CardHeader>
          <CardTitle className="flex-1">Fleet rate classes</CardTitle>
          <Button disabled={config.readOnly} onClick={() => setEditing(null)}>
            New class
          </Button>
        </CardHeader>
        <CardContent>
          {rates.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Budget</TableHead>
                  <TableHead>Limit</TableHead>
                  <TableHead>Waiting</TableHead>
                  <TableHead>
                    <span className="sr-only">Actions</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rates.map((rate) => {
                  const percent = rate.burst
                    ? Math.max(
                        0,
                        Math.min(
                          100,
                          (rate.tokens_available * 100) / rate.burst
                        )
                      )
                    : 0;
                  return (
                    <TableRow key={rate.name}>
                      <TableCell className="font-mono text-xs">
                        {rate.name}{" "}
                        {rate.paused && (
                          <Badge variant="destructive">paused</Badge>
                        )}
                      </TableCell>
                      <TableCell className="min-w-48">
                        <div className="mb-1 text-xs">
                          {rate.tokens_available}/{rate.burst}
                        </div>
                        <Progress
                          aria-label={`${rate.name} token budget`}
                          value={percent}
                        />
                      </TableCell>
                      <TableCell>
                        {rate.limit_per_window}/{formatDuration(rate.window_ms)}
                      </TableCell>
                      <TableCell>{rate.jobs_waiting}</TableCell>
                      <TableCell>
                        <Button
                          disabled={config.readOnly}
                          onClick={() => setEditing(rate)}
                          size="sm"
                          variant="outline"
                        >
                          Edit
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          ) : (
            <Empty>No rate classes configured.</Empty>
          )}
        </CardContent>
      </Card>
      <RateDialog
        close={() => setEditing(undefined)}
        key={editing === undefined ? "closed" : (editing?.name ?? "new")}
        notify={notify}
        open={editing !== undefined}
        value={editing}
      />
    </>
  );
}

function RateDialog({
  value,
  open,
  close,
  notify,
}: {
  value: RateClass | null | undefined;
  open: boolean;
  close: () => void;
  notify: ViewProps["notify"];
}) {
  const saveMutation = useApiMutation();
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const fields = new FormData(event.currentTarget);
    const name = String(fields.get("name") ?? "").trim();
    try {
      await saveMutation.mutateAsync({
        body: {
          burst: Number(fields.get("burst")),
          limit: Number(fields.get("limit")),
          paused: fields.get("paused") === "on",
          window_ms: Number(fields.get("window")),
        },
        method: "PUT",
        path: `/rate-classes/${encodeURIComponent(name)}`,
      });
      notify("Rate class saved");
      close();
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    }
  };
  return (
    <Dialog onOpenChange={(next) => !next && close()} open={open}>
      <DialogContent className="inset-auto top-1/2 left-1/2 h-auto max-w-md -translate-x-1/2 -translate-y-1/2 rounded-xl border">
        <DialogHeader>
          <DialogTitle>
            {value ? `Edit ${value.name}` : "New rate class"}
          </DialogTitle>
          <DialogDescription>
            Configure the shared budget and its operational kill switch.
          </DialogDescription>
        </DialogHeader>
        <form className="grid gap-4" onSubmit={submit}>
          <div className="grid gap-1">
            <Label htmlFor="rate-name">Name</Label>
            <Input
              defaultValue={value?.name}
              disabled={Boolean(value)}
              id="rate-name"
              name="name"
              required
            />
          </div>
          <div className="grid gap-1">
            <Label htmlFor="rate-limit">Limit per window</Label>
            <Input
              defaultValue={value?.limit_per_window ?? 0}
              id="rate-limit"
              min="0"
              name="limit"
              required
              type="number"
            />
          </div>
          <div className="grid gap-1">
            <Label htmlFor="rate-window">Window (milliseconds)</Label>
            <Input
              defaultValue={value?.window_ms ?? 1000}
              id="rate-window"
              min="1"
              name="window"
              required
              type="number"
            />
          </div>
          <div className="grid gap-1">
            <Label htmlFor="rate-burst">Burst</Label>
            <Input
              defaultValue={value?.burst ?? 1}
              id="rate-burst"
              min="1"
              name="burst"
              required
              type="number"
            />
          </div>
          <div className="flex items-center gap-2">
            <Checkbox
              defaultChecked={value?.paused}
              id="rate-paused"
              name="paused"
            />
            <Label htmlFor="rate-paused">Paused</Label>
          </div>
          <div className="flex justify-end gap-2">
            <Button
              disabled={saveMutation.isPending}
              onClick={close}
              type="button"
              variant="outline"
            >
              Cancel
            </Button>
            <ActionButton
              pending={saveMutation.isPending}
              pendingLabel="Saving…"
              type="submit"
            >
              Save
            </ActionButton>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
