import { useEffect, useRef, useState } from "react";

import { api } from "@/lib/api";
import { type DisplayPayload, displayPayload } from "@/lib/payload";

interface RevealResponse {
  plaintext: string;
}

interface RevealedValue {
  jobId: string;
  payload: DisplayPayload;
}

/**
 * Keeps sensitive plaintext outside React Query and any durable browser storage. The
 * value belongs only to the currently open drawer and is actively discarded when the
 * drawer closes, changes jobs, hides the value, or unmounts.
 */
export function usePayloadReveal(jobId: string | null, open: boolean) {
  const abortRef = useRef<AbortController | null>(null);
  const [value, setValue] = useState<RevealedValue | null>(null);
  const [pendingJobId, setPendingJobId] = useState<string | null>(null);
  const [failure, setFailure] = useState<{
    jobId: string;
    message: string;
  } | null>(null);

  const clear = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setValue(null);
    setFailure(null);
    setPendingJobId(null);
  };

  useEffect(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setValue((current) => (open && current?.jobId === jobId ? current : null));
    setFailure((current) =>
      open && current?.jobId === jobId ? current : null
    );
    setPendingJobId((current) => (open && current === jobId ? current : null));
    return () => abortRef.current?.abort();
  }, [jobId, open]);

  const reveal = async () => {
    if (!(jobId && open)) {
      return;
    }
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setPendingJobId(jobId);
    setFailure(null);
    try {
      const response = await api<RevealResponse>(
        `/jobs/${encodeURIComponent(jobId)}/payload/reveal`,
        { signal: controller.signal }
      );
      if (abortRef.current !== controller || controller.signal.aborted) {
        return;
      }
      setValue({ jobId, payload: displayPayload(response.plaintext) });
    } catch (reason) {
      if (abortRef.current !== controller || controller.signal.aborted) {
        return;
      }
      setFailure({
        jobId,
        message: reason instanceof Error ? reason.message : String(reason),
      });
    } finally {
      if (abortRef.current === controller) {
        abortRef.current = null;
        setPendingJobId(null);
      }
    }
  };

  return {
    clear,
    error: failure?.jobId === jobId && open ? failure.message : null,
    pending: pendingJobId === jobId && open,
    reveal,
    revealed: value?.jobId === jobId && open ? value.payload : null,
  };
}
