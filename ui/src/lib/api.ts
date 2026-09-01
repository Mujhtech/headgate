import { config } from "@/lib/config";

export class ApiError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

export async function api<T>(
  path: string,
  options: Omit<RequestInit, "body"> & { body?: BodyInit | object | null } = {}
): Promise<T> {
  const method = options.method ?? "GET";
  if (config.readOnly && method !== "GET") {
    throw new ApiError("This console is read-only.", 403);
  }

  const headers = new Headers(options.headers);
  let { body } = options;
  if (method !== "GET") {
    headers.set("Idempotency-Key", crypto.randomUUID());
    if (body != null && typeof body === "object" && !(body instanceof Blob)) {
      headers.set("Content-Type", "application/json");
      body = JSON.stringify(body);
    }
  }

  const response = await fetch(`${config.apiBase}${path}`, {
    ...options,
    body: body as BodyInit | null | undefined,
    headers,
    method,
  });
  const text = await response.text();
  let value: unknown = null;
  try {
    value = text ? JSON.parse(text) : null;
  } catch {
    value = text;
  }

  if (!response.ok) {
    const message =
      value && typeof value === "object" && "error" in value
        ? String(value.error)
        : `Request failed (${response.status})`;
    throw new ApiError(message, response.status);
  }
  return value as T;
}
