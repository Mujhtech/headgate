import { config } from "@/lib/config"

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
  }
}

export async function api<T>(
  path: string,
  options: Omit<RequestInit, "body"> & { body?: BodyInit | object | null } = {},
): Promise<T> {
  const method = options.method ?? "GET"
  if (config.readOnly && method !== "GET") {
    throw new ApiError("This console is read-only.", 403)
  }

  const headers = new Headers(options.headers)
  let body = options.body
  if (method !== "GET") {
    headers.set("Idempotency-Key", crypto.randomUUID())
    if (body != null && typeof body === "object" && !(body instanceof Blob)) {
      headers.set("Content-Type", "application/json")
      body = JSON.stringify(body)
    }
  }

  const response = await fetch(`${config.apiBase}${path}`, {
    ...options,
    method,
    headers,
    body: body as BodyInit | null | undefined,
  })
  const text = await response.text()
  let value: unknown = null
  try {
    value = text ? JSON.parse(text) : null
  } catch {
    value = text
  }

  if (!response.ok) {
    const message =
      value && typeof value === "object" && "error" in value
        ? String(value.error)
        : `Request failed (${response.status})`
    throw new ApiError(message, response.status)
  }
  return value as T
}
