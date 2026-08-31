import { describe, expect, it } from "vitest"

import { displayPayload } from "@/lib/payload"

function base64(value: string) {
  return btoa(value)
}

describe("payload display", () => {
  it("formats JSON payloads for operators", () => {
    expect(displayPayload(base64('{"invoice_id":"inv-42"}'))).toEqual({
      content: '{\n  "invoice_id": "inv-42"\n}',
      format: "JSON",
    })
  })

  it("shows UTF-8 text without JSON quoting", () => {
    expect(displayPayload(base64("send the report"))).toEqual({
      content: "send the report",
      format: "Text",
    })
  })

  it("keeps malformed or binary input as base64", () => {
    expect(displayPayload("not base64!")).toEqual({
      content: "not base64!",
      format: "Base64",
    })
  })

  it("recognizes the versioned encrypted envelope without decrypting it", () => {
    const key = "2026-08"
    const bytes = new Uint8Array(5 + 2 + key.length + 12 + 16)
    bytes.set([72, 71, 69, 67, 1, 0, key.length])
    bytes.set(new TextEncoder().encode(key), 7)
    const encoded = btoa(String.fromCharCode(...bytes))

    expect(displayPayload(encoded)).toEqual({
      content: encoded,
      format: "Encrypted",
      encrypted: { keyId: key, version: 1 },
    })
  })
})
