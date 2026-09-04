export interface DisplayPayload {
  content: string;
  encrypted?: {
    keyId: string;
    version: number;
  };
  format: "JSON" | "Text" | "Base64" | "Encrypted";
}

export function displayPayload(payload: string): DisplayPayload {
  try {
    const bytes = Uint8Array.from(atob(payload), (character) =>
      character.charCodeAt(0)
    );
    if (
      bytes.length >= 7 &&
      bytes[0] === 72 &&
      bytes[1] === 71 &&
      bytes[2] === 69 &&
      bytes[3] === 67
    ) {
      const [, , , , version, keyLengthHigh, keyLengthLow] = bytes;
      const keyLength = keyLengthHigh * 256 + keyLengthLow;
      const keyEnd = 7 + keyLength;
      if (version === 1 && keyLength > 0 && keyEnd + 12 + 16 <= bytes.length) {
        const keyId = new TextDecoder("utf-8", { fatal: true }).decode(
          bytes.slice(7, keyEnd)
        );
        return {
          content: payload,
          encrypted: { keyId, version },
          format: "Encrypted",
        };
      }
    }
    const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    try {
      return {
        content: JSON.stringify(JSON.parse(text), null, 2),
        format: "JSON",
      };
    } catch {
      return { content: text, format: "Text" };
    }
  } catch {
    return { content: payload, format: "Base64" };
  }
}
