package headgate

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// Fingerprint derives the crash quarantine fingerprint of (kind, payload), specified in
// ARCHITECTURE.md content fingerprinting and nowhere else:
//
//	lowercase_hex( SHA256( u32_le(len(kind)) || kind || u32_le(len(payload)) || payload )[0..16] )
//
// Length-prefixed so ("a","bc") and ("ab","c") cannot collide; truncated to 128 bits
// because a collision over-quarantines. Derived CLIENT-SIDE at enqueue when the caller
// does not supply one; stores pass the value through untouched. The Rust implementation
// must produce identical output — the content fingerprinting test vectors are the conformance scenario.
func Fingerprint(kind string, payload []byte) string {
	h := sha256.New()
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(kind)))
	h.Write(n[:])
	h.Write([]byte(kind))
	binary.LittleEndian.PutUint32(n[:], uint32(len(payload)))
	h.Write(n[:])
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil)[:16])
}
