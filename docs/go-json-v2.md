# JSON v2 and headgate's wire contract

Go 1.27 implements the existing `encoding/json` API through the new v2 engine
with compatibility options. Headgate therefore uses that engine already.
Importing `encoding/json/v2` directly selects different defaults; it is not a
necessary step to receive the engine update.

The tests in `go/headgateshared/json_v2_test.go` execute both APIs and verify:

| Case | Compatibility API | Default v2 API |
| --- | --- | --- |
| Nil string slice | `null` | `[]` |
| Zero integer tagged `omitempty` | Omitted | Included as `0` |
| Duplicate object keys | Later value wins | Error |
| Differently cased struct field | Matches | Does not match by default |
| Invalid UTF-8 string | Replacement character | Error |

Compatibility tests also pin sorted map keys, HTML escaping, and empty-list
bytes. `jsonv2.Marshal(value, jsonv1.DefaultOptionsV1())` matches the current v1
API for these fixtures. Default v2 map ordering is not deterministic; any
canonical serializer must explicitly select deterministic ordering.

## Migration decision

Keep existing payload, checkpoint, cursor and HTTP codecs on their compatibility
contracts. Payload bytes feed `Fingerprint`, and checkpoint/header bytes and
HTTP responses participate in cross-language conformance. Changing nil handling,
field omission, escaping or map ordering can change those bytes even when an
application sees equivalent values. Strict input validation also changes which
requests and persisted payloads are accepted.

Adopt a direct v2 API only at a bounded boundary with explicit options and golden
tests. A general payload switch would require a versioned codec contract and a
cross-language migration plan, including user-defined marshalers. The fixture
tests cover the named cases, not all possible application payload types.

## An upgrade-level byte difference

An executed comparison with Go 1.27.1 found that even the compatibility API
encodes an invalid UTF-8 byte in a Go string differently under the two engines:
the default engine writes the literal replacement character, while
`GOEXPERIMENT=nojsonv2` writes `\ufffd`. Both decode to the same Unicode value,
but their byte fingerprints differ. Callers that previously serialized invalid
UTF-8 into payloads must account for this during an upgrade. The compatibility
option set preserves semantic behavior, not every historical byte spelling.

Sources: [Go 1.27 JSON changes](https://go.dev/doc/go1.27#encodingjsonv2) and
the installed Go 1.27.1 `encoding/json/v2_options.go` migration documentation.

## Verification

The full repository gate passed with the default Go 1.27.1 engine against
PostgreSQL 17, Redis 7.4, and MySQL 8.4: 1,058 admission/API assertions passed
(two announced skips), and 96 scenario assertions passed. This includes the
existing Go/Rust HTTP header, raw-body and normalized-body comparisons. The
focused JSON fixture tests also passed under `-race`. These results verify the
current corpus; they do not erase the invalid-UTF-8 byte difference above.
