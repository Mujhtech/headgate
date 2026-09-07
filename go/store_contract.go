package headgate

import (
	"slices"

	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- the store port (store port boundary) ----------

// Fields that an enqueue conflict may replace on its current holder.
const (
	UniqueReplacePayload     uint32 = 1 << 0
	UniqueReplaceScheduledAt uint32 = 1 << 1
	UniqueReplacePriority    uint32 = 1 << 2
	UniqueReplaceMaxAttempts uint32 = 1 << 3
	UniqueReplaceAll                = UniqueReplacePayload | UniqueReplaceScheduledAt | UniqueReplacePriority | UniqueReplaceMaxAttempts
)

// Envelope is the portable job representation exchanged with a Store.
type Envelope struct {
	ID, Kind, Queue         string
	SchemaVersion           uint32
	Payload                 []byte
	PartitionKey, RateClass string
	// Weight is the estimated rate-budget cost. Zero is the backward-compatible
	// omitted value and is normalized to one at every store boundary.
	Weight                             uint32
	Fingerprint                        string
	Priority                           int32
	Attempt, CrashAttempt, MaxAttempts uint32
	EnqueuedAtMs, ScheduledAtMs        int64
	TimeoutMs, DeadlineMs              int64
	UniqueKey                          []byte
	UniqueStates                       uint32
	// UniqueWindowMs selects the job uniqueness mode. 0 = LIFECYCLE: one live job per
	// key, released by terminal state. > 0 = THROTTLE: at most one per window, released
	// by the clock. A caller-side duration that rounds to zero must be REJECTED (boundary validation),
	// never clamped into lifecycle mode.
	UniqueWindowMs int64
	// UniqueReplace is the request-only allowlist used when UniqueKey conflicts. It is
	// not persisted. Replacement is restricted to single-job enqueue calls.
	UniqueReplace uint32
	// UniqueDebounceMs is a trailing-edge store-clock debounce window. It requires
	// UniqueKey and reschedules the holder on every conflict.
	UniqueDebounceMs int64
	// UniqueExcludeKind removes Kind from the effective uniqueness key.
	UniqueExcludeKind bool
	// RetentionMs is how long a successful job's row is kept. 0 means DELETE on
	// completion (retention policy) — ephemeral, not keep-forever.
	RetentionMs int64
	// PeriodicScheduleID and PeriodicTickMs are typed durable origin. Empty/zero means
	// an ordinary enqueue; both fields are set together.
	PeriodicScheduleID string
	PeriodicTickMs     int64
	// Headers is opaque caller metadata carried with the job (proto field 20). The
	// store never interprets these bytes — it round-trips them. Two keys are RESERVED:
	// TraceparentHeader and TracestateHeader for W3C Trace Context.
	Headers map[string]string
	// Tags are canonical operator-indexed labels, separate from opaque headers.
	Tags []string
	// Pending inserts durably but cannot be admitted until PromoteJob.
	Pending bool
	// StickyWorker is the exact stable worker identity allowed to claim this job.
	// Empty means any worker. Stores enforce it inside atomic admission.
	StickyWorker string
}

// EffectiveWeight turns proto3 omission and Go's zero-value struct literal into the
// documented default cost of one. HTTP APIs reject an explicitly supplied zero; the
// store accepts zero only as the compatibility sentinel.
func EffectiveWeight(weight uint32) uint32 {
	return headgateshared.EffectiveWeight(weight)
}

// EffectiveSchemaVersion returns version or the default schema version when it is zero.
func EffectiveSchemaVersion(version uint32) uint32 {
	return headgateshared.EffectiveSchemaVersion(version)
}

// EffectiveMaxAttempts returns maxAttempts or its default when it is zero.
func EffectiveMaxAttempts(maxAttempts uint32) uint32 {
	return headgateshared.EffectiveMaxAttempts(maxAttempts)
}

// EffectiveUniqueKey returns the versioned store key. Kind is included by default;
// ExcludeKind uses a separate namespace so the two scopes cannot alias.
func EffectiveUniqueKey(e Envelope) []byte {
	// nil means uniqueness is disabled; a non-nil zero-length key is still an
	// explicit key. The API preserves that distinction (JSON "" decodes to []byte{}),
	// and collapsing it would make Go accept duplicates that Rust rejects.
	if e.UniqueKey == nil {
		return nil
	}
	out := make([]byte, 0, len(e.UniqueKey)+len(e.Kind)+7)
	out = append(out, 1)
	if e.UniqueExcludeKind {
		out = append(out, 'G')
	} else {
		out = append(out, 'K', byte(len(e.Kind)>>24), byte(len(e.Kind)>>16), byte(len(e.Kind)>>8), byte(len(e.Kind)))
		out = append(out, e.Kind...)
	}
	out = append(out, e.UniqueKey...)
	return out
}

// CanonicalTags returns a sorted, deduplicated copy of valid operator tags.
func CanonicalTags(tags []string) []string {
	out := append([]string(nil), tags...)
	slices.Sort(out)
	return slices.Compact(out)
}
