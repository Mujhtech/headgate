package headgateshared

import (
	"slices"
	"time"
)

// Shared default values and wire limits.
const (
	DefaultQueue         = "default"
	DefaultSchemaVersion = uint32(1)
	DefaultMaxAttempts   = uint32(25)
	DefaultWeight        = uint32(1)
	MaxOpaqueSchema      = uint32(1<<31 - 1)
)

// NormalizeQueues sorts and deduplicates a private copy of a queue selector.
func NormalizeQueues(queues []string) []string {
	queues = slices.Clone(queues)
	slices.Sort(queues)
	return slices.Compact(queues)
}

// DurationMillis validates the millisecond wire boundary without truncating a
// positive sub-millisecond duration or overflowing int64.
func DurationMillis(duration time.Duration) (int64, bool) {
	millis := duration.Milliseconds()
	return millis, millis > 0
}

// EffectiveQueue returns queue or the default queue when it is empty.
func EffectiveQueue(queue string) string {
	if queue == "" {
		return DefaultQueue
	}
	return queue
}

// EffectiveSchemaVersion returns version or the default schema version when it is zero.
func EffectiveSchemaVersion(version uint32) uint32 {
	if version == 0 {
		return DefaultSchemaVersion
	}
	return version
}

// EffectiveMaxAttempts returns maxAttempts or its default when it is zero.
func EffectiveMaxAttempts(maxAttempts uint32) uint32 {
	if maxAttempts == 0 {
		return DefaultMaxAttempts
	}
	return maxAttempts
}

// EffectiveWeight returns weight or its default when it is zero.
func EffectiveWeight(weight uint32) uint32 {
	if weight == 0 {
		return DefaultWeight
	}
	return weight
}

// AckValidation classifies acknowledgement requests before they reach a store.
type AckValidation uint8

// Acknowledgement validation results.
const (
	AckValid AckValidation = iota
	AckLeaseLost
	AckSnoozeDelayRequired
)

// ValidateAck validates outcome-specific acknowledgement requirements.
func ValidateAck(outcome Outcome, delayMs int64) AckValidation {
	if outcome == OutcomeLeaseLost {
		return AckLeaseLost
	}
	if outcome == OutcomeSnooze && delayMs <= 0 {
		return AckSnoozeDelayRequired
	}
	return AckValid
}

// OpaqueSchemaValidation classifies schema versions carried without typed decoding.
type OpaqueSchemaValidation uint8

// Opaque schema validation results.
const (
	OpaqueSchemaValid OpaqueSchemaValidation = iota
	OpaqueSchemaZero
	OpaqueSchemaTooLarge
)

// ValidateOpaqueSchema validates a schema version against the portable wire range.
func ValidateOpaqueSchema(version uint32) OpaqueSchemaValidation {
	switch {
	case version == 0:
		return OpaqueSchemaZero
	case version > MaxOpaqueSchema:
		return OpaqueSchemaTooLarge
	default:
		return OpaqueSchemaValid
	}
}
