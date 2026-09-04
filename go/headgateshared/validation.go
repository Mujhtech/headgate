package headgateshared

import (
	"slices"
	"time"
)

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

func EffectiveQueue(queue string) string {
	if queue == "" {
		return DefaultQueue
	}
	return queue
}

func EffectiveSchemaVersion(version uint32) uint32 {
	if version == 0 {
		return DefaultSchemaVersion
	}
	return version
}

func EffectiveMaxAttempts(maxAttempts uint32) uint32 {
	if maxAttempts == 0 {
		return DefaultMaxAttempts
	}
	return maxAttempts
}

func EffectiveWeight(weight uint32) uint32 {
	if weight == 0 {
		return DefaultWeight
	}
	return weight
}

type AckValidation uint8

const (
	AckValid AckValidation = iota
	AckLeaseLost
	AckSnoozeDelayRequired
)

func ValidateAck(outcome Outcome, delayMs int64) AckValidation {
	if outcome == OutcomeLeaseLost {
		return AckLeaseLost
	}
	if outcome == OutcomeSnooze && delayMs <= 0 {
		return AckSnoozeDelayRequired
	}
	return AckValid
}

type OpaqueSchemaValidation uint8

const (
	OpaqueSchemaValid OpaqueSchemaValidation = iota
	OpaqueSchemaZero
	OpaqueSchemaTooLarge
)

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
