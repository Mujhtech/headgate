package headgateshared

// Outcome is the portable lifecycle result written by every worker runtime and store.
type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeRetry
	OutcomeSkip
	OutcomeRevoke
	OutcomeSnooze
	OutcomeLeaseLost
	OutcomeUndecodable
	OutcomeRateLimited
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeRetry:
		return "retry"
	case OutcomeSkip:
		return "skip"
	case OutcomeRevoke:
		return "revoke"
	case OutcomeSnooze:
		return "snooze"
	case OutcomeLeaseLost:
		return "lease_lost"
	case OutcomeUndecodable:
		return "undecodable"
	case OutcomeRateLimited:
		return "rate_limited"
	default:
		return "unknown"
	}
}

func ParseOutcome(value string) (Outcome, bool) {
	switch value {
	case "success":
		return OutcomeSuccess, true
	case "retry":
		return OutcomeRetry, true
	case "skip":
		return OutcomeSkip, true
	case "revoke":
		return OutcomeRevoke, true
	case "snooze":
		return OutcomeSnooze, true
	case "lease_lost":
		return OutcomeLeaseLost, true
	case "undecodable":
		return OutcomeUndecodable, true
	case "rate_limited":
		return OutcomeRateLimited, true
	default:
		return 0, false
	}
}

// SaturationStrategy is the wire/storage spelling used by every admission gate.
type SaturationStrategy string

const (
	SaturateQueue          SaturationStrategy = "queue"
	SaturateDiscard        SaturationStrategy = "discard"
	SaturateCancelRunning  SaturationStrategy = "cancel_running"
	SaturateCancelIncoming SaturationStrategy = "cancel_incoming"
)

func (s SaturationStrategy) Valid() bool {
	switch s {
	case SaturateQueue, SaturateDiscard, SaturateCancelRunning, SaturateCancelIncoming:
		return true
	default:
		return false
	}
}

// MissedPolicy decides what happens to periodic runs missed during downtime.
type MissedPolicy int

const (
	MissedSkip MissedPolicy = iota
	MissedRunOnce
	MissedBackfill
)

func (p MissedPolicy) String() string {
	switch p {
	case MissedRunOnce:
		return "run_once"
	case MissedBackfill:
		return "backfill"
	default:
		return "skip"
	}
}

func ParseMissedPolicy(value string) (MissedPolicy, bool) {
	switch value {
	case "skip":
		return MissedSkip, true
	case "run_once":
		return MissedRunOnce, true
	case "backfill":
		return MissedBackfill, true
	default:
		return 0, false
	}
}
