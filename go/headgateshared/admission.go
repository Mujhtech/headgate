package headgateshared

import "strconv"

type AdmissionFacts struct {
	State, Fingerprint, RateClass, Saturation string
	NowMs, ScheduledAtMs, Weight              int64
	TokensAvailable                           *int64
	TokensAhead, LimitPerWindow, WindowMs     int64
	MaxConcurrent                             *int64
	Inflight, Position, Deficit               int64
	QueuePaused, Quarantined                  bool
}

type AdmissionEvaluation struct {
	Admissible bool
	BlockedBy  string
	Detail     map[string]string
	ETA        *int64
}

func EvaluateAdmission(f AdmissionFacts) AdmissionEvaluation {
	detail := map[string]string{"state": f.State}
	result := AdmissionEvaluation{Detail: detail}
	block := func(by string, eta *int64) AdmissionEvaluation {
		result.BlockedBy, result.ETA = by, eta
		return result
	}
	zero := int64(0)
	switch f.State {
	case "running":
		result.Admissible, result.ETA = true, &zero
		return result
	case "scheduled", "retryable":
		detail["scheduled_at_ms"] = strconv.FormatInt(f.ScheduledAtMs, 10)
		eta := max(f.ScheduledAtMs-f.NowMs, 0)
		return block("schedule", &eta)
	case "quarantined":
		return block("quarantine", nil)
	case "available":
	default:
		return result
	}
	if f.QueuePaused {
		return block("queue_paused", nil)
	}
	if f.ScheduledAtMs > f.NowMs {
		detail["scheduled_at_ms"] = strconv.FormatInt(f.ScheduledAtMs, 10)
		eta := f.ScheduledAtMs - f.NowMs
		return block("schedule", &eta)
	}
	if f.Quarantined {
		detail["fingerprint"] = f.Fingerprint
		return block("quarantine", nil)
	}
	if f.RateClass != "" {
		weight := max(f.Weight, 1)
		required := f.TokensAhead + weight
		detail["rate_class"] = f.RateClass
		detail["weight"] = strconv.FormatInt(weight, 10)
		detail["tokens_ahead_in_class"] = strconv.FormatInt(f.TokensAhead, 10)
		if f.TokensAvailable == nil {
			detail["tokens_available"] = "unlimited (no such rate class)"
		} else {
			detail["tokens_available"] = strconv.FormatInt(*f.TokensAvailable, 10)
			if *f.TokensAvailable < required {
				var eta *int64
				if f.LimitPerWindow > 0 {
					value := max(required-*f.TokensAvailable, 1) * f.WindowMs / f.LimitPerWindow
					eta = &value
				}
				return block("rate_class", eta)
			}
		}
	}
	if f.MaxConcurrent != nil {
		strategy := f.Saturation
		if strategy == "" {
			strategy = "queue"
		}
		detail["max_concurrent"] = strconv.FormatInt(*f.MaxConcurrent, 10)
		detail["inflight"] = strconv.FormatInt(f.Inflight, 10)
		detail["on_saturated"] = strategy
		if f.Inflight >= *f.MaxConcurrent && strategy != "cancel_running" {
			return block("concurrency_limit", nil)
		}
	}
	detail["position_in_partition"] = strconv.FormatInt(f.Position, 10)
	detail["partition_deficit"] = strconv.FormatInt(f.Deficit, 10)
	result.Admissible, result.ETA = true, &zero
	return result
}
