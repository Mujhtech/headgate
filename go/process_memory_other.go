//go:build !linux && !darwin

package headgate

import "runtime"

// The portable fallback measures memory obtained from the OS by the Go runtime. Users
// that need whole-process RSS on another platform can inject Config.MemorySampler.
type processMemorySampler struct{}

func (processMemorySampler) MemoryBytes() (uint64, error) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Sys, nil
}
