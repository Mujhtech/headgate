//go:build linux || darwin

package headgate

import (
	"runtime"
	"syscall"
)

type processMemorySampler struct{}

func (processMemorySampler) MemoryBytes() (uint64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	bytes := uint64(max(usage.Maxrss, 0))
	if runtime.GOOS != "darwin" {
		bytes *= 1024 // Linux reports KiB; Darwin reports bytes.
	}
	return bytes, nil
}
