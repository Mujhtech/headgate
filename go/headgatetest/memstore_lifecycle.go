package headgatetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// Renew extends current leases and returns IDs whose lease identity was lost.
func (m *MemStore) Renew(_ context.Context, leases []headgate.LeaseRef, lease time.Duration) ([]string, error) {
	if lease <= 0 {
		return nil, errors.New("headgatetest: lease must be >= 1ms")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var lost []string
	for _, l := range leases {
		j, ok := m.jobs[l.JobID]
		if !ok || j.state != "running" || j.leaseID != l.LeaseID || j.fence != l.Fence {
			lost = append(lost, l.JobID)
			continue
		}
		j.leaseExpires = now + lease.Milliseconds()
	}
	return lost, nil
}

// Checkpoint persists resumable progress under the active fence.
func (m *MemStore) Checkpoint(_ context.Context, lease headgate.LeaseRef, cp headgate.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.identity(lease)
	if err != nil {
		return err
	}
	j.checkpoint = cp
	return nil
}

// ReclaimExpired crash-attributes and requeues expired leases.
func (m *MemStore) ReclaimExpired(_ context.Context, limit int64) ([]headgate.Reclaimed, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var out []headgate.Reclaimed
	for _, id := range m.sortedIDs() {
		if int64(len(out)) >= limit {
			break
		}
		j := m.jobs[id]
		if j.state != "running" || j.leaseExpires > now {
			continue
		}
		j.env.CrashAttempt++
		m.dropLease(j)
		j.errs = append(j.errs, fmt.Sprintf("lease_lost (crash %d)", j.env.CrashAttempt))
		// crash quarantine step attribution: the checkpoint was durable BEFORE the in-progress
		// step's side effects, so the crash lands on exactly that step.
		if s := j.checkpoint.InProgressStep; s != "" {
			if j.checkpoint.CrashesByStep == nil {
				j.checkpoint.CrashesByStep = map[string]uint32{}
			}
			j.checkpoint.CrashesByStep[s]++
		}
		q := false
		if j.env.CrashAttempt >= m.CrashLimit {
			j.state = "quarantined"
			j.finalizedAt = now
			m.releaseUnique(j)
			if j.env.Fingerprint != "" {
				m.quarantine[j.env.Fingerprint] = true
			}
			q = true
		} else {
			j.state = "retryable"
			j.env.ScheduledAtMs = now + defaultBackoff(int64(j.env.CrashAttempt), m.RetryBaseMs, m.RetryCapMs)
		}
		out = append(out, headgate.Reclaimed{
			JobID: id, Fingerprint: j.env.Fingerprint,
			CrashAttempt: j.env.CrashAttempt, Quarantined: q,
		})
	}
	return out, nil
}

// PromoteDue makes bounded scheduled and retryable work available.
func (m *MemStore) PromoteDue(_ context.Context, limit int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var n int64
	for _, id := range m.sortedIDs() {
		if n >= limit {
			break
		}
		j := m.jobs[id]
		if (j.state == "scheduled" || j.state == "retryable") && j.env.ScheduledAtMs <= now {
			j.state = "available"
			n++
		}
	}
	return n, nil
}

// EvictRetained deletes bounded terminal jobs whose retention has elapsed.
func (m *MemStore) EvictRetained(_ context.Context, limit int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var n int64
	for _, id := range m.sortedIDs() {
		if n >= limit {
			break
		}
		j := m.jobs[id]
		switch j.state {
		case "completed", "archived", "cancelled", "undecodable":
			if j.env.RetentionMs > 0 && j.finalizedAt+j.env.RetentionMs <= now {
				delete(m.jobs, id) // quarantined exempt by design (retention and eviction contract)
				n++
			}
		}
	}
	return n, nil
}

// ClaimDuty acquires or renews a singleton duty lease.
func (m *MemStore) ClaimDuty(_ context.Context, name, holder string, lease time.Duration) (bool, error) {
	if lease <= 0 {
		return false, errors.New("headgatetest: duty lease must be >= 1ms")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	d, held := m.duties[name]
	if held && d.expires > now && d.holder != holder {
		return false, nil
	}
	m.duties[name] = struct {
		holder  string
		expires int64
	}{holder, now + lease.Milliseconds()}
	return true, nil
}

// ReleaseDuty gives up a singleton duty held by holder.
func (m *MemStore) ReleaseDuty(_ context.Context, name, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d, ok := m.duties[name]; ok && d.holder == holder {
		delete(m.duties, name)
	}
	return nil
}

// Caps reports the optional store capabilities implemented by MemStore.
func (m *MemStore) Caps() headgate.Caps {
	// runtime capability boundary capability honesty: no Transactional, no Inspect, no Notifying. The duties
	// that need Inspect idle; Job.Once errors; runners poll. See the package docs.
	return 0
}

// sortedIDs keeps sweep order deterministic (map iteration is not).
func (m *MemStore) sortedIDs() []string {
	ids := make([]string, 0, len(m.jobs))
	for id := range m.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func defaultBackoff(attempt, base, ceiling int64) int64 {
	shift := attempt - 1
	if shift > 20 {
		shift = 20
	}
	b := base * (1 << shift)
	if b > ceiling {
		return ceiling
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
