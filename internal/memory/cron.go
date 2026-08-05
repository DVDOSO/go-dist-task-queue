package memory

import (
	"context"
	"fmt"
	"sort"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// Compile-time proof of the cron contract.
var _ taskq.CronStore = (*Broker)(nil)

// ScheduleCronIfAbsent implements taskq.CronStore.
func (b *Broker) ScheduleCronIfAbsent(_ context.Context, id string, at time.Time) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("%w: cron entry has no ID", taskq.ErrInvalidTask)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return false, err
	}

	if _, exists := b.cron[id]; exists {
		return false, nil
	}
	b.cron[id] = at
	return true, nil
}

// DueCron implements taskq.CronStore.
func (b *Broker) DueCron(_ context.Context, limit int) ([]taskq.DueCronEntry, error) {
	if limit <= 0 {
		return nil, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return nil, err
	}

	now := b.clock.Now()
	out := make([]taskq.DueCronEntry, 0, limit)
	for id, at := range b.cron {
		if at.After(now) {
			continue
		}
		out = append(out, taskq.DueCronEntry{ID: id, FireAt: at})
	}

	// Soonest first, ties broken by ID, so the order is total and tests are
	// deterministic.
	sort.Slice(out, func(i, k int) bool {
		if out[i].FireAt.Equal(out[k].FireAt) {
			return out[i].ID < out[k].ID
		}
		return out[i].FireAt.Before(out[k].FireAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ClaimCron implements taskq.CronStore.
func (b *Broker) ClaimCron(_ context.Context, id string, firedAt, nextAt time.Time) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return false, err
	}

	// Compare-and-set on the expected fire time, matching the Redis script:
	// two schedulers seeing the same tick as due must produce exactly one
	// winner.
	at, ok := b.cron[id]
	if !ok || !at.Equal(firedAt) {
		return false, nil
	}
	b.cron[id] = nextAt
	return true, nil
}

// RemoveCron implements taskq.CronStore.
func (b *Broker) RemoveCron(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return err
	}
	delete(b.cron, id)
	return nil
}
