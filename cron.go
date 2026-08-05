package taskq

import (
	"context"
	"fmt"
	"time"

	"github.com/DVDOSO/go-dist-task-queue/internal/cron"
)

// CronEntry is a recurring job definition.
//
// Entries live in your code and are passed to the worker in [Config.Cron]; only
// the *next fire time* is kept in the broker. That split is deliberate: a
// schedule is part of an application's source, versioned and reviewed with it,
// while the thing that genuinely has to be shared between processes is which
// tick has already been claimed.
type CronEntry struct {
	// ID is a stable identifier for this schedule. Changing it starts a fresh
	// schedule; reusing it across deploys continues the existing one.
	ID string
	// Spec is a five-field cron expression or a descriptor such as "@hourly"
	// or "@every 30s".
	Spec string
	// Type routes the materialised job to a handler.
	Type string
	// Payload is the job payload, identical on every fire.
	Payload []byte
	// Queue defaults to DefaultQueue.
	Queue string
	// MaxAttempts defaults to DefaultMaxAttempts.
	MaxAttempts int
}

// DueCronEntry is a schedule whose next fire time has arrived.
type DueCronEntry struct {
	// ID identifies the entry.
	ID string
	// FireAt is the tick that came due. It is passed back to [CronStore.ClaimCron]
	// so the claim can be a compare-and-set rather than a blind overwrite.
	FireAt time.Time
}

// CronStore is the scheduler's view of the broker: just enough to know which
// schedules are due and to claim a tick exactly once across many schedulers.
//
// Kept separate from [Maintenance] because it has exactly one consumer — the
// leader-elected scheduler loop — and a broker with no cron support should be
// able to say so by simply not implementing it.
type CronStore interface {
	// ScheduleCronIfAbsent records an entry's first fire time, leaving an
	// existing schedule untouched. It reports whether it created one.
	//
	// Absent-only so that restarting a worker does not reset a schedule that
	// is already running.
	ScheduleCronIfAbsent(ctx context.Context, id string, at time.Time) (bool, error)

	// DueCron returns entries whose fire time has arrived.
	DueCron(ctx context.Context, limit int) ([]DueCronEntry, error)

	// ClaimCron advances an entry from firedAt to nextAt, reporting whether
	// this caller won the tick.
	//
	// A compare-and-set on firedAt rather than a blind write: two schedulers
	// that both see the same tick as due will both call this, and exactly one
	// must win.
	ClaimCron(ctx context.Context, id string, firedAt, nextAt time.Time) (bool, error)

	// RemoveCron deletes a schedule.
	RemoveCron(ctx context.Context, id string) error
}

// compiledEntry is a CronEntry with its spec parsed once at startup.
type compiledEntry struct {
	entry    CronEntry
	schedule *cron.Schedule
}

// compile validates an entry and parses its spec.
func compile(e CronEntry) (*compiledEntry, error) {
	if e.ID == "" {
		return nil, fmt.Errorf("%w: cron entry has no ID", ErrInvalidTask)
	}
	if e.Type == "" {
		return nil, fmt.Errorf("%w: cron entry %q has no task type", ErrInvalidTask, e.ID)
	}

	schedule, err := cron.Parse(e.Spec)
	if err != nil {
		return nil, fmt.Errorf("%w: cron entry %q: %w", ErrInvalidTask, e.ID, err)
	}

	if e.Queue == "" {
		e.Queue = DefaultQueue
	}
	if e.MaxAttempts <= 0 {
		e.MaxAttempts = DefaultMaxAttempts
	}
	return &compiledEntry{entry: e, schedule: schedule}, nil
}

// cronUniqueKey identifies one tick of one schedule.
//
// Including the fire time is what makes it safe: successive ticks get distinct
// keys, so the key deduplicates a retried fire without ever suppressing the
// next legitimate one.
func cronUniqueKey(id string, fireAt time.Time) string {
	return fmt.Sprintf("cron:%s:%d", id, fireAt.UnixMilli())
}
