package rdb

import (
	"context"
	"fmt"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// defaultUniqueTTL is used when a job sets a unique key but no TTL. An hour is
// long enough to absorb a producer's retry storm and short enough that a wedged
// key is self-healing.
const defaultUniqueTTL = time.Hour

// Enqueue implements taskq.Broker.
func (b *Broker) Enqueue(ctx context.Context, j *taskq.Job) error {
	if j == nil {
		return fmt.Errorf("%w: nil job", taskq.ErrInvalidTask)
	}
	if j.Queue == "" {
		return fmt.Errorf("%w: job has no queue", taskq.ErrInvalidTask)
	}
	if j.Type == "" {
		return fmt.Errorf("%w: job has no type", taskq.ErrInvalidTask)
	}

	if j.ID == "" {
		j.ID = taskq.NewID()
	}
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = taskq.DefaultMaxAttempts
	}

	// With no unique key the script must still receive something in KEYS[5], so
	// pass a placeholder it will never touch because the TTL argument is zero.
	uniqueTTL := int64(0)
	uniqueKey := b.kUnique("__none__")
	if j.UniqueKey != "" {
		ttl := j.UniqueTTL
		if ttl <= 0 {
			ttl = defaultUniqueTTL
		}
		uniqueTTL = ttl.Milliseconds()
		uniqueKey = b.kUnique(j.UniqueKey)
	}

	keys := []string{
		b.kJob(j.ID),
		b.kQueue(j.Queue),
		b.kDelayed(),
		b.kQueues(),
		uniqueKey,
	}
	args := []any{
		j.ID,
		j.Queue,
		j.Type,
		string(j.Payload),
		j.MaxAttempts,
		msFromTime(j.RunAt),
		uniqueTTL,
		j.UniqueKey,
	}

	res, err := b.enqueue.Run(ctx, b.client, keys, args...).Slice()
	if err != nil {
		return fmt.Errorf("taskq/rdb: enqueue %s: %w", j.ID, err)
	}
	if len(res) == 0 {
		return fmt.Errorf("taskq/rdb: empty enqueue reply for job %s", j.ID)
	}

	if toInt64(res[0]) == 0 {
		existing := ""
		if len(res) > 1 {
			existing, _ = res[1].(string)
		}
		return &taskq.DuplicateError{UniqueKey: j.UniqueKey, ExistingID: existing}
	}

	// Mirror back the routing decision so the caller's job reflects what the
	// broker actually did with it.
	if !j.RunAt.IsZero() && j.RunAt.After(time.Now()) {
		j.State = taskq.StateScheduled
	} else {
		j.State = taskq.StatePending
	}
	return nil
}
