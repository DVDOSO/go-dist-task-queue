package rdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// Dequeue implements taskq.Broker.
func (b *Broker) Dequeue(ctx context.Context, o taskq.ClaimOpts) (*taskq.Job, error) {
	if o.WorkerID == "" {
		return nil, fmt.Errorf("%w: empty worker ID", taskq.ErrInvalidTask)
	}
	if o.VisibilityTimeout <= 0 {
		return nil, fmt.Errorf("%w: visibility timeout must be positive", taskq.ErrInvalidTask)
	}
	if len(o.Queues) == 0 {
		return nil, taskq.ErrNoJob
	}

	// Ready and active keys interleaved, one pair per queue, in the caller's
	// order. That order already encodes the worker's weighting policy; the
	// script deliberately knows nothing about priority.
	keys := make([]string, 0, len(o.Queues)*2)
	for _, q := range o.Queues {
		keys = append(keys, b.kQueue(q), b.kActive(q))
	}
	args := []any{o.WorkerID, o.VisibilityTimeout.Milliseconds(), b.pJob()}

	res, err := b.dequeue.Run(ctx, b.client, keys, args...).Slice()
	if errors.Is(err, redis.Nil) {
		// An empty queue set is the common case, not an error condition.
		return nil, taskq.ErrNoJob
	}
	if err != nil {
		return nil, fmt.Errorf("taskq/rdb: dequeue: %w", err)
	}
	return decodeJob(res)
}

// Extend implements taskq.Broker.
func (b *Broker) Extend(ctx context.Context, workerID string, jobIDs []string, vt time.Duration) ([]string, error) {
	if vt <= 0 {
		return nil, fmt.Errorf("%w: visibility timeout must be positive", taskq.ErrInvalidTask)
	}
	if len(jobIDs) == 0 {
		return nil, nil
	}

	// Job IDs travel in ARGV rather than KEYS because the active set each one
	// belongs to is only discoverable by reading that job's queue field. This
	// is the deliberate cluster-incompatibility documented on the package.
	args := make([]any, 0, 4+len(jobIDs))
	args = append(args, workerID, vt.Milliseconds(), b.pJob(), b.pActive())
	for _, id := range jobIDs {
		args = append(args, id)
	}

	res, err := b.extend.Run(ctx, b.client, nil, args...).Slice()
	if err != nil {
		return nil, fmt.Errorf("taskq/rdb: extend: %w", err)
	}
	if len(res) == 0 {
		return nil, nil
	}

	lost := make([]string, 0, len(res))
	for _, v := range res {
		if s, ok := v.(string); ok {
			lost = append(lost, s)
		}
	}
	return lost, nil
}
