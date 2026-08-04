package rdb

import (
	"context"
	"fmt"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// Compile-time proof of the hot-path interface. Maintenance and Inspector
// arrive in later stages.
var _ taskq.Broker = (*Broker)(nil)

// Ack implements taskq.Broker.
func (b *Broker) Ack(ctx context.Context, workerID, jobID string) error {
	keys := []string{b.kJob(jobID), b.kProcessed()}
	args := []any{workerID, b.completedTTL.Milliseconds(), b.pActive()}

	res, err := b.ack.Run(ctx, b.client, keys, args...).Slice()
	if err != nil {
		return fmt.Errorf("taskq/rdb: ack %s: %w", jobID, err)
	}
	return fenceError(res, jobID, workerID)
}

// Nack implements taskq.Broker.
func (b *Broker) Nack(ctx context.Context, workerID, jobID string, retryAt time.Time, reason string) error {
	keys := []string{b.kJob(jobID), b.kRetry(), b.kDead(), b.kFailed()}
	args := []any{workerID, msFromTime(retryAt), reason, b.pActive()}

	res, err := b.nack.Run(ctx, b.client, keys, args...).Slice()
	if err != nil {
		return fmt.Errorf("taskq/rdb: nack %s: %w", jobID, err)
	}
	return fenceError(res, jobID, workerID)
}

// Kill implements taskq.Broker.
func (b *Broker) Kill(ctx context.Context, workerID, jobID, reason string) error {
	keys := []string{b.kJob(jobID), b.kDead(), b.kFailed()}
	args := []any{workerID, reason, b.pActive()}

	res, err := b.kill.Run(ctx, b.client, keys, args...).Slice()
	if err != nil {
		return fmt.Errorf("taskq/rdb: kill %s: %w", jobID, err)
	}
	return fenceError(res, jobID, workerID)
}

// fenceError translates a script's {code, reason, ...} reply into an error.
//
// The scripts return a status code rather than raising a Lua error because a
// lost lease is an expected outcome that callers branch on, not an exception:
// a worker that has been overtaken should abandon its job quietly.
func fenceError(res []any, jobID, workerID string) error {
	if len(res) == 0 {
		return fmt.Errorf("taskq/rdb: empty script reply for job %s", jobID)
	}
	if toInt64(res[0]) == 1 {
		return nil
	}

	reason := ""
	if len(res) > 1 {
		reason, _ = res[1].(string)
	}
	switch reason {
	case "notfound":
		return fmt.Errorf("%w: %s", taskq.ErrJobNotFound, jobID)
	case "lease":
		state, owner := "?", "?"
		if len(res) > 2 {
			state, _ = res[2].(string)
		}
		if len(res) > 3 {
			owner, _ = res[3].(string)
		}
		return fmt.Errorf("%w: job %s is %s owned by %q, not %q",
			taskq.ErrLeaseLost, jobID, state, owner, workerID)
	default:
		return fmt.Errorf("taskq/rdb: job %s rejected: %s", jobID, reason)
	}
}
