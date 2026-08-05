package rdb

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// Compile-time proof that the Redis broker covers the whole maintenance
// contract.
//
// This assertion is load-bearing rather than decorative: Server discovers
// orphan recovery by type-asserting its broker to taskq.Maintenance, so a
// missing method here would not fail to compile anywhere -- it would silently
// disable reaping, promotion, and heartbeats at runtime and only show up as
// jobs quietly going missing after a crash.
var _ taskq.Maintenance = (*Broker)(nil)

// Reap implements taskq.Maintenance.
//
// The limit applies per queue rather than across all of them, so one deeply
// backed-up queue cannot starve recovery on the others.
func (b *Broker) Reap(ctx context.Context, queues []string, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	total := 0
	for _, q := range queues {
		keys := []string{b.kActive(q), b.kDead(), b.kQueue(q), b.kFailed()}
		args := []any{limit, b.pJob()}

		res, err := b.reap.Run(ctx, b.client, keys, args...).Slice()
		if err != nil {
			return total, fmt.Errorf("taskq/rdb: reap queue %s: %w", q, err)
		}
		if len(res) < 2 {
			return total, fmt.Errorf("taskq/rdb: malformed reap reply for queue %s", q)
		}
		total += int(toInt64(res[0])) + int(toInt64(res[1]))
	}
	return total, nil
}

// ReapDetail is Reap with the outcome broken down, for callers that want to log
// or alert on orphans that were dead-lettered rather than merely re-delivered.
func (b *Broker) ReapDetail(ctx context.Context, queue string, limit int) (recovered, dead int, err error) {
	if limit <= 0 {
		return 0, 0, nil
	}

	keys := []string{b.kActive(queue), b.kDead(), b.kQueue(queue), b.kFailed()}
	args := []any{limit, b.pJob()}

	res, err := b.reap.Run(ctx, b.client, keys, args...).Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("taskq/rdb: reap queue %s: %w", queue, err)
	}
	if len(res) < 2 {
		return 0, 0, fmt.Errorf("taskq/rdb: malformed reap reply for queue %s", queue)
	}
	return int(toInt64(res[0])), int(toInt64(res[1])), nil
}

// Promote implements taskq.Maintenance.
func (b *Broker) Promote(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	keys := []string{b.kDelayed(), b.kRetry()}
	args := []any{limit, b.pJob(), b.prefix + ":q:"}

	n, err := b.promote.Run(ctx, b.client, keys, args...).Int()
	if err != nil {
		return 0, fmt.Errorf("taskq/rdb: promote: %w", err)
	}
	return n, nil
}

// Heartbeat implements taskq.Maintenance.
func (b *Broker) Heartbeat(ctx context.Context, w taskq.WorkerInfo, ttl time.Duration) error {
	if w.ID == "" {
		return fmt.Errorf("%w: empty worker ID", taskq.ErrInvalidTask)
	}
	if ttl <= 0 {
		return fmt.Errorf("%w: heartbeat ttl must be positive", taskq.ErrInvalidTask)
	}

	keys := []string{b.kWorkers(), b.kWorker(w.ID)}
	args := []any{
		w.ID,
		ttl.Milliseconds(),
		w.Host,
		strconv.Itoa(w.PID),
		strings.Join(w.Queues, ","),
		strconv.Itoa(w.Concurrency),
		strconv.Itoa(w.InFlight),
		msFromTime(w.StartedAt),
	}

	if err := b.heartbeat.Run(ctx, b.client, keys, args...).Err(); err != nil {
		return fmt.Errorf("taskq/rdb: heartbeat %s: %w", w.ID, err)
	}
	return nil
}

// AcquireLease implements taskq.Maintenance.
func (b *Broker) AcquireLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	if name == "" || owner == "" {
		return false, fmt.Errorf("%w: lease name and owner must be non-empty", taskq.ErrInvalidTask)
	}
	if ttl <= 0 {
		return false, fmt.Errorf("%w: lease ttl must be positive", taskq.ErrInvalidTask)
	}

	held, err := b.leaseAcquire.Run(ctx, b.client, []string{b.kLease(name)}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("taskq/rdb: acquire lease %s: %w", name, err)
	}
	return held == 1, nil
}

// ReleaseLease implements taskq.Maintenance.
func (b *Broker) ReleaseLease(ctx context.Context, name, owner string) error {
	if name == "" || owner == "" {
		return fmt.Errorf("%w: lease name and owner must be non-empty", taskq.ErrInvalidTask)
	}

	if err := b.leaseRelease.Run(ctx, b.client, []string{b.kLease(name)}, owner).Err(); err != nil {
		return fmt.Errorf("taskq/rdb: release lease %s: %w", name, err)
	}
	return nil
}
