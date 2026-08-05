package rdb

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// Compile-time proof of the cron contract. Like the Maintenance assertion, this
// is load-bearing: Server discovers cron support by type assertion, so a
// missing method would disable scheduling silently rather than fail to build.
var _ taskq.CronStore = (*Broker)(nil)

// ScheduleCronIfAbsent implements taskq.CronStore.
func (b *Broker) ScheduleCronIfAbsent(ctx context.Context, id string, at time.Time) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("%w: cron entry has no ID", taskq.ErrInvalidTask)
	}

	// NX is the whole point: a restarting worker must not reset a schedule that
	// is already running, or a fleet rolling one process at a time would keep
	// pushing the next fire into the future and the job would never run.
	n, err := b.client.ZAddNX(ctx, b.kCron(), redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: id,
	}).Result()
	if err != nil {
		return false, fmt.Errorf("taskq/rdb: schedule cron %s: %w", id, err)
	}
	return n == 1, nil
}

// DueCron implements taskq.CronStore.
func (b *Broker) DueCron(ctx context.Context, limit int) ([]taskq.DueCronEntry, error) {
	if limit <= 0 {
		return nil, nil
	}

	// Server time, not the caller's: a scheduler with a fast clock must not be
	// able to fire every schedule early.
	now, err := b.client.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("taskq/rdb: read server time: %w", err)
	}

	entries, err := b.client.ZRangeByScoreWithScores(ctx, b.kCron(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(now.UnixMilli(), 10),
		Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("taskq/rdb: read due cron entries: %w", err)
	}

	out := make([]taskq.DueCronEntry, 0, len(entries))
	for _, e := range entries {
		id, ok := e.Member.(string)
		if !ok {
			continue
		}
		out = append(out, taskq.DueCronEntry{
			ID:     id,
			FireAt: time.UnixMilli(int64(e.Score)).UTC(),
		})
	}
	return out, nil
}

// ClaimCron implements taskq.CronStore.
func (b *Broker) ClaimCron(ctx context.Context, id string, firedAt, nextAt time.Time) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("%w: cron entry has no ID", taskq.ErrInvalidTask)
	}

	won, err := b.cronClaim.Run(ctx, b.client, []string{b.kCron()},
		id, firedAt.UnixMilli(), nextAt.UnixMilli()).Int()
	if err != nil {
		return false, fmt.Errorf("taskq/rdb: claim cron %s: %w", id, err)
	}
	return won == 1, nil
}

// RemoveCron implements taskq.CronStore.
func (b *Broker) RemoveCron(ctx context.Context, id string) error {
	if err := b.client.ZRem(ctx, b.kCron(), id).Err(); err != nil {
		return fmt.Errorf("taskq/rdb: remove cron %s: %w", id, err)
	}
	return nil
}
