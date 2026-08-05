package taskq

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// The leader-elected cron scheduler.

// schedulerLoop elects a leader and fires due cron schedules.
//
// Election is an efficiency measure, not a correctness one. Every claim is a
// compare-and-set at the broker, so N schedulers racing still fire each tick
// exactly once; the lease simply stops fifty workers all doing the same
// bookkeeping every few seconds. That is worth knowing when the lease lapses
// and two schedulers briefly overlap: the answer is that nothing breaks.
//
// Unlike reaping and promotion, which every worker does unconditionally
// because they are cheap and latency-sensitive, cron is a fan-out point where
// redundant work is more wasteful and less urgent.
func (s *Server) schedulerLoop(ctx context.Context) {
	interval := min(5*time.Second, s.cfg.VisibilityTimeout/2)
	if interval <= 0 {
		interval = time.Second
	}
	// Three intervals, so one slow round trip does not trigger a needless
	// leadership handover.
	leaseTTL := 3 * interval

	defer func() {
		if !s.leader.Load() {
			return
		}
		// Releasing on the way out makes failover immediate instead of
		// TTL-delayed, which matters during a rolling deploy.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
		defer cancel()
		if err := s.maint.ReleaseLease(releaseCtx, schedulerLease, s.cfg.WorkerID); err != nil {
			s.log.Error("releasing scheduler lease failed", slog.String("error", err.Error()))
		}
		s.leader.Store(false)
	}()

	if err := s.seedSchedules(ctx); err != nil {
		s.log.Error("seeding cron schedules failed", slog.String("error", err.Error()))
	}

	for {
		if !sleepCtx(ctx, interval) {
			return
		}

		held, err := s.maint.AcquireLease(ctx, schedulerLease, s.cfg.WorkerID, leaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("acquiring scheduler lease failed", slog.String("error", err.Error()))
			continue
		}

		if was := s.leader.Swap(held); was != held {
			if held {
				s.log.Info("became scheduler leader")
			} else {
				s.log.Warn("lost scheduler leadership")
			}
		}
		if !held {
			continue
		}

		s.fireDueSchedules(ctx)
	}
}

// seedSchedules records a first fire time for any schedule that does not have
// one, leaving running schedules untouched.
func (s *Server) seedSchedules(ctx context.Context) error {
	now := time.Now()
	for id, ce := range s.entries {
		next := ce.schedule.Next(now)
		if next.IsZero() {
			s.log.Error("cron schedule can never fire",
				slog.String("entry_id", id),
				slog.String("spec", ce.entry.Spec))
			continue
		}
		created, err := s.cron.ScheduleCronIfAbsent(ctx, id, next)
		if err != nil {
			return err
		}
		if created {
			s.log.Info("cron schedule registered",
				slog.String("entry_id", id),
				slog.String("spec", ce.entry.Spec),
				slog.Time("first_fire", next))
		}
	}
	return nil
}

// fireDueSchedules materialises a job for each schedule whose tick has come up.
func (s *Server) fireDueSchedules(ctx context.Context) {
	due, err := s.cron.DueCron(ctx, cronBatch)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("reading due cron entries failed", slog.String("error", err.Error()))
		}
		return
	}

	for _, d := range due {
		ce, known := s.entries[d.ID]
		if !known {
			// A schedule from a different build of the application. Leaving it
			// alone is the only safe choice: deleting it would break whichever
			// deploy does own it.
			continue
		}

		// Advance from now rather than from the missed tick, so a scheduler
		// that was down for an hour does not enqueue an hour of backlog the
		// moment it returns. Missed ticks are skipped, not replayed.
		base := time.Now()
		if base.Before(d.FireAt) {
			base = d.FireAt
		}
		next := ce.schedule.Next(base)
		if next.IsZero() {
			s.log.Error("cron schedule can never fire again",
				slog.String("entry_id", d.ID),
				slog.String("spec", ce.entry.Spec))
			continue
		}

		// Enqueue first, then claim. If this worker dies in between, the next
		// scheduler re-fires the same tick and the unique key collapses it --
		// whereas claiming first and then failing to enqueue would silently
		// skip the tick altogether.
		job := &Job{
			Queue:       ce.entry.Queue,
			Type:        ce.entry.Type,
			Payload:     ce.entry.Payload,
			MaxAttempts: ce.entry.MaxAttempts,
			UniqueKey:   cronUniqueKey(d.ID, d.FireAt),
			UniqueTTL:   cronUniqueTTL,
		}
		if err := s.broker.Enqueue(ctx, job); err != nil && !errors.Is(err, ErrDuplicate) {
			s.log.Error("enqueuing cron job failed",
				slog.String("entry_id", d.ID),
				slog.String("error", err.Error()))
			// Deliberately not claiming, so the tick is retried next pass.
			continue
		}

		won, err := s.cron.ClaimCron(ctx, d.ID, d.FireAt, next)
		if err != nil {
			s.log.Error("claiming cron tick failed",
				slog.String("entry_id", d.ID),
				slog.String("error", err.Error()))
			continue
		}
		if !won {
			// Another scheduler advanced this entry first. The unique key
			// means only one job exists regardless.
			continue
		}

		s.log.Info("cron job enqueued",
			slog.String("entry_id", d.ID),
			slog.String("type", ce.entry.Type),
			slog.String("job_id", job.ID),
			slog.Time("next_fire", next))
	}
}
