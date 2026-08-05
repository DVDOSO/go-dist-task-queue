package taskq

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// Background loops that keep delivery guarantees true: lease renewal, orphan
// recovery, retry promotion, and worker heartbeats.

// renewLoop extends the leases of every in-flight job.
//
// One batched call for the whole pool rather than one per job: at a renewal
// interval of a third of the visibility timeout, per-job renewals would put a
// worker's lease traffic in the same order of magnitude as its actual work.
func (s *Server) renewLoop(ctx context.Context) {
	interval := s.cfg.VisibilityTimeout / 3
	if interval <= 0 {
		interval = time.Second
	}

	failures := 0
	for {
		if !sleepCtx(ctx, interval) {
			return
		}

		ids := s.inFlightIDs()
		if len(ids) == 0 {
			failures = 0
			continue
		}

		lost, err := s.broker.Extend(ctx, s.cfg.WorkerID, ids, s.cfg.VisibilityTimeout)
		if err != nil {
			failures++
			s.log.Error("lease renewal failed",
				slog.Int("in_flight", len(ids)),
				slog.Int("consecutive_failures", failures),
				slog.String("error", err.Error()))

			// Repeated failure almost certainly means the broker is
			// unreachable and every lease has expired. Continuing to run burns
			// capacity on work that is already being redone elsewhere, so stop
			// and let the retry path handle it. A false positive here costs one
			// wasted attempt; a false negative costs duplicated side effects.
			if failures >= maxRenewFailures {
				s.log.Error("giving up on lease renewal, cancelling in-flight jobs",
					slog.Int("in_flight", len(ids)))
				for _, id := range ids {
					s.cancelJob(id)
				}
				failures = 0
			}
			continue
		}
		failures = 0

		for _, id := range lost {
			// Someone else owns this job now. Whatever this handler does from
			// here is duplicate work at best.
			s.log.Warn("lease lost, cancelling job", slog.String("job_id", id))
			s.cancelJob(id)
		}
	}
}

// reapLoop recovers jobs whose leases expired.
//
// Every worker runs this, not just an elected leader. Reaping is idempotent --
// the broker guarantees only one caller acts on a given job -- so redundancy is
// free, and it keeps orphan recovery off the critical path of leader election.
func (s *Server) reapLoop(ctx context.Context) {
	interval := s.cfg.VisibilityTimeout / 2
	if interval <= 0 {
		interval = time.Second
	}

	for {
		// Jittered so a fleet that started together does not sweep in lockstep.
		if !sleepCtx(ctx, jitter(interval)) {
			return
		}

		n, err := s.maint.Reap(ctx, s.cfg.Queues, reapBatch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("reap failed", slog.String("error", err.Error()))
			continue
		}
		if n > 0 {
			s.log.Warn("recovered orphaned jobs", slog.Int("count", n))
		}
	}
}

// promoteLoop moves due retries and delayed jobs onto their ready queues.
func (s *Server) promoteLoop(ctx context.Context) {
	interval := min(time.Second, s.cfg.VisibilityTimeout/2)
	if interval <= 0 {
		interval = time.Second
	}

	for {
		if !sleepCtx(ctx, jitter(interval)) {
			return
		}

		n, err := s.maint.Promote(ctx, promoteBatch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("promote failed", slog.String("error", err.Error()))
			continue
		}
		if n > 0 {
			s.log.Debug("promoted due jobs", slog.Int("count", n))
		}
	}
}

// heartbeatLoop publishes this worker's liveness and load.
func (s *Server) heartbeatLoop(ctx context.Context, started time.Time) {
	interval := min(5*time.Second, s.cfg.VisibilityTimeout/2)
	if interval <= 0 {
		interval = time.Second
	}
	// Three missed beats before a worker is presumed gone, so one slow round
	// trip does not make a healthy worker vanish from the operator's view.
	ttl := 3 * interval

	beat := func() {
		if err := s.maint.Heartbeat(ctx, WorkerInfo{
			ID:          s.cfg.WorkerID,
			Host:        s.host,
			PID:         os.Getpid(),
			Queues:      s.cfg.Queues,
			Concurrency: s.cfg.Concurrency,
			InFlight:    s.inFlightCount(),
			StartedAt:   started,
		}, ttl); err != nil && ctx.Err() == nil {
			s.log.Error("heartbeat failed", slog.String("error", err.Error()))
		}
	}

	beat() // register immediately rather than after the first interval
	for {
		if !sleepCtx(ctx, interval) {
			return
		}
		beat()
	}
}
