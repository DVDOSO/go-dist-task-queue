package taskq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// Job execution: claiming work, running handlers, and tracking what is in flight.

// fetchLoop claims work until ctx is cancelled.
func (s *Server) fetchLoop(ctx, jobCtx context.Context, h Handler, slots chan struct{}, wg *sync.WaitGroup) {
	delay := minPollInterval

	for {
		// Take a slot *before* asking for work. Claiming a job we have no
		// capacity to start would mean holding its lease while it sits in a
		// local buffer -- and losing it outright if this process died, since
		// nothing would ack or nack it.
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}

		// Reshuffled per poll, so priority is expressed as a probability of
		// being tried first rather than as an absolute ordering.
		job, err := s.broker.Dequeue(ctx, ClaimOpts{
			WorkerID:          s.cfg.WorkerID,
			Queues:            s.queues.next(),
			VisibilityTimeout: s.cfg.VisibilityTimeout,
		})

		switch {
		case err == nil:
			delay = minPollInterval
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				s.runJob(jobCtx, h, job)
			}()

		case errors.Is(err, ErrNoJob):
			<-slots
			// Idle: back off so an empty queue is cheap, with jitter so a fleet
			// of workers that started together does not poll in lockstep.
			if !sleepCtx(ctx, jitter(delay)) {
				return
			}
			delay = min(delay*2, maxPollInterval)

		default:
			<-slots
			if ctx.Err() != nil {
				// The error is just our own shutdown racing the call.
				return
			}
			s.log.Error("dequeue failed", slog.String("error", err.Error()))
			if !sleepCtx(ctx, jitter(maxPollInterval)) {
				return
			}
		}
	}
}

// runJob executes one job and reports the outcome.
func (s *Server) runJob(ctx context.Context, h Handler, j *Job) {
	// Each job gets its own cancellable context so that losing one lease stops
	// exactly one handler.
	jobCtx, cancel := context.WithCancel(ctx)
	s.trackJob(j.ID, cancel)
	defer func() {
		s.untrackJob(j.ID)
		cancel()
	}()

	start := time.Now()
	err := s.invoke(jobCtx, h, j)
	elapsed := time.Since(start)

	// The outcome must be reported even when the job's own context has been
	// cancelled, so this gets a fresh one. Reusing the cancelled context would
	// mean a shutdown left every in-flight job's lease dangling until the
	// reaper noticed -- turning a clean stop into a pile of duplicate work.
	reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancelReport()

	switch {
	case err == nil:
		if ackErr := s.broker.Ack(reportCtx, s.cfg.WorkerID, j.ID); ackErr != nil {
			s.log.Error("ack failed",
				slog.String("job_id", j.ID),
				slog.String("type", j.Type),
				slog.String("error", ackErr.Error()))
			return
		}
		s.log.Info("job completed",
			slog.String("job_id", j.ID),
			slog.String("type", j.Type),
			slog.Int("attempt", j.Attempt),
			slog.Duration("elapsed", elapsed))

	case errors.Is(err, ErrSkipRetry):
		if killErr := s.broker.Kill(reportCtx, s.cfg.WorkerID, j.ID, err.Error()); killErr != nil {
			s.log.Error("kill failed",
				slog.String("job_id", j.ID),
				slog.String("error", killErr.Error()))
			return
		}
		s.log.Warn("job dead-lettered without retry",
			slog.String("job_id", j.ID),
			slog.String("type", j.Type),
			slog.String("error", err.Error()))

	default:
		delay := s.cfg.Backoff.Next(j.Attempt)
		retryAt := time.Now().Add(delay)
		if nackErr := s.broker.Nack(reportCtx, s.cfg.WorkerID, j.ID, retryAt, err.Error()); nackErr != nil {
			// ErrLeaseLost here is expected and benign: the job was already
			// re-delivered, so someone else owns its outcome.
			level := slog.LevelError
			if errors.Is(nackErr, ErrLeaseLost) {
				level = slog.LevelWarn
			}
			s.log.Log(reportCtx, level, "nack failed",
				slog.String("job_id", j.ID),
				slog.String("error", nackErr.Error()))
			return
		}
		s.log.Warn("job failed",
			slog.String("job_id", j.ID),
			slog.String("type", j.Type),
			slog.Int("attempt", j.Attempt),
			slog.Int("max_attempts", j.MaxAttempts),
			slog.Duration("retry_in", delay),
			slog.String("error", err.Error()))
	}
}

// invoke runs a handler, converting a panic into an ordinary error.
//
// One bad handler must not take down a worker that is running nine other jobs
// correctly. The stack goes to the log rather than into the returned error,
// because that error is stored on the job envelope and a full stack trace would
// bloat every dead-letter entry.
func (s *Server) invoke(ctx context.Context, h Handler, j *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("handler panicked",
				slog.String("job_id", j.ID),
				slog.String("type", j.Type),
				slog.String("panic", fmt.Sprint(r)),
				slog.String("stack", string(debug.Stack())))
			err = fmt.Errorf("taskq: handler panicked: %v", r)
		}
	}()
	return h.ProcessTask(ctx, j)
}

// trackJob records a running job so its lease can be renewed and, if lost,
// cancelled.
func (s *Server) trackJob(id string, cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight[id] = cancel
}

func (s *Server) untrackJob(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, id)
}

func (s *Server) inFlightIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.inFlight))
	for id := range s.inFlight {
		ids = append(ids, id)
	}
	return ids
}

func (s *Server) inFlightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inFlight)
}

// cancelJob stops one running job without disturbing the others.
func (s *Server) cancelJob(id string) {
	s.mu.Lock()
	cancel, ok := s.inFlight[id]
	s.mu.Unlock()
	if ok {
		cancel()
	}
}
