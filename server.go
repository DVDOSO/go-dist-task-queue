package taskq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

// Worker runtime defaults.
const (
	DefaultConcurrency       = 10
	DefaultVisibilityTimeout = 30 * time.Second
	DefaultShutdownTimeout   = 30 * time.Second

	// Poll pacing. The fetch loop has no long-poll to block on, so it asks,
	// sleeps, and asks again -- backing off while idle so an empty queue costs
	// four requests a second per worker rather than thousands.
	minPollInterval = 5 * time.Millisecond
	maxPollInterval = 250 * time.Millisecond

	// How long a completion report (ack/nack) may take. Deliberately short and
	// independent of the job's own context.
	reportTimeout = 10 * time.Second

	// Grace period after cancelling in-flight jobs at the shutdown deadline,
	// so their nacks have a chance to land before the process exits.
	forceGracePeriod = 5 * time.Second
)

// Config configures a [Server].
type Config struct {
	// Queues to consume, in the order they should be tried. Required.
	Queues []string

	// Concurrency is the maximum number of jobs running at once. Defaults to
	// DefaultConcurrency.
	Concurrency int

	// VisibilityTimeout is how long a claim is valid before the job becomes
	// eligible for re-delivery. Until lease renewal exists, a handler that runs
	// longer than this will have its job re-delivered while it is still going.
	VisibilityTimeout time.Duration

	// ShutdownTimeout is how long a graceful shutdown waits for in-flight jobs
	// before cancelling them.
	ShutdownTimeout time.Duration

	// Backoff computes the retry delay after a failed attempt. Defaults to
	// Exponential with full jitter.
	Backoff Backoff

	// WorkerID identifies this process. Generated if empty.
	WorkerID string

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Server is the worker runtime: it claims jobs, runs them through a handler,
// and reports the outcome.
type Server struct {
	broker Broker
	cfg    Config
	log    *slog.Logger
}

// NewServer validates a configuration and returns a worker.
func NewServer(b Broker, cfg Config) (*Server, error) {
	if b == nil {
		return nil, errors.New("taskq: nil broker")
	}
	if len(cfg.Queues) == 0 {
		return nil, errors.New("taskq: Config.Queues must name at least one queue")
	}
	for _, q := range cfg.Queues {
		if q == "" {
			return nil, errors.New("taskq: Config.Queues contains an empty queue name")
		}
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultConcurrency
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = DefaultVisibilityTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.Backoff == nil {
		cfg.Backoff = &Exponential{}
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = generateWorkerID()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Server{
		broker: b,
		cfg:    cfg,
		log:    cfg.Logger.With(slog.String("worker_id", cfg.WorkerID)),
	}, nil
}

// WorkerID returns this server's identity, which is also the owner stamped on
// every job it claims.
func (s *Server) WorkerID() string { return s.cfg.WorkerID }

// Run consumes jobs until ctx is cancelled, then shuts down gracefully.
//
// Cancelling ctx does not cancel jobs that are already running. It stops new
// claims and starts draining: in-flight handlers get up to ShutdownTimeout to
// finish normally. Only if that expires are they cancelled, and even then their
// failures are reported so the jobs retry promptly instead of waiting out a
// visibility timeout.
//
// Returns nil on a clean drain, or an error if jobs were still running after
// the deadline and the grace period.
func (s *Server) Run(ctx context.Context, h Handler) error {
	if h == nil {
		return errors.New("taskq: nil handler")
	}

	// Jobs deliberately do not inherit cancellation from ctx. If they did,
	// SIGTERM would kill every in-flight handler instantly and "graceful
	// shutdown" would mean nothing. Instead they get a detached context that
	// this function holds the only cancel for, used as a last resort.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	// A buffered channel is the whole concurrency limiter: one slot per
	// permitted in-flight job.
	slots := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup

	s.log.Info("worker started",
		slog.Any("queues", s.cfg.Queues),
		slog.Int("concurrency", s.cfg.Concurrency),
		slog.Duration("visibility_timeout", s.cfg.VisibilityTimeout))

	s.fetchLoop(ctx, jobCtx, h, slots, &wg)

	s.log.Info("worker draining", slog.Duration("shutdown_timeout", s.cfg.ShutdownTimeout))
	return s.drain(&wg, cancelJobs)
}

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

		job, err := s.broker.Dequeue(ctx, ClaimOpts{
			WorkerID:          s.cfg.WorkerID,
			Queues:            s.cfg.Queues,
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
	start := time.Now()
	err := s.invoke(ctx, h, j)
	elapsed := time.Since(start)

	// The outcome must be reported even when the job's own context has been
	// cancelled, so this gets a fresh one. Reusing the cancelled context would
	// mean a shutdown left every in-flight job's lease dangling until the
	// reaper noticed -- turning a clean stop into a pile of duplicate work.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()

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
			s.log.Error("nack failed",
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

// drain waits for in-flight jobs, escalating to cancellation if they overrun.
func (s *Server) drain(wg *sync.WaitGroup, cancelJobs func()) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.log.Info("shutdown complete")
		return nil
	case <-time.After(s.cfg.ShutdownTimeout):
	}

	// Out of patience. Cancelling gives handlers that respect their context a
	// chance to return an error, which is then nacked -- so the job retries
	// promptly rather than sitting claimed until its lease expires.
	s.log.Warn("shutdown deadline exceeded, cancelling in-flight jobs",
		slog.Duration("waited", s.cfg.ShutdownTimeout))
	cancelJobs()

	select {
	case <-done:
		s.log.Info("shutdown complete after cancellation")
		return nil
	case <-time.After(forceGracePeriod):
		// Handlers that ignore their context cannot be stopped. Their jobs will
		// be re-delivered once the visibility timeout expires.
		s.log.Error("abandoning jobs that ignored cancellation",
			slog.Duration("grace", forceGracePeriod))
		return errors.New("taskq: shutdown timed out with jobs still running")
	}
}

// sleepCtx waits for d, reporting false if ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// jitter spreads a poll interval over [d/2, d) so workers that started together
// do not settle into synchronised polling.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half))) //nolint:gosec // G404: poll spacing, not security
}

// generateWorkerID builds an identifier that is unique across processes and
// recognisable to a human reading a job's owner field.
func generateWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), NewID()[:8])
}
