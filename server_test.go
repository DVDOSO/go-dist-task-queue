// This suite lives in package taskq_test rather than taskq because it drives a
// real broker: internal/memory imports taskq, so an in-package test importing
// it back would be an import cycle.
package taskq_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
)

// quietConfig returns a Config that logs nowhere, so a passing test run is not
// buried under structured logs.
func quietConfig(queues ...string) taskq.Config {
	return taskq.Config{
		Queues: queues,
		Logger: slog.New(slog.DiscardHandler),
	}
}

// waitFor polls cond until it holds or the timeout expires. Polling rather than
// signalling keeps the tests decoupled from the server's internals.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func stats(t *testing.T, b *memory.Broker) taskq.Stats {
	t.Helper()
	s, err := b.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	return s
}

// runServer starts a server in the background and returns a stop function that
// cancels it and reports Run's error.
func runServer(t *testing.T, b taskq.Broker, cfg taskq.Config, h taskq.Handler) func() error {
	t.Helper()

	srv, err := taskq.NewServer(b, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(ctx, h) }()

	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-errCh:
			return err
		case <-time.After(20 * time.Second):
			return errors.New("Run did not return after cancellation")
		}
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func TestNewServerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		broker taskq.Broker
		cfg    taskq.Config
	}{
		{"nil broker", nil, quietConfig("default")},
		{"no queues", memory.New(), quietConfig()},
		{"empty queue name", memory.New(), quietConfig("default", "")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := taskq.NewServer(tt.broker, tt.cfg); err == nil {
				t.Error("NewServer should have rejected this configuration")
			}
		})
	}
}

func TestNewServerAppliesDefaults(t *testing.T) {
	t.Parallel()

	srv, err := taskq.NewServer(memory.New(), quietConfig("default"))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.WorkerID() == "" {
		t.Error("WorkerID should be generated when not configured")
	}
}

func TestRunRejectsNilHandler(t *testing.T) {
	t.Parallel()

	srv, err := taskq.NewServer(memory.New(), quietConfig("default"))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := srv.Run(context.Background(), nil); err == nil {
		t.Error("Run(nil handler) should fail")
	}
}

func TestServerProcessesJobs(t *testing.T) {
	t.Parallel()

	const jobs = 50
	b := memory.New()
	client := taskq.NewClient(b)

	var done atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("work", func(context.Context, *taskq.Job) error {
		done.Add(1)
		return nil
	})

	cfg := quietConfig("default")
	cfg.Concurrency = 4
	stop := runServer(t, b, cfg, mux)

	for i := range jobs {
		if _, err := client.EnqueueTask(context.Background(), "work", map[string]int{"n": i}); err != nil {
			t.Fatalf("EnqueueTask: %v", err)
		}
	}

	waitFor(t, 10*time.Second, "all jobs to run", func() bool { return done.Load() == jobs })

	if err := stop(); err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	if s := stats(t, b); s.Processed != jobs {
		t.Errorf("Processed = %d, want %d — every job should have been acked", s.Processed, jobs)
	}
}

// TestServerRespectsConcurrencyLimit is the point of the semaphore: capacity is
// a hard ceiling, not a target.
func TestServerRespectsConcurrencyLimit(t *testing.T) {
	t.Parallel()

	const (
		jobs        = 40
		concurrency = 3
	)

	b := memory.New()
	client := taskq.NewClient(b)

	var (
		mu        sync.Mutex
		inFlight  int
		maxInFlyt int
		completed atomic.Int64
	)

	mux := taskq.NewMux()
	mux.HandleFunc("work", func(context.Context, *taskq.Job) error {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlyt {
			maxInFlyt = inFlight
		}
		mu.Unlock()

		time.Sleep(5 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
		completed.Add(1)
		return nil
	})

	cfg := quietConfig("default")
	cfg.Concurrency = concurrency
	runServer(t, b, cfg, mux)

	for range jobs {
		if _, err := client.EnqueueTask(context.Background(), "work", nil); err != nil {
			t.Fatalf("EnqueueTask: %v", err)
		}
	}

	waitFor(t, 15*time.Second, "all jobs to run", func() bool { return completed.Load() == jobs })

	mu.Lock()
	peak := maxInFlyt
	mu.Unlock()

	if peak > concurrency {
		t.Errorf("peak concurrency was %d, which exceeds the configured limit of %d", peak, concurrency)
	}
	if peak < 2 {
		t.Errorf("peak concurrency was %d; the pool does not appear to run jobs in parallel at all", peak)
	}
}

func TestServerRetriesFailedJobs(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var attempts atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("flaky", func(context.Context, *taskq.Job) error {
		attempts.Add(1)
		return errors.New("connection refused")
	})

	cfg := quietConfig("default")
	// A short, deterministic backoff keeps the test fast and its assertions
	// about state rather than timing.
	cfg.Backoff = &taskq.Exponential{Base: time.Hour, Cap: time.Hour, Jitter: taskq.JitterNone}
	runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "flaky", nil, taskq.MaxAttempts(3)); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	waitFor(t, 10*time.Second, "the first attempt", func() bool { return attempts.Load() == 1 })
	waitFor(t, 10*time.Second, "the job to land in retry", func() bool {
		return stats(t, b).Queues["default"].Retry == 1
	})

	s := stats(t, b)
	if s.Processed != 0 {
		t.Errorf("Processed = %d, want 0 — a failing job must not be acked", s.Processed)
	}
	if s.Dead != 0 {
		t.Errorf("Dead = %d, want 0 — attempts remain, so it should retry", s.Dead)
	}
}

func TestServerDeadLettersWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	mux := taskq.NewMux()
	mux.HandleFunc("doomed", func(context.Context, *taskq.Job) error {
		return errors.New("always fails")
	})

	cfg := quietConfig("default")
	cfg.Backoff = &taskq.Exponential{Base: time.Millisecond, Cap: time.Millisecond, Jitter: taskq.JitterNone}
	runServer(t, b, cfg, mux)

	// One attempt only, so the first failure exhausts it.
	if _, err := client.EnqueueTask(context.Background(), "doomed", nil, taskq.MaxAttempts(1)); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	waitFor(t, 10*time.Second, "the job to be dead-lettered", func() bool {
		return stats(t, b).Dead == 1
	})
}

func TestServerSkipRetryDeadLettersImmediately(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var calls atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("malformed", func(context.Context, *taskq.Job) error {
		calls.Add(1)
		return fmt.Errorf("payload is not valid json: %w", taskq.ErrSkipRetry)
	})

	runServer(t, b, quietConfig("default"), mux)

	// Twenty attempts available, none of which should be used.
	if _, err := client.EnqueueTask(context.Background(), "malformed", nil, taskq.MaxAttempts(20)); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	waitFor(t, 10*time.Second, "the job to be dead-lettered", func() bool {
		return stats(t, b).Dead == 1
	})

	// Give the loop a moment to prove it is not retrying.
	time.Sleep(100 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Errorf("handler ran %d times, want 1 — ErrSkipRetry must bypass the attempt budget", n)
	}
}

// TestServerRecoversHandlerPanics: one bad handler must not take down a worker
// that is running other jobs correctly.
func TestServerRecoversHandlerPanics(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var good atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("panics", func(context.Context, *taskq.Job) error {
		panic("handler exploded")
	})
	mux.HandleFunc("fine", func(context.Context, *taskq.Job) error {
		good.Add(1)
		return nil
	})

	cfg := quietConfig("default")
	cfg.Concurrency = 2
	cfg.Backoff = &taskq.Exponential{Base: time.Hour, Cap: time.Hour, Jitter: taskq.JitterNone}
	runServer(t, b, cfg, mux)

	ctx := context.Background()
	if _, err := client.EnqueueTask(ctx, "panics", nil, taskq.MaxAttempts(3)); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}
	waitFor(t, 10*time.Second, "the panicking job to be retried", func() bool {
		return stats(t, b).Queues["default"].Retry == 1
	})

	// The pool is still alive and doing work.
	for range 5 {
		if _, err := client.EnqueueTask(ctx, "fine", nil); err != nil {
			t.Fatalf("EnqueueTask: %v", err)
		}
	}
	waitFor(t, 10*time.Second, "later jobs to run after the panic", func() bool {
		return good.Load() == 5
	})
}

// TestGracefulShutdownDrainsInFlightWork: cancelling the context must let
// running handlers finish, not kill them.
func TestGracefulShutdownDrainsInFlightWork(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	started := make(chan struct{})
	var finished atomic.Bool

	mux := taskq.NewMux()
	mux.HandleFunc("slow", func(ctx context.Context, _ *taskq.Job) error {
		close(started)
		// Long enough that an abrupt shutdown would visibly cut it short.
		select {
		case <-time.After(300 * time.Millisecond):
			finished.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	cfg := quietConfig("default")
	cfg.ShutdownTimeout = 10 * time.Second
	stop := runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "slow", nil); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	<-started
	if err := stop(); err != nil {
		t.Errorf("Run returned %v, want a clean drain", err)
	}

	if !finished.Load() {
		t.Error("the handler was cut short; shutdown should have waited for it")
	}
	if s := stats(t, b); s.Processed != 1 {
		t.Errorf("Processed = %d, want 1 — a drained job should still be acked", s.Processed)
	}
}

// TestShutdownDeadlineCancelsAndRequeues: a handler that overruns the deadline
// is cancelled, and its job is nacked so it retries promptly rather than
// sitting claimed until the visibility timeout expires.
func TestShutdownDeadlineCancelsAndRequeues(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	started := make(chan struct{})
	var wasCancelled atomic.Bool

	mux := taskq.NewMux()
	mux.HandleFunc("stubborn", func(ctx context.Context, _ *taskq.Job) error {
		close(started)
		<-ctx.Done()
		wasCancelled.Store(true)
		return ctx.Err()
	})

	cfg := quietConfig("default")
	cfg.ShutdownTimeout = 150 * time.Millisecond
	cfg.Backoff = &taskq.Exponential{Base: time.Millisecond, Cap: time.Millisecond, Jitter: taskq.JitterNone}
	stop := runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "stubborn", nil, taskq.MaxAttempts(5)); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	<-started
	if err := stop(); err != nil {
		t.Errorf("Run returned %v; the handler honours cancellation so the drain should complete", err)
	}

	if !wasCancelled.Load() {
		t.Error("the handler was never cancelled after the shutdown deadline")
	}

	s := stats(t, b)
	if s.Processed != 0 {
		t.Errorf("Processed = %d, want 0 — an abandoned job must not be acked", s.Processed)
	}
	if s.Queues["default"].Retry != 1 {
		t.Errorf("Retry = %d, want 1 — the cancelled job should be scheduled to retry, not left claimed",
			s.Queues["default"].Retry)
	}
}

func TestServerConsumesMultipleQueuesInOrder(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var mu sync.Mutex
	var order []string

	mux := taskq.NewMux()
	mux.HandleFunc("work", func(_ context.Context, j *taskq.Job) error {
		mu.Lock()
		order = append(order, j.Queue)
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	for range 3 {
		if _, err := client.EnqueueTask(ctx, "work", nil, taskq.Queue("low")); err != nil {
			t.Fatalf("EnqueueTask: %v", err)
		}
	}
	for range 3 {
		if _, err := client.EnqueueTask(ctx, "work", nil, taskq.Queue("high")); err != nil {
			t.Fatalf("EnqueueTask: %v", err)
		}
	}

	cfg := quietConfig("high", "low")
	// Serial, so the observed order is the fetch order rather than a race
	// between concurrent workers.
	cfg.Concurrency = 1
	runServer(t, b, cfg, mux)

	waitFor(t, 10*time.Second, "all six jobs to run", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 6
	})

	mu.Lock()
	defer mu.Unlock()
	for i, q := range order[:3] {
		if q != "high" {
			t.Errorf("job %d came from %q, want the high queue drained first", i, q)
		}
	}
}
