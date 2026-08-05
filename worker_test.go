package taskq_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
)

// Job execution: the pool, retries, dead-lettering, panics, and queue weighting.

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
	// Strict priority, because that is what an absolute ordering assertion is
	// testing. The default is weighted-random, where "high before low" is a
	// probability rather than a guarantee -- see TestWeightedConsumption.
	cfg.StrictPriority = true
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

// TestWeightedConsumption is the end-to-end version of the weighting policy:
// with every queue kept full, the share of jobs consumed from each should track
// its configured weight, and no queue should be starved out entirely.
func TestWeightedConsumption(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var (
		mu     sync.Mutex
		counts = map[string]int{}
		total  int
	)
	mux := taskq.NewMux()
	mux.HandleFunc("work", func(_ context.Context, j *taskq.Job) error {
		mu.Lock()
		counts[j.Queue]++
		total++
		mu.Unlock()
		return nil
	})

	// Deep enough that no queue drains before the sample is taken; otherwise
	// the ratio would be measuring exhaustion rather than policy.
	ctx := context.Background()
	const perQueue = 4000
	for _, q := range []string{"critical", "default", "low"} {
		for range perQueue {
			if _, err := client.EnqueueTask(ctx, "work", nil, taskq.Queue(q)); err != nil {
				t.Fatalf("EnqueueTask: %v", err)
			}
		}
	}

	cfg := quietConfig("critical", "default", "low")
	cfg.Weights = map[string]int{"critical": 6, "default": 3, "low": 1}
	cfg.Concurrency = 1
	runServer(t, b, cfg, mux)

	const sample = 3000
	waitFor(t, 30*time.Second, "enough jobs to measure a ratio", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return total >= sample
	})

	mu.Lock()
	defer mu.Unlock()

	want := map[string]float64{"critical": 0.6, "default": 0.3, "low": 0.1}
	const tolerance = 0.05
	for q, wantShare := range want {
		got := float64(counts[q]) / float64(total)
		if got < wantShare-tolerance || got > wantShare+tolerance {
			t.Errorf("%s took %.1f%% of %d jobs, want ~%.0f%%", q, got*100, total, wantShare*100)
		}
	}
	if counts["low"] == 0 {
		t.Error("the low queue was starved entirely")
	}
}
