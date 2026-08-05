package taskq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
)

// TestLeaseRenewalOutlivesTheVisibilityTimeout is the reason renewal exists: a
// handler is allowed to run far longer than the visibility timeout, and must
// not be reaped out from under itself while it is still working.
//
// Without renewal the reaper would re-deliver this job at ~200ms and the
// handler would run a second time.
func TestLeaseRenewalOutlivesTheVisibilityTimeout(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var runs atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("slow", func(ctx context.Context, _ *taskq.Job) error {
		runs.Add(1)
		select {
		case <-time.After(800 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	cfg := quietConfig("default")
	// Renewal every ~66ms, reaping every ~100ms: the job outlives four
	// visibility timeouts.
	cfg.VisibilityTimeout = 200 * time.Millisecond
	cfg.ShutdownTimeout = 5 * time.Second
	stop := runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "slow", nil); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	waitFor(t, 10*time.Second, "the long job to complete", func() bool {
		return stats(t, b).Processed == 1
	})
	if err := stop(); err != nil {
		t.Errorf("Run returned %v", err)
	}

	if n := runs.Load(); n != 1 {
		t.Errorf("handler ran %d times, want 1 — renewal should have kept the lease alive", n)
	}
}

// lostLeaseBroker reports every renewal as a lost lease, which is what a worker
// sees after it has stalled long enough to be reaped and overtaken.
type lostLeaseBroker struct {
	*memory.Broker
}

func (l *lostLeaseBroker) Extend(_ context.Context, _ string, ids []string, _ time.Duration) ([]string, error) {
	return ids, nil
}

// TestLostLeaseCancelsTheJob: continuing to run a job another worker now owns
// is duplicate work by definition, so the handler's context is cancelled.
func TestLostLeaseCancelsTheJob(t *testing.T) {
	t.Parallel()

	b := &lostLeaseBroker{Broker: memory.New()}
	client := taskq.NewClient(b)

	cancelled := make(chan struct{})
	mux := taskq.NewMux()
	mux.HandleFunc("stalled", func(ctx context.Context, _ *taskq.Job) error {
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})

	cfg := quietConfig("default")
	cfg.VisibilityTimeout = 150 * time.Millisecond // renewal every 50ms
	cfg.ShutdownTimeout = 5 * time.Second
	runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "stalled", nil); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was never cancelled after its lease was reported lost")
	}
}

// brokenExtendBroker fails every renewal, as a worker would see if it lost its
// connection to the broker.
type brokenExtendBroker struct {
	*memory.Broker
	calls atomic.Int64
}

func (e *brokenExtendBroker) Extend(context.Context, string, []string, time.Duration) ([]string, error) {
	e.calls.Add(1)
	return nil, errors.New("connection refused")
}

// TestRepeatedRenewalFailureCancelsJobs: after enough consecutive failures the
// worker concludes it has lost every lease and stops, rather than burning
// capacity on work that is already being redone elsewhere.
func TestRepeatedRenewalFailureCancelsJobs(t *testing.T) {
	t.Parallel()

	b := &brokenExtendBroker{Broker: memory.New()}
	client := taskq.NewClient(b)

	cancelled := make(chan struct{})
	mux := taskq.NewMux()
	mux.HandleFunc("stalled", func(ctx context.Context, _ *taskq.Job) error {
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})

	cfg := quietConfig("default")
	cfg.VisibilityTimeout = 150 * time.Millisecond // renewal every 50ms
	cfg.ShutdownTimeout = 5 * time.Second
	runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "stalled", nil); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was never cancelled after repeated renewal failures")
	}

	if n := b.calls.Load(); n < 3 {
		t.Errorf("Extend was called %d times; the worker should tolerate a few failures before giving up", n)
	}
}

// TestReaperRecoversAnOrphanedJob simulates a worker that claimed a job and
// died: the claim exists with a deadline nobody will renew, and no ack or nack
// is ever coming. Nothing but the reaper will ever look at it again.
func TestReaperRecoversAnOrphanedJob(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	job, err := client.EnqueueTask(context.Background(), "orphaned", nil)
	if err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	// A worker claims it and is never heard from again.
	claimed, err := b.Dequeue(context.Background(), taskq.ClaimOpts{
		WorkerID:          "worker-that-died",
		Queues:            []string{"default"},
		VisibilityTimeout: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if claimed.ID != job.ID {
		t.Fatalf("claimed %s, want %s", claimed.ID, job.ID)
	}

	var runs atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("orphaned", func(context.Context, *taskq.Job) error {
		runs.Add(1)
		return nil
	})

	cfg := quietConfig("default")
	cfg.VisibilityTimeout = 150 * time.Millisecond
	stop := runServer(t, b, cfg, mux)

	waitFor(t, 10*time.Second, "the orphan to be recovered and run", func() bool {
		return stats(t, b).Processed == 1
	})
	if err := stop(); err != nil {
		t.Errorf("Run returned %v", err)
	}

	if n := runs.Load(); n != 1 {
		t.Errorf("handler ran %d times, want 1", n)
	}
}

// TestRetryCycleCompletes proves the loop actually closes: a job that fails is
// nacked with a backoff, promoted back onto its queue when the backoff elapses,
// and run again. Before promotion existed the job simply sat in the retry set
// forever.
func TestRetryCycleCompletes(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var attempts atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("flaky", func(_ context.Context, j *taskq.Job) error {
		attempts.Add(1)
		if j.Attempt < 3 {
			return errors.New("connection refused")
		}
		return nil
	})

	cfg := quietConfig("default")
	cfg.VisibilityTimeout = 200 * time.Millisecond // promotion every ~100ms
	cfg.Backoff = &taskq.Exponential{Base: 10 * time.Millisecond, Cap: 20 * time.Millisecond, Jitter: taskq.JitterNone}
	stop := runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "flaky", nil, taskq.MaxAttempts(5)); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	waitFor(t, 15*time.Second, "the job to succeed on its third attempt", func() bool {
		return stats(t, b).Processed == 1
	})
	if err := stop(); err != nil {
		t.Errorf("Run returned %v", err)
	}

	if n := attempts.Load(); n != 3 {
		t.Errorf("handler ran %d times, want 3 — two failures then a success", n)
	}
	if s := stats(t, b); s.Dead != 0 {
		t.Errorf("Dead = %d, want 0", s.Dead)
	}
}

// TestExhaustedRetriesReachTheDeadLetterQueue is the same loop with a job that
// never succeeds: it must terminate rather than cycle forever.
func TestExhaustedRetriesReachTheDeadLetterQueue(t *testing.T) {
	t.Parallel()

	b := memory.New()
	client := taskq.NewClient(b)

	var attempts atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("doomed", func(context.Context, *taskq.Job) error {
		attempts.Add(1)
		return errors.New("always fails")
	})

	cfg := quietConfig("default")
	cfg.VisibilityTimeout = 200 * time.Millisecond
	cfg.Backoff = &taskq.Exponential{Base: 10 * time.Millisecond, Cap: 10 * time.Millisecond, Jitter: taskq.JitterNone}
	stop := runServer(t, b, cfg, mux)

	if _, err := client.EnqueueTask(context.Background(), "doomed", nil, taskq.MaxAttempts(3)); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	waitFor(t, 15*time.Second, "the job to be dead-lettered", func() bool {
		return stats(t, b).Dead == 1
	})
	if err := stop(); err != nil {
		t.Errorf("Run returned %v", err)
	}

	if n := attempts.Load(); n != 3 {
		t.Errorf("handler ran %d times, want exactly the 3 attempts it was allowed", n)
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

// TestHeartbeatIsPublished: an operator should be able to see which workers are
// alive and what they are carrying.
func TestHeartbeatIsPublished(t *testing.T) {
	t.Parallel()

	b := memory.New()
	cfg := quietConfig("default")
	cfg.VisibilityTimeout = 200 * time.Millisecond
	cfg.Concurrency = 7

	srv, err := taskq.NewServer(b, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux := taskq.NewMux()
	mux.HandleFunc("noop", func(context.Context, *taskq.Job) error { return nil })
	go func() { _ = srv.Run(ctx, mux) }()

	waitFor(t, 10*time.Second, "a heartbeat to appear", func() bool {
		workers, err := b.Workers(context.Background())
		return err == nil && len(workers) == 1
	})

	workers, err := b.Workers(context.Background())
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	w := workers[0]
	if w.ID != srv.WorkerID() {
		t.Errorf("worker ID = %q, want %q", w.ID, srv.WorkerID())
	}
	if w.Concurrency != 7 {
		t.Errorf("Concurrency = %d, want 7", w.Concurrency)
	}
	if len(w.Queues) != 1 || w.Queues[0] != "default" {
		t.Errorf("Queues = %v, want [default]", w.Queues)
	}
	if w.PID == 0 {
		t.Error("PID should be recorded")
	}
}
