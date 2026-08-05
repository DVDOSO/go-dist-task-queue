package taskq_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
)

// Server construction, configuration, and shutdown.

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
