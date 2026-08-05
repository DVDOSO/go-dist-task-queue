// This suite lives in package taskq_test because internal/memory imports
// taskq, so an in-package test importing it back would be an import cycle.
package taskq_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
)

// Shared fixtures for the worker, maintenance, and scheduler suites.

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

// brokerOnly hides everything but the hot path. Embedding the interface rather
// than the concrete type means Maintenance and CronStore are not promoted, which
// is how a broker without scheduling support looks to a Server.
type brokerOnly struct{ taskq.Broker }

// startServer runs a server in the background and returns a stop function.
func startServer(t *testing.T, b taskq.Broker, cfg taskq.Config, h taskq.Handler) (srv *taskq.Server, stop func()) {
	t.Helper()

	srv, err := taskq.NewServer(b, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Run(ctx, h)
	}()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Error("Run did not return after cancellation")
			}
		})
	}
	t.Cleanup(stop)
	return srv, stop
}

func countLeaders(servers []*taskq.Server) int {
	n := 0
	for _, s := range servers {
		if s.IsLeader() {
			n++
		}
	}
	return n
}
