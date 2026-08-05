package taskq_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
)

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

func cronConfig(entries ...taskq.CronEntry) taskq.Config {
	cfg := quietConfig("default")
	// Short so the scheduler loop ticks quickly (its interval is
	// min(5s, VisibilityTimeout/2)), but not so short that a loaded
	// machine under -race can miss a window.
	cfg.VisibilityTimeout = 600 * time.Millisecond
	cfg.Cron = entries
	return cfg
}

func TestCronConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		broker taskq.Broker
		cron   []taskq.CronEntry
	}{
		{
			name:   "broker without CronStore",
			broker: &brokerOnly{Broker: memory.New()},
			cron:   []taskq.CronEntry{{ID: "a", Spec: "@every 1m", Type: "t"}},
		},
		{
			name:   "unparseable spec",
			broker: memory.New(),
			cron:   []taskq.CronEntry{{ID: "a", Spec: "not a cron spec", Type: "t"}},
		},
		{
			name:   "missing entry ID",
			broker: memory.New(),
			cron:   []taskq.CronEntry{{Spec: "@every 1m", Type: "t"}},
		},
		{
			name:   "missing task type",
			broker: memory.New(),
			cron:   []taskq.CronEntry{{ID: "a", Spec: "@every 1m"}},
		},
		{
			name:   "duplicate entry IDs",
			broker: memory.New(),
			cron: []taskq.CronEntry{
				{ID: "dup", Spec: "@every 1m", Type: "t"},
				{ID: "dup", Spec: "@every 2m", Type: "t"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := quietConfig("default")
			cfg.Cron = tt.cron
			if _, err := taskq.NewServer(tt.broker, cfg); err == nil {
				t.Error("NewServer should have rejected this cron configuration")
			}
		})
	}
}

func TestCronEntryFires(t *testing.T) {
	t.Parallel()

	b := memory.New()
	var runs atomic.Int64
	mux := taskq.NewMux()
	mux.HandleFunc("cron:tick", func(context.Context, *taskq.Job) error {
		runs.Add(1)
		return nil
	})

	startServer(t, b, cronConfig(taskq.CronEntry{
		ID:   "tick",
		Spec: "@every 200ms",
		Type: "cron:tick",
	}), mux)

	waitFor(t, 15*time.Second, "the schedule to fire at least twice", func() bool {
		return runs.Load() >= 2
	})
}

// TestCronFiresOncePerTickAcrossSchedulers is the property leader election and
// the compare-and-set claim exist for: three schedulers watching one schedule
// must not produce three jobs per tick.
//
// Every executed job carries a unique key derived from its tick, so asserting
// the keys are all distinct is a direct check that no tick fired twice.
func TestCronFiresOncePerTickAcrossSchedulers(t *testing.T) {
	t.Parallel()

	b := memory.New()

	var (
		mu   sync.Mutex
		keys []string
	)
	mux := taskq.NewMux()
	mux.HandleFunc("cron:tick", func(_ context.Context, j *taskq.Job) error {
		mu.Lock()
		keys = append(keys, j.UniqueKey)
		mu.Unlock()
		return nil
	})

	entry := taskq.CronEntry{ID: "shared", Spec: "@every 200ms", Type: "cron:tick"}
	for range 3 {
		startServer(t, b, cronConfig(entry), mux)
	}

	waitFor(t, 20*time.Second, "several ticks to fire", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(keys) >= 4
	})

	mu.Lock()
	defer mu.Unlock()

	seen := make(map[string]int, len(keys))
	for _, k := range keys {
		if k == "" {
			t.Fatal("a cron job was enqueued without a unique key")
		}
		seen[k]++
	}
	for k, n := range seen {
		if n > 1 {
			t.Errorf("tick %q fired %d times, want 1", k, n)
		}
	}
	if len(seen) != len(keys) {
		t.Errorf("%d executions across %d distinct ticks; every tick should be unique",
			len(keys), len(seen))
	}
}

// TestExactlyOneLeader: the lease is what stops every worker doing the same
// scheduling bookkeeping.
func TestExactlyOneLeader(t *testing.T) {
	t.Parallel()

	b := memory.New()
	mux := taskq.NewMux()
	mux.HandleFunc("cron:tick", func(context.Context, *taskq.Job) error { return nil })

	entry := taskq.CronEntry{ID: "solo", Spec: "@every 1h", Type: "cron:tick"}
	servers := make([]*taskq.Server, 0, 3)
	for range 3 {
		srv, _ := startServer(t, b, cronConfig(entry), mux)
		servers = append(servers, srv)
	}

	waitFor(t, 15*time.Second, "a leader to be elected", func() bool {
		return countLeaders(servers) == 1
	})

	// And it stays that way rather than flapping.
	for range 5 {
		time.Sleep(200 * time.Millisecond)
		if n := countLeaders(servers); n != 1 {
			t.Fatalf("%d leaders, want exactly 1", n)
		}
	}
}

// TestLeadershipFailsOverOnShutdown: releasing the lease on the way out is what
// makes a rolling deploy pause the scheduler for a moment rather than for a
// full lease period.
func TestLeadershipFailsOverOnShutdown(t *testing.T) {
	t.Parallel()

	b := memory.New()
	mux := taskq.NewMux()
	mux.HandleFunc("cron:tick", func(context.Context, *taskq.Job) error { return nil })

	entry := taskq.CronEntry{ID: "failover", Spec: "@every 1h", Type: "cron:tick"}

	type node struct {
		srv  *taskq.Server
		stop func()
	}
	nodes := make([]node, 0, 3)
	for range 3 {
		srv, stop := startServer(t, b, cronConfig(entry), mux)
		nodes = append(nodes, node{srv: srv, stop: stop})
	}

	servers := make([]*taskq.Server, len(nodes))
	for i, n := range nodes {
		servers[i] = n.srv
	}

	waitFor(t, 15*time.Second, "a leader to be elected", func() bool {
		return countLeaders(servers) == 1
	})

	// Stop whichever one is leading.
	var remaining []*taskq.Server
	for _, n := range nodes {
		if n.srv.IsLeader() {
			n.stop()
			continue
		}
		remaining = append(remaining, n.srv)
	}

	waitFor(t, 15*time.Second, "a survivor to take over", func() bool {
		return countLeaders(remaining) == 1
	})
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
