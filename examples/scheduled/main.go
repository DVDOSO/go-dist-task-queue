// Command scheduled demonstrates cron scheduling and leader election.
//
// Three workers are given the same schedule. Only one of them fires it: they
// elect a leader through a lease, and every tick is claimed with a
// compare-and-set so even an overlap during failover cannot double-fire.
//
//	go run ./examples/scheduled
//
// Or against a real Redis, where the leadership is genuinely contended across
// connections rather than within one process:
//
//	docker run -d -p 6379:6379 redis:7-alpine
//	REDIS_ADDR=localhost:6379 go run ./examples/scheduled
//
// Partway through, the current leader is stopped to show a survivor taking
// over. Because the leader releases its lease on shutdown, failover is
// immediate rather than waiting out the lease TTL.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
	"github.com/DVDOSO/go-dist-task-queue/internal/rdb"

	"github.com/redis/go-redis/v9"
)

const (
	workers   = 3
	tickEvery = 400 * time.Millisecond
	runFor    = 4 * time.Second
	killAfter = 2 * time.Second
)

type node struct {
	name string
	srv  *taskq.Server
	stop func()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	broker, describe, cleanup, err := openBroker()
	if err != nil {
		return err
	}
	defer cleanup()
	fmt.Printf("broker: %s\n", describe)
	fmt.Printf("%d workers share one schedule firing every %v\n\n", workers, tickEvery)

	// Every fired tick is recorded by its unique key, which encodes the exact
	// scheduled time. Duplicate keys would mean a tick fired twice.
	var (
		mu    sync.Mutex
		fired []string
	)

	mux := taskq.NewMux()
	mux.HandleFunc("report:generate", func(_ context.Context, j *taskq.Job) error {
		mu.Lock()
		fired = append(fired, j.UniqueKey)
		mu.Unlock()
		fmt.Printf("   [%s] ran %s\n", time.Now().Format("15:04:05.000"), j.UniqueKey)
		return nil
	})

	entry := taskq.CronEntry{
		ID:   "nightly-report",
		Spec: "@every " + tickEvery.String(),
		Type: "report:generate",
	}

	nodes := make([]*node, 0, workers)
	// One deferred sweep rather than a defer per iteration: stop is
	// idempotent, so stopping an already-stopped node is harmless.
	defer func() {
		for _, n := range nodes {
			n.stop()
		}
	}()
	for i := range workers {
		n, err := startWorker(broker, fmt.Sprintf("worker-%d", i), entry, mux)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}

	// Let an election settle.
	time.Sleep(600 * time.Millisecond)
	fmt.Printf(">>> leader: %s\n\n", leaderName(nodes))

	time.Sleep(killAfter)

	leader := currentLeader(nodes)
	if leader != nil {
		fmt.Printf("\n>>> stopping the leader (%s)\n", leader.name)
		leader.stop()
		leader.srv = nil
	}

	time.Sleep(700 * time.Millisecond)
	fmt.Printf(">>> new leader: %s\n\n", leaderName(nodes))

	time.Sleep(runFor - killAfter)

	mu.Lock()
	defer mu.Unlock()
	return report(fired)
}

func report(fired []string) error {
	seen := make(map[string]int, len(fired))
	for _, k := range fired {
		seen[k]++
	}

	dupes := make([]string, 0)
	for k, n := range seen {
		if n > 1 {
			dupes = append(dupes, fmt.Sprintf("%s x%d", k, n))
		}
	}
	sort.Strings(dupes)

	fmt.Printf("\n--- summary ---\n")
	fmt.Printf("ticks fired      : %d\n", len(fired))
	fmt.Printf("distinct ticks   : %d\n", len(seen))
	fmt.Printf("double fires     : %d\n", len(dupes))
	for _, d := range dupes {
		fmt.Printf("   %s\n", d)
	}

	if len(dupes) > 0 {
		return fmt.Errorf("a tick fired more than once")
	}
	fmt.Printf("\nEvery tick fired exactly once, across a leadership handover.\n")
	return nil
}

func startWorker(broker taskq.Broker, name string, entry taskq.CronEntry, h taskq.Handler) (*node, error) {
	srv, err := taskq.NewServer(broker, taskq.Config{
		Queues: []string{taskq.DefaultQueue},
		// The scheduler loop ticks at min(5s, VisibilityTimeout/2), so a short
		// visibility timeout keeps the demo responsive.
		VisibilityTimeout: 400 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
		Concurrency:       4,
		WorkerID:          name,
		Cron:              []taskq.CronEntry{entry},
		Logger:            slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Run(ctx, h)
	}()

	var once sync.Once
	return &node{
		name: name,
		srv:  srv,
		stop: func() {
			once.Do(func() {
				cancel()
				<-done
			})
		},
	}, nil
}

func currentLeader(nodes []*node) *node {
	for _, n := range nodes {
		if n.srv != nil && n.srv.IsLeader() {
			return n
		}
	}
	return nil
}

func leaderName(nodes []*node) string {
	if n := currentLeader(nodes); n != nil {
		return n.name
	}
	return "(none yet)"
}

// openBroker returns the Redis broker when REDIS_ADDR is set, otherwise the
// in-memory one.
func openBroker() (b taskq.Broker, describe string, cleanup func(), err error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return memory.New(), "in-memory (set REDIS_ADDR to use Redis)", func() {}, nil
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, "", nil, fmt.Errorf("connect to redis at %s: %w", addr, err)
	}

	prefix := fmt.Sprintf("taskq:sched:%d", time.Now().UnixNano())
	rb, err := rdb.New(client, rdb.WithPrefix(prefix))
	if err != nil {
		_ = client.Close()
		return nil, "", nil, err
	}
	return rb, "redis at " + addr, func() {
		scanDelete(client, prefix)
		_ = client.Close()
	}, nil
}

func scanDelete(client *redis.Client, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 500).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}
