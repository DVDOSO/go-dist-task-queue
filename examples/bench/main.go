// Command bench measures sustained throughput and enqueue-to-start latency.
//
// Workers are real OS processes, not goroutines, so the numbers include process
// boundaries and every Redis round trip a real deployment would pay.
//
//	docker run -d -p 6379:6379 redis:7-alpine
//	REDIS_ADDR=localhost:6379 go run ./examples/bench
//
// # What is being measured
//
// The throughput handler does nothing at all. That is deliberate: the number
// then describes the queue's own overhead -- enqueue, claim, ack, and the
// polling around them -- rather than how fast some arbitrary handler runs.
// Real throughput is whatever the handler allows, and this is its ceiling.
//
// Each job costs three Redis round trips (one EVALSHA to enqueue, one to claim,
// one to ack), so the ceiling is bounded by Redis round-trip time far more than
// by anything in Go.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/rdb"

	"github.com/redis/go-redis/v9"
)

const (
	queueName   = "bench"
	roleEnv     = "TASKQ_BENCH_ROLE"
	prefixEnv   = "TASKQ_BENCH_PREFIX"
	concurrency = 25

	throughputJobs = 5000
	latencyJobs    = 500
	producers      = 8

	runLimit = 5 * time.Minute
)

// workerCounts is the scaling ladder. Reporting the curve rather than one
// number is what makes the result meaningful: a queue that does not improve
// with more consumers is not really a queue.
var workerCounts = []int{1, 2, 4, 8}

type latencySample struct {
	Enqueued int64 `json:"t"`
}

func main() {
	var err error
	if os.Getenv(roleEnv) == "worker" {
		err = runWorker()
	} else {
		err = runCoordinator()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------- coordinator

type result struct {
	workers      int
	enqueueRate  float64
	endToEndRate float64
	elapsed      time.Duration
}

func runCoordinator() error {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return errors.New("REDIS_ADDR is required: workers are separate processes " +
			"and need a shared broker")
	}

	client := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), runLimit)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connect to redis at %s: %w", addr, err)
	}

	fmt.Printf("throughput: %d jobs per run, no-op handler, %d concurrency per worker\n",
		throughputJobs, concurrency)
	fmt.Printf("redis: %s\n\n", addr)

	results := make([]result, 0, len(workerCounts))
	for _, w := range workerCounts {
		r, err := throughputRun(ctx, client, addr, w)
		if err != nil {
			return err
		}
		results = append(results, r)
		fmt.Printf("  %d worker(s): %7.0f jobs/sec end-to-end  (enqueue %7.0f/sec, drained in %s)\n",
			r.workers, r.endToEndRate, r.enqueueRate, r.elapsed.Round(time.Millisecond))
	}

	fmt.Printf("\nlatency: %d jobs onto an idle queue with 4 workers already running\n", latencyJobs)
	percentiles, err := latencyRun(ctx, client, addr)
	if err != nil {
		return err
	}

	printSummary(results, percentiles)
	return nil
}

// throughputRun measures a full pipeline: producers enqueueing while consumers
// drain, which is what a busy queue actually looks like.
func throughputRun(ctx context.Context, client *redis.Client, addr string, workers int) (result, error) {
	prefix := fmt.Sprintf("taskq:bench:tp:%d:%d", workers, time.Now().UnixNano())
	defer cleanup(client, prefix)

	broker, err := rdb.New(client, rdb.WithPrefix(prefix))
	if err != nil {
		return result{}, err
	}

	children, err := spawnWorkers(ctx, prefix, addr, workers)
	if err != nil {
		return result{}, err
	}
	defer stopAll(children)

	// Wait until every worker has registered, so process startup is not counted
	// against the throughput number.
	if err := waitForWorkers(ctx, client, prefix, workers); err != nil {
		return result{}, err
	}

	start := time.Now()
	if err := enqueueConcurrently(ctx, broker, "bench:noop", throughputJobs, nil); err != nil {
		return result{}, err
	}
	enqueueDone := time.Now()

	if err := waitForProcessed(ctx, client, prefix, throughputJobs); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)

	return result{
		workers:      workers,
		enqueueRate:  float64(throughputJobs) / enqueueDone.Sub(start).Seconds(),
		endToEndRate: float64(throughputJobs) / elapsed.Seconds(),
		elapsed:      elapsed,
	}, nil
}

// latencyRun measures the delay between enqueueing a job and a handler starting
// it, on a queue that is otherwise empty. This is the number that the polling
// fetch strategy costs; a blocking fetch would improve it at the expense of
// weighted queue selection.
func latencyRun(ctx context.Context, client *redis.Client, addr string) ([]time.Duration, error) {
	prefix := fmt.Sprintf("taskq:bench:lat:%d", time.Now().UnixNano())
	defer cleanup(client, prefix)

	broker, err := rdb.New(client, rdb.WithPrefix(prefix))
	if err != nil {
		return nil, err
	}

	children, err := spawnWorkers(ctx, prefix, addr, 4)
	if err != nil {
		return nil, err
	}
	defer stopAll(children)

	if err := waitForWorkers(ctx, client, prefix, 4); err != nil {
		return nil, err
	}

	// One at a time with a gap, so each job meets an idle queue rather than
	// queueing behind the previous one. Measuring pickup latency under
	// saturation would just be measuring the backlog.
	tqClient := taskq.NewClient(broker, taskq.WithDefaultQueue(queueName))
	for range latencyJobs {
		payload := latencySample{Enqueued: time.Now().UnixNano()}
		if _, err := tqClient.EnqueueTask(ctx, "bench:latency", payload); err != nil {
			return nil, err
		}
		if !sleepCtx(ctx, 2*time.Millisecond) {
			return nil, ctx.Err()
		}
	}

	if err := waitForProcessed(ctx, client, prefix, latencyJobs); err != nil {
		return nil, err
	}

	raw, err := client.LRange(ctx, prefix+":bench:latencies", 0, -1).Result()
	if err != nil {
		return nil, err
	}
	samples := make([]time.Duration, 0, len(raw))
	for _, s := range raw {
		var ns int64
		if _, err := fmt.Sscan(s, &ns); err == nil {
			samples = append(samples, time.Duration(ns))
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples, nil
}

func enqueueConcurrently(ctx context.Context, broker taskq.Broker, taskType string, total int, payload any) error {
	client := taskq.NewClient(broker, taskq.WithDefaultQueue(queueName))

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs error
	)
	per := total / producers

	for p := range producers {
		count := per
		if p == producers-1 {
			count = total - per*(producers-1)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range count {
				if _, err := client.EnqueueTask(ctx, taskType, payload); err != nil {
					mu.Lock()
					errs = errors.Join(errs, err)
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return errs
}

// waitForProcessed polls the broker's own durable completion counter, which the
// ack script already maintains -- so measuring costs nothing extra per job.
func waitForProcessed(ctx context.Context, client *redis.Client, prefix string, want int) error {
	for {
		n, err := client.Get(ctx, prefix+":stat:processed").Int()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		if n >= want {
			return nil
		}
		if !sleepCtx(ctx, 5*time.Millisecond) {
			return fmt.Errorf("timed out with %d/%d processed", n, want)
		}
	}
}

func waitForWorkers(ctx context.Context, client *redis.Client, prefix string, want int) error {
	for {
		n, err := client.ZCard(ctx, prefix+":workers").Result()
		if err != nil {
			return err
		}
		if int(n) >= want {
			return nil
		}
		if !sleepCtx(ctx, 10*time.Millisecond) {
			return fmt.Errorf("only %d/%d workers registered", n, want)
		}
	}
}

func printSummary(results []result, samples []time.Duration) {
	fmt.Printf("\n--- results ---\n\n")
	fmt.Printf("%-9s %-18s %-18s %s\n", "workers", "end-to-end/sec", "enqueue/sec", "drain time")
	for _, r := range results {
		fmt.Printf("%-9d %-18.0f %-18.0f %s\n",
			r.workers, r.endToEndRate, r.enqueueRate, r.elapsed.Round(time.Millisecond))
	}

	if len(samples) > 0 {
		fmt.Printf("\nenqueue-to-start latency on an idle queue (%d samples)\n", len(samples))
		fmt.Printf("  p50 %v\n", pct(samples, 0.50).Round(time.Microsecond))
		fmt.Printf("  p95 %v\n", pct(samples, 0.95).Round(time.Microsecond))
		fmt.Printf("  p99 %v\n", pct(samples, 0.99).Round(time.Microsecond))
		fmt.Printf("  max %v\n", samples[len(samples)-1].Round(time.Microsecond))
	}

	best := results[0]
	for _, r := range results {
		if r.endToEndRate > best.endToEndRate {
			best = r
		}
	}
	fmt.Printf("\npeak: %.0f jobs/sec sustained across %d worker processes\n",
		best.endToEndRate, best.workers)
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return sorted[i]
}

// --------------------------------------------------------------- process mgmt

func spawnWorkers(ctx context.Context, prefix, addr string, n int) ([]*exec.Cmd, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate own binary: %w", err)
	}

	children := make([]*exec.Cmd, 0, n)
	for range n {
		// self is os.Executable(), not user input. Bound to ctx so no child can
		// outlive the coordinator.
		cmd := exec.CommandContext(ctx, self) //nolint:gosec // G204: the binary is our own path
		cmd.Env = append(os.Environ(),
			roleEnv+"=worker",
			prefixEnv+"="+prefix,
			"REDIS_ADDR="+addr,
		)
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return children, fmt.Errorf("start worker: %w", err)
		}
		children = append(children, cmd)
	}
	return children, nil
}

func stopAll(children []*exec.Cmd) {
	for _, c := range children {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}
	for _, c := range children {
		_ = c.Wait()
	}
}

func cleanup(client *redis.Client, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 1000).Result()
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

// --------------------------------------------------------------------- worker

func runWorker() error {
	addr := os.Getenv("REDIS_ADDR")
	prefix := os.Getenv(prefixEnv)

	client := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	defer func() { _ = client.Close() }()

	broker, err := rdb.New(client, rdb.WithPrefix(prefix))
	if err != nil {
		return err
	}

	mux := taskq.NewMux()

	// Does nothing on purpose: the throughput number should describe the
	// queue's overhead, not a handler's.
	mux.HandleFunc("bench:noop", func(context.Context, *taskq.Job) error { return nil })

	mux.HandleFunc("bench:latency", func(ctx context.Context, j *taskq.Job) error {
		var s latencySample
		if err := json.Unmarshal(j.Payload, &s); err != nil {
			return err
		}
		delta := time.Now().UnixNano() - s.Enqueued
		return client.RPush(ctx, prefix+":bench:latencies", delta).Err()
	})

	srv, err := taskq.NewServer(broker, taskq.Config{
		Queues:            []string{queueName},
		Concurrency:       concurrency,
		VisibilityTimeout: 30 * time.Second,
		ShutdownTimeout:   2 * time.Second,
		Logger:            slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return err
	}
	return srv.Run(context.Background(), mux)
}

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
