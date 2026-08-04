// Command worker demonstrates the worker runtime: a producer enqueuing jobs, a
// bounded pool consuming them, failure handling with backoff, and graceful
// shutdown.
//
// It runs against the in-memory broker by default, so it needs nothing
// installed:
//
//	go run ./examples/worker
//
// Point it at a real Redis to run the identical code against the Redis broker.
// Nothing below changes -- that is the whole reason Broker is an interface:
//
//	docker run -d -p 6379:6379 redis:7-alpine
//	REDIS_ADDR=localhost:6379 go run ./examples/worker
//
// Press Ctrl-C while it is working to watch the drain: in-flight jobs finish,
// no new ones start, and the process exits cleanly.
//
// # What this demo cannot show yet
//
// A job that fails is nacked into the retry set with a backoff, and stays
// there: promoting due jobs back onto their queue is the scheduler's job, and
// the scheduler arrives in stage 5. So "retried" below means "correctly
// scheduled for a retry that nothing has come back for yet", not "ran again".
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
	"github.com/DVDOSO/go-dist-task-queue/internal/rdb"

	"github.com/redis/go-redis/v9"
)

const (
	totalJobs   = 60
	concurrency = 8
)

// outcomes tracks how each job settled, and signals when every job has reached
// some terminal state so the demo can stop on its own.
type outcomes struct {
	succeeded    atomic.Int64
	retried      atomic.Int64
	deadLettered atomic.Int64

	settled  atomic.Int64
	once     sync.Once
	finished chan struct{}
}

func newOutcomes() *outcomes {
	return &outcomes{finished: make(chan struct{})}
}

// settle records that one job reached a terminal state for the purposes of this
// demo. sync.Once guards the close because many worker goroutines can cross the
// finish line at the same instant.
func (o *outcomes) settle() {
	if o.settled.Add(1) >= totalJobs {
		o.once.Do(func() { close(o.finished) })
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	broker, describe, cleanup, err := openBroker()
	if err != nil {
		return err
	}
	defer cleanup()
	fmt.Printf("broker: %s\n", describe)

	// Ctrl-C or SIGTERM begins a graceful shutdown rather than killing the
	// process: the context is cancelled, and Run drains from there.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	out := newOutcomes()
	mux := taskq.NewMux()

	// An ordinary job that does some work and succeeds.
	mux.HandleFunc("resize:image", func(ctx context.Context, _ *taskq.Job) error {
		select {
		case <-time.After(120 * time.Millisecond):
		case <-ctx.Done():
			// Honouring cancellation is what makes a forced shutdown orderly.
			return ctx.Err()
		}
		out.succeeded.Add(1)
		out.settle()
		return nil
	})

	// A job that fails, to show the retry decision and the computed backoff.
	mux.HandleFunc("charge:card", func(context.Context, *taskq.Job) error {
		out.retried.Add(1)
		out.settle()
		return errors.New("payment gateway timeout")
	})

	// A job that can never succeed, to show the dead-letter path skipping the
	// remaining attempt budget entirely.
	mux.HandleFunc("parse:garbage", func(context.Context, *taskq.Job) error {
		out.deadLettered.Add(1)
		out.settle()
		return fmt.Errorf("payload is not valid json: %w", taskq.ErrSkipRetry)
	})

	srv, err := taskq.NewServer(broker, taskq.Config{
		Queues:            []string{"critical", "default"},
		Concurrency:       concurrency,
		VisibilityTimeout: 30 * time.Second,
		ShutdownTimeout:   10 * time.Second,
		Backoff:           &taskq.Exponential{Base: time.Second, Cap: time.Minute, Jitter: taskq.JitterFull},
		Logger:            log,
	})
	if err != nil {
		return err
	}

	// Produce first, so there is work waiting the moment the pool starts.
	if err := enqueueAll(ctx, broker); err != nil {
		return err
	}

	// Stop once every job has settled, so the demo ends on its own if left be.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		select {
		case <-out.finished:
			fmt.Println("\n>>> all jobs settled, beginning graceful shutdown")
			cancelRun()
		case <-runCtx.Done():
		}
	}()

	fmt.Printf("running %d jobs across %d workers — press Ctrl-C to watch a graceful drain\n\n",
		totalJobs, concurrency)

	start := time.Now()
	runErr := srv.Run(runCtx, mux)
	elapsed := time.Since(start)

	fmt.Printf("\n--- summary ---\n")
	fmt.Printf("succeeded     : %d\n", out.succeeded.Load())
	fmt.Printf("dead-lettered : %d  (ErrSkipRetry, budget untouched)\n", out.deadLettered.Load())
	fmt.Printf("retried       : %d  (waiting in the retry set; the scheduler that\n", out.retried.Load())
	fmt.Printf("                     promotes them back arrives in stage 5)\n")
	fmt.Printf("elapsed       : %s\n", elapsed.Round(time.Millisecond))
	if runErr != nil {
		fmt.Printf("shutdown      : %v\n", runErr)
	} else {
		fmt.Printf("shutdown      : clean\n")
	}
	return nil
}

// enqueueAll produces the demo workload through the public producer API.
func enqueueAll(ctx context.Context, broker taskq.Broker) error {
	client := taskq.NewClient(broker)

	for i := range totalJobs {
		var err error
		switch i % 10 {
		case 7:
			_, err = client.EnqueueTask(ctx, "charge:card", map[string]int{"order": i},
				taskq.Queue("critical"), taskq.MaxAttempts(5))
		case 3:
			_, err = client.EnqueueTask(ctx, "parse:garbage", map[string]int{"n": i})
		default:
			_, err = client.EnqueueTask(ctx, "resize:image", map[string]int{"n": i})
		}
		if err != nil {
			return fmt.Errorf("enqueue job %d: %w", i, err)
		}
	}
	return nil
}

// openBroker returns the Redis broker when REDIS_ADDR is set, otherwise the
// in-memory one. Everything downstream is written against taskq.Broker and does
// not know or care which it got.
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

	rb, err := rdb.New(client, rdb.WithPrefix("taskq:demo"))
	if err != nil {
		_ = client.Close()
		return nil, "", nil, err
	}
	return rb, "redis at " + addr, func() { _ = client.Close() }, nil
}
