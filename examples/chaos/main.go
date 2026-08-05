// Command chaos proves the at-least-once claim by breaking it on purpose.
//
// It spawns real worker processes, hard-kills one mid-job with no chance to
// clean up, and then checks that every enqueued job still completed. A worker
// killed this way never acks, never nacks, and never releases its lease: the
// only thing that can recover its work is the visibility timeout expiring and
// another worker's reaper noticing.
//
// Requires a real Redis, because the whole point is that the workers are
// separate processes:
//
//	docker run -d -p 6379:6379 redis:7-alpine
//	REDIS_ADDR=localhost:6379 go run ./examples/chaos
//
// The process re-executes itself as a worker child, which is why there is a
// role switch at the top of main.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/rdb"

	"github.com/redis/go-redis/v9"
)

const (
	totalJobs    = 300
	workerCount  = 4
	concurrency  = 5
	jobDuration  = 150 * time.Millisecond
	killAfter    = 2 * time.Second
	queueName    = "chaos"
	roleEnv      = "TASKQ_CHAOS_ROLE"
	prefixEnv    = "TASKQ_CHAOS_PREFIX"
	workerIDEnv  = "TASKQ_CHAOS_WORKER_ID"
	overallLimit = 90 * time.Second

	// Deliberately short so the demo finishes quickly. Recovery after a kill is
	// bounded by roughly visibilityTimeout + the reap interval, and the reaper
	// runs every visibilityTimeout/2.
	visibilityTimeout = 2 * time.Second
)

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

func runCoordinator() error {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return errors.New("REDIS_ADDR is required: this demo needs real worker processes, " +
			"so an in-memory broker cannot be shared between them")
	}

	// A run-scoped prefix keeps repeat runs from seeing each other's keys, and
	// means the demo never touches anything else in the database.
	prefix := fmt.Sprintf("taskq:chaos:%d", time.Now().UnixNano())

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), overallLimit)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connect to redis at %s: %w", addr, err)
	}
	defer cleanup(client, prefix)

	broker, err := rdb.New(client, rdb.WithPrefix(prefix))
	if err != nil {
		return err
	}

	fmt.Printf("chaos test: %d jobs, %d worker processes, %d concurrency each\n",
		totalJobs, workerCount, concurrency)
	fmt.Printf("visibility timeout %v, reaper sweeps every %v\n\n", visibilityTimeout, visibilityTimeout/2)

	if err := enqueueAll(ctx, broker); err != nil {
		return err
	}

	children, err := spawnWorkers(ctx, prefix, addr)
	if err != nil {
		return err
	}
	defer stopAll(children)

	// Let the fleet pick up work so the kill lands mid-job rather than before
	// anything has started.
	fmt.Printf("workers running, killing one in %v...\n", killAfter)
	if !sleepCtx(ctx, killAfter) {
		return ctx.Err()
	}

	victim := children[0]
	victimID := workerID(0)
	orphans, err := jobsOwnedBy(ctx, client, prefix, victimID)
	if err != nil {
		return err
	}

	killedAt := time.Now()
	if err := victim.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill worker: %w", err)
	}
	victim.killed = true
	fmt.Printf("\n>>> SIGKILLed %s (pid %d) holding %d in-flight job(s)\n",
		victimID, victim.cmd.Process.Pid, len(orphans))
	fmt.Println(">>> it will never ack, nack, or release its leases")

	// Wait for every job to be accounted for.
	done, err := waitForCompletion(ctx, client, prefix)
	if err != nil {
		return err
	}
	finishedAt := time.Now()

	orphanRecovery := time.Duration(0)
	if len(orphans) > 0 {
		orphanRecovery, err = timeToRecover(ctx, client, prefix, orphans, killedAt)
		if err != nil {
			return err
		}
	}

	runs, err := client.Get(ctx, prefix+":chaos:runs").Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	report(done, runs, len(orphans), orphanRecovery, finishedAt.Sub(killedAt))

	if done != totalJobs {
		return fmt.Errorf("JOB LOSS: %d of %d jobs completed", done, totalJobs)
	}
	return nil
}

func enqueueAll(ctx context.Context, broker taskq.Broker) error {
	client := taskq.NewClient(broker, taskq.WithDefaultQueue(queueName))
	for i := range totalJobs {
		if _, err := client.EnqueueTask(ctx, "chaos:work", map[string]int{"n": i},
			taskq.MaxAttempts(10)); err != nil {
			return fmt.Errorf("enqueue job %d: %w", i, err)
		}
	}
	fmt.Printf("enqueued %d jobs\n", totalJobs)
	return nil
}

type child struct {
	cmd    *exec.Cmd
	killed bool
}

func workerID(i int) string { return fmt.Sprintf("chaos-worker-%d", i) }

func spawnWorkers(ctx context.Context, prefix, addr string) ([]*child, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate own binary: %w", err)
	}

	children := make([]*child, 0, workerCount)
	for i := range workerCount {
		// self is os.Executable(), not user input: this process re-executing
		// itself is the whole mechanism for getting real worker processes.
		// Bound to ctx so no child can outlive the coordinator that spawned it.
		cmd := exec.CommandContext(ctx, self) //nolint:gosec // G204: the binary is our own path
		cmd.Env = append(os.Environ(),
			roleEnv+"=worker",
			prefixEnv+"="+prefix,
			workerIDEnv+"="+workerID(i),
			"REDIS_ADDR="+addr,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return children, fmt.Errorf("start worker %d: %w", i, err)
		}
		children = append(children, &child{cmd: cmd})
	}
	fmt.Printf("spawned %d worker processes\n", workerCount)
	return children, nil
}

func stopAll(children []*child) {
	for _, c := range children {
		if c.killed || c.cmd.Process == nil {
			continue
		}
		_ = c.cmd.Process.Kill()
	}
	for _, c := range children {
		_ = c.cmd.Wait()
	}
}

// jobsOwnedBy finds the jobs a given worker currently holds a lease on, by
// reading the active set and checking each envelope's owner.
func jobsOwnedBy(ctx context.Context, client *redis.Client, prefix, owner string) ([]string, error) {
	ids, err := client.ZRange(ctx, prefix+":active:"+queueName, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read active set: %w", err)
	}

	var owned []string
	for _, id := range ids {
		got, err := client.HGet(ctx, prefix+":job:"+id, "owner").Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read owner of %s: %w", id, err)
		}
		if got == owner {
			owned = append(owned, id)
		}
	}
	return owned, nil
}

// waitForCompletion blocks until every job has been marked done, or the context
// expires.
func waitForCompletion(ctx context.Context, client *redis.Client, prefix string) (int, error) {
	last := -1
	for {
		n, err := client.SCard(ctx, prefix+":chaos:done").Result()
		if err != nil {
			return 0, fmt.Errorf("read completion set: %w", err)
		}
		if int(n) != last {
			last = int(n)
			fmt.Printf("\rcompleted %d/%d", n, totalJobs)
		}
		if int(n) >= totalJobs {
			fmt.Println()
			return int(n), nil
		}
		if !sleepCtx(ctx, 50*time.Millisecond) {
			fmt.Println()
			return int(n), fmt.Errorf("timed out with %d/%d jobs completed", n, totalJobs)
		}
	}
}

// timeToRecover reports how long after the kill the orphaned jobs finished.
//
// Measured from the kill rather than from the lease expiring, because that is
// the number an operator actually cares about: how long between losing a worker
// and its work being done by someone else.
func timeToRecover(ctx context.Context, client *redis.Client, prefix string, orphans []string, killedAt time.Time) (time.Duration, error) {
	for {
		missing := 0
		for _, id := range orphans {
			ok, err := client.SIsMember(ctx, prefix+":chaos:done", id).Result()
			if err != nil {
				return 0, err
			}
			if !ok {
				missing++
			}
		}
		if missing == 0 {
			return time.Since(killedAt), nil
		}
		if !sleepCtx(ctx, 20*time.Millisecond) {
			return 0, fmt.Errorf("%d orphaned jobs never completed", missing)
		}
	}
}

func report(done, runs, orphans int, orphanRecovery, totalAfterKill time.Duration) {
	duplicates := runs - done
	if duplicates < 0 {
		duplicates = 0
	}

	fmt.Printf("\n--- result ---\n")
	fmt.Printf("jobs enqueued          : %d\n", totalJobs)
	fmt.Printf("jobs completed         : %d\n", done)
	fmt.Printf("jobs lost              : %d\n", totalJobs-done)
	fmt.Printf("handler executions     : %d\n", runs)
	fmt.Printf("duplicate executions   : %d  (expected: at-least-once permits these)\n", duplicates)
	fmt.Printf("orphaned by the kill   : %d\n", orphans)
	if orphans > 0 {
		fmt.Printf("orphan recovery time   : %s  (bound: visibility timeout %v + reap interval %v)\n",
			orphanRecovery.Round(time.Millisecond), visibilityTimeout, visibilityTimeout/2)
	}
	fmt.Printf("drained after kill in  : %s\n", totalAfterKill.Round(time.Millisecond))

	if done == totalJobs {
		fmt.Printf("\nZERO JOB LOSS across a hard kill.\n")
	}
}

// cleanup removes this run's keys so a repeated demo does not accumulate state.
func cleanup(client *redis.Client, prefix string) {
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

// --------------------------------------------------------------------- worker

func runWorker() error {
	addr := os.Getenv("REDIS_ADDR")
	prefix := os.Getenv(prefixEnv)
	id := os.Getenv(workerIDEnv)

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()

	broker, err := rdb.New(client, rdb.WithPrefix(prefix))
	if err != nil {
		return err
	}

	mux := taskq.NewMux()
	mux.HandleFunc("chaos:work", func(ctx context.Context, j *taskq.Job) error {
		select {
		case <-time.After(jobDuration):
		case <-ctx.Done():
			return ctx.Err()
		}
		// Two separate records: a set for unique completions, so loss is
		// visible, and a counter for total executions, so duplicates are too.
		pipe := client.Pipeline()
		pipe.SAdd(ctx, prefix+":chaos:done", j.ID)
		pipe.Incr(ctx, prefix+":chaos:runs")
		_, err := pipe.Exec(ctx)
		return err
	})

	srv, err := taskq.NewServer(broker, taskq.Config{
		Queues:            []string{queueName},
		Concurrency:       concurrency,
		VisibilityTimeout: visibilityTimeout,
		ShutdownTimeout:   5 * time.Second,
		WorkerID:          id,
		Backoff:           &taskq.Exponential{Base: 50 * time.Millisecond, Cap: time.Second, Jitter: taskq.JitterFull},
		// Workers are noisy in a fleet of four; the coordinator does the
		// reporting.
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return err
	}

	// Runs until the coordinator kills it.
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
