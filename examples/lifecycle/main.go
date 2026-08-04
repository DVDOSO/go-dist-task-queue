// Command lifecycle demonstrates the taskq job lifecycle end to end against the
// in-memory broker: claim, ack, orphan recovery, retry, dead-lettering, and
// operator replay.
//
// It drives a manual clock rather than sleeping, so the whole lifecycle —
// including a visibility timeout expiring — runs instantly and deterministically.
//
//	go run ./examples/lifecycle
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/memory"
)

// manualClock lets the demo expire a 30s visibility timeout without waiting 30s.
// This is the same seam the Redis broker uses to read time from the server
// instead of the local machine.
type manualClock struct{ t time.Time }

func (c *manualClock) Now() time.Time          { return c.t }
func (c *manualClock) advance(d time.Duration) { c.t = c.t.Add(d) }

const visibilityTimeout = 30 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	clk := &manualClock{t: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	b := memory.New(memory.WithClock(clk))
	defer func() {
		if err := b.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "close:", err)
		}
	}()

	claim := func(worker string) (*taskq.Job, error) {
		return b.Dequeue(ctx, taskq.ClaimOpts{
			WorkerID:          worker,
			Queues:            []string{"default"},
			VisibilityTimeout: visibilityTimeout,
		})
	}

	// ---- 1. happy path -----------------------------------------------------
	step("a job is enqueued, claimed, and acked")
	if err := b.Enqueue(ctx, &taskq.Job{Queue: "default", Type: "email:send", MaxAttempts: 3}); err != nil {
		return err
	}
	j, err := claim("worker-A")
	if err != nil {
		return err
	}
	fmt.Printf("   claimed  %s\n", j)
	fmt.Printf("   lease held by %s until %s\n", j.Owner, j.Deadline.Format(time.TimeOnly))
	if err := b.Ack(ctx, "worker-A", j.ID); err != nil {
		return err
	}
	fmt.Println("   acked, job complete")

	// ---- 2. a worker dies mid-job -----------------------------------------
	step("a worker is killed mid-job; the reaper recovers the work")
	if err := b.Enqueue(ctx, &taskq.Job{Queue: "default", Type: "report:build", MaxAttempts: 3}); err != nil {
		return err
	}
	orphan, err := claim("worker-B")
	if err != nil {
		return err
	}
	fmt.Printf("   claimed  %s by worker-B\n", orphan)
	fmt.Println("   worker-B is SIGKILLed — no ack, no nack, nothing")

	clk.advance(visibilityTimeout + time.Second)
	fmt.Println("   ...visibility timeout expires...")

	n, err := b.Reap(ctx, []string{"default"}, 100)
	if err != nil {
		return err
	}
	fmt.Printf("   reaper recovered %d job(s)\n", n)

	recovered, err := claim("worker-C")
	if err != nil {
		return err
	}
	fmt.Printf("   re-delivered to worker-C: attempt=%d recoveries=%d\n",
		recovered.Attempt, recovered.Recoveries)

	// worker-B wakes up from the dead and tries to ack. It must be refused.
	if err := b.Ack(ctx, "worker-B", orphan.ID); err != nil {
		fmt.Printf("   zombie worker-B tried to ack and was fenced off: %v\n", err)
	} else {
		return fmt.Errorf("zombie ack succeeded; fencing is broken")
	}
	if err := b.Ack(ctx, "worker-C", recovered.ID); err != nil {
		return err
	}
	fmt.Println("   worker-C acked successfully")

	// ---- 3. retries and the dead-letter queue ------------------------------
	step("a job fails repeatedly, backs off, and is dead-lettered")
	if err := b.Enqueue(ctx, &taskq.Job{Queue: "default", Type: "flaky:call", MaxAttempts: 3}); err != nil {
		return err
	}
	backoff := &taskq.Exponential{Base: time.Second, Cap: time.Minute, Jitter: taskq.JitterFull}

	for attempt := 1; ; attempt++ {
		job, err := claim("worker-D")
		if err != nil {
			return err
		}
		delay := backoff.Next(job.Attempt)
		retryAt := clk.Now().Add(delay)
		if err := b.Nack(ctx, "worker-D", job.ID, retryAt, "connection refused"); err != nil {
			return err
		}
		fmt.Printf("   attempt %d/%d failed, backing off %v\n", job.Attempt, job.MaxAttempts, delay.Round(time.Millisecond))

		if job.Attempt >= job.MaxAttempts {
			fmt.Println("   attempts exhausted, job moved to the dead-letter queue")
			break
		}
		clk.advance(delay + time.Second)
		if _, err := b.Promote(ctx, 100); err != nil {
			return err
		}
	}

	// ---- 4. operator replay ------------------------------------------------
	step("an operator inspects and replays the dead-letter queue")
	dead, err := b.ListDead(ctx, 0, 10)
	if err != nil {
		return err
	}
	for _, d := range dead {
		fmt.Printf("   dead: %s last_err=%q\n", d, d.LastErr)
	}
	replayed, err := b.RetryDead(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("   replayed %d job(s) back onto their queues\n", replayed)

	revived, err := claim("worker-E")
	if err != nil {
		return err
	}
	fmt.Printf("   %s is claimable again with a fresh budget (attempt=%d)\n", revived.Type, revived.Attempt)
	if err := b.Ack(ctx, "worker-E", revived.ID); err != nil {
		return err
	}

	// ---- 5. final tally ----------------------------------------------------
	step("final stats")
	stats, err := b.Stats(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("   processed=%d failed=%d dead=%d\n", stats.Processed, stats.Failed, stats.Dead)
	for name, qs := range stats.Queues {
		fmt.Printf("   queue %-8s pending=%d active=%d scheduled=%d retry=%d\n",
			name, qs.Pending, qs.Active, qs.Scheduled, qs.Retry)
	}

	return nil
}

func step(title string) {
	fmt.Printf("\n== %s ==\n", title)
}
