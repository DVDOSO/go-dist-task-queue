//go:build integration

package rdb_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/rdb"
	"github.com/DVDOSO/go-dist-task-queue/internal/testutil"
)

// shortVT is long enough to claim under and short enough to wait out.
const shortVT = 200 * time.Millisecond

func claimShort(t *testing.T, b *rdb.Broker, worker string, queues ...string) *taskq.Job {
	t.Helper()
	j, err := b.Dequeue(context.Background(), taskq.ClaimOpts{
		WorkerID: worker, Queues: queues, VisibilityTimeout: shortVT,
	})
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	return j
}

func TestReapRecoversExpiredLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	mustEnqueue(t, b, testJob("q", "t"))
	orphan := claimShort(t, b, "worker-that-died", "q")

	// While the lease is live the reaper must leave it alone.
	if n, err := b.Reap(ctx, []string{"q"}, 100); err != nil || n != 0 {
		t.Fatalf("Reap on a live lease = (%d, %v), want (0, nil)", n, err)
	}

	time.Sleep(shortVT + 100*time.Millisecond)

	n, err := b.Reap(ctx, []string{"q"}, 100)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("Reap = %d, want 1", n)
	}

	// Back on the queue and out of the active set.
	if depth, err := testClient.LLen(ctx, prefix+":q:q").Result(); err != nil || depth != 1 {
		t.Errorf("ready list = %d (err %v), want 1", depth, err)
	}
	if active, err := testClient.ZCard(ctx, prefix+":active:q").Result(); err != nil || active != 0 {
		t.Errorf("active set = %d (err %v), want 0", active, err)
	}

	// Claimable again, with the recovery recorded.
	again := claimShort(t, b, "worker-b", "q")
	if again.ID != orphan.ID {
		t.Fatalf("claimed %s, want the recovered job %s", again.ID, orphan.ID)
	}
	if again.Recoveries != 1 {
		t.Errorf("Recoveries = %d, want 1", again.Recoveries)
	}
	if again.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2 — the orphaned claim was consumed", again.Attempt)
	}
}

// TestReapDeadLettersExhaustedOrphan: a job that reliably kills its worker
// never reaches Nack, so the reaper has to make the dead-letter call itself.
// Without this a poison pill cycles forever.
func TestReapDeadLettersExhaustedOrphan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	j := testJob("q", "poison")
	j.MaxAttempts = 1
	mustEnqueue(t, b, j)
	claimShort(t, b, "doomed-worker", "q")

	time.Sleep(shortVT + 100*time.Millisecond)

	if n, err := b.Reap(ctx, []string{"q"}, 100); err != nil || n != 1 {
		t.Fatalf("Reap = (%d, %v), want (1, nil)", n, err)
	}

	if dead, err := testClient.ZCard(ctx, prefix+":dead").Result(); err != nil || dead != 1 {
		t.Errorf("dead set = %d (err %v), want 1", dead, err)
	}
	if depth, err := testClient.LLen(ctx, prefix+":q:q").Result(); err != nil || depth != 0 {
		t.Errorf("ready list = %d (err %v), want 0 — an exhausted orphan must not requeue", depth, err)
	}

	lastErr, err := testClient.HGet(ctx, prefix+":job:"+j.ID, "last_err").Result()
	if err != nil {
		t.Fatalf("HGET last_err: %v", err)
	}
	if lastErr == "" {
		t.Error("a dead-lettered orphan should record why it died")
	}
}

func TestReapDetailSplitsOutcomes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	healthy := testJob("q", "healthy")
	healthy.MaxAttempts = 5
	mustEnqueue(t, b, healthy)

	poison := testJob("q", "poison")
	poison.MaxAttempts = 1
	mustEnqueue(t, b, poison)

	claimShort(t, b, "dead-worker", "q")
	claimShort(t, b, "dead-worker", "q")

	time.Sleep(shortVT + 100*time.Millisecond)

	recovered, dead, err := b.ReapDetail(ctx, "q", 100)
	if err != nil {
		t.Fatalf("ReapDetail: %v", err)
	}
	if recovered != 1 || dead != 1 {
		t.Errorf("ReapDetail = (%d recovered, %d dead), want (1, 1)", recovered, dead)
	}
}

// TestReapIsExactlyOnceUnderConcurrency: every worker reaps, by design. Two
// reapers must never both recover the same job.
func TestReapIsExactlyOnceUnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	const jobs = 100
	for range jobs {
		mustEnqueue(t, b, testJob("q", "t"))
	}
	for range jobs {
		claimShort(t, b, "dead-worker", "q")
	}

	time.Sleep(shortVT + 100*time.Millisecond)

	var (
		total atomic.Int64
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, err := b.Reap(ctx, []string{"q"}, jobs)
			if err != nil {
				t.Errorf("Reap: %v", err)
				return
			}
			total.Add(int64(n))
		}()
	}
	close(start)
	wg.Wait()

	if got := total.Load(); got != jobs {
		t.Errorf("reapers recovered %d jobs in total, want exactly %d", got, jobs)
	}
	depth, err := testClient.LLen(ctx, prefix+":q:q").Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if depth != jobs {
		t.Errorf("ready list = %d, want %d — a duplicate recovery would inflate this", depth, jobs)
	}
}

func TestPromoteMovesDueRetries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	mustEnqueue(t, b, testJob("q", "t"))
	j := claimShort(t, b, "w1", "q")

	// Nack it into the retry set with a backoff that has already elapsed.
	if err := b.Nack(ctx, "w1", j.ID, time.Now().Add(-time.Second), "boom"); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if n, err := testClient.ZCard(ctx, prefix+":retry").Result(); err != nil || n != 1 {
		t.Fatalf("retry set = %d (err %v), want 1", n, err)
	}

	n, err := b.Promote(ctx, 100)
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if n != 1 {
		t.Fatalf("Promote = %d, want 1", n)
	}

	if depth, err := testClient.LLen(ctx, prefix+":q:q").Result(); err != nil || depth != 1 {
		t.Errorf("ready list = %d (err %v), want 1", depth, err)
	}
	if again := claimShort(t, b, "w2", "q"); again.ID != j.ID {
		t.Errorf("claimed %s, want the promoted job %s", again.ID, j.ID)
	}
}

func TestPromoteLeavesFutureJobsAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	future := testJob("q", "later")
	future.RunAt = time.Now().Add(time.Hour)
	mustEnqueue(t, b, future)

	n, err := b.Promote(ctx, 100)
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if n != 0 {
		t.Errorf("Promote = %d, want 0 — the job is not due for an hour", n)
	}
}

func TestPromoteMovesDueDelayedJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	due := testJob("q", "soon")
	due.RunAt = time.Now().Add(150 * time.Millisecond)
	mustEnqueue(t, b, due)

	time.Sleep(300 * time.Millisecond)

	n, err := b.Promote(ctx, 100)
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if n != 1 {
		t.Fatalf("Promote = %d, want 1", n)
	}
	if got := claimShort(t, b, "w1", "q"); got.ID != due.ID {
		t.Errorf("claimed %s, want the promoted job %s", got.ID, due.ID)
	}
}

// TestPromoteIsExactlyOnceUnderConcurrency is what makes leader election an
// optimisation rather than a correctness requirement: N schedulers racing must
// promote each job exactly once.
func TestPromoteIsExactlyOnceUnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	const jobs = 100
	for range jobs {
		mustEnqueue(t, b, testJob("q", "t"))
	}
	for range jobs {
		j := claimShort(t, b, "w1", "q")
		if err := b.Nack(ctx, "w1", j.ID, time.Now().Add(-time.Second), "boom"); err != nil {
			t.Fatalf("Nack: %v", err)
		}
	}

	var (
		total atomic.Int64
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, err := b.Promote(ctx, jobs)
			if err != nil {
				t.Errorf("Promote: %v", err)
				return
			}
			total.Add(int64(n))
		}()
	}
	close(start)
	wg.Wait()

	if got := total.Load(); got != jobs {
		t.Errorf("schedulers promoted %d jobs in total, want exactly %d", got, jobs)
	}
	depth, err := testClient.LLen(ctx, prefix+":q:q").Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if depth != jobs {
		t.Errorf("ready list = %d, want %d — a double promotion would inflate this", depth, jobs)
	}
}

func TestHeartbeatRegistersAndExpires(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	info := taskq.WorkerInfo{
		ID:          "worker-1",
		Host:        "localhost",
		PID:         4242,
		Queues:      []string{"critical", "default"},
		Concurrency: 8,
		InFlight:    3,
		StartedAt:   time.Now(),
	}
	if err := b.Heartbeat(ctx, info, 400*time.Millisecond); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	fields, err := testClient.HMGet(ctx, prefix+":worker:worker-1",
		"id", "host", "pid", "queues", "concurrency", "in_flight").Result()
	if err != nil {
		t.Fatalf("HMGET: %v", err)
	}
	want := []any{"worker-1", "localhost", "4242", "critical,default", "8", "3"}
	for i := range want {
		if fields[i] != want[i] {
			t.Errorf("field %d = %v, want %v", i, fields[i], want[i])
		}
	}
	if n, err := testClient.ZCard(ctx, prefix+":workers").Result(); err != nil || n != 1 {
		t.Errorf("workers set = %d (err %v), want 1", n, err)
	}

	// A worker that stops reporting evicts itself rather than lingering.
	time.Sleep(600 * time.Millisecond)
	exists, err := testClient.Exists(ctx, prefix+":worker:worker-1").Result()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if exists != 0 {
		t.Error("the worker hash should have expired on its own")
	}

	// The next heartbeat from anyone prunes the stale sorted-set entry.
	if err := b.Heartbeat(ctx, taskq.WorkerInfo{ID: "worker-2"}, 400*time.Millisecond); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	ids, err := testClient.ZRange(ctx, prefix+":workers", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRANGE: %v", err)
	}
	if len(ids) != 1 || ids[0] != "worker-2" {
		t.Errorf("workers = %v, want only worker-2", ids)
	}
}

func TestLeases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	const name = "scheduler"

	ok, err := b.AcquireLease(ctx, name, "leader-1", 400*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("AcquireLease = (%v, %v), want (true, nil)", ok, err)
	}

	// A contender is refused while the lease is live.
	if ok, err := b.AcquireLease(ctx, name, "leader-2", time.Minute); err != nil || ok {
		t.Errorf("contender AcquireLease = (%v, %v), want (false, nil)", ok, err)
	}
	// The holder renewing is the same operation as acquiring.
	if ok, err := b.AcquireLease(ctx, name, "leader-1", 400*time.Millisecond); err != nil || !ok {
		t.Errorf("holder renewal = (%v, %v), want (true, nil)", ok, err)
	}

	// After expiry the contender takes over.
	time.Sleep(600 * time.Millisecond)
	if ok, err := b.AcquireLease(ctx, name, "leader-2", time.Minute); err != nil || !ok {
		t.Errorf("takeover after expiry = (%v, %v), want (true, nil)", ok, err)
	}

	// The lapsed leader must not be able to release its successor's lease.
	if err := b.ReleaseLease(ctx, name, "leader-1"); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	if ok, err := b.AcquireLease(ctx, name, "leader-3", time.Minute); err != nil || ok {
		t.Error("a lapsed leader released a lease it no longer owned")
	}

	// The real owner releasing makes it immediately available, which is what
	// turns failover from TTL-delayed into instant.
	if err := b.ReleaseLease(ctx, name, "leader-2"); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	if ok, err := b.AcquireLease(ctx, name, "leader-3", time.Minute); err != nil || !ok {
		t.Errorf("acquire after release = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestLeaseIsExclusiveUnderConcurrency: many contenders, exactly one winner.
func TestLeaseIsExclusiveUnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	var (
		winners atomic.Int64
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := b.AcquireLease(ctx, "sched", fmt.Sprintf("candidate-%d", i), time.Minute)
			if err != nil {
				t.Errorf("AcquireLease: %v", err)
				return
			}
			if ok {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if n := winners.Load(); n != 1 {
		t.Errorf("%d candidates won the lease, want exactly 1", n)
	}
}

func TestMaintenanceValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	if n, err := b.Reap(ctx, []string{"q"}, 0); err != nil || n != 0 {
		t.Errorf("Reap with a zero limit = (%d, %v), want (0, nil)", n, err)
	}
	if n, err := b.Promote(ctx, 0); err != nil || n != 0 {
		t.Errorf("Promote with a zero limit = (%d, %v), want (0, nil)", n, err)
	}
	if err := b.Heartbeat(ctx, taskq.WorkerInfo{}, time.Minute); err == nil {
		t.Error("Heartbeat should reject an empty worker ID")
	}
	if err := b.Heartbeat(ctx, taskq.WorkerInfo{ID: "w"}, 0); err == nil {
		t.Error("Heartbeat should reject a non-positive TTL")
	}
}
