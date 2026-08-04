package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// fakeClock is a manually advanced Clock, so visibility timeouts can be expired
// instantly instead of slept through. It is mutex-guarded because the
// concurrency tests read it from many goroutines.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const vt = 30 * time.Second

func testJob(queue, typ string) *taskq.Job {
	return &taskq.Job{Queue: queue, Type: typ, MaxAttempts: 3}
}

func mustEnqueue(t *testing.T, b *Broker, j *taskq.Job) *taskq.Job {
	t.Helper()
	if err := b.Enqueue(context.Background(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return j
}

func mustClaim(t *testing.T, b *Broker, worker string, queues ...string) *taskq.Job {
	t.Helper()
	j, err := b.Dequeue(context.Background(), taskq.ClaimOpts{
		WorkerID: worker, Queues: queues, VisibilityTimeout: vt,
	})
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	return j
}

func TestEnqueueValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name string
		job  *taskq.Job
	}{
		{"nil job", nil},
		{"missing queue", &taskq.Job{Type: "t"}},
		{"missing type", &taskq.Job{Queue: "q"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := New()
			err := b.Enqueue(ctx, tt.job)
			if !errors.Is(err, taskq.ErrInvalidTask) {
				t.Errorf("Enqueue = %v, want ErrInvalidTask", err)
			}
		})
	}
}

func TestEnqueueAppliesDefaults(t *testing.T) {
	t.Parallel()

	clk := newFakeClock()
	b := New(WithClock(clk))
	j := mustEnqueue(t, b, &taskq.Job{Queue: "default", Type: "noop"})

	if j.ID == "" {
		t.Error("Enqueue should assign an ID")
	}
	if j.MaxAttempts != taskq.DefaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", j.MaxAttempts, taskq.DefaultMaxAttempts)
	}
	if !j.EnqueuedAt.Equal(clk.Now()) {
		t.Errorf("EnqueuedAt = %v, want %v", j.EnqueuedAt, clk.Now())
	}
}

func TestEnqueueRoutesByRunAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))

	now := mustEnqueue(t, b, testJob("q", "now"))
	later := testJob("q", "later")
	later.RunAt = clk.Now().Add(time.Minute)
	mustEnqueue(t, b, later)

	// Only the immediate job should be claimable.
	got := mustClaim(t, b, "w1", "q")
	if got.ID != now.ID {
		t.Errorf("claimed %s, want the immediately-runnable job %s", got.ID, now.ID)
	}
	if _, err := b.Dequeue(ctx, taskq.ClaimOpts{WorkerID: "w1", Queues: []string{"q"}, VisibilityTimeout: vt}); !errors.Is(err, taskq.ErrNoJob) {
		t.Errorf("Dequeue = %v, want ErrNoJob while the delayed job is not due", err)
	}

	// Once its time arrives, promotion makes it claimable.
	clk.Advance(2 * time.Minute)
	n, err := b.Promote(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("Promote = (%d, %v), want (1, nil)", n, err)
	}
	if got := mustClaim(t, b, "w1", "q"); got.ID != later.ID {
		t.Errorf("claimed %s, want the promoted job %s", got.ID, later.ID)
	}
}

func TestUniqueKeyCollapsesDuplicates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))

	first := testJob("q", "charge")
	first.UniqueKey = "order-42"
	first.UniqueTTL = time.Minute
	mustEnqueue(t, b, first)

	second := testJob("q", "charge")
	second.UniqueKey = "order-42"
	second.UniqueTTL = time.Minute
	err := b.Enqueue(ctx, second)

	if !errors.Is(err, taskq.ErrDuplicate) {
		t.Fatalf("Enqueue = %v, want ErrDuplicate", err)
	}
	var de *taskq.DuplicateError
	if !errors.As(err, &de) {
		t.Fatal("error should carry a *DuplicateError")
	}
	if de.ExistingID != first.ID {
		t.Errorf("ExistingID = %q, want the winning job %q", de.ExistingID, first.ID)
	}

	// The key is released by TTL, not by completion, so a producer crash cannot
	// wedge it permanently.
	clk.Advance(2 * time.Minute)
	third := testJob("q", "charge")
	third.UniqueKey = "order-42"
	third.UniqueTTL = time.Minute
	if err := b.Enqueue(ctx, third); err != nil {
		t.Errorf("Enqueue after TTL expiry = %v, want nil", err)
	}
}

func TestDequeueOrdering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := New()

	a := mustEnqueue(t, b, testJob("low", "a"))
	c := mustEnqueue(t, b, testJob("high", "c"))
	d := mustEnqueue(t, b, testJob("high", "d"))

	// The caller's queue ordering is authoritative: "high" first drains "high"
	// before "low" is even looked at.
	want := []string{c.ID, d.ID, a.ID}
	for i, wantID := range want {
		got := mustClaim(t, b, "w1", "high", "low")
		if got.ID != wantID {
			t.Errorf("claim %d = %s, want %s", i, got.ID, wantID)
		}
	}

	if _, err := b.Dequeue(ctx, taskq.ClaimOpts{WorkerID: "w1", Queues: []string{"high", "low"}, VisibilityTimeout: vt}); !errors.Is(err, taskq.ErrNoJob) {
		t.Errorf("Dequeue on drained queues = %v, want ErrNoJob", err)
	}
}

func TestDequeueStampsLease(t *testing.T) {
	t.Parallel()

	clk := newFakeClock()
	b := New(WithClock(clk))
	mustEnqueue(t, b, testJob("q", "t"))

	j := mustClaim(t, b, "worker-1", "q")

	if j.State != taskq.StateActive {
		t.Errorf("State = %s, want active", j.State)
	}
	if j.Owner != "worker-1" {
		t.Errorf("Owner = %q, want worker-1", j.Owner)
	}
	if j.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1 — attempts are consumed at claim time", j.Attempt)
	}
	if want := clk.Now().Add(vt); !j.Deadline.Equal(want) {
		t.Errorf("Deadline = %v, want %v", j.Deadline, want)
	}
}

func TestDequeueValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := New()

	tests := []struct {
		name string
		opts taskq.ClaimOpts
	}{
		{"no worker ID", taskq.ClaimOpts{Queues: []string{"q"}, VisibilityTimeout: vt}},
		{"zero visibility timeout", taskq.ClaimOpts{WorkerID: "w", Queues: []string{"q"}}},
		{"negative visibility timeout", taskq.ClaimOpts{WorkerID: "w", Queues: []string{"q"}, VisibilityTimeout: -time.Second}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := b.Dequeue(ctx, tt.opts); !errors.Is(err, taskq.ErrInvalidTask) {
				t.Errorf("Dequeue = %v, want ErrInvalidTask", err)
			}
		})
	}
}

func TestAck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := New()

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	if err := b.Ack(ctx, "w1", j.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	s, err := b.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Processed != 1 {
		t.Errorf("Processed = %d, want 1", s.Processed)
	}
	if s.Queues["q"].Active != 0 {
		t.Errorf("Active = %d, want 0 after ack", s.Queues["q"].Active)
	}
}

// TestAckIsFencedOnOwner is the anti-zombie property: a worker that lost its
// lease must not be able to ack work another worker has taken over.
func TestAckIsFencedOnOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))
	mustEnqueue(t, b, testJob("q", "t"))

	stalled := mustClaim(t, b, "worker-A", "q")

	// worker-A stalls past its deadline; the reaper hands the job to worker-B.
	clk.Advance(vt + time.Second)
	if n, err := b.Reap(ctx, []string{"q"}, 10); err != nil || n != 1 {
		t.Fatalf("Reap = (%d, %v), want (1, nil)", n, err)
	}
	taken := mustClaim(t, b, "worker-B", "q")
	if taken.ID != stalled.ID {
		t.Fatalf("worker-B claimed %s, want the recovered job %s", taken.ID, stalled.ID)
	}

	// worker-A wakes up and tries to finish.
	if err := b.Ack(ctx, "worker-A", stalled.ID); !errors.Is(err, taskq.ErrLeaseLost) {
		t.Errorf("stale Ack = %v, want ErrLeaseLost", err)
	}
	// worker-B, the real owner, still succeeds.
	if err := b.Ack(ctx, "worker-B", taken.ID); err != nil {
		t.Errorf("owner Ack = %v, want nil", err)
	}
}

func TestAckUnknownJob(t *testing.T) {
	t.Parallel()
	b := New()
	if err := b.Ack(context.Background(), "w1", "nope"); !errors.Is(err, taskq.ErrJobNotFound) {
		t.Errorf("Ack = %v, want ErrJobNotFound", err)
	}
}

func TestNack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name        string
		maxAttempts int
		wantState   taskq.State
		wantDead    int64
	}{
		{"with attempts remaining goes to retry", 3, taskq.StateRetry, 0},
		{"with attempts exhausted goes to the dead-letter queue", 1, taskq.StateDead, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock()
			b := New(WithClock(clk))

			j := testJob("q", "t")
			j.MaxAttempts = tt.maxAttempts
			mustEnqueue(t, b, j)
			claimed := mustClaim(t, b, "w1", "q")

			retryAt := clk.Now().Add(time.Minute)
			if err := b.Nack(ctx, "w1", claimed.ID, retryAt, "boom"); err != nil {
				t.Fatalf("Nack: %v", err)
			}

			s, err := b.Stats(ctx)
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if s.Dead != tt.wantDead {
				t.Errorf("Dead = %d, want %d", s.Dead, tt.wantDead)
			}
			if s.Failed != 1 && tt.wantDead == 1 {
				t.Errorf("Failed = %d, want 1", s.Failed)
			}

			if tt.wantState == taskq.StateRetry {
				if s.Queues["q"].Retry != 1 {
					t.Errorf("Retry depth = %d, want 1", s.Queues["q"].Retry)
				}
				// Not claimable until the backoff elapses and it is promoted.
				if _, err := b.Dequeue(ctx, taskq.ClaimOpts{WorkerID: "w1", Queues: []string{"q"}, VisibilityTimeout: vt}); !errors.Is(err, taskq.ErrNoJob) {
					t.Errorf("Dequeue during backoff = %v, want ErrNoJob", err)
				}
				clk.Advance(2 * time.Minute)
				if n, err := b.Promote(ctx, 10); err != nil || n != 1 {
					t.Fatalf("Promote = (%d, %v), want (1, nil)", n, err)
				}
				if got := mustClaim(t, b, "w1", "q"); got.Attempt != 2 {
					t.Errorf("Attempt after retry = %d, want 2", got.Attempt)
				}
			}
		})
	}
}

func TestNackIsFencedOnOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := New()

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	err := b.Nack(ctx, "impostor", j.ID, time.Now(), "boom")
	if !errors.Is(err, taskq.ErrLeaseLost) {
		t.Errorf("Nack from a non-owner = %v, want ErrLeaseLost", err)
	}
}

func TestKillBypassesRemainingAttempts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := New()

	j := testJob("q", "t")
	j.MaxAttempts = 10
	mustEnqueue(t, b, j)
	claimed := mustClaim(t, b, "w1", "q")

	if err := b.Kill(ctx, "w1", claimed.ID, "unrecoverable"); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	dead, err := b.ListDead(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("dead = %d jobs, want 1", len(dead))
	}
	if dead[0].LastErr != "unrecoverable" {
		t.Errorf("LastErr = %q, want unrecoverable", dead[0].LastErr)
	}
	if dead[0].Attempt >= dead[0].MaxAttempts {
		t.Error("Kill should dead-letter with attempts still on the clock")
	}
}

func TestExtendRenewsAndReportsLostLeases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))
	mustEnqueue(t, b, testJob("q", "a"))
	mustEnqueue(t, b, testJob("q", "b"))

	a := mustClaim(t, b, "w1", "q")
	bJob := mustClaim(t, b, "w1", "q")

	// Let one lease lapse and have the reaper take it away.
	clk.Advance(vt + time.Second)
	if _, err := b.Reap(ctx, []string{"q"}, 1); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	stolen := mustClaim(t, b, "w2", "q")

	lost, err := b.Extend(ctx, "w1", []string{a.ID, bJob.ID}, vt)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if len(lost) != 1 || lost[0] != stolen.ID {
		t.Errorf("lost = %v, want exactly [%s]", lost, stolen.ID)
	}

	// The lease w1 still holds must have been renewed despite the other failing.
	survivor := a.ID
	if survivor == stolen.ID {
		survivor = bJob.ID
	}
	clk.Advance(vt - time.Second)
	n, err := b.Reap(ctx, []string{"q"}, 10)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 0 {
		t.Errorf("Reap recovered %d jobs, want 0 — job %s should still be leased", n, survivor)
	}
}

func TestReap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name           string
		maxAttempts    int
		wantPending    int64
		wantDead       int64
		wantRecoveries int
	}{
		{"with attempts remaining returns to the queue", 3, 1, 0, 1},
		{"with attempts exhausted is dead-lettered", 1, 0, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock()
			b := New(WithClock(clk))

			j := testJob("q", "t")
			j.MaxAttempts = tt.maxAttempts
			mustEnqueue(t, b, j)
			mustClaim(t, b, "w1", "q")

			// Not yet expired: reaping must be a no-op.
			if n, err := b.Reap(ctx, []string{"q"}, 10); err != nil || n != 0 {
				t.Fatalf("Reap before expiry = (%d, %v), want (0, nil)", n, err)
			}

			clk.Advance(vt + time.Second)
			n, err := b.Reap(ctx, []string{"q"}, 10)
			if err != nil {
				t.Fatalf("Reap: %v", err)
			}
			if n != 1 {
				t.Fatalf("Reap = %d, want 1", n)
			}

			s, err := b.Stats(ctx)
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if s.Queues["q"].Pending != tt.wantPending {
				t.Errorf("Pending = %d, want %d", s.Queues["q"].Pending, tt.wantPending)
			}
			if s.Dead != tt.wantDead {
				t.Errorf("Dead = %d, want %d", s.Dead, tt.wantDead)
			}

			if tt.wantPending == 1 {
				got := mustClaim(t, b, "w2", "q")
				if got.Recoveries != tt.wantRecoveries {
					t.Errorf("Recoveries = %d, want %d", got.Recoveries, tt.wantRecoveries)
				}
				if got.Attempt != 2 {
					t.Errorf("Attempt = %d, want 2 — the orphaned claim was consumed", got.Attempt)
				}
			}
		})
	}
}

// TestReapReturnsJobToHeadOfQueue: a job that already waited out a full
// visibility timeout should not then queue behind freshly enqueued work.
func TestReapReturnsJobToHeadOfQueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))

	orphan := mustEnqueue(t, b, testJob("q", "orphan"))
	mustClaim(t, b, "w1", "q")
	mustEnqueue(t, b, testJob("q", "fresh"))

	clk.Advance(vt + time.Second)
	if _, err := b.Reap(ctx, []string{"q"}, 10); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	if got := mustClaim(t, b, "w2", "q"); got.ID != orphan.ID {
		t.Errorf("claimed %s, want the recovered job %s to jump the queue", got.ID, orphan.ID)
	}
}

func TestReapFiltersByQueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))
	mustEnqueue(t, b, testJob("alpha", "t"))
	mustEnqueue(t, b, testJob("beta", "t"))
	mustClaim(t, b, "w1", "alpha")
	mustClaim(t, b, "w1", "beta")

	clk.Advance(vt + time.Second)
	n, err := b.Reap(ctx, []string{"alpha"}, 10)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 1 {
		t.Errorf("Reap = %d, want 1 — only the alpha job was in scope", n)
	}
}

func TestReapRespectsLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))
	for range 5 {
		mustEnqueue(t, b, testJob("q", "t"))
		mustClaim(t, b, "w1", "q")
	}

	clk.Advance(vt + time.Second)
	n, err := b.Reap(ctx, []string{"q"}, 2)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 2 {
		t.Errorf("Reap = %d, want 2", n)
	}
}

func TestLeases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))
	const name = "scheduler"

	ok, err := b.AcquireLease(ctx, name, "leader-1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("AcquireLease = (%v, %v), want (true, nil)", ok, err)
	}

	// A contender is refused while the lease is live.
	if ok, err := b.AcquireLease(ctx, name, "leader-2", time.Minute); err != nil || ok {
		t.Errorf("contender AcquireLease = (%v, %v), want (false, nil)", ok, err)
	}
	// The holder renewing is the same operation as acquiring.
	if ok, err := b.AcquireLease(ctx, name, "leader-1", time.Minute); err != nil || !ok {
		t.Errorf("holder renewal = (%v, %v), want (true, nil)", ok, err)
	}

	// After expiry the contender takes over.
	clk.Advance(2 * time.Minute)
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

	// The real owner releasing makes it immediately available.
	if err := b.ReleaseLease(ctx, name, "leader-2"); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	if ok, err := b.AcquireLease(ctx, name, "leader-3", time.Minute); err != nil || !ok {
		t.Errorf("acquire after release = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestHeartbeatExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := newFakeClock()
	b := New(WithClock(clk))

	info := taskq.WorkerInfo{ID: "w1", Host: "localhost", PID: 1, Concurrency: 4}
	if err := b.Heartbeat(ctx, info, time.Minute); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	workers, err := b.Workers(ctx)
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if len(workers) != 1 || workers[0].ID != "w1" {
		t.Fatalf("Workers = %v, want one entry for w1", workers)
	}
	if !workers[0].LastBeat.Equal(clk.Now()) {
		t.Errorf("LastBeat = %v, want %v", workers[0].LastBeat, clk.Now())
	}

	// A worker that stops reporting disappears on its own.
	clk.Advance(2 * time.Minute)
	workers, err = b.Workers(ctx)
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if len(workers) != 0 {
		t.Errorf("Workers = %v, want none after the heartbeat TTL lapsed", workers)
	}
}

func TestDeadLetterInspection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := New()

	var ids []string
	for range 3 {
		j := testJob("q", "t")
		j.MaxAttempts = 1
		mustEnqueue(t, b, j)
		claimed := mustClaim(t, b, "w1", "q")
		if err := b.Nack(ctx, "w1", claimed.ID, time.Now(), "boom"); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		ids = append(ids, claimed.ID)
	}

	t.Run("lists newest first", func(t *testing.T) {
		dead, err := b.ListDead(ctx, 0, 10)
		if err != nil {
			t.Fatalf("ListDead: %v", err)
		}
		if len(dead) != 3 {
			t.Fatalf("ListDead = %d jobs, want 3", len(dead))
		}
		if dead[0].ID != ids[2] {
			t.Errorf("first entry = %s, want the most recent death %s", dead[0].ID, ids[2])
		}
	})

	t.Run("paginates", func(t *testing.T) {
		dead, err := b.ListDead(ctx, 1, 1)
		if err != nil {
			t.Fatalf("ListDead: %v", err)
		}
		if len(dead) != 1 || dead[0].ID != ids[1] {
			t.Errorf("ListDead(1,1) = %v, want just %s", dead, ids[1])
		}
	})

	t.Run("retry restores a fresh attempt budget", func(t *testing.T) {
		n, err := b.RetryDead(ctx, ids[0])
		if err != nil || n != 1 {
			t.Fatalf("RetryDead = (%d, %v), want (1, nil)", n, err)
		}
		got := mustClaim(t, b, "w1", "q")
		if got.ID != ids[0] {
			t.Fatalf("claimed %s, want the replayed job %s", got.ID, ids[0])
		}
		if got.Attempt != 1 {
			t.Errorf("Attempt = %d, want 1 — a replayed job must not arrive exhausted", got.Attempt)
		}
	})

	t.Run("purge removes the rest", func(t *testing.T) {
		n, err := b.PurgeDead(ctx)
		if err != nil {
			t.Fatalf("PurgeDead: %v", err)
		}
		if n != 2 {
			t.Errorf("PurgeDead = %d, want 2", n)
		}
		s, err := b.Stats(ctx)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if s.Dead != 0 {
			t.Errorf("Dead = %d, want 0", s.Dead)
		}
	})
}

func TestClosedBrokerRejectsEverything(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	b := New()
	mustEnqueue(t, b, testJob("q", "t"))
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is idempotent.
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	tests := []struct {
		name string
		call func() error
	}{
		{"Enqueue", func() error { return b.Enqueue(ctx, testJob("q", "t")) }},
		{"Dequeue", func() error {
			_, err := b.Dequeue(ctx, taskq.ClaimOpts{WorkerID: "w", Queues: []string{"q"}, VisibilityTimeout: vt})
			return err
		}},
		{"Ack", func() error { return b.Ack(ctx, "w", "id") }},
		{"Nack", func() error { return b.Nack(ctx, "w", "id", time.Now(), "r") }},
		{"Kill", func() error { return b.Kill(ctx, "w", "id", "r") }},
		{"Extend", func() error { _, err := b.Extend(ctx, "w", []string{"id"}, vt); return err }},
		{"Reap", func() error { _, err := b.Reap(ctx, []string{"q"}, 1); return err }},
		{"Promote", func() error { _, err := b.Promote(ctx, 1); return err }},
		{"Heartbeat", func() error { return b.Heartbeat(ctx, taskq.WorkerInfo{ID: "w"}, time.Minute) }},
		{"AcquireLease", func() error { _, err := b.AcquireLease(ctx, "n", "o", time.Minute); return err }},
		{"ReleaseLease", func() error { return b.ReleaseLease(ctx, "n", "o") }},
		{"Stats", func() error { _, err := b.Stats(ctx); return err }},
		{"ListDead", func() error { _, err := b.ListDead(ctx, 0, 1); return err }},
		{"RetryDead", func() error { _, err := b.RetryDead(ctx); return err }},
		{"PurgeDead", func() error { _, err := b.PurgeDead(ctx); return err }},
		{"Workers", func() error { _, err := b.Workers(ctx); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, taskq.ErrClosed) {
				t.Errorf("%s on a closed broker = %v, want ErrClosed", tt.name, err)
			}
		})
	}
}

// TestConcurrentClaimIsExactlyOnce is the core safety property, and the reason
// this package is worth having: with the race detector on, many workers hammering
// one broker must never see the same job twice.
func TestConcurrentClaimIsExactlyOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const (
		jobs    = 500
		workers = 8
	)

	b := New()
	for i := range jobs {
		j := testJob("q", "t")
		j.Payload = []byte{byte(i)}
		mustEnqueue(t, b, j)
	}

	var (
		mu      sync.Mutex
		claimed = make(map[string]string) // job ID -> claiming worker
		wg      sync.WaitGroup
	)

	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerID := "worker-" + string(rune('A'+w))
			for {
				j, err := b.Dequeue(ctx, taskq.ClaimOpts{
					WorkerID: workerID, Queues: []string{"q"}, VisibilityTimeout: time.Hour,
				})
				if errors.Is(err, taskq.ErrNoJob) {
					return
				}
				if err != nil {
					t.Errorf("Dequeue: %v", err)
					return
				}
				mu.Lock()
				if prev, dup := claimed[j.ID]; dup {
					t.Errorf("job %s claimed twice: by %s and %s", j.ID, prev, workerID)
				}
				claimed[j.ID] = workerID
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimed) != jobs {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
}
