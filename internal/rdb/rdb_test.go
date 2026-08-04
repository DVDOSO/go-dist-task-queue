//go:build integration

package rdb_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	taskq "github.com/DVDOSO/go-dist-task-queue"
	"github.com/DVDOSO/go-dist-task-queue/internal/rdb"
	"github.com/DVDOSO/go-dist-task-queue/internal/testutil"
)

// One container for the whole binary; tests isolate by key prefix.
var testClient *redis.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	client, terminate, err := testutil.StartRedis(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot start redis:", err)
		os.Exit(1)
	}
	testClient = client

	code := m.Run()

	// os.Exit skips deferred calls, so terminate explicitly.
	terminate()
	os.Exit(code)
}

const vt = 30 * time.Second

func newBroker(t *testing.T, opts ...rdb.Option) *rdb.Broker {
	t.Helper()
	opts = append([]rdb.Option{rdb.WithPrefix(testutil.Prefix(t))}, opts...)
	b, err := rdb.New(testClient, opts...)
	if err != nil {
		t.Fatalf("rdb.New: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return b
}

func testJob(queue, typ string) *taskq.Job {
	return &taskq.Job{Queue: queue, Type: typ, MaxAttempts: 3}
}

func mustEnqueue(t *testing.T, b *rdb.Broker, j *taskq.Job) *taskq.Job {
	t.Helper()
	if err := b.Enqueue(context.Background(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return j
}

func mustClaim(t *testing.T, b *rdb.Broker, worker string, queues ...string) *taskq.Job {
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
	b := newBroker(t)

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
			if err := b.Enqueue(ctx, tt.job); !errors.Is(err, taskq.ErrInvalidTask) {
				t.Errorf("Enqueue = %v, want ErrInvalidTask", err)
			}
		})
	}
}

func TestEnqueueRoundTrip(t *testing.T) {
	t.Parallel()
	b := newBroker(t)

	j := testJob("default", "email:send")
	j.Payload = []byte(`{"to":"a@b.c"}`)
	mustEnqueue(t, b, j)

	got := mustClaim(t, b, "w1", "default")

	if got.ID != j.ID {
		t.Errorf("ID = %q, want %q", got.ID, j.ID)
	}
	if got.Queue != "default" || got.Type != "email:send" {
		t.Errorf("queue/type = %q/%q", got.Queue, got.Type)
	}
	if string(got.Payload) != `{"to":"a@b.c"}` {
		t.Errorf("Payload = %s", got.Payload)
	}
	if got.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", got.MaxAttempts)
	}
	if got.EnqueuedAt.IsZero() {
		t.Error("EnqueuedAt should be stamped by the server")
	}
}

func TestEnqueueAppliesDefaults(t *testing.T) {
	t.Parallel()
	b := newBroker(t)

	j := &taskq.Job{Queue: "default", Type: "noop"}
	mustEnqueue(t, b, j)

	if j.ID == "" {
		t.Error("Enqueue should assign an ID")
	}
	got := mustClaim(t, b, "w1", "default")
	if got.MaxAttempts != taskq.DefaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", got.MaxAttempts, taskq.DefaultMaxAttempts)
	}
}

// TestDeadlineComesFromRedisNotTheClient is the clock-skew defence: lease
// arithmetic must be done by the server, so a worker with a wrong clock cannot
// grant itself a lease into the next century or expire one instantly.
func TestDeadlineComesFromRedisNotTheClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	serverTime, err := testClient.Time(ctx).Result()
	if err != nil {
		t.Fatalf("TIME: %v", err)
	}

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	want := serverTime.Add(vt)
	if diff := j.Deadline.Sub(want); diff > 5*time.Second || diff < -5*time.Second {
		t.Errorf("Deadline = %v, want within 5s of server time + visibility timeout (%v)",
			j.Deadline, want)
	}
	if j.StartedAt.IsZero() {
		t.Error("StartedAt should be stamped from server time")
	}
}

func TestEnqueueRoutesByRunAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	now := mustEnqueue(t, b, testJob("q", "now"))

	later := testJob("q", "later")
	later.RunAt = time.Now().Add(time.Hour)
	mustEnqueue(t, b, later)

	if got := mustClaim(t, b, "w1", "q"); got.ID != now.ID {
		t.Errorf("claimed %s, want the immediately-runnable job %s", got.ID, now.ID)
	}

	// The delayed job must not be claimable; it sits in the delayed set until a
	// promoter moves it, which arrives in stage 5.
	_, err := b.Dequeue(ctx, taskq.ClaimOpts{WorkerID: "w1", Queues: []string{"q"}, VisibilityTimeout: vt})
	if !errors.Is(err, taskq.ErrNoJob) {
		t.Errorf("Dequeue = %v, want ErrNoJob while the delayed job is not due", err)
	}

	n, err := testClient.ZCard(ctx, testutil.Prefix(t)+":delayed").Result()
	if err != nil || n != 1 {
		t.Errorf("delayed set has %d entries (err %v), want 1", n, err)
	}
}

func TestUniqueKeyCollapsesDuplicates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

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

	// Exactly one job should have made it onto the queue.
	n, err := testClient.LLen(ctx, testutil.Prefix(t)+":q:q").Result()
	if err != nil || n != 1 {
		t.Errorf("ready list has %d entries (err %v), want 1", n, err)
	}
}

func TestUniqueKeyExpiresOnTTL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	first := testJob("q", "charge")
	first.UniqueKey = "order-ttl"
	first.UniqueTTL = 150 * time.Millisecond
	mustEnqueue(t, b, first)

	// Released by TTL rather than by completion, so a producer that crashes
	// between enqueue and completion cannot wedge the key forever.
	time.Sleep(300 * time.Millisecond)

	second := testJob("q", "charge")
	second.UniqueKey = "order-ttl"
	second.UniqueTTL = time.Minute
	if err := b.Enqueue(ctx, second); err != nil {
		t.Errorf("Enqueue after TTL expiry = %v, want nil", err)
	}
}

func TestDequeueRespectsQueueOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	low := mustEnqueue(t, b, testJob("low", "a"))
	hi1 := mustEnqueue(t, b, testJob("high", "b"))
	hi2 := mustEnqueue(t, b, testJob("high", "c"))

	for i, wantID := range []string{hi1.ID, hi2.ID, low.ID} {
		if got := mustClaim(t, b, "w1", "high", "low"); got.ID != wantID {
			t.Errorf("claim %d = %s, want %s", i, got.ID, wantID)
		}
	}

	_, err := b.Dequeue(ctx, taskq.ClaimOpts{WorkerID: "w1", Queues: []string{"high", "low"}, VisibilityTimeout: vt})
	if !errors.Is(err, taskq.ErrNoJob) {
		t.Errorf("Dequeue on drained queues = %v, want ErrNoJob", err)
	}
}

func TestDequeueValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	tests := []struct {
		name string
		opts taskq.ClaimOpts
		want error
	}{
		{"no worker ID", taskq.ClaimOpts{Queues: []string{"q"}, VisibilityTimeout: vt}, taskq.ErrInvalidTask},
		{"zero visibility timeout", taskq.ClaimOpts{WorkerID: "w", Queues: []string{"q"}}, taskq.ErrInvalidTask},
		{"no queues", taskq.ClaimOpts{WorkerID: "w", VisibilityTimeout: vt}, taskq.ErrNoJob},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := b.Dequeue(ctx, tt.opts); !errors.Is(err, tt.want) {
				t.Errorf("Dequeue = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestClaimIsAtomic checks the invariant the Lua script exists to provide:
// after a claim the job is in the active set and gone from the ready list,
// with no window in which it is in neither or both.
func TestClaimIsAtomic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	ready, err := testClient.LLen(ctx, prefix+":q:q").Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if ready != 0 {
		t.Errorf("ready list has %d entries, want 0", ready)
	}

	score, err := testClient.ZScore(ctx, prefix+":active:q", j.ID).Result()
	if err != nil {
		t.Fatalf("ZSCORE: %v", err)
	}
	if int64(score) != j.Deadline.UnixMilli() {
		t.Errorf("active score = %d, want the job's deadline %d", int64(score), j.Deadline.UnixMilli())
	}

	if j.State != taskq.StateActive || j.Owner != "w1" || j.Attempt != 1 {
		t.Errorf("claimed job = state %s owner %q attempt %d; want active/w1/1",
			j.State, j.Owner, j.Attempt)
	}
}

func TestAck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	prefix := testutil.Prefix(t)

	t.Run("removes the lease and counts a completion", func(t *testing.T) {
		b := newBroker(t, rdb.WithCompletedTTL(0))
		mustEnqueue(t, b, testJob("q", "t"))
		j := mustClaim(t, b, "w1", "q")

		if err := b.Ack(ctx, "w1", j.ID); err != nil {
			t.Fatalf("Ack: %v", err)
		}

		n, err := testClient.ZCard(ctx, testutil.Prefix(t)+":active:q").Result()
		if err != nil || n != 0 {
			t.Errorf("active set has %d entries (err %v), want 0", n, err)
		}
		exists, err := testClient.Exists(ctx, testutil.Prefix(t)+":job:"+j.ID).Result()
		if err != nil || exists != 0 {
			t.Errorf("job hash still exists (%d, err %v) with a zero completed TTL", exists, err)
		}
	})

	t.Run("retains the envelope when a completed TTL is set", func(t *testing.T) {
		b := newBroker(t, rdb.WithCompletedTTL(time.Hour))
		mustEnqueue(t, b, testJob("q", "t"))
		j := mustClaim(t, b, "w1", "q")

		if err := b.Ack(ctx, "w1", j.ID); err != nil {
			t.Fatalf("Ack: %v", err)
		}

		key := testutil.Prefix(t) + ":job:" + j.ID
		state, err := testClient.HGet(ctx, key, "state").Result()
		if err != nil {
			t.Fatalf("HGET: %v", err)
		}
		if state != string(taskq.StateCompleted) {
			t.Errorf("state = %q, want completed", state)
		}
		ttl, err := testClient.PTTL(ctx, key).Result()
		if err != nil || ttl <= 0 {
			t.Errorf("PTTL = %v (err %v), want a positive expiry", ttl, err)
		}
	})

	_ = prefix
}

// TestAckIsFencedOnOwner is the anti-zombie property: only the current lease
// holder may complete a job.
func TestAckIsFencedOnOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "worker-A", "q")

	if err := b.Ack(ctx, "worker-B", j.ID); !errors.Is(err, taskq.ErrLeaseLost) {
		t.Errorf("Ack from a non-owner = %v, want ErrLeaseLost", err)
	}
	if err := b.Ack(ctx, "worker-A", j.ID); err != nil {
		t.Errorf("Ack from the owner = %v, want nil", err)
	}

	// A second ack is refused either way, but which error you get depends on
	// retention: with a completed TTL the envelope is still there in state
	// "completed", so the fence rejects it as a lost lease rather than as a
	// missing job. Both are correct; asserting it pins the distinction down.
	if err := b.Ack(ctx, "worker-A", j.ID); !errors.Is(err, taskq.ErrLeaseLost) {
		t.Errorf("double Ack with retention on = %v, want ErrLeaseLost", err)
	}
}

// TestDoubleAckWithoutRetention is the same check with the envelope deleted on
// completion, where the job genuinely no longer exists.
func TestDoubleAckWithoutRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t, rdb.WithCompletedTTL(0))

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	if err := b.Ack(ctx, "w1", j.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := b.Ack(ctx, "w1", j.ID); !errors.Is(err, taskq.ErrJobNotFound) {
		t.Errorf("double Ack without retention = %v, want ErrJobNotFound", err)
	}
}

func TestAckUnknownJob(t *testing.T) {
	t.Parallel()
	b := newBroker(t)
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
		wantSet     string
		wantState   taskq.State
	}{
		{"with attempts remaining goes to retry", 3, ":retry", taskq.StateRetry},
		{"with attempts exhausted is dead-lettered", 1, ":dead", taskq.StateDead},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBroker(t)
			prefix := testutil.Prefix(t)

			j := testJob("q", "t")
			j.MaxAttempts = tt.maxAttempts
			mustEnqueue(t, b, j)
			claimed := mustClaim(t, b, "w1", "q")

			retryAt := time.Now().Add(time.Minute)
			if err := b.Nack(ctx, "w1", claimed.ID, retryAt, "connection refused"); err != nil {
				t.Fatalf("Nack: %v", err)
			}

			n, err := testClient.ZCard(ctx, prefix+tt.wantSet).Result()
			if err != nil || n != 1 {
				t.Errorf("%s has %d entries (err %v), want 1", tt.wantSet, n, err)
			}

			state, err := testClient.HGet(ctx, prefix+":job:"+claimed.ID, "state").Result()
			if err != nil {
				t.Fatalf("HGET state: %v", err)
			}
			if state != string(tt.wantState) {
				t.Errorf("state = %q, want %q", state, tt.wantState)
			}

			lastErr, err := testClient.HGet(ctx, prefix+":job:"+claimed.ID, "last_err").Result()
			if err != nil || lastErr != "connection refused" {
				t.Errorf("last_err = %q (err %v), want the failure reason", lastErr, err)
			}

			// The lease is released either way.
			active, err := testClient.ZCard(ctx, prefix+":active:q").Result()
			if err != nil || active != 0 {
				t.Errorf("active set has %d entries (err %v), want 0", active, err)
			}

			// Not claimable again until something promotes it.
			if _, err := b.Dequeue(ctx, taskq.ClaimOpts{WorkerID: "w1", Queues: []string{"q"}, VisibilityTimeout: vt}); !errors.Is(err, taskq.ErrNoJob) {
				t.Errorf("Dequeue after nack = %v, want ErrNoJob", err)
			}
		})
	}
}

func TestNackRecordsRetryTime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	retryAt := time.Now().Add(90 * time.Second).Truncate(time.Millisecond)
	if err := b.Nack(ctx, "w1", j.ID, retryAt, "boom"); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	score, err := testClient.ZScore(ctx, prefix+":retry", j.ID).Result()
	if err != nil {
		t.Fatalf("ZSCORE: %v", err)
	}
	if int64(score) != retryAt.UnixMilli() {
		t.Errorf("retry score = %d, want %d", int64(score), retryAt.UnixMilli())
	}
}

func TestNackIsFencedOnOwner(t *testing.T) {
	t.Parallel()
	b := newBroker(t)

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	err := b.Nack(context.Background(), "impostor", j.ID, time.Now(), "boom")
	if !errors.Is(err, taskq.ErrLeaseLost) {
		t.Errorf("Nack from a non-owner = %v, want ErrLeaseLost", err)
	}
}

func TestKillBypassesRemainingAttempts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	j := testJob("q", "t")
	j.MaxAttempts = 10
	mustEnqueue(t, b, j)
	claimed := mustClaim(t, b, "w1", "q")

	if err := b.Kill(ctx, "w1", claimed.ID, "unrecoverable"); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	n, err := testClient.ZCard(ctx, prefix+":dead").Result()
	if err != nil || n != 1 {
		t.Errorf("dead set has %d entries (err %v), want 1", n, err)
	}
	fields, err := testClient.HMGet(ctx, prefix+":job:"+claimed.ID, "state", "last_err", "attempt").Result()
	if err != nil {
		t.Fatalf("HMGET: %v", err)
	}
	if fields[0] != string(taskq.StateDead) {
		t.Errorf("state = %v, want dead", fields[0])
	}
	if fields[1] != "unrecoverable" {
		t.Errorf("last_err = %v, want unrecoverable", fields[1])
	}
	if fields[2] != "1" {
		t.Errorf("attempt = %v, want 1 — Kill should not burn the remaining budget", fields[2])
	}
}

func TestKillIsFencedOnOwner(t *testing.T) {
	t.Parallel()
	b := newBroker(t)

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "w1", "q")

	if err := b.Kill(context.Background(), "impostor", j.ID, "nope"); !errors.Is(err, taskq.ErrLeaseLost) {
		t.Errorf("Kill from a non-owner = %v, want ErrLeaseLost", err)
	}
}

func TestExtend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	mustEnqueue(t, b, testJob("q", "a"))
	mustEnqueue(t, b, testJob("q", "b"))
	a := mustClaim(t, b, "w1", "q")
	bJob := mustClaim(t, b, "w1", "q")

	before, err := testClient.ZScore(ctx, prefix+":active:q", a.ID).Result()
	if err != nil {
		t.Fatalf("ZSCORE: %v", err)
	}

	lost, err := b.Extend(ctx, "w1", []string{a.ID, bJob.ID}, 5*time.Minute)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if len(lost) != 0 {
		t.Errorf("lost = %v, want none", lost)
	}

	after, err := testClient.ZScore(ctx, prefix+":active:q", a.ID).Result()
	if err != nil {
		t.Fatalf("ZSCORE: %v", err)
	}
	if after <= before {
		t.Errorf("deadline did not move: before %v, after %v", before, after)
	}
}

// TestExtendReportsLostWithoutFailingTheBatch: one expired lease must not stop
// a worker's other jobs from renewing.
func TestExtendReportsLostWithoutFailingTheBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)
	prefix := testutil.Prefix(t)

	mustEnqueue(t, b, testJob("q", "mine"))
	mine := mustClaim(t, b, "w1", "q")

	mustEnqueue(t, b, testJob("q", "theirs"))
	theirs := mustClaim(t, b, "w2", "q")

	lost, err := b.Extend(ctx, "w1", []string{mine.ID, theirs.ID, "does-not-exist"}, 5*time.Minute)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}

	want := map[string]bool{theirs.ID: true, "does-not-exist": true}
	if len(lost) != len(want) {
		t.Fatalf("lost = %v, want %d entries", lost, len(want))
	}
	for _, id := range lost {
		if !want[id] {
			t.Errorf("unexpected lost ID %s", id)
		}
	}

	// The lease w1 legitimately holds was still renewed.
	score, err := testClient.ZScore(ctx, prefix+":active:q", mine.ID).Result()
	if err != nil {
		t.Fatalf("ZSCORE: %v", err)
	}
	if int64(score) <= time.Now().Add(4*time.Minute).UnixMilli() {
		t.Error("the surviving lease was not extended")
	}
}

func TestExtendEmptyBatch(t *testing.T) {
	t.Parallel()
	b := newBroker(t)
	lost, err := b.Extend(context.Background(), "w1", nil, time.Minute)
	if err != nil || len(lost) != 0 {
		t.Errorf("Extend(nil) = (%v, %v), want (nil, nil)", lost, err)
	}
}

// TestConcurrentClaimIsExactlyOnce is the property the Lua script exists for:
// many workers hammering one Redis must never see the same job twice.
func TestConcurrentClaimIsExactlyOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	const (
		jobs    = 300
		workers = 8
	)

	for i := range jobs {
		j := testJob("q", "t")
		j.Payload = []byte(fmt.Sprintf(`{"n":%d}`, i))
		mustEnqueue(t, b, j)
	}

	var (
		mu      sync.Mutex
		claimed = make(map[string]string)
		wg      sync.WaitGroup
	)

	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%d", w)
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

// TestConcurrentAckRaceHasOneWinner: two workers racing to ack the same job,
// only one of which owns it, must produce exactly one success.
func TestConcurrentAckRaceHasOneWinner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	mustEnqueue(t, b, testJob("q", "t"))
	j := mustClaim(t, b, "owner", "q")

	const contenders = 16
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
	)

	start := make(chan struct{})
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := "owner"
			if i%2 == 1 {
				worker = fmt.Sprintf("impostor-%d", i)
			}
			<-start
			if err := b.Ack(ctx, worker, j.ID); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if successes != 1 {
		t.Errorf("%d acks succeeded, want exactly 1", successes)
	}
}
