//go:build integration

package rdb_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduleCronIfAbsent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	first := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	created, err := b.ScheduleCronIfAbsent(ctx, "nightly", first)
	if err != nil || !created {
		t.Fatalf("ScheduleCronIfAbsent = (%v, %v), want (true, nil)", created, err)
	}

	// A restarting worker must not reset a schedule that is already running:
	// a fleet rolling one process at a time would otherwise keep pushing the
	// next fire into the future and the job would never run.
	created, err = b.ScheduleCronIfAbsent(ctx, "nightly", time.Now().Add(48*time.Hour))
	if err != nil {
		t.Fatalf("ScheduleCronIfAbsent: %v", err)
	}
	if created {
		t.Error("the second call reported creating a schedule that already existed")
	}

	due, err := b.DueCron(ctx, 10)
	if err != nil {
		t.Fatalf("DueCron: %v", err)
	}
	if len(due) != 0 {
		t.Errorf("DueCron = %v, want none — the entry is an hour out", due)
	}
}

func TestDueCron(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	past := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	future := time.Now().Add(time.Hour).Truncate(time.Millisecond)

	if _, err := b.ScheduleCronIfAbsent(ctx, "overdue", past); err != nil {
		t.Fatalf("ScheduleCronIfAbsent: %v", err)
	}
	if _, err := b.ScheduleCronIfAbsent(ctx, "later", future); err != nil {
		t.Fatalf("ScheduleCronIfAbsent: %v", err)
	}

	due, err := b.DueCron(ctx, 10)
	if err != nil {
		t.Fatalf("DueCron: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("DueCron returned %d entries, want 1", len(due))
	}
	if due[0].ID != "overdue" {
		t.Errorf("due entry = %q, want overdue", due[0].ID)
	}
	if !due[0].FireAt.Equal(past.UTC()) {
		t.Errorf("FireAt = %v, want %v", due[0].FireAt, past.UTC())
	}
}

func TestDueCronRespectsLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	past := time.Now().Add(-time.Minute)
	for i := range 5 {
		if _, err := b.ScheduleCronIfAbsent(ctx, string(rune('a'+i)), past); err != nil {
			t.Fatalf("ScheduleCronIfAbsent: %v", err)
		}
	}

	due, err := b.DueCron(ctx, 2)
	if err != nil {
		t.Fatalf("DueCron: %v", err)
	}
	if len(due) != 2 {
		t.Errorf("DueCron returned %d entries, want 2", len(due))
	}
}

// TestClaimCronIsCompareAndSet: the claim must be conditional on the tick the
// caller believes is due, not a blind overwrite.
func TestClaimCronIsCompareAndSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	fireAt := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	if _, err := b.ScheduleCronIfAbsent(ctx, "entry", fireAt); err != nil {
		t.Fatalf("ScheduleCronIfAbsent: %v", err)
	}

	next := fireAt.Add(time.Hour)

	t.Run("stale fire time loses", func(t *testing.T) {
		won, err := b.ClaimCron(ctx, "entry", fireAt.Add(-time.Hour), next)
		if err != nil {
			t.Fatalf("ClaimCron: %v", err)
		}
		if won {
			t.Error("a claim with the wrong expected tick should not win")
		}
	})

	t.Run("unknown entry loses", func(t *testing.T) {
		won, err := b.ClaimCron(ctx, "no-such-entry", fireAt, next)
		if err != nil {
			t.Fatalf("ClaimCron: %v", err)
		}
		if won {
			t.Error("claiming a schedule that does not exist should not win")
		}
	})

	t.Run("correct fire time wins and advances", func(t *testing.T) {
		won, err := b.ClaimCron(ctx, "entry", fireAt, next)
		if err != nil {
			t.Fatalf("ClaimCron: %v", err)
		}
		if !won {
			t.Fatal("the claim with the correct expected tick should win")
		}

		due, err := b.DueCron(ctx, 10)
		if err != nil {
			t.Fatalf("DueCron: %v", err)
		}
		if len(due) != 0 {
			t.Errorf("DueCron = %v, want none — the entry was advanced an hour out", due)
		}
	})

	t.Run("replaying the same tick loses", func(t *testing.T) {
		won, err := b.ClaimCron(ctx, "entry", fireAt, next)
		if err != nil {
			t.Fatalf("ClaimCron: %v", err)
		}
		if won {
			t.Error("the same tick must not be claimable twice")
		}
	})
}

// TestClaimCronHasOneWinnerUnderConcurrency is what makes leader election an
// optimisation rather than a correctness requirement: even with several
// schedulers overlapping, a tick fires once.
func TestClaimCronHasOneWinnerUnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	fireAt := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	if _, err := b.ScheduleCronIfAbsent(ctx, "contended", fireAt); err != nil {
		t.Fatalf("ScheduleCronIfAbsent: %v", err)
	}

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
			// Each scheduler computes its own next fire; only one may win.
			next := fireAt.Add(time.Duration(i+1) * time.Hour)
			won, err := b.ClaimCron(ctx, "contended", fireAt, next)
			if err != nil {
				t.Errorf("ClaimCron: %v", err)
				return
			}
			if won {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if n := winners.Load(); n != 1 {
		t.Errorf("%d schedulers won the same tick, want exactly 1", n)
	}
}

func TestRemoveCron(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	past := time.Now().Add(-time.Minute)
	if _, err := b.ScheduleCronIfAbsent(ctx, "doomed", past); err != nil {
		t.Fatalf("ScheduleCronIfAbsent: %v", err)
	}
	if err := b.RemoveCron(ctx, "doomed"); err != nil {
		t.Fatalf("RemoveCron: %v", err)
	}

	due, err := b.DueCron(ctx, 10)
	if err != nil {
		t.Fatalf("DueCron: %v", err)
	}
	if len(due) != 0 {
		t.Errorf("DueCron = %v, want none after removal", due)
	}

	// Removing something that is not there is not an error.
	if err := b.RemoveCron(ctx, "never-existed"); err != nil {
		t.Errorf("RemoveCron on a missing entry = %v, want nil", err)
	}
}

func TestCronValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newBroker(t)

	if _, err := b.ScheduleCronIfAbsent(ctx, "", time.Now()); err == nil {
		t.Error("ScheduleCronIfAbsent should reject an empty ID")
	}
	if _, err := b.ClaimCron(ctx, "", time.Now(), time.Now()); err == nil {
		t.Error("ClaimCron should reject an empty ID")
	}
	if due, err := b.DueCron(ctx, 0); err != nil || due != nil {
		t.Errorf("DueCron with a zero limit = (%v, %v), want (nil, nil)", due, err)
	}
}
