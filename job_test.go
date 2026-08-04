package taskq

import (
	"errors"
	"testing"
)

func TestCanTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from State
		to   State
		want bool
	}{
		{"new job becomes pending", "", StatePending, true},
		{"new job becomes scheduled", "", StateScheduled, true},
		{"new job cannot start active", "", StateActive, false},

		{"scheduled is promoted to pending", StateScheduled, StatePending, true},
		{"scheduled cannot jump straight to active", StateScheduled, StateActive, false},

		{"retry is promoted to pending", StateRetry, StatePending, true},
		{"retry cannot jump straight to active", StateRetry, StateActive, false},

		{"pending is claimed", StatePending, StateActive, true},
		{"pending cannot complete without running", StatePending, StateCompleted, false},

		{"active completes on ack", StateActive, StateCompleted, true},
		{"active retries on failure", StateActive, StateRetry, true},
		{"active dies when exhausted", StateActive, StateDead, true},
		{"active returns to pending on orphan recovery", StateActive, StatePending, true},

		{"completed is terminal", StateCompleted, StatePending, false},
		{"completed cannot be reactivated", StateCompleted, StateActive, false},

		{"dead can be replayed by an operator", StateDead, StatePending, true},
		{"dead cannot be reactivated directly", StateDead, StateActive, false},

		{"unknown source state permits nothing", State("bogus"), StatePending, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := CanTransition(tt.from, tt.to); got != tt.want {
				t.Errorf("CanTransition(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestStateValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state State
		want  bool
	}{
		{StatePending, true},
		{StateScheduled, true},
		{StateRetry, true},
		{StateActive, true},
		{StateCompleted, true},
		{StateDead, true},
		{State(""), false},
		{State("bogus"), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			t.Parallel()
			if got := tt.state.Valid(); got != tt.want {
				t.Errorf("State(%q).Valid() = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}

// TestCloneIsDeep guards the aliasing bug that would let a handler mutating its
// payload corrupt the copy the in-memory broker still holds.
func TestCloneIsDeep(t *testing.T) {
	t.Parallel()

	orig := &Job{ID: "a", Payload: []byte(`{"n":1}`), Attempt: 2}
	clone := orig.Clone()

	if clone == orig {
		t.Fatal("Clone returned the same pointer")
	}
	clone.Payload[0] = 'X'
	clone.Attempt = 99

	if orig.Payload[0] == 'X' {
		t.Error("mutating the clone's payload changed the original")
	}
	if orig.Attempt != 2 {
		t.Errorf("orig.Attempt = %d, want 2", orig.Attempt)
	}
}

func TestCloneNil(t *testing.T) {
	t.Parallel()
	var j *Job
	if got := j.Clone(); got != nil {
		t.Errorf("(*Job)(nil).Clone() = %v, want nil", got)
	}
}

func TestExhausted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		attempt int
		max     int
		want    bool
	}{
		{"fresh job has attempts left", 0, 3, false},
		{"partway through", 2, 3, false},
		{"exactly at the limit is exhausted", 3, 3, true},
		{"past the limit is exhausted", 4, 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			j := &Job{Attempt: tt.attempt, MaxAttempts: tt.max}
			if got := j.Exhausted(); got != tt.want {
				t.Errorf("Exhausted() with %d/%d = %v, want %v", tt.attempt, tt.max, got, tt.want)
			}
		})
	}
}

func TestNewTask(t *testing.T) {
	t.Parallel()

	t.Run("marshals payload to json", func(t *testing.T) {
		t.Parallel()
		task, err := NewTask("email:send", map[string]string{"to": "a@b.c"})
		if err != nil {
			t.Fatalf("NewTask: %v", err)
		}
		if task.Type != "email:send" {
			t.Errorf("Type = %q, want email:send", task.Type)
		}
		if string(task.Payload) != `{"to":"a@b.c"}` {
			t.Errorf("Payload = %s", task.Payload)
		}
	})

	t.Run("nil payload is allowed", func(t *testing.T) {
		t.Parallel()
		task, err := NewTask("cache:flush", nil)
		if err != nil {
			t.Fatalf("NewTask: %v", err)
		}
		if task.Payload != nil {
			t.Errorf("Payload = %v, want nil", task.Payload)
		}
	})

	t.Run("empty type is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewTask("", nil)
		if !errors.Is(err, ErrInvalidTask) {
			t.Errorf("err = %v, want ErrInvalidTask", err)
		}
	})

	t.Run("unmarshalable payload is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewTask("bad", make(chan int))
		if err == nil {
			t.Fatal("expected an error for an unmarshalable payload")
		}
		if errors.Is(err, ErrInvalidTask) {
			t.Error("a marshal failure should not be reported as ErrInvalidTask")
		}
	})
}

func TestNewIDIsUnique(t *testing.T) {
	t.Parallel()

	const n = 10_000
	seen := make(map[string]struct{}, n)
	for range n {
		id := NewID()
		if len(id) != 32 {
			t.Fatalf("NewID() = %q, want 32 hex chars", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("NewID() produced a duplicate: %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestDuplicateErrorUnwraps(t *testing.T) {
	t.Parallel()

	err := error(&DuplicateError{UniqueKey: "k", ExistingID: "job-1"})
	if !errors.Is(err, ErrDuplicate) {
		t.Error("DuplicateError should satisfy errors.Is(err, ErrDuplicate)")
	}

	var de *DuplicateError
	if !errors.As(err, &de) {
		t.Fatal("DuplicateError should be recoverable with errors.As")
	}
	if de.ExistingID != "job-1" {
		t.Errorf("ExistingID = %q, want job-1", de.ExistingID)
	}
}
