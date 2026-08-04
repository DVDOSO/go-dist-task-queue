package taskq

import (
	"context"
	"errors"
	"sort"
	"testing"
)

func TestMuxRoutesByType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var ran string
	mux := NewMux()
	mux.HandleFunc("email:send", func(context.Context, *Job) error {
		ran = "email"
		return nil
	})
	mux.HandleFunc("report:build", func(context.Context, *Job) error {
		ran = "report"
		return nil
	})

	if err := mux.ProcessTask(ctx, &Job{Type: "report:build"}); err != nil {
		t.Fatalf("ProcessTask: %v", err)
	}
	if ran != "report" {
		t.Errorf("ran %q handler, want report", ran)
	}
}

func TestMuxUnknownType(t *testing.T) {
	t.Parallel()

	mux := NewMux()
	mux.HandleFunc("known", func(context.Context, *Job) error { return nil })

	err := mux.ProcessTask(context.Background(), &Job{Type: "unknown"})
	if !errors.Is(err, ErrNoHandler) {
		t.Errorf("ProcessTask = %v, want ErrNoHandler", err)
	}
	// Retryable, not fatal: a rolling deploy legitimately produces this.
	if errors.Is(err, ErrSkipRetry) {
		t.Error("a missing handler should not skip retries")
	}
}

func TestMuxPropagatesHandlerError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	mux := NewMux()
	mux.HandleFunc("t", func(context.Context, *Job) error { return sentinel })

	if err := mux.ProcessTask(context.Background(), &Job{Type: "t"}); !errors.Is(err, sentinel) {
		t.Errorf("ProcessTask = %v, want the handler's error", err)
	}
}

func TestMuxRegistrationPanics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		register func(m *Mux)
	}{
		{"empty type", func(m *Mux) { m.Handle("", HandlerFunc(func(context.Context, *Job) error { return nil })) }},
		{"nil handler", func(m *Mux) { m.Handle("t", nil) }},
		{"duplicate type", func(m *Mux) {
			h := HandlerFunc(func(context.Context, *Job) error { return nil })
			m.Handle("dup", h)
			m.Handle("dup", h)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Error("expected a panic: registration errors are programming errors")
				}
			}()
			tt.register(NewMux())
		})
	}
}

func TestMuxTypes(t *testing.T) {
	t.Parallel()

	mux := NewMux()
	mux.HandleFunc("b", func(context.Context, *Job) error { return nil })
	mux.HandleFunc("a", func(context.Context, *Job) error { return nil })

	got := mux.Types()
	sort.Strings(got)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Types() = %v, want [a b]", got)
	}
}
