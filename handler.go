package taskq

import (
	"context"
	"fmt"
	"sync"
)

// Handler runs one job.
//
// Returning nil means the job is done and will be acked. Returning an error
// means it failed and will be retried after a backoff, unless the error wraps
// [ErrSkipRetry], which dead-letters it immediately.
//
// The context is cancelled when the worker is shutting down and has run out of
// patience. A handler that ignores it will be abandoned mid-flight and its job
// re-delivered, so long-running handlers should check it.
//
// Handlers must be safe for concurrent use, and must be idempotent: this queue
// is at-least-once, so the same job can legitimately run twice.
type Handler interface {
	ProcessTask(ctx context.Context, j *Job) error
}

// HandlerFunc adapts a plain function to [Handler].
type HandlerFunc func(ctx context.Context, j *Job) error

// ProcessTask implements Handler.
func (f HandlerFunc) ProcessTask(ctx context.Context, j *Job) error { return f(ctx, j) }

// Mux routes jobs to handlers by task type, in the spirit of http.ServeMux but
// without the pattern matching: task types are matched exactly.
//
// The zero value is not usable; call [NewMux].
type Mux struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewMux returns an empty registry.
func NewMux() *Mux {
	return &Mux{handlers: make(map[string]Handler)}
}

// Handle registers h for a task type. It panics if taskType is empty, h is nil,
// or the type is already registered.
//
// Panicking is deliberate: registration happens once at startup, from a single
// goroutine, with values known at compile time. A duplicate registration is a
// programming error that should stop the process immediately rather than
// silently shadow a handler and surface as jobs mysteriously doing the wrong
// thing in production.
func (m *Mux) Handle(taskType string, h Handler) {
	if taskType == "" {
		panic("taskq: Handle called with an empty task type")
	}
	if h == nil {
		panic("taskq: Handle called with a nil handler for type " + taskType)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.handlers[taskType]; dup {
		panic("taskq: duplicate handler registration for task type " + taskType)
	}
	m.handlers[taskType] = h
}

// HandleFunc registers a function for a task type.
func (m *Mux) HandleFunc(taskType string, f func(ctx context.Context, j *Job) error) {
	m.Handle(taskType, HandlerFunc(f))
}

// ProcessTask implements Handler by dispatching on the job's task type.
func (m *Mux) ProcessTask(ctx context.Context, j *Job) error {
	m.mu.RLock()
	h, ok := m.handlers[j.Type]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w for task type %q", ErrNoHandler, j.Type)
	}
	return h.ProcessTask(ctx, j)
}

// Types returns the registered task types, for logging what a worker can do.
func (m *Mux) Types() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.handlers))
	for t := range m.handlers {
		out = append(out, t)
	}
	return out
}

// Compile-time proof that a Mux is itself a Handler, which is what lets a
// Server take either one without caring.
var _ Handler = (*Mux)(nil)
