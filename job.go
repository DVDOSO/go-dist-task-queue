// Package taskq is a Redis-backed distributed task queue with at-least-once
// delivery, visibility timeouts, and orphan recovery.
//
// The delivery contract is stated in full in README.md. In short: every job that
// [Client.Enqueue] returned a nil error for will be handed to a handler at least
// once. Duplicates are possible by construction — handlers must be idempotent.
package taskq

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// State is the position of a job in its lifecycle.
//
// The full state machine, including which transitions are legal, is encoded in
// [CanTransition]. Every broker implementation must enforce it identically; the
// in-memory and Redis brokers are tested against the same table.
type State string

const (
	// StateScheduled means the job is waiting for its RunAt to arrive.
	StateScheduled State = "scheduled"
	// StateRetry means the job failed and is waiting out its backoff.
	StateRetry State = "retry"
	// StatePending means the job is queued and claimable.
	StatePending State = "pending"
	// StateActive means a worker holds a lease on the job and is running it.
	StateActive State = "active"
	// StateCompleted means a handler returned nil and the ack succeeded.
	StateCompleted State = "completed"
	// StateDead means the job exhausted its attempts or was killed outright.
	StateDead State = "dead"
)

// validTransitions is the authoritative lifecycle graph.
//
// The empty state is the pseudo-state of a job that has not been enqueued yet,
// which is why it appears as a source but never as a destination.
var validTransitions = map[State][]State{
	"":             {StatePending, StateScheduled},
	StateScheduled: {StatePending},
	StateRetry:     {StatePending},
	StatePending:   {StateActive},
	// active -> pending is orphan recovery: the lease expired and the reaper
	// put the job back. active -> retry is a handler error with attempts left.
	StateActive:    {StateCompleted, StateRetry, StateDead, StatePending},
	StateCompleted: {},
	// dead -> pending is an operator replaying from the dead-letter queue.
	StateDead: {StatePending},
}

// CanTransition reports whether moving a job from one state to another is legal.
func CanTransition(from, to State) bool {
	for _, s := range validTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Valid reports whether s is a state this package knows about.
func (s State) Valid() bool {
	_, ok := validTransitions[s]
	return ok && s != ""
}

// String implements fmt.Stringer.
func (s State) String() string { return string(s) }

// Job is the unit of work as it exists in the broker: the envelope the queue
// cares about wrapped around a payload the queue never interprets.
type Job struct {
	// ID is globally unique and assigned at enqueue time.
	ID string `json:"id"`
	// Queue is the named queue this job is consumed from.
	Queue string `json:"queue"`
	// Type routes the job to a registered handler.
	Type string `json:"type"`
	// Payload is opaque, handler-defined bytes. This package only ever moves it
	// around; it is JSON by convention, not by enforcement.
	Payload []byte `json:"payload,omitempty"`

	// Attempt counts how many times this job has been *claimed by a worker*,
	// not how many times it has failed.
	//
	// Incrementing at claim time rather than at failure time is deliberate: a
	// worker that is SIGKILLed mid-job never gets to report a failure, so a
	// failure-time counter would let a job that reliably crashes its worker
	// cycle forever. Counting claims makes poison pills terminate without
	// needing a second "recoveries" ceiling to reason about.
	Attempt int `json:"attempt"`
	// MaxAttempts is the number of claims after which the job is dead-lettered.
	MaxAttempts int `json:"max_attempts"`
	// Recoveries counts how many of those claims ended in orphan recovery rather
	// than in a handler returning. Purely observational — it is what tells you,
	// from a dead-letter entry, whether a job was failing or killing its workers.
	Recoveries int `json:"recoveries"`

	// State is the job's lifecycle position.
	State State `json:"state"`
	// Owner is the ID of the worker currently holding the lease, empty if none.
	// Every mutating broker call is fenced on this value.
	Owner string `json:"owner,omitempty"`
	// UniqueKey, when set, collapses duplicate enqueues within UniqueTTL.
	UniqueKey string `json:"unique_key,omitempty"`
	// UniqueTTL is how long UniqueKey is held. Ignored when UniqueKey is empty.
	//
	// The key is released on TTL expiry rather than on completion: a producer
	// crash between enqueue and completion must not wedge the key forever.
	UniqueTTL time.Duration `json:"unique_ttl,omitempty"`
	// LastErr is the most recent handler error, truncated for storage.
	LastErr string `json:"last_err,omitempty"`

	// EnqueuedAt is when the job first entered the broker.
	EnqueuedAt time.Time `json:"enqueued_at"`
	// RunAt is when the job becomes eligible to run. Zero means immediately.
	RunAt time.Time `json:"run_at,omitempty"`
	// StartedAt is when the current claim began.
	StartedAt time.Time `json:"started_at,omitempty"`
	// Deadline is the visibility deadline of the current lease. Past this
	// instant the reaper is entitled to re-deliver the job.
	Deadline time.Time `json:"deadline,omitempty"`
}

// Clone returns a deep copy. Brokers hand clones to callers so that a caller
// mutating a returned job cannot corrupt broker-internal state — which the
// in-memory broker would otherwise be wide open to.
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	cp := *j
	if j.Payload != nil {
		cp.Payload = make([]byte, len(j.Payload))
		copy(cp.Payload, j.Payload)
	}
	return &cp
}

// Exhausted reports whether the job has used up its attempts and must be
// dead-lettered rather than retried or recovered.
func (j *Job) Exhausted() bool {
	return j.Attempt >= j.MaxAttempts
}

// String implements fmt.Stringer with a form that is useful in logs.
func (j *Job) String() string {
	if j == nil {
		return "<nil job>"
	}
	return fmt.Sprintf("job(%s type=%s queue=%s state=%s attempt=%d/%d)",
		j.ID, j.Type, j.Queue, j.State, j.Attempt, j.MaxAttempts)
}

// Task is a job before it has been enqueued: a type, a payload, and nothing the
// broker has assigned yet.
type Task struct {
	// Type routes the task to a registered handler.
	Type string
	// Payload is the marshaled task input.
	Payload []byte
}

// NewTask marshals payload to JSON and pairs it with a task type.
//
// payload may be nil for tasks that carry no input. Any JSON-marshalable value
// is accepted; the queue never unmarshals it, so the handler owns the schema.
func NewTask(taskType string, payload any) (*Task, error) {
	if taskType == "" {
		return nil, fmt.Errorf("taskq: %w: task type is empty", ErrInvalidTask)
	}
	var b []byte
	if payload != nil {
		var err error
		b, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("taskq: marshal payload for task %q: %w", taskType, err)
		}
	}
	return &Task{Type: taskType, Payload: b}, nil
}

// NewID returns a random 128-bit job identifier as a hex string.
//
// Randomness rather than a monotonic counter because IDs are minted by many
// producer processes with no coordination between them.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read does not fail on any supported platform. If the
		// system entropy source is genuinely broken, minting a predictable job
		// ID is worse than not starting.
		panic("taskq: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
