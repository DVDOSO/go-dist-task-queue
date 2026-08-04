package taskq

import (
	"context"
	"time"
)

// ClaimOpts parameterises a single Dequeue call.
type ClaimOpts struct {
	// WorkerID identifies the claiming worker and becomes the job's Owner. All
	// later mutations of this job are fenced on it.
	WorkerID string

	// Queues is the ordered list of queues to try. The broker walks it and
	// returns the first job it can claim.
	//
	// Ordering is the caller's job, not the broker's: the worker applies its
	// weighting policy to produce this slice before every call. That keeps
	// priority policy in one testable place in Go rather than duplicated across
	// every broker implementation (and, for Redis, inside Lua).
	Queues []string

	// VisibilityTimeout is how long the claim is valid before the reaper may
	// re-deliver the job. The worker renews it while the handler runs.
	VisibilityTimeout time.Duration
}

// Broker is the hot path: everything called per-job or per-poll.
//
// Implementations must be safe for concurrent use by many goroutines, and every
// method must be atomic with respect to other brokers pointed at the same
// backing store. The Redis implementation gets that from Lua; the in-memory one
// from a mutex.
type Broker interface {
	// Enqueue durably stores a job. If j.RunAt is in the future the job lands in
	// the scheduled set instead of the ready queue. j.ID is populated if empty.
	//
	// A nil return is the point at which the at-least-once guarantee attaches.
	Enqueue(ctx context.Context, j *Job) error

	// Dequeue atomically claims the next available job across the given queues,
	// moving it to the active set with a visibility deadline and stamping the
	// caller as its owner. It returns ErrNoJob when every queue was empty.
	Dequeue(ctx context.Context, o ClaimOpts) (*Job, error)

	// Ack marks a job completed and releases its lease. It returns ErrLeaseLost
	// if workerID no longer owns the job.
	Ack(ctx context.Context, workerID string, jobID string) error

	// Nack reports a failed attempt. If the job has attempts remaining it is
	// scheduled for retry at retryAt; otherwise it is dead-lettered. Returns
	// ErrLeaseLost if workerID no longer owns the job.
	Nack(ctx context.Context, workerID string, jobID string, retryAt time.Time, reason string) error

	// Kill dead-letters a job immediately, bypassing remaining attempts. Used
	// for errors a handler has declared non-retryable.
	Kill(ctx context.Context, workerID string, jobID string, reason string) error

	// Extend pushes back the visibility deadline of jobs this worker owns. IDs
	// the worker no longer owns are reported in lost rather than failing the
	// whole call — one expired lease must not stop the others from renewing.
	Extend(ctx context.Context, workerID string, jobIDs []string, vt time.Duration) (lost []string, err error)

	// Close releases broker resources. Idempotent.
	Close() error
}

// Maintenance is the background-loop path: orphan recovery and scheduling.
//
// It is separated from Broker because its callers are different. Reaping runs in
// every worker, since it is idempotent and keeping it off the leader's critical
// path means reliability does not depend on leader election. Only promotion
// needs a leader, and only to avoid redundant work.
type Maintenance interface {
	// Reap re-delivers jobs whose visibility lease has expired, up to limit per
	// call. Jobs that have exhausted their attempts are dead-lettered instead of
	// being requeued. Returns the number of jobs acted on.
	Reap(ctx context.Context, queues []string, limit int) (int, error)

	// Promote moves due scheduled and retrying jobs into their ready queues, up
	// to limit per call. Returns the number promoted.
	//
	// Safe to call concurrently from several schedulers: promotion is atomic, so
	// a job is promoted exactly once even without a leader.
	Promote(ctx context.Context, limit int) (int, error)

	// Heartbeat registers or refreshes a worker's liveness record, which expires
	// after ttl if the worker stops reporting.
	Heartbeat(ctx context.Context, w WorkerInfo, ttl time.Duration) error

	// AcquireLease takes or renews a named lease for owner, returning whether
	// the caller now holds it. Renewal is fenced on owner, so a leader that
	// already lapsed cannot stomp its successor.
	AcquireLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error)

	// ReleaseLease drops a lease the caller holds, so failover is immediate
	// rather than waiting out the TTL. A non-owner's call is a no-op.
	ReleaseLease(ctx context.Context, name, owner string) error
}

// Inspector is the CLI and observability path. Nothing here is on the hot path,
// so implementations may favour completeness over speed.
type Inspector interface {
	// Stats returns a point-in-time view of every known queue.
	Stats(ctx context.Context) (Stats, error)

	// ListDead pages through the dead-letter queue, newest first.
	ListDead(ctx context.Context, offset, limit int) ([]*Job, error)

	// RetryDead moves dead-lettered jobs back to their ready queues with their
	// attempt counters reset. Passing no IDs retries every dead job.
	RetryDead(ctx context.Context, ids ...string) (int, error)

	// PurgeDead permanently removes dead-lettered jobs. Passing no IDs purges
	// the entire dead-letter queue.
	PurgeDead(ctx context.Context, ids ...string) (int, error)

	// Workers lists workers currently reporting heartbeats.
	Workers(ctx context.Context) ([]WorkerInfo, error)
}

// WorkerInfo is a worker's self-reported identity and load.
type WorkerInfo struct {
	// ID is unique per worker process.
	ID string `json:"id"`
	// Host and PID locate the process for an operator.
	Host string `json:"host"`
	PID  int    `json:"pid"`
	// Queues is the set of queues this worker consumes.
	Queues []string `json:"queues"`
	// Concurrency is the worker's configured job slot count.
	Concurrency int `json:"concurrency"`
	// InFlight is how many of those slots are currently occupied.
	InFlight int `json:"in_flight"`
	// StartedAt is process start time; LastBeat is the most recent heartbeat.
	StartedAt time.Time `json:"started_at"`
	LastBeat  time.Time `json:"last_beat"`
}

// QueueStats is the depth of one named queue, broken down by state.
type QueueStats struct {
	Queue     string `json:"queue"`
	Pending   int64  `json:"pending"`
	Active    int64  `json:"active"`
	Scheduled int64  `json:"scheduled"`
	Retry     int64  `json:"retry"`
}

// Stats is a snapshot across every queue the broker knows about.
type Stats struct {
	// Queues is keyed by queue name.
	Queues map[string]QueueStats `json:"queues"`
	// Dead is the total dead-letter depth, which is not per-queue because the
	// dead-letter queue is global.
	Dead int64 `json:"dead"`
	// Processed and Failed are cumulative counters that survive process
	// restarts, unlike the in-process Prometheus counters.
	Processed int64 `json:"processed"`
	Failed    int64 `json:"failed"`
	// Workers is the number of workers currently reporting heartbeats.
	Workers int `json:"workers"`
}

// Clock supplies the current time. Every broker takes one so that tests can
// drive visibility timeouts and backoff deterministically instead of sleeping.
//
// The Redis broker reads time from the Redis server rather than the local
// machine, so worker clock skew cannot corrupt lease arithmetic. This interface
// is what lets the in-memory broker model the same property.
type Clock interface {
	Now() time.Time
}

// SystemClock is a Clock backed by time.Now.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// Compile-time proof that the standard clock satisfies the interface.
var _ Clock = SystemClock{}
