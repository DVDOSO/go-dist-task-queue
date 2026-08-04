package taskq

import "errors"

// Sentinel errors. Callers compare with errors.Is; broker implementations wrap
// these with context rather than inventing their own equivalents, so that code
// written against one broker keeps working against another.
var (
	// ErrNoJob is returned by Dequeue when no queue had a claimable job. It is
	// an expected, non-exceptional result — the fetch loop sees it constantly,
	// so it must never be logged as an error.
	ErrNoJob = errors.New("taskq: no job available")

	// ErrLeaseLost is returned when a worker tries to act on a job it no longer
	// owns, because its visibility lease expired and the job was re-delivered.
	//
	// This is the fencing check that stops a stalled worker from acking work
	// another worker has since taken over. A worker seeing this should abandon
	// the job quietly: the work is already someone else's responsibility.
	ErrLeaseLost = errors.New("taskq: lease lost")

	// ErrDuplicate is returned by Enqueue when a unique key is already held.
	// Use errors.As with *DuplicateError to recover the winning job's ID.
	ErrDuplicate = errors.New("taskq: duplicate job")

	// ErrJobNotFound is returned when a job ID does not exist in the broker.
	ErrJobNotFound = errors.New("taskq: job not found")

	// ErrInvalidTask is returned for malformed tasks or enqueue options.
	ErrInvalidTask = errors.New("taskq: invalid task")

	// ErrClosed is returned by every method once the broker has been closed.
	ErrClosed = errors.New("taskq: broker closed")

	// ErrSkipRetry, when wrapped by a handler's returned error, sends the job
	// straight to the dead-letter queue instead of consuming its remaining
	// attempts. For failures that retrying cannot fix, such as a malformed
	// payload.
	ErrSkipRetry = errors.New("taskq: skip retry")
)

// DuplicateError carries the ID of the job that won a unique-key race, so a
// producer that loses can still refer to the work it collapsed into rather than
// being told only that it lost.
type DuplicateError struct {
	// UniqueKey is the key that was already held.
	UniqueKey string
	// ExistingID is the job holding it.
	ExistingID string
}

func (e *DuplicateError) Error() string {
	return "taskq: duplicate job: unique key " + e.UniqueKey + " already held by job " + e.ExistingID
}

// Unwrap makes errors.Is(err, ErrDuplicate) report true.
func (e *DuplicateError) Unwrap() error { return ErrDuplicate }
