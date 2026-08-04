// Package memory provides an in-process implementation of the taskq broker
// interfaces.
//
// It exists so that the worker runtime, retry policy, and shutdown logic can be
// tested without Docker or a network, and so that the Redis broker has a
// reference implementation to be differentially tested against: both are driven
// by the same conformance suite.
//
// It is not a production broker. State lives in one process and dies with it.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// Broker is an in-memory implementation of taskq.Broker, taskq.Maintenance, and
// taskq.Inspector.
//
// Every operation is guarded by a single mutex. That is deliberately the
// simplest thing that can work: this broker is a test double, so contention is
// irrelevant, and one lock makes it obvious that each operation is atomic —
// which is the property the Redis implementation buys with Lua.
type Broker struct {
	mu     sync.Mutex
	clock  taskq.Clock
	closed bool

	jobs      map[string]*taskq.Job
	pending   map[string][]string // queue -> job IDs, head is next to claim
	active    map[string]time.Time
	scheduled map[string]time.Time
	retry     map[string]time.Time
	dead      []string // oldest first
	queues    map[string]struct{}

	unique map[string]uniqueEntry
	leases map[string]leaseEntry
	// Stored by pointer: WorkerInfo is large enough that ranging over values
	// copies it needlessly on every Stats and Workers call.
	workers map[string]*workerEntry

	processed int64
	failed    int64
}

type uniqueEntry struct {
	jobID     string
	expiresAt time.Time
}

type leaseEntry struct {
	owner     string
	expiresAt time.Time
}

type workerEntry struct {
	info      taskq.WorkerInfo
	expiresAt time.Time
}

// Option configures a Broker.
type Option func(*Broker)

// WithClock replaces the time source, so tests can advance visibility deadlines
// without sleeping.
func WithClock(c taskq.Clock) Option {
	return func(b *Broker) { b.clock = c }
}

// New returns an empty in-memory broker.
func New(opts ...Option) *Broker {
	b := &Broker{
		clock:     taskq.SystemClock{},
		jobs:      make(map[string]*taskq.Job),
		pending:   make(map[string][]string),
		active:    make(map[string]time.Time),
		scheduled: make(map[string]time.Time),
		retry:     make(map[string]time.Time),
		queues:    make(map[string]struct{}),
		unique:    make(map[string]uniqueEntry),
		leases:    make(map[string]leaseEntry),
		workers:   make(map[string]*workerEntry),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Compile-time proof that one type covers all three interfaces, which is what
// lets the conformance suite treat brokers uniformly.
var (
	_ taskq.Broker      = (*Broker)(nil)
	_ taskq.Maintenance = (*Broker)(nil)
	_ taskq.Inspector   = (*Broker)(nil)
)

// checkOpen must be called with the mutex held.
func (b *Broker) checkOpen() error {
	if b.closed {
		return taskq.ErrClosed
	}
	return nil
}

// Enqueue implements taskq.Broker.
func (b *Broker) Enqueue(_ context.Context, j *taskq.Job) error {
	if j == nil {
		return fmt.Errorf("%w: nil job", taskq.ErrInvalidTask)
	}
	if j.Queue == "" {
		return fmt.Errorf("%w: job has no queue", taskq.ErrInvalidTask)
	}
	if j.Type == "" {
		return fmt.Errorf("%w: job has no type", taskq.ErrInvalidTask)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return err
	}

	now := b.clock.Now()

	if j.UniqueKey != "" {
		if e, ok := b.unique[j.UniqueKey]; ok && e.expiresAt.After(now) {
			return &taskq.DuplicateError{UniqueKey: j.UniqueKey, ExistingID: e.jobID}
		}
	}

	if j.ID == "" {
		j.ID = taskq.NewID()
	}
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = taskq.DefaultMaxAttempts
	}
	if j.EnqueuedAt.IsZero() {
		j.EnqueuedAt = now
	}

	stored := j.Clone()
	stored.Owner = ""

	// A job with a future RunAt waits in the scheduled set; the promoter moves
	// it to the ready queue when it comes due.
	if stored.RunAt.After(now) {
		stored.State = taskq.StateScheduled
		b.scheduled[stored.ID] = stored.RunAt
	} else {
		stored.State = taskq.StatePending
		b.pending[stored.Queue] = append(b.pending[stored.Queue], stored.ID)
	}

	b.jobs[stored.ID] = stored
	b.queues[stored.Queue] = struct{}{}

	if stored.UniqueKey != "" {
		ttl := stored.UniqueTTL
		if ttl <= 0 {
			ttl = time.Hour
		}
		b.unique[stored.UniqueKey] = uniqueEntry{jobID: stored.ID, expiresAt: now.Add(ttl)}
	}

	return nil
}

// Dequeue implements taskq.Broker.
func (b *Broker) Dequeue(_ context.Context, o taskq.ClaimOpts) (*taskq.Job, error) {
	if o.WorkerID == "" {
		return nil, fmt.Errorf("%w: empty worker ID", taskq.ErrInvalidTask)
	}
	if o.VisibilityTimeout <= 0 {
		return nil, fmt.Errorf("%w: visibility timeout must be positive", taskq.ErrInvalidTask)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return nil, err
	}

	now := b.clock.Now()

	// Walk the queues in the order the caller gave us; that ordering is where
	// the weighting policy has already been applied.
	for _, q := range o.Queues {
		ids := b.pending[q]
		if len(ids) == 0 {
			continue
		}
		id := ids[0]
		b.pending[q] = ids[1:]

		j, ok := b.jobs[id]
		if !ok {
			// Should not happen; skip rather than hand back a dangling ID.
			continue
		}

		j.Attempt++
		j.State = taskq.StateActive
		j.Owner = o.WorkerID
		j.StartedAt = now
		j.Deadline = now.Add(o.VisibilityTimeout)
		b.active[id] = j.Deadline

		return j.Clone(), nil
	}

	return nil, taskq.ErrNoJob
}

// ownedActive returns the job if workerID still holds its lease. Called with the
// mutex held. This is the fencing check: a worker whose lease expired and was
// reaped will fail here rather than clobbering the new owner's work.
func (b *Broker) ownedActive(workerID, jobID string) (*taskq.Job, error) {
	j, ok := b.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", taskq.ErrJobNotFound, jobID)
	}
	if j.State != taskq.StateActive || j.Owner != workerID {
		return nil, fmt.Errorf("%w: job %s is %s owned by %q, not %q",
			taskq.ErrLeaseLost, jobID, j.State, j.Owner, workerID)
	}
	return j, nil
}

// Ack implements taskq.Broker.
func (b *Broker) Ack(_ context.Context, workerID, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return err
	}

	j, err := b.ownedActive(workerID, jobID)
	if err != nil {
		return err
	}
	if !taskq.CanTransition(j.State, taskq.StateCompleted) {
		return fmt.Errorf("taskq/memory: cannot complete job %s from state %s", jobID, j.State)
	}

	delete(b.active, jobID)
	delete(b.jobs, jobID)
	b.processed++
	return nil
}

// Nack implements taskq.Broker.
func (b *Broker) Nack(_ context.Context, workerID, jobID string, retryAt time.Time, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return err
	}

	j, err := b.ownedActive(workerID, jobID)
	if err != nil {
		return err
	}

	delete(b.active, jobID)
	j.Owner = ""
	j.LastErr = reason

	// The attempt was already consumed at claim time, so this is a plain check
	// rather than an increment.
	if j.Exhausted() {
		b.moveToDeadLocked(j)
		return nil
	}

	j.State = taskq.StateRetry
	b.retry[jobID] = retryAt
	return nil
}

// Kill implements taskq.Broker.
func (b *Broker) Kill(_ context.Context, workerID, jobID, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return err
	}

	j, err := b.ownedActive(workerID, jobID)
	if err != nil {
		return err
	}

	delete(b.active, jobID)
	j.Owner = ""
	j.LastErr = reason
	b.moveToDeadLocked(j)
	return nil
}

// moveToDeadLocked dead-letters a job. Called with the mutex held.
func (b *Broker) moveToDeadLocked(j *taskq.Job) {
	j.State = taskq.StateDead
	j.Owner = ""
	b.dead = append(b.dead, j.ID)
	b.failed++
}

// Extend implements taskq.Broker.
func (b *Broker) Extend(_ context.Context, workerID string, jobIDs []string, vt time.Duration) ([]string, error) {
	if vt <= 0 {
		return nil, fmt.Errorf("%w: visibility timeout must be positive", taskq.ErrInvalidTask)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return nil, err
	}

	now := b.clock.Now()
	var lost []string

	// One expired lease must not prevent the worker's other jobs from renewing,
	// so failures are collected rather than returned.
	for _, id := range jobIDs {
		j, err := b.ownedActive(workerID, id)
		if err != nil {
			lost = append(lost, id)
			continue
		}
		j.Deadline = now.Add(vt)
		b.active[id] = j.Deadline
	}

	return lost, nil
}

// Reap implements taskq.Maintenance.
func (b *Broker) Reap(_ context.Context, queues []string, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return 0, err
	}

	now := b.clock.Now()
	want := make(map[string]struct{}, len(queues))
	for _, q := range queues {
		want[q] = struct{}{}
	}

	type candidate struct {
		id       string
		deadline time.Time
	}
	var expired []candidate
	for id, deadline := range b.active {
		if deadline.After(now) {
			continue
		}
		j, ok := b.jobs[id]
		if !ok {
			continue
		}
		if len(want) > 0 {
			if _, match := want[j.Queue]; !match {
				continue
			}
		}
		expired = append(expired, candidate{id: id, deadline: deadline})
	}

	// Oldest lease first, mirroring the Redis ZRANGEBYSCORE ordering. Ties broken
	// by ID so the order is total and tests are deterministic.
	sort.Slice(expired, func(i, k int) bool {
		if expired[i].deadline.Equal(expired[k].deadline) {
			return expired[i].id < expired[k].id
		}
		return expired[i].deadline.Before(expired[k].deadline)
	})

	n := 0
	for _, c := range expired {
		if n >= limit {
			break
		}
		j := b.jobs[c.id]
		delete(b.active, c.id)
		j.Owner = ""
		j.Recoveries++

		// A job that has burned through its attempts by repeatedly killing its
		// worker never reaches Nack, so the dead-letter decision has to be made
		// here too. Without this, a segfaulting job cycles forever.
		if j.Exhausted() {
			j.LastErr = fmt.Sprintf("orphaned: lease expired after %d attempts", j.Attempt)
			b.moveToDeadLocked(j)
		} else {
			j.State = taskq.StatePending
			// Head of the queue, not the tail: a job that already waited out a
			// full visibility timeout should not queue behind fresh work.
			b.pending[j.Queue] = append([]string{c.id}, b.pending[j.Queue]...)
		}
		n++
	}

	return n, nil
}

// Promote implements taskq.Maintenance.
func (b *Broker) Promote(_ context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return 0, err
	}

	now := b.clock.Now()

	type candidate struct {
		id  string
		due time.Time
		set map[string]time.Time
	}
	var due []candidate
	for _, set := range []map[string]time.Time{b.scheduled, b.retry} {
		for id, at := range set {
			if at.After(now) {
				continue
			}
			due = append(due, candidate{id: id, due: at, set: set})
		}
	}

	sort.Slice(due, func(i, k int) bool {
		if due[i].due.Equal(due[k].due) {
			return due[i].id < due[k].id
		}
		return due[i].due.Before(due[k].due)
	})

	n := 0
	for _, c := range due {
		if n >= limit {
			break
		}
		j, ok := b.jobs[c.id]
		if !ok {
			delete(c.set, c.id)
			continue
		}
		delete(c.set, c.id)
		j.State = taskq.StatePending
		b.pending[j.Queue] = append(b.pending[j.Queue], c.id)
		n++
	}

	return n, nil
}

// Heartbeat implements taskq.Maintenance.
func (b *Broker) Heartbeat(_ context.Context, w taskq.WorkerInfo, ttl time.Duration) error {
	if w.ID == "" {
		return fmt.Errorf("%w: empty worker ID", taskq.ErrInvalidTask)
	}
	if ttl <= 0 {
		return fmt.Errorf("%w: heartbeat ttl must be positive", taskq.ErrInvalidTask)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return err
	}

	now := b.clock.Now()
	w.LastBeat = now
	b.workers[w.ID] = &workerEntry{info: w, expiresAt: now.Add(ttl)}
	return nil
}

// AcquireLease implements taskq.Maintenance.
func (b *Broker) AcquireLease(_ context.Context, name, owner string, ttl time.Duration) (bool, error) {
	if name == "" || owner == "" {
		return false, fmt.Errorf("%w: lease name and owner must be non-empty", taskq.ErrInvalidTask)
	}
	if ttl <= 0 {
		return false, fmt.Errorf("%w: lease ttl must be positive", taskq.ErrInvalidTask)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return false, err
	}

	now := b.clock.Now()
	e, held := b.leases[name]

	// Free if it never existed or the holder let it lapse. Renewal by the
	// current owner is the same operation as acquisition.
	if held && e.expiresAt.After(now) && e.owner != owner {
		return false, nil
	}

	b.leases[name] = leaseEntry{owner: owner, expiresAt: now.Add(ttl)}
	return true, nil
}

// ReleaseLease implements taskq.Maintenance.
func (b *Broker) ReleaseLease(_ context.Context, name, owner string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return err
	}

	// Fenced on owner: a lapsed leader must not release its successor's lease.
	if e, ok := b.leases[name]; ok && e.owner == owner {
		delete(b.leases, name)
	}
	return nil
}

// Stats implements taskq.Inspector.
func (b *Broker) Stats(_ context.Context) (taskq.Stats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return taskq.Stats{}, err
	}

	now := b.clock.Now()
	s := taskq.Stats{
		Queues:    make(map[string]taskq.QueueStats, len(b.queues)),
		Dead:      int64(len(b.dead)),
		Processed: b.processed,
		Failed:    b.failed,
	}

	for q := range b.queues {
		s.Queues[q] = taskq.QueueStats{Queue: q, Pending: int64(len(b.pending[q]))}
	}

	bump := func(id string, f func(*taskq.QueueStats)) {
		j, ok := b.jobs[id]
		if !ok {
			return
		}
		qs := s.Queues[j.Queue]
		qs.Queue = j.Queue
		f(&qs)
		s.Queues[j.Queue] = qs
	}
	for id := range b.active {
		bump(id, func(qs *taskq.QueueStats) { qs.Active++ })
	}
	for id := range b.scheduled {
		bump(id, func(qs *taskq.QueueStats) { qs.Scheduled++ })
	}
	for id := range b.retry {
		bump(id, func(qs *taskq.QueueStats) { qs.Retry++ })
	}

	for _, w := range b.workers {
		if w.expiresAt.After(now) {
			s.Workers++
		}
	}

	return s, nil
}

// ListDead implements taskq.Inspector, newest first.
func (b *Broker) ListDead(_ context.Context, offset, limit int) ([]*taskq.Job, error) {
	if offset < 0 || limit <= 0 {
		return nil, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return nil, err
	}

	out := make([]*taskq.Job, 0, limit)
	// b.dead is oldest-first, so walk it backwards.
	for i := len(b.dead) - 1 - offset; i >= 0 && len(out) < limit; i-- {
		if j, ok := b.jobs[b.dead[i]]; ok {
			out = append(out, j.Clone())
		}
	}
	return out, nil
}

// RetryDead implements taskq.Inspector.
func (b *Broker) RetryDead(_ context.Context, ids ...string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return 0, err
	}

	selected := b.selectDeadLocked(ids)
	n := 0
	for _, id := range selected {
		j, ok := b.jobs[id]
		if !ok {
			continue
		}
		// A replayed job gets a fresh budget; otherwise it would be exhausted on
		// arrival and dead-letter itself again on the first claim.
		j.State = taskq.StatePending
		j.Attempt = 0
		j.Recoveries = 0
		j.Owner = ""
		b.pending[j.Queue] = append(b.pending[j.Queue], id)
		b.removeDeadLocked(id)
		n++
	}
	return n, nil
}

// PurgeDead implements taskq.Inspector.
func (b *Broker) PurgeDead(_ context.Context, ids ...string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return 0, err
	}

	selected := b.selectDeadLocked(ids)
	n := 0
	for _, id := range selected {
		if _, ok := b.jobs[id]; !ok {
			continue
		}
		delete(b.jobs, id)
		b.removeDeadLocked(id)
		n++
	}
	return n, nil
}

// selectDeadLocked resolves an ID filter to concrete dead job IDs, where no IDs
// means every dead job. Called with the mutex held.
func (b *Broker) selectDeadLocked(ids []string) []string {
	if len(ids) == 0 {
		return append([]string(nil), b.dead...)
	}
	inDLQ := make(map[string]struct{}, len(b.dead))
	for _, id := range b.dead {
		inDLQ[id] = struct{}{}
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := inDLQ[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// removeDeadLocked drops one ID from the dead-letter list. Mutex held.
func (b *Broker) removeDeadLocked(id string) {
	for i, d := range b.dead {
		if d == id {
			b.dead = append(b.dead[:i], b.dead[i+1:]...)
			return
		}
	}
}

// Workers implements taskq.Inspector.
func (b *Broker) Workers(_ context.Context) ([]taskq.WorkerInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkOpen(); err != nil {
		return nil, err
	}

	now := b.clock.Now()
	out := make([]taskq.WorkerInfo, 0, len(b.workers))
	for _, w := range b.workers {
		if w.expiresAt.After(now) {
			out = append(out, w.info)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	return out, nil
}

// Close implements taskq.Broker. Idempotent.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}
