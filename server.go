package taskq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Worker runtime defaults.
const (
	DefaultConcurrency       = 10
	DefaultVisibilityTimeout = 30 * time.Second
	DefaultShutdownTimeout   = 30 * time.Second

	// Poll pacing. The fetch loop has no long-poll to block on, so it asks,
	// sleeps, and asks again -- backing off while idle so an empty queue costs
	// four requests a second per worker rather than thousands.
	minPollInterval = 5 * time.Millisecond
	maxPollInterval = 250 * time.Millisecond

	// How long a completion report (ack/nack) may take. Deliberately short and
	// independent of the job's own context.
	reportTimeout = 10 * time.Second

	// Grace period after cancelling in-flight jobs at the shutdown deadline,
	// so their nacks have a chance to land before the process exits.
	forceGracePeriod = 5 * time.Second

	// Consecutive lease-renewal failures tolerated before a worker gives up and
	// cancels its own in-flight jobs.
	maxRenewFailures = 3

	// Per-call ceilings on the background maintenance loops, so one sweep of a
	// large backlog cannot monopolise a Redis event loop.
	reapBatch    = 100
	promoteBatch = 100
	cronBatch    = 100

	// schedulerLease is the name of the lease that elects the cron scheduler.
	schedulerLease = "scheduler"

	// cronUniqueTTL is how long a fired tick's unique key is held. It only has
	// to outlive a retry of the same tick; successive ticks get distinct keys
	// because the fire time is part of the key.
	cronUniqueTTL = 5 * time.Minute
)

// Config configures a [Server].
type Config struct {
	// Queues to consume. Required.
	//
	// With StrictPriority they are tried in this order; otherwise the order is
	// reshuffled on every poll according to Weights.
	Queues []string

	// Weights biases which queue is tried first, keyed by queue name. A queue
	// with no entry weighs 1, so naming a weight for one queue does not
	// silently zero the others.
	//
	// With weights {critical: 6, default: 3, low: 1}, the critical queue is
	// tried first on roughly 60% of polls. Every queue keeps a non-zero share,
	// which is what stops a saturated high-priority queue from starving the
	// others outright.
	Weights map[string]int

	// StrictPriority tries queues in the configured order every time, ignoring
	// Weights.
	//
	// Off by default because it starves: while a high-priority queue has work,
	// nothing below it runs at all. Available because some workloads genuinely
	// want that, and it is better to opt into starvation knowingly than to
	// approximate it with lopsided weights.
	StrictPriority bool

	// Concurrency is the maximum number of jobs running at once. Defaults to
	// DefaultConcurrency.
	Concurrency int

	// VisibilityTimeout is how long a claim is valid before the job becomes
	// eligible for re-delivery.
	//
	// It is also the knob the maintenance loops are derived from: leases are
	// renewed every VisibilityTimeout/3 and expired ones reaped every
	// VisibilityTimeout/2, so worst-case recovery after a worker dies is
	// roughly one and a half times this value.
	//
	// A handler may run far longer than this: renewal keeps extending the lease
	// for as long as the worker is alive.
	VisibilityTimeout time.Duration

	// ShutdownTimeout is how long a graceful shutdown waits for in-flight jobs
	// before cancelling them.
	ShutdownTimeout time.Duration

	// Backoff computes the retry delay after a failed attempt. Defaults to
	// Exponential with full jitter.
	Backoff Backoff

	// WorkerID identifies this process. Generated if empty.
	WorkerID string

	// Cron are recurring schedules this worker knows how to fire.
	//
	// Every worker should be given the same list. Only one of them fires a
	// given tick: the schedulers elect a leader, and the claim is a
	// compare-and-set on the broker regardless, so a brief overlap during
	// failover cannot double-fire.
	//
	// Requires a broker implementing [CronStore]; NewServer fails otherwise
	// rather than silently never running them.
	Cron []CronEntry

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Server is the worker runtime: it claims jobs, runs them through a handler,
// renews their leases, and reports the outcome.
type Server struct {
	broker Broker
	// maint is non-nil when the broker also implements Maintenance, which is
	// what enables orphan recovery, retry promotion, and heartbeats.
	maint Maintenance
	// cron is non-nil when the broker also implements CronStore.
	cron CronStore

	cfg  Config
	log  *slog.Logger
	host string

	// queues decides which order to try the queues in on each poll.
	queues *queueSelector
	// entries holds the parsed schedules, keyed by entry ID.
	entries map[string]*compiledEntry
	// leader records whether this worker currently holds the scheduler lease,
	// for logging and for tests to observe failover.
	leader atomic.Bool

	// mu guards inFlight, which maps a running job's ID to the cancel function
	// for its individual context. Per-job cancellation is what lets a single
	// lost lease stop one job without disturbing the others.
	mu       sync.Mutex
	inFlight map[string]context.CancelFunc
}

// NewServer validates a configuration and returns a worker.
func NewServer(b Broker, cfg Config) (*Server, error) {
	if b == nil {
		return nil, errors.New("taskq: nil broker")
	}
	if len(cfg.Queues) == 0 {
		return nil, errors.New("taskq: Config.Queues must name at least one queue")
	}
	seenQueue := make(map[string]struct{}, len(cfg.Queues))
	for _, q := range cfg.Queues {
		if q == "" {
			return nil, errors.New("taskq: Config.Queues contains an empty queue name")
		}
		if _, dup := seenQueue[q]; dup {
			return nil, fmt.Errorf("taskq: Config.Queues lists %q twice", q)
		}
		seenQueue[q] = struct{}{}
	}
	for name, w := range cfg.Weights {
		if _, known := seenQueue[name]; !known {
			return nil, fmt.Errorf("taskq: Config.Weights names %q, which is not in Config.Queues", name)
		}
		if w < 1 {
			return nil, fmt.Errorf("taskq: Config.Weights[%q] = %d, must be at least 1", name, w)
		}
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultConcurrency
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = DefaultVisibilityTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.Backoff == nil {
		cfg.Backoff = &Exponential{}
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = generateWorkerID()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	// Parse every schedule up front so a typo in a cron spec fails at startup
	// rather than at 3am when the first tick would have fired.
	entries := make(map[string]*compiledEntry, len(cfg.Cron))
	for _, e := range cfg.Cron {
		ce, err := compile(e)
		if err != nil {
			return nil, err
		}
		if _, dup := entries[ce.entry.ID]; dup {
			return nil, fmt.Errorf("taskq: duplicate cron entry ID %q", ce.entry.ID)
		}
		entries[ce.entry.ID] = ce
	}

	s := &Server{
		broker:   b,
		cfg:      cfg,
		log:      cfg.Logger.With(slog.String("worker_id", cfg.WorkerID)),
		host:     host,
		inFlight: make(map[string]context.CancelFunc),
		queues:   newQueueSelector(cfg.Queues, cfg.Weights, cfg.StrictPriority),
		entries:  entries,
	}
	// Optional rather than required: a broker that only implements the hot path
	// is still usable, it just cannot recover orphans. Saying so at startup
	// beats silently running without a safety net.
	if m, ok := b.(Maintenance); ok {
		s.maint = m
	}
	if c, ok := b.(CronStore); ok {
		s.cron = c
	}
	// Cron is different: asking for schedules and silently not running them is
	// never what the caller meant, so this is an error rather than a warning.
	if len(entries) > 0 {
		if s.cron == nil {
			return nil, errors.New("taskq: Config.Cron was set but the broker does not implement taskq.CronStore")
		}
		if s.maint == nil {
			return nil, errors.New("taskq: Config.Cron requires a broker implementing taskq.Maintenance for leader election")
		}
	}
	return s, nil
}

// IsLeader reports whether this worker currently holds the scheduler lease.
func (s *Server) IsLeader() bool { return s.leader.Load() }

// WorkerID returns this server's identity, which is also the owner stamped on
// every job it claims.
func (s *Server) WorkerID() string { return s.cfg.WorkerID }

// Run consumes jobs until ctx is cancelled, then shuts down gracefully.
//
// Cancelling ctx does not cancel jobs that are already running. It stops new
// claims and starts draining: in-flight handlers get up to ShutdownTimeout to
// finish normally, with their leases still being renewed throughout. Only if
// that expires are they cancelled, and even then their failures are reported so
// the jobs retry promptly instead of waiting out a visibility timeout.
func (s *Server) Run(ctx context.Context, h Handler) error {
	if h == nil {
		return errors.New("taskq: nil handler")
	}

	// Jobs deliberately do not inherit cancellation from ctx. If they did,
	// SIGTERM would kill every in-flight handler instantly and "graceful
	// shutdown" would mean nothing.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	// Lease renewal and heartbeats must outlive ctx too: a job draining for
	// twenty seconds still needs its lease extended, or it gets reaped and
	// duplicated by another worker while it is busy finishing.
	maintCtx, stopMaint := context.WithCancel(context.WithoutCancel(ctx))
	defer stopMaint()

	started := time.Now()
	var maintWG sync.WaitGroup
	s.startMaintenance(ctx, maintCtx, started, &maintWG)

	// A buffered channel is the whole concurrency limiter: one slot per
	// permitted in-flight job.
	slots := make(chan struct{}, s.cfg.Concurrency)
	var jobWG sync.WaitGroup

	s.log.Info("worker started",
		slog.Any("queues", s.cfg.Queues),
		slog.Bool("strict_priority", s.cfg.StrictPriority),
		slog.Int("concurrency", s.cfg.Concurrency),
		slog.Duration("visibility_timeout", s.cfg.VisibilityTimeout),
		slog.Bool("maintenance", s.maint != nil))

	s.fetchLoop(ctx, jobCtx, h, slots, &jobWG)

	s.log.Info("worker draining", slog.Duration("shutdown_timeout", s.cfg.ShutdownTimeout))
	err := s.drain(&jobWG, cancelJobs)

	stopMaint()
	maintWG.Wait()
	return err
}

// startMaintenance launches the background loops.
//
// runCtx stops at shutdown; maintCtx outlives it so that draining jobs keep
// their leases. Reaping and promoting are other workers' problem once this one
// is going away, but renewal and heartbeats are not.
func (s *Server) startMaintenance(runCtx, maintCtx context.Context, started time.Time, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.renewLoop(maintCtx)
	}()

	if s.maint == nil {
		s.log.Warn("orphan recovery disabled: broker does not implement taskq.Maintenance")
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.heartbeatLoop(maintCtx, started)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.reapLoop(runCtx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.promoteLoop(runCtx)
	}()

	if len(s.entries) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.schedulerLoop(runCtx)
		}()
	}
}

// schedulerLoop elects a leader and fires due cron schedules.
//
// Election is an efficiency measure, not a correctness one. Every claim is a
// compare-and-set at the broker, so N schedulers racing still fire each tick
// exactly once; the lease simply stops fifty workers all doing the same
// bookkeeping every few seconds. That is worth knowing when the lease lapses
// and two schedulers briefly overlap: the answer is that nothing breaks.
//
// Unlike reaping and promotion, which every worker does unconditionally
// because they are cheap and latency-sensitive, cron is a fan-out point where
// redundant work is more wasteful and less urgent.
func (s *Server) schedulerLoop(ctx context.Context) {
	interval := min(5*time.Second, s.cfg.VisibilityTimeout/2)
	if interval <= 0 {
		interval = time.Second
	}
	// Three intervals, so one slow round trip does not trigger a needless
	// leadership handover.
	leaseTTL := 3 * interval

	defer func() {
		if !s.leader.Load() {
			return
		}
		// Releasing on the way out makes failover immediate instead of
		// TTL-delayed, which matters during a rolling deploy.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
		defer cancel()
		if err := s.maint.ReleaseLease(releaseCtx, schedulerLease, s.cfg.WorkerID); err != nil {
			s.log.Error("releasing scheduler lease failed", slog.String("error", err.Error()))
		}
		s.leader.Store(false)
	}()

	if err := s.seedSchedules(ctx); err != nil {
		s.log.Error("seeding cron schedules failed", slog.String("error", err.Error()))
	}

	for {
		if !sleepCtx(ctx, interval) {
			return
		}

		held, err := s.maint.AcquireLease(ctx, schedulerLease, s.cfg.WorkerID, leaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("acquiring scheduler lease failed", slog.String("error", err.Error()))
			continue
		}

		if was := s.leader.Swap(held); was != held {
			if held {
				s.log.Info("became scheduler leader")
			} else {
				s.log.Warn("lost scheduler leadership")
			}
		}
		if !held {
			continue
		}

		s.fireDueSchedules(ctx)
	}
}

// seedSchedules records a first fire time for any schedule that does not have
// one, leaving running schedules untouched.
func (s *Server) seedSchedules(ctx context.Context) error {
	now := time.Now()
	for id, ce := range s.entries {
		next := ce.schedule.Next(now)
		if next.IsZero() {
			s.log.Error("cron schedule can never fire",
				slog.String("entry_id", id),
				slog.String("spec", ce.entry.Spec))
			continue
		}
		created, err := s.cron.ScheduleCronIfAbsent(ctx, id, next)
		if err != nil {
			return err
		}
		if created {
			s.log.Info("cron schedule registered",
				slog.String("entry_id", id),
				slog.String("spec", ce.entry.Spec),
				slog.Time("first_fire", next))
		}
	}
	return nil
}

// fireDueSchedules materialises a job for each schedule whose tick has come up.
func (s *Server) fireDueSchedules(ctx context.Context) {
	due, err := s.cron.DueCron(ctx, cronBatch)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("reading due cron entries failed", slog.String("error", err.Error()))
		}
		return
	}

	for _, d := range due {
		ce, known := s.entries[d.ID]
		if !known {
			// A schedule from a different build of the application. Leaving it
			// alone is the only safe choice: deleting it would break whichever
			// deploy does own it.
			continue
		}

		// Advance from now rather than from the missed tick, so a scheduler
		// that was down for an hour does not enqueue an hour of backlog the
		// moment it returns. Missed ticks are skipped, not replayed.
		base := time.Now()
		if base.Before(d.FireAt) {
			base = d.FireAt
		}
		next := ce.schedule.Next(base)
		if next.IsZero() {
			s.log.Error("cron schedule can never fire again",
				slog.String("entry_id", d.ID),
				slog.String("spec", ce.entry.Spec))
			continue
		}

		// Enqueue first, then claim. If this worker dies in between, the next
		// scheduler re-fires the same tick and the unique key collapses it --
		// whereas claiming first and then failing to enqueue would silently
		// skip the tick altogether.
		job := &Job{
			Queue:       ce.entry.Queue,
			Type:        ce.entry.Type,
			Payload:     ce.entry.Payload,
			MaxAttempts: ce.entry.MaxAttempts,
			UniqueKey:   cronUniqueKey(d.ID, d.FireAt),
			UniqueTTL:   cronUniqueTTL,
		}
		if err := s.broker.Enqueue(ctx, job); err != nil && !errors.Is(err, ErrDuplicate) {
			s.log.Error("enqueuing cron job failed",
				slog.String("entry_id", d.ID),
				slog.String("error", err.Error()))
			// Deliberately not claiming, so the tick is retried next pass.
			continue
		}

		won, err := s.cron.ClaimCron(ctx, d.ID, d.FireAt, next)
		if err != nil {
			s.log.Error("claiming cron tick failed",
				slog.String("entry_id", d.ID),
				slog.String("error", err.Error()))
			continue
		}
		if !won {
			// Another scheduler advanced this entry first. The unique key
			// means only one job exists regardless.
			continue
		}

		s.log.Info("cron job enqueued",
			slog.String("entry_id", d.ID),
			slog.String("type", ce.entry.Type),
			slog.String("job_id", job.ID),
			slog.Time("next_fire", next))
	}
}

// renewLoop extends the leases of every in-flight job.
//
// One batched call for the whole pool rather than one per job: at a renewal
// interval of a third of the visibility timeout, per-job renewals would put a
// worker's lease traffic in the same order of magnitude as its actual work.
func (s *Server) renewLoop(ctx context.Context) {
	interval := s.cfg.VisibilityTimeout / 3
	if interval <= 0 {
		interval = time.Second
	}

	failures := 0
	for {
		if !sleepCtx(ctx, interval) {
			return
		}

		ids := s.inFlightIDs()
		if len(ids) == 0 {
			failures = 0
			continue
		}

		lost, err := s.broker.Extend(ctx, s.cfg.WorkerID, ids, s.cfg.VisibilityTimeout)
		if err != nil {
			failures++
			s.log.Error("lease renewal failed",
				slog.Int("in_flight", len(ids)),
				slog.Int("consecutive_failures", failures),
				slog.String("error", err.Error()))

			// Repeated failure almost certainly means the broker is
			// unreachable and every lease has expired. Continuing to run burns
			// capacity on work that is already being redone elsewhere, so stop
			// and let the retry path handle it. A false positive here costs one
			// wasted attempt; a false negative costs duplicated side effects.
			if failures >= maxRenewFailures {
				s.log.Error("giving up on lease renewal, cancelling in-flight jobs",
					slog.Int("in_flight", len(ids)))
				for _, id := range ids {
					s.cancelJob(id)
				}
				failures = 0
			}
			continue
		}
		failures = 0

		for _, id := range lost {
			// Someone else owns this job now. Whatever this handler does from
			// here is duplicate work at best.
			s.log.Warn("lease lost, cancelling job", slog.String("job_id", id))
			s.cancelJob(id)
		}
	}
}

// reapLoop recovers jobs whose leases expired.
//
// Every worker runs this, not just an elected leader. Reaping is idempotent --
// the broker guarantees only one caller acts on a given job -- so redundancy is
// free, and it keeps orphan recovery off the critical path of leader election.
func (s *Server) reapLoop(ctx context.Context) {
	interval := s.cfg.VisibilityTimeout / 2
	if interval <= 0 {
		interval = time.Second
	}

	for {
		// Jittered so a fleet that started together does not sweep in lockstep.
		if !sleepCtx(ctx, jitter(interval)) {
			return
		}

		n, err := s.maint.Reap(ctx, s.cfg.Queues, reapBatch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("reap failed", slog.String("error", err.Error()))
			continue
		}
		if n > 0 {
			s.log.Warn("recovered orphaned jobs", slog.Int("count", n))
		}
	}
}

// promoteLoop moves due retries and delayed jobs onto their ready queues.
func (s *Server) promoteLoop(ctx context.Context) {
	interval := min(time.Second, s.cfg.VisibilityTimeout/2)
	if interval <= 0 {
		interval = time.Second
	}

	for {
		if !sleepCtx(ctx, jitter(interval)) {
			return
		}

		n, err := s.maint.Promote(ctx, promoteBatch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("promote failed", slog.String("error", err.Error()))
			continue
		}
		if n > 0 {
			s.log.Debug("promoted due jobs", slog.Int("count", n))
		}
	}
}

// heartbeatLoop publishes this worker's liveness and load.
func (s *Server) heartbeatLoop(ctx context.Context, started time.Time) {
	interval := min(5*time.Second, s.cfg.VisibilityTimeout/2)
	if interval <= 0 {
		interval = time.Second
	}
	// Three missed beats before a worker is presumed gone, so one slow round
	// trip does not make a healthy worker vanish from the operator's view.
	ttl := 3 * interval

	beat := func() {
		if err := s.maint.Heartbeat(ctx, WorkerInfo{
			ID:          s.cfg.WorkerID,
			Host:        s.host,
			PID:         os.Getpid(),
			Queues:      s.cfg.Queues,
			Concurrency: s.cfg.Concurrency,
			InFlight:    s.inFlightCount(),
			StartedAt:   started,
		}, ttl); err != nil && ctx.Err() == nil {
			s.log.Error("heartbeat failed", slog.String("error", err.Error()))
		}
	}

	beat() // register immediately rather than after the first interval
	for {
		if !sleepCtx(ctx, interval) {
			return
		}
		beat()
	}
}

// fetchLoop claims work until ctx is cancelled.
func (s *Server) fetchLoop(ctx, jobCtx context.Context, h Handler, slots chan struct{}, wg *sync.WaitGroup) {
	delay := minPollInterval

	for {
		// Take a slot *before* asking for work. Claiming a job we have no
		// capacity to start would mean holding its lease while it sits in a
		// local buffer -- and losing it outright if this process died, since
		// nothing would ack or nack it.
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}

		// Reshuffled per poll, so priority is expressed as a probability of
		// being tried first rather than as an absolute ordering.
		job, err := s.broker.Dequeue(ctx, ClaimOpts{
			WorkerID:          s.cfg.WorkerID,
			Queues:            s.queues.next(),
			VisibilityTimeout: s.cfg.VisibilityTimeout,
		})

		switch {
		case err == nil:
			delay = minPollInterval
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				s.runJob(jobCtx, h, job)
			}()

		case errors.Is(err, ErrNoJob):
			<-slots
			// Idle: back off so an empty queue is cheap, with jitter so a fleet
			// of workers that started together does not poll in lockstep.
			if !sleepCtx(ctx, jitter(delay)) {
				return
			}
			delay = min(delay*2, maxPollInterval)

		default:
			<-slots
			if ctx.Err() != nil {
				// The error is just our own shutdown racing the call.
				return
			}
			s.log.Error("dequeue failed", slog.String("error", err.Error()))
			if !sleepCtx(ctx, jitter(maxPollInterval)) {
				return
			}
		}
	}
}

// runJob executes one job and reports the outcome.
func (s *Server) runJob(ctx context.Context, h Handler, j *Job) {
	// Each job gets its own cancellable context so that losing one lease stops
	// exactly one handler.
	jobCtx, cancel := context.WithCancel(ctx)
	s.trackJob(j.ID, cancel)
	defer func() {
		s.untrackJob(j.ID)
		cancel()
	}()

	start := time.Now()
	err := s.invoke(jobCtx, h, j)
	elapsed := time.Since(start)

	// The outcome must be reported even when the job's own context has been
	// cancelled, so this gets a fresh one. Reusing the cancelled context would
	// mean a shutdown left every in-flight job's lease dangling until the
	// reaper noticed -- turning a clean stop into a pile of duplicate work.
	reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancelReport()

	switch {
	case err == nil:
		if ackErr := s.broker.Ack(reportCtx, s.cfg.WorkerID, j.ID); ackErr != nil {
			s.log.Error("ack failed",
				slog.String("job_id", j.ID),
				slog.String("type", j.Type),
				slog.String("error", ackErr.Error()))
			return
		}
		s.log.Info("job completed",
			slog.String("job_id", j.ID),
			slog.String("type", j.Type),
			slog.Int("attempt", j.Attempt),
			slog.Duration("elapsed", elapsed))

	case errors.Is(err, ErrSkipRetry):
		if killErr := s.broker.Kill(reportCtx, s.cfg.WorkerID, j.ID, err.Error()); killErr != nil {
			s.log.Error("kill failed",
				slog.String("job_id", j.ID),
				slog.String("error", killErr.Error()))
			return
		}
		s.log.Warn("job dead-lettered without retry",
			slog.String("job_id", j.ID),
			slog.String("type", j.Type),
			slog.String("error", err.Error()))

	default:
		delay := s.cfg.Backoff.Next(j.Attempt)
		retryAt := time.Now().Add(delay)
		if nackErr := s.broker.Nack(reportCtx, s.cfg.WorkerID, j.ID, retryAt, err.Error()); nackErr != nil {
			// ErrLeaseLost here is expected and benign: the job was already
			// re-delivered, so someone else owns its outcome.
			level := slog.LevelError
			if errors.Is(nackErr, ErrLeaseLost) {
				level = slog.LevelWarn
			}
			s.log.Log(reportCtx, level, "nack failed",
				slog.String("job_id", j.ID),
				slog.String("error", nackErr.Error()))
			return
		}
		s.log.Warn("job failed",
			slog.String("job_id", j.ID),
			slog.String("type", j.Type),
			slog.Int("attempt", j.Attempt),
			slog.Int("max_attempts", j.MaxAttempts),
			slog.Duration("retry_in", delay),
			slog.String("error", err.Error()))
	}
}

// invoke runs a handler, converting a panic into an ordinary error.
//
// One bad handler must not take down a worker that is running nine other jobs
// correctly. The stack goes to the log rather than into the returned error,
// because that error is stored on the job envelope and a full stack trace would
// bloat every dead-letter entry.
func (s *Server) invoke(ctx context.Context, h Handler, j *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("handler panicked",
				slog.String("job_id", j.ID),
				slog.String("type", j.Type),
				slog.String("panic", fmt.Sprint(r)),
				slog.String("stack", string(debug.Stack())))
			err = fmt.Errorf("taskq: handler panicked: %v", r)
		}
	}()
	return h.ProcessTask(ctx, j)
}

// drain waits for in-flight jobs, escalating to cancellation if they overrun.
func (s *Server) drain(wg *sync.WaitGroup, cancelJobs func()) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.log.Info("shutdown complete")
		return nil
	case <-time.After(s.cfg.ShutdownTimeout):
	}

	// Out of patience. Cancelling gives handlers that respect their context a
	// chance to return an error, which is then nacked -- so the job retries
	// promptly rather than sitting claimed until its lease expires.
	s.log.Warn("shutdown deadline exceeded, cancelling in-flight jobs",
		slog.Duration("waited", s.cfg.ShutdownTimeout))
	cancelJobs()

	select {
	case <-done:
		s.log.Info("shutdown complete after cancellation")
		return nil
	case <-time.After(forceGracePeriod):
		// Handlers that ignore their context cannot be stopped. Their jobs will
		// be re-delivered once the visibility timeout expires.
		s.log.Error("abandoning jobs that ignored cancellation",
			slog.Duration("grace", forceGracePeriod))
		return errors.New("taskq: shutdown timed out with jobs still running")
	}
}

// trackJob records a running job so its lease can be renewed and, if lost,
// cancelled.
func (s *Server) trackJob(id string, cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight[id] = cancel
}

func (s *Server) untrackJob(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, id)
}

func (s *Server) inFlightIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.inFlight))
	for id := range s.inFlight {
		ids = append(ids, id)
	}
	return ids
}

func (s *Server) inFlightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inFlight)
}

// cancelJob stops one running job without disturbing the others.
func (s *Server) cancelJob(id string) {
	s.mu.Lock()
	cancel, ok := s.inFlight[id]
	s.mu.Unlock()
	if ok {
		cancel()
	}
}

// sleepCtx waits for d, reporting false if ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// jitter spreads an interval over [d/2, d) so workers that started together do
// not settle into synchronised polling or sweeping.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half))) //nolint:gosec // G404: interval spacing, not security
}

// generateWorkerID builds an identifier that is unique across processes and
// recognisable to a human reading a job's owner field.
func generateWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), NewID()[:8])
}
