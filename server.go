package taskq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
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
