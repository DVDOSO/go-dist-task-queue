// Package rdb implements the taskq broker interfaces on Redis.
//
// Every state transition is a single Lua script, so that a job is never
// observable in two places or in neither. That property is what the whole
// at-least-once guarantee rests on; see the individual scripts for the failure
// each one is defending against.
//
// # Supported topologies
//
// Single-node and Sentinel. Redis Cluster is not supported and this package
// does not pretend otherwise: the delayed, retry, and dead-letter sorted sets
// are global while the ready and active keys are per-queue, so scripts touching
// both inherently span hash slots. Making cluster work would mean either
// sharding those global sets per queue or pinning the entire keyspace to one
// slot, and neither is worth doing before there is a reason to.
package rdb

import (
	"embed"
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	taskq "github.com/DVDOSO/go-dist-task-queue"
)

// Defaults applied when the corresponding option is not supplied.
const (
	DefaultPrefix = "taskq"
	// DefaultCompletedTTL keeps a finished job's envelope around briefly so an
	// operator can answer "what happened to job X". Set to 0 to delete on ack.
	DefaultCompletedTTL = time.Hour
)

//go:embed scripts/*.lua
var scriptFS embed.FS

// Broker is a Redis-backed taskq.Broker.
type Broker struct {
	client       redis.UniversalClient
	prefix       string
	completedTTL time.Duration

	enqueue      *redis.Script
	dequeue      *redis.Script
	ack          *redis.Script
	nack         *redis.Script
	kill         *redis.Script
	extend       *redis.Script
	reap         *redis.Script
	promote      *redis.Script
	heartbeat    *redis.Script
	leaseAcquire *redis.Script
	leaseRelease *redis.Script
	cronClaim    *redis.Script
}

// Option configures a Broker.
type Option func(*Broker)

// WithPrefix sets the key namespace. Useful for isolating environments, and for
// giving each test its own keyspace so they can run in parallel.
func WithPrefix(p string) Option {
	return func(b *Broker) { b.prefix = p }
}

// WithCompletedTTL sets how long a completed job's envelope is retained. Zero
// deletes it on ack.
func WithCompletedTTL(d time.Duration) Option {
	return func(b *Broker) { b.completedTTL = d }
}

// New returns a Broker using the supplied client.
//
// The client is not owned by the Broker: Close does not close it, because the
// caller may well be sharing it with the rest of their application.
func New(client redis.UniversalClient, opts ...Option) (*Broker, error) {
	if client == nil {
		return nil, errors.New("taskq/rdb: nil redis client")
	}
	b := &Broker{
		client:       client,
		prefix:       DefaultPrefix,
		completedTTL: DefaultCompletedTTL,
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.prefix == "" {
		return nil, errors.New("taskq/rdb: empty key prefix")
	}

	for _, l := range []struct {
		name string
		dst  **redis.Script
	}{
		{"enqueue", &b.enqueue},
		{"dequeue", &b.dequeue},
		{"ack", &b.ack},
		{"nack", &b.nack},
		{"kill", &b.kill},
		{"extend", &b.extend},
		{"reap", &b.reap},
		{"promote", &b.promote},
		{"heartbeat", &b.heartbeat},
		{"lease_acquire", &b.leaseAcquire},
		{"lease_release", &b.leaseRelease},
		{"cron_claim", &b.cronClaim},
	} {
		s, err := loadScript(l.name)
		if err != nil {
			return nil, err
		}
		*l.dst = s
	}

	return b, nil
}

// loadScript reads an embedded script. redis.Script already runs EVALSHA first
// and falls back to EVAL on a NOSCRIPT error, which covers a Redis that was
// restarted or had its script cache flushed underneath us, so there is nothing
// to reimplement here.
func loadScript(name string) (*redis.Script, error) {
	src, err := scriptFS.ReadFile(path.Join("scripts", name+".lua"))
	if err != nil {
		return nil, fmt.Errorf("taskq/rdb: load script %s: %w", name, err)
	}
	return redis.NewScript(string(src)), nil
}

// Close implements taskq.Broker.
//
// It does not close the underlying Redis client: that client was supplied by
// the caller, who may be sharing it, so closing it here would be a surprising
// side effect of shutting down a queue.
func (b *Broker) Close() error { return nil }

// Key builders. Kept together so the whole layout can be read in one place.
//
// The p* helpers return prefixes rather than complete keys, for the scripts
// that must derive a key from data they read at runtime: Ack knows only a job
// ID, and the active set it has to clear depends on that job's queue.
func (b *Broker) kJob(id string) string     { return b.prefix + ":job:" + id }
func (b *Broker) kQueue(q string) string    { return b.prefix + ":q:" + q }
func (b *Broker) kActive(q string) string   { return b.prefix + ":active:" + q }
func (b *Broker) kUnique(key string) string { return b.prefix + ":unique:" + key }
func (b *Broker) kDelayed() string          { return b.prefix + ":delayed" }
func (b *Broker) kRetry() string            { return b.prefix + ":retry" }
func (b *Broker) kDead() string             { return b.prefix + ":dead" }
func (b *Broker) kQueues() string           { return b.prefix + ":queues" }
func (b *Broker) kProcessed() string        { return b.prefix + ":stat:processed" }
func (b *Broker) kFailed() string           { return b.prefix + ":stat:failed" }
func (b *Broker) kWorkers() string          { return b.prefix + ":workers" }
func (b *Broker) kWorker(id string) string  { return b.prefix + ":worker:" + id }
func (b *Broker) kLease(name string) string { return b.prefix + ":lease:" + name }
func (b *Broker) kCron() string             { return b.prefix + ":cron" }
func (b *Broker) pJob() string              { return b.prefix + ":job:" }
func (b *Broker) pActive() string           { return b.prefix + ":active:" }

// decodeJob rebuilds a Job from a flat HGETALL field/value array.
func decodeJob(flat []any) (*taskq.Job, error) {
	if len(flat)%2 != 0 {
		return nil, fmt.Errorf("taskq/rdb: malformed job hash: %d fields", len(flat))
	}
	m := make(map[string]string, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		k, okKey := flat[i].(string)
		v, okVal := flat[i+1].(string)
		if !okKey || !okVal {
			continue
		}
		m[k] = v
	}
	if m["id"] == "" {
		return nil, fmt.Errorf("%w: job hash has no id", taskq.ErrJobNotFound)
	}

	j := &taskq.Job{
		ID:          m["id"],
		Queue:       m["queue"],
		Type:        m["type"],
		Attempt:     atoi(m["attempt"]),
		MaxAttempts: atoi(m["max_attempts"]),
		Recoveries:  atoi(m["recoveries"]),
		State:       taskq.State(m["state"]),
		Owner:       m["owner"],
		UniqueKey:   m["unique_key"],
		LastErr:     m["last_err"],
		EnqueuedAt:  timeFromMs(atoi64(m["enqueued_at"])),
		RunAt:       timeFromMs(atoi64(m["run_at"])),
		StartedAt:   timeFromMs(atoi64(m["started_at"])),
		Deadline:    timeFromMs(atoi64(m["deadline"])),
	}
	if p := m["payload"]; p != "" {
		j.Payload = []byte(p)
	}
	return j, nil
}

// msFromTime encodes a time as unix milliseconds, mapping the zero time to 0.
//
// Milliseconds rather than RFC 3339 strings because these values are sorted-set
// scores: the reaper and promoter compare them numerically inside Lua.
func msFromTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func timeFromMs(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// toInt64 normalises the numeric types a Lua reply can arrive as. Anything
// unrecognised yields 0, which every caller treats as "not the success code".
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case string:
		p, _ := strconv.ParseInt(n, 10, 64)
		return p
	default:
		return 0
	}
}
