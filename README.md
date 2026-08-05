# Distributed Task Queue in Go

A distributed task queue for Go, backed by Redis. At-least-once delivery, visibility-timeout leases,
retries with backoff, dead-lettering, cron scheduling with leader election, and weighted queue
priorities.

Every state transition is a single Lua script, so a job is never observable in two places or in
neither. That property is what the delivery guarantee rests on.

```go
// Producer
client := taskq.NewClient(broker)
client.EnqueueTask(ctx, "email:send", Email{To: "a@b.c"}, taskq.Queue("critical"))

// Worker
mux := taskq.NewMux()
mux.HandleFunc("email:send", func(ctx context.Context, j *taskq.Job) error {
    var e Email
    if err := json.Unmarshal(j.Payload, &e); err != nil {
        return fmt.Errorf("bad payload: %w", taskq.ErrSkipRetry) // straight to the dead-letter queue
    }
    return send(ctx, e)
})

srv, _ := taskq.NewServer(broker, taskq.Config{
    Queues:      []string{"critical", "default"},
    Weights:     map[string]int{"critical": 6, "default": 1},
    Concurrency: 25,
})
srv.Run(ctx, mux) // drains gracefully when ctx is cancelled
```

## Measured

Reproducible from `examples/`. Intel Core Ultra 7 268V (8 cores), Redis 7.4.10 in Docker over
loopback, every process on one machine.

**Reliability** — `go run ./examples/chaos` spawns real worker _processes_ and `SIGKILL`s one
mid-job. Over 5 runs of 300 jobs across 4 workers: **zero job loss**, zero duplicates, orphans
recovered **2.17–2.64s** after the kill against a 3s bound (2s visibility timeout + 1s reap
interval).

**Throughput** — `go run ./examples/bench`, 5,000 jobs, 25 concurrency per worker, **no-op handler**
so the figure is queue overhead rather than handler cost:

| Worker processes | 1            | 2            | 4            | 8                                     |
| ---------------- | ------------ | ------------ | ------------ | ------------------------------------- |
| Jobs/sec         | ~1,400–1,900 | ~2,300–2,600 | ~2,700–3,600 | **~3,800 median** (3.3k–4.4k, 7 runs) |

Enqueue-to-start on an idle queue: **p50 2.6ms, p95 5.6ms, p99 6.8ms.**

Scaling is sublinear — about 2.7× for 8× the workers. One Redis serialises every state transition at
three round trips per job, and nine processes share eight cores. Getting past that means sharding by
queue across Redis instances.

**Scheduling** — `go run ./examples/scheduled` runs three workers over one schedule and stops the
leader partway through: **12 ticks, 12 distinct, 0 double fires** across the handover.

## The delivery contract

> Every job that `Enqueue` returned `nil` for will be handed to a handler **at least once**, and is
> not complete until a handler returns `nil` _and_ the resulting `Ack` succeeds.

Three consequences worth knowing before you build on it:

1. **Duplicates are possible and are not a bug.** If a worker stalls past its visibility deadline the
   reaper re-delivers while the original may still be running. **Handlers must be idempotent.** There
   is no exactly-once mode.
2. **An `Enqueue` error is ambiguous** — a timed-out round trip may mean the job landed or did not.
   Pass `taskq.Unique(key, ttl)` and treat `ErrDuplicate` as success.
3. **Poison pills terminate.** Attempts are consumed at claim time, so a job that reliably crashes
   its worker reaches the dead-letter queue instead of cycling forever.

```
                    Enqueue(RunAt>now)          promote (leader-elected)
              ┌───────────────────────► delayed ──────────────┐
              │                                               ▼
   (new) ─────┤ Enqueue ────────────────────────────────► pending ◄─────┐
              │                                               │         │
                                                     dequeue  ▼         │ reap
                                                            active ─────┘ (lease expired)
                                                          /   │   \
                                            ack          /    │    \  nack (attempts left)
                                        (completed) ◄───┘     │     └──────► retry ──┐
                                                              │                      │
                                          nack (exhausted) / kill                    │ promote
                                                              ▼                      │
                                                            dead ◄───────────────────┘
```

## Redis layout

There is no coordinator process. Redis _is_ the coordination point, and every shared-state mutation
is a Lua script. Single-node or Sentinel; Cluster is not supported, because the global sorted sets
and the per-queue keys would span hash slots.

| Key                                    | Type            | Contents                             |
| -------------------------------------- | --------------- | ------------------------------------ |
| `taskq:q:<queue>`                      | LIST            | ready job IDs                        |
| `taskq:job:<id>`                       | HASH            | the job envelope                     |
| `taskq:active:<queue>`                 | ZSET            | job ID → visibility deadline         |
| `taskq:delayed` / `:retry` / `:dead`   | ZSET            | job ID → run-at / retry-at / died-at |
| `taskq:cron`                           | ZSET            | schedule ID → next fire              |
| `taskq:workers` / `:worker:<id>`       | ZSET / HASH+TTL | heartbeats and worker metadata       |
| `taskq:unique:<key>` / `:lease:<name>` | STRING NX PX    | idempotency and leader election      |

The active set being a ZSET scored by deadline is the whole reliability mechanism: "which jobs have
lost their worker" is exactly `ZRANGEBYSCORE 0 now`.

## Design decisions

**Attempts are consumed at claim time, not at failure time.** A `SIGKILL`ed worker never reports a
failure, so a failure-counting scheme would let a job that reliably crashes its worker cycle forever.

**All time comes from the Redis server, never a worker.** Lease deadlines are compared across
machines, so one client with a skewed clock must not be able to grant itself extra runtime or expire
its own lease on arrival.

**Concurrency slots are taken before claiming, not after.** Claiming a job with no capacity to run it
means holding a lease on work sitting in a local buffer — and losing it outright if the process dies,
since nothing would ack or nack it.

**Jobs run on a context detached with `context.WithoutCancel`.** If handlers inherited cancellation,
SIGTERM would kill every in-flight job instantly and "graceful shutdown" would mean nothing.
Cancellation is the last resort after `ShutdownTimeout`. Ack and nack then use a _fresh_ context,
since the job's own may already be cancelled — reusing it would leave every in-flight lease dangling
at shutdown.

**Leader election is an efficiency measure, not a correctness one.** Reaping, promotion, and cron
claims are all compare-and-set: only the caller whose `ZREM` removed the member, or whose expected
score still matched, goes on to act. N schedulers racing still fire each tick exactly once. The lease
just stops fifty workers doing identical bookkeeping — which is why reaping and promotion run in
_every_ worker while cron is gated on leadership.

**Priority is a probability, not an ordering.** Queue order is reshuffled every poll in proportion to
weights, so a queue weighted 6-of-10 leads about 60% of the time and nothing starves.
`StrictPriority` is available for workloads that want starvation, but you have to ask for it.

## Demos

```bash
go run ./examples/lifecycle   # the job lifecycle end to end, in memory, no dependencies
go run ./examples/worker      # producer, worker pool, retries, graceful drain (Ctrl-C to watch)
go run ./examples/scheduled   # cron with leader election and a failover
```

With a real Redis — `docker run -d -p 6379:6379 redis:7-alpine`:

```bash
REDIS_ADDR=localhost:6379 go run ./examples/chaos
```

```bash
REDIS_ADDR=localhost:6379 go run ./examples/bench
```

`worker` and `scheduled` accept `REDIS_ADDR` too and run identical code against either broker, which
is the `Broker` interface earning its keep.

## Development

```bash
make check
```

Build, vet, race tests, and lint — the gate every commit is held to.

```bash
make test-integration
```

The Redis suite, behind a `//go:build integration` tag so the default `go test ./...` stays hermetic
and fast. It provisions Redis itself via testcontainers. Needs Go 1.25+ and a running Docker daemon.

```
job.go broker.go errors.go       core types, the three interfaces, sentinel errors
client.go handler.go             producer API and handler registry
server.go                        worker lifecycle: config, startup, shutdown
worker.go                        claiming work, running handlers, tracking in-flight jobs
maintenance.go                   lease renewal, orphan recovery, promotion, heartbeats
scheduler.go cron.go             leader-elected cron
queues.go backoff.go             priority and retry policy
internal/rdb/                    Redis broker + 12 embedded Lua scripts
internal/memory/                 in-process broker: tests, local dev, reference implementation
internal/cron/                   five-field cron parser
```

The in-memory broker exists so the worker runtime, retry policy, and shutdown logic can be tested
with no Docker and no network — and so the Redis broker has a reference implementation to check
against.
