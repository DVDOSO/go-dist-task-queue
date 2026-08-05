# taskq

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
        return fmt.Errorf("bad payload: %w", taskq.ErrSkipRetry)
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

---

## Measured results

All reproducible from `examples/`. Hardware: Intel Core Ultra 7 268V (8 cores), 31.5 GB RAM,
Windows 11, Redis 7.4.10 in Docker over loopback, every process on one machine.

### Reliability — `go run ./examples/chaos`

Spawns real worker **processes**, `SIGKILL`s one mid-job, and checks every job still completed. A
worker killed this way never acks, never nacks, and never releases its leases.

| Metric | Result (5 runs) |
| --- | --- |
| Jobs enqueued | 300, across 4 worker processes |
| **Jobs lost** | **0, every run** |
| Duplicate executions | 0, every run |
| Orphans recovered after the kill | 2.17s – 2.64s |
| Theoretical bound | 3s = 2s visibility timeout + 1s reap interval |

### Throughput — `go run ./examples/bench`

5,000 jobs per run, 25 concurrency per worker process, **no-op handler** — this measures the queue's
own overhead (enqueue, claim, ack), not handler cost.

| Worker processes | End-to-end jobs/sec |
| --- | --- |
| 1 | ~1,400 – 1,900 |
| 2 | ~2,300 – 2,600 |
| 4 | ~2,700 – 3,600 |
| **8** | **~3,800 median** (3,331 – 4,405 across 7 runs) |

Enqueue-to-start latency, onto an **idle** queue: **p50 2.6ms, p95 5.6ms, p99 6.8ms.**

**Scaling is sublinear — about 2.7× for 8× the workers.** Two reasons, both properties of the rig
rather than the design: one Redis instance serialises every state transition at three round trips per
job, and nine processes share eight cores. Scaling past this means sharding by queue across Redis
instances, or moving to a broker with partitions.

### Scheduling — `go run ./examples/scheduled`

Three workers share one schedule; the current leader is stopped partway through.

**12 ticks fired, 12 distinct, 0 double fires** across the handover — on both the Redis and in-memory
brokers.

---

## The delivery contract

> Every job that `Enqueue` returned `nil` for will be handed to a registered handler **at least
> once**, and is not considered complete until a handler returns `nil` *and* the resulting `Ack`
> succeeds.

Four corollaries, stated plainly because each one is a real constraint:

1. **Duplicates are possible and are not a bug.** If a worker stalls past its visibility deadline,
   the reaper re-delivers the job while the original may still be running. **Handlers must be
   idempotent.** There is no exactly-once mode.
2. **An `Enqueue` error is ambiguous.** A timed-out round trip may mean the job landed or did not.
   Producers that care should pass `taskq.Unique(key, ttl)` and treat `ErrDuplicate` as success.
3. **The guarantee is conditional on Redis durability.** With `appendfsync everysec` you can lose up
   to a second of acknowledged enqueues on an unclean shutdown.
4. **Poison pills terminate.** Attempts are consumed at claim time, so a job that reliably crashes
   its worker reaches the dead-letter queue instead of cycling forever.

### Job lifecycle

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

---

## Architecture

```
┌────────────┐   Enqueue / EnqueueTask       ┌──────────────────────────────┐
│ Producer   │ ────────────────────────────► │            Redis             │
└────────────┘                               │                              │
                                             │  lists  : ready queues       │
┌────────────┐   Dequeue → run → Ack/Nack    │  zsets  : active / delayed / │
│ Worker     │ ◄───────────────────────────► │           retry / dead /     │
│  ├ pool    │   Extend  (lease renewal)     │           workers / cron     │
│  ├ reaper  │   Reap    (orphan recovery)   │  hashes : job envelopes      │
│  ├ promoter│   Promote (retries + delayed) │  strings: unique keys, leases│
│  └ schedulr│   Cron    (leader only)       │                              │
└────────────┘                               └──────────────────────────────┘
```

There is no coordinator process. Redis *is* the coordination point, and every shared-state mutation
is a Lua script.

### Redis key layout

| Key | Type | Contents |
| --- | --- | --- |
| `taskq:q:<queue>` | LIST | ready job IDs; `RPUSH` to enqueue, `LPOP` to claim |
| `taskq:job:<id>` | HASH | the job envelope; fields mutated individually from Lua |
| `taskq:active:<queue>` | ZSET | job ID → visibility deadline (unix ms) |
| `taskq:delayed` | ZSET | job ID → run-at |
| `taskq:retry` | ZSET | job ID → retry-at |
| `taskq:dead` | ZSET | job ID → died-at |
| `taskq:cron` | ZSET | schedule ID → next fire |
| `taskq:workers` | ZSET | worker ID → last heartbeat |
| `taskq:worker:<id>` | HASH + TTL | self-expiring worker metadata |
| `taskq:unique:<key>` | STRING NX PX | idempotency lock |
| `taskq:lease:<name>` | STRING NX PX | leader election |
| `taskq:queues` | SET | known queue names, so nothing ever runs `KEYS` |
| `taskq:stat:{processed,failed}` | counter | durable totals across restarts |

The active set being a ZSET scored by deadline is the whole reliability mechanism: "which jobs have
lost their worker" is exactly `ZRANGEBYSCORE 0 now`.

---

## Design decisions

**Attempts are consumed at claim time, not at failure time.** A worker that is `SIGKILL`ed never
reports a failure, so a failure-counting scheme would let a job that reliably crashes its worker
cycle forever. Counting claims bounds poison pills without a second ceiling to reason about. The
`Recoveries` field survives as pure observability — it is what tells you, from a dead-letter entry,
whether a job was *failing* or *killing workers*.

**All time comes from the Redis server, never from a worker.** Lease deadlines are compared across
machines, so one client with a skewed clock must not be able to grant itself extra runtime or expire
its own lease on arrival. Every script reads `TIME`.

**Concurrency slots are taken before claiming, not after.** Claiming a job with no capacity to run it
means holding a lease on work sitting in a local buffer — and losing it outright if the process dies,
since nothing would ack or nack it.

**Jobs run on a context detached with `context.WithoutCancel`.** If handlers inherited cancellation,
SIGTERM would kill every in-flight job instantly and "graceful shutdown" would mean nothing.
Cancellation is the last resort after `ShutdownTimeout`, and even then the failures are reported so
jobs retry promptly instead of waiting out a visibility timeout.

**Ack and nack use a fresh context.** The job's own context may already be cancelled; reusing it
would leave every in-flight lease dangling at shutdown, turning a clean stop into duplicate work.

**Leader election is an efficiency measure, not a correctness one.** Reaping, promotion, and cron
claims are all compare-and-set at the broker — only the caller whose `ZREM` removed the member, or
whose expected score still matched, goes on to act. N schedulers racing still fire each tick exactly
once. The lease just stops fifty workers doing identical bookkeeping. That is why reaping and
promotion run in *every* worker (cheap, latency-sensitive) while cron is gated on leadership
(fan-out, less urgent).

**Priority is a probability, not an ordering.** Queue order is reshuffled every poll in proportion to
weights, so a queue weighted 6-of-10 is tried first about 60% of the time and no queue is ever
starved outright. `StrictPriority` is available for workloads that genuinely want starvation, but you
have to ask for it.

**Three narrow interfaces instead of one wide one.** `Broker` (hot path), `Maintenance` (background
loops), `CronStore` (scheduler). Split by consumer: a test double should not have to implement
`Promote` in order to run a job.

---

## What this deliberately does not do

- **No exactly-once.** See the contract above.
- **No Redis Cluster.** The delayed, retry, and dead-letter sets are global while ready and active
  keys are per-queue, so scripts touching both span hash slots. Single-node and Sentinel only.
  `rdb.New` takes a `redis.UniversalClient`, so Sentinel is a configuration change — but it is
  untested here and should not be claimed otherwise.
- **No multi-machine validation.** Workers are independent processes coordinating only through Redis,
  and nothing assumes co-location, but every measurement above ran on one host.
- **No metrics endpoint or CLI.** Structured logging via `log/slog` is throughout; Prometheus and a
  Cobra CLI were scoped out.
- **No DST handling in cron.** Schedules are computed in the caller's location, so a wall clock that
  skips or repeats an hour will skip or repeat that day's fire. Run schedulers in UTC.
- **Minute granularity for cron.** Five fields, as standard. `@every 30s` covers the sub-minute case.

---

## Demos

Each is self-contained and prints what it proves.

```bash
go run ./examples/lifecycle   # job lifecycle end to end, in memory, no dependencies
go run ./examples/worker      # producer + worker pool + graceful drain (Ctrl-C to watch it)
go run ./examples/scheduled   # cron with leader election and a failover
```

With a real Redis:

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

```bash
REDIS_ADDR=localhost:6379 go run ./examples/chaos
```

```bash
REDIS_ADDR=localhost:6379 go run ./examples/bench
```

`worker` and `scheduled` also accept `REDIS_ADDR` and run the identical code against either broker,
which is the `Broker` interface earning its keep.

---

## Development

```bash
make check
```

Runs build, vet, race tests, and lint — the gate every commit is held to.

```bash
make test-integration
```

The Redis-backed suite, behind a `//go:build integration` tag so the default `go test ./...` stays
hermetic and fast (no Docker, no network). It provisions Redis itself via testcontainers.

Requires Go 1.25+ and, for the integration suite, a running Docker daemon.

### Layout

```
job.go broker.go errors.go       core types, the three interfaces, sentinel errors
client.go handler.go server.go   producer API, handler registry, worker runtime
cron.go queues.go backoff.go     scheduling, priority policy, retry policy
internal/rdb/                    Redis broker + 12 embedded Lua scripts
internal/memory/                 in-process broker: tests, local dev, reference implementation
internal/cron/                   five-field cron parser
examples/                        five runnable demos
```

The in-memory broker exists so the worker runtime, retry policy, and shutdown logic can be tested
with no Docker and no network — and so the Redis broker has a reference implementation to be checked
against.
