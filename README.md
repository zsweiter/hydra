# Hydra

Hydra is a lightweight background job processing library for Go.
It provides a simple API for submitting jobs, scheduling recurring work,
and registering workers — while keeping scheduling, retries, elasticity,
and storage concerns internal.

**Design:** idle-first actors, signal-based wakeup, zero CPU when idle.

## Features

- Simple producer/consumer API
- Immediate and delayed jobs
- Recurring jobs (cron, RRULE, fixed interval)
- Exponential backoff retries
- Elastic worker pool (signal-based scaling)
- Misfire policies (skip, run-once, catch-up)
- Queue-based workload isolation
- Idempotency support
- Dead-letter queue
- Pluggable storage (in-memory default, SQLite, Redis/Valkey/Dragonfly, PostgreSQL)
- Zero CPU consumption when idle

## Installation

```bash
go get github.com/zsweiter/hydra
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/zsweiter/hydra"
)

func main() {
    q := hydra.New(hydra.Config{
        MinWorkers: 1,
        MaxWorkers: 4,
        BatchSize:  10,
        LeaseTTL:   time.Minute,
    })

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    q.Handle("emails", func(ctx context.Context, job *hydra.Job) error {
        fmt.Printf("Processing: %s\n", job.Name)
        return nil
    })

    if err := q.Start(ctx); err != nil {
        panic(err)
    }
    defer q.Stop(ctx)

    q.Submit(ctx, "emails", hydra.NewJob("", "welcome-email", []byte("hello"), nil))

    <-ctx.Done()
}
```

## Jobs

### Submit a job

```go
job := hydra.NewJob("", "send-report", payload, nil)
q.Submit(ctx, "reports", job)
```

If `id` is empty, an ULID is generated automatically on submit.

### Delayed job

```go
job := hydra.NewJob("", "nightly-task", payload, nil)
job.RunAt = time.Now().Add(5 * time.Minute)
q.Submit(ctx, "tasks", job)
```

### Custom retry policy

```go
job := hydra.NewJob("", "flaky-op", payload, nil)
job.MaxAttempts = 5
job.Backoff = hydra.BackoffPolicy{
    Type:   hydra.BackoffExponential,
    BaseMs: 200,
    MaxMs:  30_000,
}
q.Submit(ctx, "ops", job)
```

### Idempotent job

```go
job := hydra.NewJob("my-unique-id", "deduped-op", payload, nil)
job.IdempotencyKey = "process-invoice-42"
q.Submit(ctx, "invoices", job)
```

### Remove a pending job

```go
q.RemoveJob(ctx, "emails", "01ARZ3NDEKTSV4RRFFQ69G5FAV")
```

## Schedules

### Cron schedule

```go
schedule := hydra.NewJobSchedule("my-unique-id", "hourly-cleanup", payload,
    hydra.CronRule("0 * * * *"),
)
q.UpsertSchedule(ctx, "tasks", schedule)
```

### RRULE schedule

```go
schedule := hydra.NewJobSchedule("my-unique-id", "weekly-report", payload,
    hydra.RRuleRule("FREQ=WEEKLY;BYDAY=MO"),
)
q.UpsertSchedule(ctx, "reports", schedule)
```

### One-shot schedule

```go
schedule := hydra.NewJobSchedule("my-unique-id", "trial-ends", payload,
    hydra.OnceRule(time.Now().Add(14 * 24 * time.Hour)),
)
q.UpsertSchedule(ctx, "billing", schedule)
```

### Offset and timezone

`WithOffset` shifts every occurrence, and the offset can be negative. For
example, a reminder can fire five minutes before 09:00. `WithTimezone`
evaluates cron and RRULE expressions in an IANA zone:

```go
hydra.CronRule("0 9 * * *").WithTimezone("America/Santiago").WithOffset(-5 * time.Minute)
```

Upserting a schedule again with an unchanged rule keeps its progress
(`GeneratedJobs`, `NextRunAt`), so replaying a definition does not restart
it. Occurrences missed while the process was down are handled by the
schedule's `MisfirePolicy` on restart.

### Fixed interval

```go
schedule := hydra.NewJobSchedule("my-unique-id", "heartbeat", payload,
    hydra.IntervalRule(30*time.Second),
)
q.UpsertSchedule(ctx, "monitoring", schedule)
```

### Add start time or end time

```go
schedule := hydra.NewJobSchedule("my-unique-id", "heartbeat", payload,
    hydra.IntervalRule(30*time.Second).WithStartAt(time.Now().UTC()).WithEndAt(time.Now().UTC().Add(12 * time.Hour))
)
q.UpsertSchedule(ctx, "monitoring", schedule)
```

### Max jobs limit

```go
schedule := hydra.NewJobSchedule("my-unique-id", "limited-task", payload,
    hydra.IntervalRule(time.Second),
)
schedule.MaxJobs = 10
q.UpsertSchedule(ctx, "tasks", schedule)
```

### Misfire policy

```go
schedule := hydra.NewJobSchedule("my-unique-id", "catchup-task", payload,
    hydra.IntervalRule(time.Minute),
)
schedule.MisfirePolicy = hydra.MisfireCatchUp
q.UpsertSchedule(ctx, "tasks", schedule)
```

`MisfireSkip` drops occurrences missed while hydra was down, `MisfireRunOnce`
fires one job for them, and `MisfireCatchUp` replays every one of them, in
order.

A long downtime can leave a catch-up schedule thousands of occurrences behind.
They are replayed in batches of `Config.CatchUpBatch` (default 100) per
storage write, never as one huge write:

- **In order.** Each batch continues where the previous one stopped. The
  position is persisted, so a restart, or another instance, resumes there
  without losing or repeating an occurrence.
- **Time sliced.** On-time occurrences go first. A backlog gets one batch
  between them, so it never delays them by more than one batch.
- **By importance.** Among schedules behind, the lowest `Priority` value
  catches up first (the same order jobs are claimed in). Schedules with the
  same priority are interleaved in the order their occurrences were due.

### Pause / Resume / Remove

```go
q.PauseSchedule(ctx, "tasks", scheduleID)
q.ResumeSchedule(ctx, "tasks", scheduleID) // continues after now: paused occurrences are skipped
q.RemoveSchedule(ctx, "tasks", scheduleID) // kept as history with status cancelled
q.DeleteSchedule(ctx, "tasks", scheduleID) // removed for good
```

### History

```go
done, _ := q.ListSchedules(ctx, hydra.ScheduleQuery{
    Statuses: []hydra.ScheduleStatus{hydra.ScheduleFinished, hydra.ScheduleCancelled},
    Limit:    50,
})
occurrences, _ := q.ListJobs(ctx, hydra.JobQuery{ScheduleID: scheduleID, Limit: 50})
```

History grows with every occurrence. Set `HistoryTTL` to delete completed
jobs and ended schedules older than it; every instance purges in small
batches.

## Workers

### Register a worker

```go
q.Handle("emails", func(ctx context.Context, job *hydra.Job) error {
    return sendEmail(job)
})
```

Multiple workers can be registered for the same queue;
jobs are distributed in round-robin fashion.

### Direct delivery

A queue fed by schedules can take its occurrences as soon as they are due,
in batches, before anything is stored:

```go
q.HandleDirect("reminders", func(ctx context.Context, jobs []*hydra.Job) []error {
    return push.SendAll(ctx, jobs) // one error per job, nil when all were sent
})
q.Handle("reminders", sendOne) // the fallback for those that failed
```

A delivered occurrence costs one write (the claim that advances its
schedule) instead of three (store the job, lease it, acknowledge it), and
no worker round trip. An occurrence that fails is stored with the claim and
retried by the queue's worker as usual. Delivery must be idempotent: an
occurrence whose claim fails after delivery, or fired by two instances while
shards move, is delivered again. Key it by `ScheduleID` and
`MetadataScheduledFor`.

### Scheduler only

When occurrences only need to be delivered somewhere (a broker, a webhook)
and you want no job queue at all, package `scheduling` runs the scheduler on
its own, on a `storage.ScheduleStore`:

```go
svc, _ := scheduling.New(scheduling.Config{
    Store: store,
    Deliver: func(ctx context.Context, occurrences []*domain.Job) []error {
        return send(ctx, occurrences) // one error per occurrence
    },
})
svc.Start(ctx)
```

Nothing but the schedules is stored. An occurrence is claimed once `Deliver`
succeeded for it; one that failed is delivered again with exponential backoff,
and its schedule does not advance meanwhile. hydra-scheduler is built on it.

### Error handling

- Return `nil` → job is acknowledged and removed.
- Return an error → job is retried with backoff.
- Return `&hydra.RetryableError{Err: err}` → forced retry.
- Max attempts exceeded → job moves to dead-letter queue.
- Panics are recovered and treated as failures.

## Queue lifecycle

```go
if err := q.Start(ctx); err != nil { ... }
defer q.Stop(ctx)
```

`Start` performs:

1. Recover orphaned leases
2. Load active schedules into the scheduler
3. Start worker pool
4. Start dispatcher
5. Start scheduler

`Stop` performs a graceful shutdown in reverse order.

### Pause / Resume queue

```go
q.Pause(ctx, "emails")  // stops dispatching jobs for this queue
q.Resume(ctx, "emails") // resumes dispatching
```

## Configuration

```go
type Config struct {
    MinWorkers       int           // default: 1
    MaxWorkers       int           // default: 16
    ScaleUpThreshold float64       // channel occupancy (0.0–1.0) to trigger scale-up; default 0.25
    ScaleDownIdle    time.Duration // default: 10s
    BatchSize        int           // default: 10
    LeaseTTL         time.Duration // default: 30s
    MaxRetries       int           // default: 3
    SyncInterval     time.Duration // 0 = single instance (default); see Running several instances
    CatchUpBatch     int           // occurrences replayed per write by a catch-up schedule; default 100
    ScheduleWindow      time.Duration // how far ahead schedules are held in memory; default 1m
    ScheduleWindowLimit int           // schedules loaded at most per window; default 10000
    FireBatch        int           // due schedules fired together; default 512
    ClaimConcurrency int           // concurrent claim round trips per batch; default 4
    HistoryTTL       time.Duration // delete completed jobs and ended schedules older than this; 0 keeps them (default)
    Logger           logger.Logger
    Storage          storage.Storage // default: in-memory
}
```

### Running several instances

Several instances can share a Redis (Valkey, Dragonfly) or PostgreSQL store.
Each schedule occurrence is
claimed atomically: one storage operation advances `NextRunAt` with a
compare-and-swap and persists the generated jobs. Exactly one instance
generates each occurrence, and none is lost if an instance crashes.

Paused queues are stored as well, so `Pause` applies to every instance.
While a handler runs, its lease is extended automatically, so a long job is
not recovered and run twice.

Set `SyncInterval` (for example `5 * time.Second`) on every instance. With
it, each instance periodically:

- renews its membership and takes its share of the schedules,
- picks up schedules created or changed by the other instances,
- recovers jobs whose lease expired because their instance died,
- checks for jobs submitted elsewhere.

### Scaling

The scheduler keeps in memory only the schedules due within
`ScheduleWindow` (one minute by default), and without their payload: about
320 bytes per schedule, so a million due within the same minute take some
300 MB. Every 30 seconds it extends the window with the schedules that
entered it, without reloading those it holds. If more than
`ScheduleWindowLimit` are due at once, the rest arrive as the first ones fire.

Due schedules fire in batches of `FireBatch`: their payloads are loaded in
one round trip, their occurrences delivered (see
[Direct delivery](#direct-delivery)), and their claims written in
`ClaimConcurrency` concurrent round trips. A peak of a million reminders at
09:00 therefore costs a few thousand round trips, not millions. Each claim
is still atomic on its own.

Instances that share a storage split the schedules. Each schedule belongs to
one of 256 shards (a stable hash of its id). The live instances, tracked with
heartbeats in the storage, divide the shards between them by rendezvous
hashing. Each instance loads and fires only its own shards, so adding
instances spreads both the load and the memory. Every sync, an instance loads
only the schedules of its window changed since the previous one.

When an instance joins or leaves, the shards are redistributed on the next
sync. Only the shards the newcomer takes, or the leaver had, move (about 1/n
of them); the others keep their windows. An instance that stops cleanly hands over its shards immediately. A
crashed one hands them over once its heartbeat expires (three sync
intervals). While membership changes, two instances may briefly own the same
shard. The atomic claim still generates each occurrence only once.

### Performance

Measured on one laptop (i5-12500H), stores in local Docker containers,
NATS JetStream with file storage. Treat them as orders of magnitude.

A peak of reminders due at the same second (100,000 unless noted), defined
through NATS and fired by two `hydra-scheduler` instances (about 110 MB of
memory each at a million). Measured before the binary moved to the CloudEvents
binary contract and batched definition writes:

| Store                    | Definitions ingested | Peak published (first → last) | Duplicates |
| ------------------------ | -------------------- | ----------------------------- | ---------- |
| Valkey 9                 | 100,000 in 10 s      | +16 ms → +4.5 s               | 0          |
| PostgreSQL 17            | 100,000 in 7.6 s     | +11 ms → +2.7 s               | 0          |
| PostgreSQL 17, 1,000,000 | 1,000,000 in 80 s    | +15 ms → +22.9 s              | 0          |

Per occurrence, the scheduler alone on each store (`make bench-stores`,
`FirePeakDirect`: payload load, direct delivery and batched claims), against
the path before batching and direct delivery (claim the occurrence, then
claim and acknowledge its job):

| Store         | Before  | Now   |
| ------------- | ------- | ----- |
| SQLite (file) | ~620 µs | 28 µs |
| Valkey        | ~670 µs | 42 µs |
| Dragonfly     | ~1.4 ms | 48 µs |
| PostgreSQL    | ~5.2 ms | 26 µs |

The scheduler with the memory store fires about 400,000 occurrences per second
and holds about 320 bytes per schedule in its window (`make loadtest`).

## Storage

Hydra abstracts persistence behind the `storage.Storage` interface.
The default is in-memory (suitable for development and testing).

Each backend is a separate Go module — you import only the one you need.

### Interface

```go
import "github.com/zsweiter/hydra/storage"

type Storage interface {
    // Jobs
    SaveJob(ctx context.Context, job *domain.Job) error
    RemoveJob(ctx context.Context, jobID string) error
    ClaimJobs(ctx context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]LeasedJob, error)
    AckJobs(ctx context.Context, jobIDs []string) error // completed jobs are kept as history
    ExtendLease(ctx context.Context, jobID string, ttl time.Duration) error
    FailJob(ctx context.Context, job *domain.Job, failErr error) (FailResult, error)

    // Queue management
    ReadyQueues(ctx context.Context) ([]domain.QueueName, error)
    PromoteScheduled(ctx context.Context, now time.Time, limit int) (int, error)
    RecoverOrphaned(ctx context.Context, before time.Time) (int, error)

    // Schedules
    SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error // ErrStaleVersion on an older Version
    DeleteSchedule(ctx context.Context, scheduleID string) error
    // Active schedules of the window, without payload, for the given shards.
    LoadDueSchedules(ctx context.Context, q DueQuery) ([]*domain.JobSchedule, error)
    LoadSchedules(ctx context.Context, ids []string) ([]*domain.JobSchedule, error)
    LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error)
    UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error
    // A batch of claims, each an atomic compare-and-swap of NextRunAt plus its jobs.
    ClaimOccurrences(ctx context.Context, claims []Claim) ([]bool, error)

    // History
    ListSchedules(ctx context.Context, q ScheduleQuery) ([]*domain.JobSchedule, error)
    ListJobs(ctx context.Context, q JobQuery) ([]*domain.Job, error)
    PurgeHistory(ctx context.Context, before time.Time, limit int) (int, error)

    // Cluster membership: registers the instance, returns the live ones
    Heartbeat(ctx context.Context, instanceID string, ttl time.Duration) ([]string, error)

    // Queue pause, shared by every instance
    SetQueuePaused(ctx context.Context, queue domain.QueueName, paused bool) error
    PausedQueues(ctx context.Context) ([]domain.QueueName, error)

    // Dead-letter
    ListDLQ(ctx context.Context, queue domain.QueueName) ([]DeadLetter, error)
    RequeueDLQ(ctx context.Context, jobID string) error

    // Metadata
    NextScheduledAt(ctx context.Context) (time.Time, error)
    Stats(ctx context.Context) (Stats, error)
    Close() error
}
```

`storage/storagetest` checks an implementation against what the scheduler
expects (`storagetest.Run`) and benchmarks it (`storagetest.Bench`); every
bundled store runs both.

### SQLite

Uses `mattn/go-sqlite3` (CGo). WAL mode with busy timeout.

```go
import (
    "github.com/zsweiter/hydra"
    "github.com/zsweiter/hydra/storage/sqlite"
)

func main() {
    store, err := sqlite.New("file:test.db?mode=memory")             // file path
    if err != nil { panic(err) }

    q := hydra.New(hydra.Config{Storage: store})
    // ...
}
```

SQLite connection string supports query parameters:

```go
store, _ := sqlite.New("file:hydra.db?mode=memory&cache=shared")
```

### Redis, Valkey and Dragonfly

Uses `go-redis/v9` and runs on Redis, Valkey and Dragonfly (tested against
Valkey 9 and Dragonfly 1.34). Every Lua script declares the keys it touches,
as Dragonfly requires. Accepts any `redis.UniversalClient`: a single node,
Sentinel failover or a Redis (Valkey) Cluster.

Every key starts with `{hydra}:`. The braces are a cluster hash tag: all the
keys of one deployment share a slot, so its scripts and transactions run on a
cluster, on the node owning that slot. Deployments with different prefixes
(`redis.WithPrefix`) spread over the cluster's nodes.

Occurrences are claimed by a Lua script, without `WATCH`, and a batch of
claims goes in one pipeline.

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/zsweiter/hydra"
    "github.com/zsweiter/hydra/storage/redis"
)

func main() {
    store := redis.New(redis.Options{
        Addr: "localhost:6379",
        DB:   0,
    })

	// Or use custom connection
    // store := redis.Use(rdb) // rdb instance of redis library
    q := hydra.New(hydra.Config{Storage: store})
    // ...
}
```

With Sentinel, or a cluster:

```go
rdb := goredis.NewFailoverClient(&goredis.FailoverOptions{
    MasterName:    "mymaster",
    SentinelAddrs: []string{":26379"},
})
store := redis.Use(rdb)

cluster := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{":7000", ":7001", ":7002"}})
store = redis.Use(cluster, redis.WithPrefix("billing"))
```

Valkey and Redis persist with RDB snapshots by default, so a crash loses the
writes since the last one. Enable AOF (`appendonly yes`, `appendfsync
everysec`) to lose at most a second.

### PostgreSQL

Uses `jackc/pgx/v5` natively (pure Go, no CGo): a connection pool,
statements prepared once per connection, and batches that send several
statements in one round trip. Several instances can share one database. Jobs
are claimed with `FOR UPDATE SKIP LOCKED` and leased in the same statement; a
batch of occurrences is claimed, with its jobs, in one round trip. The schema
(tables prefixed `ha_`) is created on start, under an advisory lock so
instances starting together do not race. Its hot indexes are partial: they
hold only pending jobs and active schedules, however large the history grows.

```go
import "github.com/zsweiter/hydra/storage/postgres"

store, err := postgres.New("postgres://user:pass@localhost:5432/hydra?sslmode=disable&pool_max_conns=16")
if err != nil { panic(err) }
q := hydra.New(hydra.Config{Storage: store, SyncInterval: 5 * time.Second})

// Or reuse a *pgxpool.Pool:
// store, err := postgres.Use(pool)
```

`postgres.WithRelaxedCommit()` commits leases, acknowledgements, retries,
heartbeats and purges without waiting for the WAL flush
(`synchronous_commit = off`), which makes them several times faster. A
database crash may lose the last few hundred milliseconds of them, so some
jobs run again, as after an instance crash. Schedules, claims and new jobs
always wait for the flush.

### Custom backend

Implement the `storage.Storage` interface and pass it to `hydra.Config`:

```go
type MyStorage struct{ /* ... */ }

func (s *MyStorage) SaveJob(ctx context.Context, job *domain.Job) error { /* ... */ }
// ... implement every method of storage.Storage
var _ storage.Storage = (*MyStorage)(nil) // compile-time check

q := hydra.New(hydra.Config{Storage: &MyStorage{}})
```

## Broker scheduler mode

Hydra can also run as the scheduler of a message broker. `cmd/hydra-scheduler`
is a small binary with no HTTP or gRPC surface. You deploy it next to NATS
JetStream (Kafka is planned) and it only consumes and republishes:

1. A producer publishes an event whose **headers** carry a recurrence (a
   CloudEvent in binary mode with `recurrence*` extension attributes) on the
   subject of its occurrences under a prefix: `schedule.notification.adherence`
   schedules occurrences on `notification.adherence`.
2. The scheduler stores it as a schedule, the body byte for byte, without
   reading it.
3. On every occurrence it publishes the event on the subject **without the
   prefix**, with the same body and headers, a new `ce-id` and `ce-time`,
   `ce-hydra*` attributes and no `ce-recurrence*` ones.
4. Consumers consume that subject as usual: definitions never reach them.

Definitions belong in a stream of their own (`schedule.>`), ideally with
work-queue retention: hydra-scheduler is its only consumer and a definition is
removed once stored. The scheduler never reads back its own occurrences.

```bash
nats stream add SCHEDULES --subjects 'schedule.>' --retention work --dupe-window 2m --defaults
nats pub schedule.notification.adherence \
  -H ce-specversion:1.0 -H ce-id:evt-1 -H ce-source:/clinic -H ce-type:com.acme.adherence \
  -H ce-recurrenceid:adherence-p-001 -H ce-recurrencetype:cron \
  -H 'ce-recurrencerule:0 9 * * *' -H ce-recurrenceoffset:-PT5M \
  -H content-type:application/json '{"patient":"p-001"}'
```

The full contract, every attribute and what happens to each message, is in
[docs/CONTRACT.md](docs/CONTRACT.md). Package [`contract`](contract) implements
it for Go, and [examples/clients](examples/clients) shows producers and
consumers in Go, Rust and Node.js.

### Managing definitions

A `recurrenceid` (always required, chosen by the producer) has a lifecycle
driven by `recurrenceaction`. The scheduler has no API: every change goes
through the stream, so changes are ordered (by when the stream stored them)
and traced.

| Action             | Effect                                                                           | Repeated                                                                                                                                              |
| ------------------ | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `create` (default) | Stores the definition, revision 1                                                | The same definition changes nothing. A different one under the id of a live schedule is rejected (`hydra_definitions_conflict_total`): send `replace` |
| `replace`          | Archives the current revision as `<subject>\|<id>#<n>` and starts revision n+1   | The same definition changes nothing                                                                                                                   |
| `pause`            | Stops firing                                                                     | No effect                                                                                                                                             |
| `resume`           | Continues from the next occurrence after now; those due while paused are skipped | No effect                                                                                                                                             |
| `cancel`           | Stops for good, kept as history                                                  | No effect                                                                                                                                             |
| `delete`           | Removes the schedule, its body and its archived revisions, leaving a tombstone   | No effect                                                                                                                                             |

- **Idempotent.** Producers can retry freely: a definition is recognised by a
  fingerprint of its recurrence, type, source, content type and body, whatever
  event carries it. Once a schedule ended, or after a cancel or delete, a
  `create` starts a new revision.
- **Changes are revisions, not edits.** A replacement keeps the previous
  revision, with its progress, under its own id, and the new one records
  which it replaced. `hydra-scheduler schedule <subject> <id>` walks the chain
  back. Each revision stores the trace id of the change that made it (from
  `ce-traceparent`, or the event id), and pauses, resumes and cancellations
  record theirs too.
- **Occurrences say where they come from:** `ce-hydraschedule`,
  `ce-hydraoccurrence` (the nominal time, without offset or jitter),
  `ce-hydrarevision` and `ce-hydratrace`; `ce-traceparent` is inherited.
- **Out of order is safe.** A change older than the stored one (by stream
  time) is ignored. A cancel or delete processed before the definition it
  targets leaves a tombstone that rejects it.

### Delivery

hydra-scheduler stores nothing but the schedules. Due occurrences are
published in batches, with asynchronous JetStream publishes, and an occurrence
is claimed (its schedule advanced) only once JetStream acknowledged it. One
that failed is published again, with exponential backoff, and its schedule
waits: nothing is lost, and a schedule failing ten times in a row shows in
`hydra_schedules_stalled`.

Definitions are processed concurrently (in order per schedule) and stored in
batches: a message is acknowledged only after the batch holding it is
committed, so an acknowledged definition is never lost.

The occurrence id is deterministic and is sent as `Nats-Msg-Id`, so
JetStream drops a repeat inside the stream's deduplication window (two minutes
by default). A repeat happens when an occurrence is published but its claim
fails, for example while the store is unreachable; if that can last longer,
widen the stream's `duplicate_window`. Consumers should be idempotent anyway.

To run the demo, which starts NATS, the scheduler, a producer and a consumer
that logs occurrences:

```bash
docker compose -f examples/nats-scheduler/docker-compose.yml up --build
```

Several instances can run against the same Redis (Valkey, Dragonfly) or
PostgreSQL store and share the same durable consumers. They split the
schedules into shards (see [Scaling](#scaling)), each occurrence is claimed
once, and if an instance dies, the others take over its schedules. SQLite is
for a single instance.

### Configuration

All environment variables are documented in `cmd/hydra-scheduler/main.go`.
Durations are ISO 8601, like the contract's. The main ones:

| Variable                                                                              | Purpose                                                                                                                                                                                                       |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NATS_URL`, `NATS_CREDS`, `NATS_NKEY_SEED`, `NATS_USER`/`NATS_PASSWORD`, `NATS_TOKEN` | Connection and authentication                                                                                                                                                                                 |
| `NATS_TLS_CA`, `NATS_TLS_CERT`, `NATS_TLS_KEY`                                        | TLS and mutual TLS                                                                                                                                                                                            |
| `HYDRA_STREAMS`                                                                       | Streams holding the definitions, comma-separated (required)                                                                                                                                                   |
| `HYDRA_SUBJECT_PREFIX`                                                                | Prefix of the definitions' subjects: one on `<prefix><subject>` schedules occurrences on `<subject>` (default `schedule.`)                                                                                    |
| `HYDRA_SUBJECTS`                                                                      | Subject filters under the prefix, wildcards allowed; each stream is consumed through those in it (default `<prefix>>`)                                                                                        |
| `HYDRA_STORE`                                                                         | `sqlite:hydra.db` (default of the sqlite build), `redis://host:6379/0`, `redis-cluster://host:7000?addr=host:7001` or `postgres://user:pass@host:5432/db`; redis URLs take `?prefix=`                         |
| `HYDRA_HISTORY_TTL`                                                                   | Delete ended, cancelled and archived schedules older than this (default `P7D`, `0` keeps them)                                                                                                                |
| `HYDRA_CONSUMER_WORKERS`, `HYDRA_CONSUMER_BATCH`                                      | Workers processing definitions at once, at most (in order per schedule; they start with the load and stop when idle), and definitions stored per write (default 16, 2 with SQLite, which has one writer; 256) |
| `HYDRA_FIRE_BATCH`, `HYDRA_CLAIM_CONCURRENCY`                                         | Due schedules fired together, and concurrent claim round trips per batch (default 512, 4)                                                                                                                     |
| `HYDRA_CATCHUP_BATCH`                                                                 | Occurrences a `catch_up` schedule replays per claim (default 100)                                                                                                                                             |
| `HYDRA_WINDOW`, `HYDRA_WINDOW_LIMIT`                                                  | How far ahead, and how many, schedules are held in memory (default `PT1M`, 10000)                                                                                                                             |
| `HYDRA_HTTP_ADDR`                                                                     | Internal port for `/healthz`, `/readyz` and `/metrics` (off by default; `:8080` in the image, used by its `HEALTHCHECK`)                                                                                      |
| `HYDRA_LOG_FORMAT`, `HYDRA_LOG_LEVEL`                                                 | `json` (default) or `text`; `debug` to `error`                                                                                                                                                                |

### Metrics

`/metrics` exposes only what hydra knows and an operator acts on; each answers
an alert or a sizing question. Broker-side figures (consumer lag,
redeliveries, stream rates) come from NATS monitoring (`/jsz`,
`prometheus-nats-exporter`), and CPU and memory from the container platform,
so they are not repeated. No metric is labelled by subject or schedule: that
cardinality grows with the producers.

| Metric                                                                  | Why                                                                                                                                                                                                                               |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hydra_definitions_total{result}`                                       | What producers send and what came of it: `registered`, `replaced`, `duplicate`, `paused`, `resumed`, `cancelled`, `deleted`, `stale`, `conflict`, `unknown`, `invalid`. Alert on the last three: a producer breaking the contract |
| `hydra_messages_ignored_total`                                          | Messages without recurrence under the prefix: someone publishes something else there                                                                                                                                              |
| `hydra_occurrences_published_total`, `hydra_publish_errors_total`       | Throughput, and occurrences JetStream refused (retried)                                                                                                                                                                           |
| `hydra_publish_delay_seconds` (histogram)                               | How late occurrences are published: the scheduler's SLO. Its p99 shows an overloaded instance or store before anything fails                                                                                                      |
| `hydra_deliveries_retried_total`, `hydra_schedules_stalled`             | Delivery retries, and schedules failing ten times in a row (alert)                                                                                                                                                                |
| `hydra_store_errors_total{component}`                                   | Store operations that failed and are retried, while consuming definitions or while scheduling (alert)                                                                                                                             |
| `hydra_occurrences_claimed_total`, `hydra_claims_lost_total`            | Occurrences fired by this instance; lost claims grow while instances rebalance                                                                                                                                                    |
| `hydra_schedules_loaded`, `hydra_catchup_backlog`, `hydra_shards_owned` | Memory held, misfires still replaying, share of the cluster                                                                                                                                                                       |
| `hydra_history_purged_total`                                            | That `HYDRA_HISTORY_TTL` runs                                                                                                                                                                                                     |
| `hydra_nats_connected`, `hydra_nats_reconnects_total`                   | Connection up, and how often it flaps                                                                                                                                                                                             |
| `hydra_streams_down`, `hydra_consumer_restarts_total`                   | Streams not being consumed (alert), and consumers recreated after being lost                                                                                                                                                      |
| `hydra_consumer_workers`                                                | Consumer workers running: idle at 0, at `HYDRA_CONSUMER_WORKERS` under sustained load                                                                                                                                             |
| `hydra_build_info{version}`                                             | Which version runs where, during a rollout                                                                                                                                                                                        |

### Logs

Logs are structured (`slog`, JSON by default) and every entry carries a
`component`. At the default level, `info`, an instance logs only its start,
shard rebalances, history purges, NATS reconnections and recoveries; failures
are logged once per batch, not per message, so an outage does not flood them.

| Level   | What                                                                                                                                                                                                                |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `error` | The store failed (loading, claiming, syncing, storing definitions, purging); the operation is retried                                                                                                               |
| `warn`  | A definition was discarded (invalid, conflict, unknown schedule), publishes failed, a schedule stalled, a stored rule no longer compiles, NATS disconnected, a consumer was lost (deleted, or its stream recreated) |
| `info`  | Started, schedules loaded, consuming a stream, shards rebalanced, history purged, delivery recovered, NATS reconnected                                                                                              |
| `debug` | Each definition applied or ignored, stale changes, redeliveries, each delivery retry                                                                                                                                |

### Images and binaries

There is one binary and one image per store. Each carries only its own store's
code:

| Store                    | Image                                             | Binary (amd64)                              |
| ------------------------ | ------------------------------------------------- | ------------------------------------------- |
| SQLite, single instance  | `ghcr.io/zsweiter/hydra-scheduler:0.2.0-sqlite`   | 10.4 MB, CGO linked statically against musl |
| Redis, Valkey, Dragonfly | `ghcr.io/zsweiter/hydra-scheduler:0.2.0-redis`    | 9.5 MB, pure Go                             |
| PostgreSQL               | `ghcr.io/zsweiter/hydra-scheduler:0.2.0-postgres` | 12.4 MB, pure Go                            |

Images are multi-arch (`linux/amd64`, `linux/arm64`) and built `FROM scratch`.
They run as an unprivileged user with `/data` as working directory, so the
SQLite image keeps its database in `/data/hydra.db` (mount a volume there).
The `:sqlite`, `:redis` and `:postgres` tags follow the latest release.

```bash
docker run -e NATS_URL=nats://nats:4222 -e HYDRA_STREAMS=SCHEDULES \
  -e HYDRA_STORE=redis://valkey:6379/0 ghcr.io/zsweiter/hydra-scheduler:redis
```

Pushing a `v*` tag runs `.github/workflows/release.yml`:

1. It runs the unit tests, then the integration tests against Valkey,
   Dragonfly, PostgreSQL and NATS.
2. It pushes the three images to the GitHub Container Registry.
3. It attaches `hydra-scheduler-<store>-<version>-linux-<arch>.tar.gz` and
   `SHA256SUMS` to the GitHub release.

The first push creates the package as private. Make it public once in its
settings on GitHub.

### Building

Both architectures are cross-compiled with Docker, so arm64 (aarch64) builds
at native speed on amd64 and no local cross toolchain is needed:

```bash
make scheduler                 # bin/hydra-scheduler-{sqlite,redis,postgres} for this machine
make scheduler-redis           # just one (sqlite needs a C compiler)
make scheduler-binaries        # bin/linux_{amd64,arm64}/hydra-scheduler-<store>
make scheduler-images          # hydra-scheduler:0.2.0-<store> and :<store>, multi-arch
make scheduler-images PUSH=1 IMAGE=registry.example.com/hydra-scheduler
make scheduler-images STORES=redis PLATFORMS=linux/arm64
```

`VERSION`, `IMAGE`, `STORES` and `PLATFORMS` can be overridden. The store is
picked with a build tag (`go build -tags redis ./cmd/hydra-scheduler`); a build
without one includes all three. `hydra-scheduler version` prints the version
built in. Loading a multi-arch image locally needs Docker's containerd image
store; without it, push to a registry with `PUSH=1`.

### Operating it

The admin subcommands read the same store and print JSON lines. They can run
next to a live scheduler, for example with
`docker run --rm --env-file … hydra-scheduler …`:

```bash
hydra-scheduler schedules --status active
hydra-scheduler schedules --status finished --status cancelled   # history
hydra-scheduler schedule notification.billing sub-9              # current revision, then the archived ones
```

The admin commands only read. To change a schedule, publish to its stream:

```bash
nats pub notification.billing -H ce-specversion:1.0 -H ce-id:ops-12 -H ce-source:/ops \
  -H ce-type:com.acme.ops -H ce-recurrenceid:sub-9 -H ce-recurrenceaction:pause \
  -H ce-traceparent:00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01 ''
```

Ended, cancelled and archived schedules are kept as history until
`HYDRA_HISTORY_TTL`. They are indexed apart, so they do not slow down
scheduling.

In a container with a memory limit, hydra-scheduler sets the Go runtime's soft
limit to 90% of it (unless `GOMEMLIMIT` is set): the GC then runs less often
while memory is plentiful, and harder only near the limit. Go already sizes
`GOMAXPROCS` to the container's CPUs.

## Architecture

Hydra runs three independent subsystems:

```
┌──────────┐
│  Queue   │  ← public entry point (facade)
└────┬─────┘
     │
┌────▼─────┐  ┌──────────┐  ┌──────────────┐
│ Scheduler│  │Dispatcher│  │  Worker Pool │
│ (idle)   │  │ (idle)   │  │  (elastic)   │
│  ↓       │  │  ↓       │  │   ↓          │
│ heap     │  │ storage  │  │ goroutines   │
│ timer    │  │ batch    │  │ handlers     │
└──────────┘  └──────────┘  └──────────────┘
```

- **Scheduler**: transforms schedules into jobs, firing due ones in batches. Between occurrences it sleeps, waking only to extend its time window (every 30s by default).
- **Dispatcher**: claims jobs from storage and feeds the worker pool. Idle when no jobs exist.
- **Worker Pool**: executes job handlers. Scales up/down via signal (no polling).
- **All actors**: parked on channel selects when idle — zero CPU consumption.

## Stats

```go
stats, _ := q.Stats(ctx)
fmt.Printf("Pending: %d, Scheduled: %d, DLQ: %d, Workers: %d\n",
    stats.ReadyCount["emails"],
    stats.ScheduledCount,
    stats.DLQCount,
    stats.ActiveWorkers,
)
// Scheduler state of this instance
fmt.Println(stats.SchedulesLoaded, stats.CatchUpBacklog, stats.OwnedShards)
fmt.Println(stats.OccurrencesClaimed, stats.ClaimsLost, stats.OccurrencesDelivered, stats.OccurrencesOutboxed)
```

## License

MIT
