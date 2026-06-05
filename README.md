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
- Pluggable storage (in-memory default, SQLite, Redis)
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

### Pause / Resume / Remove

```go
q.PauseSchedule(ctx, "tasks", scheduleID)
q.ResumeSchedule(ctx, "tasks", scheduleID)
q.RemoveSchedule(ctx, "tasks", scheduleID)
```

## Workers

### Register a worker

```go
q.Handle("emails", func(ctx context.Context, job *hydra.Job) error {
    return sendEmail(job)
})
```

Multiple workers can be registered for the same queue;
jobs are distributed in round-robin fashion.

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
    Logger           logger.Logger
    Storage          storage.Storage // default: in-memory
}
```

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
    AckJob(ctx context.Context, jobID string) error
    FailJob(ctx context.Context, job *domain.Job, failErr error) (FailResult, error)

    // Queue management
    ReadyQueues(ctx context.Context) ([]domain.QueueName, error)
    PromoteScheduled(ctx context.Context, now time.Time, limit int) (int, error)
    RecoverOrphaned(ctx context.Context, before time.Time) (int, error)

    // Schedules
    SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error
    DeleteSchedule(ctx context.Context, scheduleID string) error
    LoadActiveSchedules(ctx context.Context) ([]*domain.JobSchedule, error)
    LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error)
    UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error
    IncrementGeneratedJobs(ctx context.Context, scheduleID string) (int, error)
    UpdateNextRunAt(ctx context.Context, scheduleID string, nextRun, lastRun time.Time) error

    // Dead-letter
    ListDLQ(ctx context.Context, queue domain.QueueName) ([]DeadLetter, error)
    RequeueDLQ(ctx context.Context, jobID string) error

    // Metadata
    NextScheduledAt(ctx context.Context) (time.Time, error)
    Stats(ctx context.Context) (Stats, error)
    Close() error
}
```

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

### Redis

Uses `go-redis/v9`. Accepts any `redis.UniversalClient` (single, cluster, sentinel).

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

With cluster or sentinel:

```go
rdb := ioredis.NewClusterClient(&ioredis.ClusterOptions{Addrs: []string{":6379"}})
store := redis.Use(rdb)
```

### Custom backend

Implement the `storage.Storage` interface and pass it to `hydra.Config`:

```go
type MyStorage struct{ /* ... */ }

func (s *MyStorage) SaveJob(ctx context.Context, job *domain.Job) error { /* ... */ }
// ... implement all 17 methods

q := hydra.New(hydra.Config{Storage: &MyStorage{}})
```

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

- **Scheduler**: transforms schedules into jobs. Idle when no schedules exist.
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
```

## License

MIT
