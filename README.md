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
- Pluggable storage (in-memory default, SQLite/PostgreSQL/Redis soon)
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
    MinWorkers    int           // default: 1
    MaxWorkers    int           // default: 16
    ScaleDownIdle time.Duration // default: 10s
    BatchSize     int           // default: 10
    LeaseTTL      time.Duration // default: 30s
    MaxRetries    int           // default: 3
    Logger        logger.Logger
    Storage       storage.Storage // default: in-memory
}
```

## Storage

Hydra abstracts persistence behind the `storage.Storage` interface.
The default is in-memory (suitable for development and testing).

To use a different backend, implement the interface and inject it:

```go
type Storage interface {
    SaveJob(ctx, *Job) error
    ClaimJobs(ctx, QueueName, workerID string, n int, ttl time.Duration) ([]LeasedJob, error)
    AckJob(ctx, jobID string) error
    FailJob(ctx, *Job, error) (FailResult, error)
    SaveSchedule(ctx, *JobSchedule) error
    DeleteSchedule(ctx, scheduleID string) error
    LoadActiveSchedules(ctx) ([]*JobSchedule, error)
    // ... see package docs for full interface
}
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
