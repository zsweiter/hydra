// Package domain defines the core domain model shared by every
// component of Hydra.
//
// The domain package is intentionally free of I/O and infrastructure
// concerns. It contains only the types and value objects that describe
// the problem space:
//
//   - ID            - a ULID-based identifier.
//   - QueueName     - a named queue.
//   - Job           - a single unit of executable work.
//   - JobSchedule   - a recurring template that generates jobs.
//   - ScheduleRule  - the recurrence definition used by a schedule.
//
// Because the domain model depends on nothing but the standard library
// and the ULID library, it can be reused by storage backends, the
// scheduler, the dispatcher, and the worker pool without introducing
// import cycles.
//
// # Identifiers
//
// Hydra identifies every entity with ID, a ULID underneath.
// ULIDs are lexicographically sortable, which means creation order and
// sort order agree, and they are practically unique without requiring
// coordination between processes.
//
// ID supports database interoperability through the driver.Valuer and
// sql.Scanner interfaces, and JSON serialization through the
// json.Marshaler and json.Unmarshaler interfaces.
//
// Constructors:
//
//	domain.NewID()         // generate a new ID (returns an error)
//	domain.MustNewID()     // generate a new ID (panics on failure)
//	domain.ParseID(s)      // parse an encoded ULID string
//	domain.MustParseID(s)  // parse an encoded ULID string (panics on failure)
//
// Use NewZero and IsZero to represent and detect the absence of an ID.
//
// # Queues
//
// QueueName identifies a logical stream of jobs. Workers register for a
// specific queue name, and submitted jobs are routed to the worker that
// owns that queue.
//
// QueueName provides deterministic hashing primitives (Hash, Shard, Bit)
// used to distribute queues across shards or worker slots.
//
// # Jobs
//
// A Job is a single unit of work: a payload (Data), an owner (Queue),
// and metadata describing how and when it should run.
//
// Every job carries:
//
//   - Status        - the lifecycle state (see JobStatus).
//   - Attempts      - how many times execution has been attempted.
//   - MaxAttempts   - the retry budget.
//   - Backoff       - the delay policy applied between retries.
//   - RunAt         - the earliest time the job may be picked up.
//   - IdempotencyKey- a caller-supplied key to deduplicate submissions.
//
// Job lifecycle:
//
//	pending -> processing -> completed
//	    |            |
//	    |            +----> failed -> dead_letter
//	    +----------------------^
//
// A job is retryable while Attempts < MaxAttempts (see IsRetryable).
//
// # Backoff
//
// BackoffPolicy controls the delay between retries. Two strategies are
// supported:
//
//   - BackoffFixed: constant delay equal to BaseMs.
//   - BackoffExponential: BaseMs * 2^(attempt-1), capped at MaxMs.
//
// NextDelay returns the delay to apply before the given attempt number.
// An unknown policy type yields a zero delay.
//
// # Schedules
//
// A JobSchedule never executes work itself. It is a persistent template
// that the scheduler turns into concrete Job instances according to a
// recurrence rule.
//
// Schedule status (see ScheduleStatus):
//
//	active -> paused -> active
//	active -> finished
//
// A schedule is finished when:
//
//   - its status is ScheduleFinished, or
//   - it generated MaxJobs jobs (MaxJobs 0 means unlimited), or
//   - its EndAt boundary has passed.
//
// See ShouldFinish and CanGenerate.
//
// # Misfire Policies
//
// When the scheduler is offline and occurrences are missed, the
// MisfirePolicy decides what to do:
//
//   - MisfireSkip: ignore every missed occurrence.
//   - MisfireRunOnce: run a single job for the most recent occurrence.
//   - MisfireCatchUp: run one job per missed occurrence.
//
// # Schedule Rules
//
// ScheduleRule describes one recurrence syntax. Exactly one model is
// active at a time:
//
//   - cron expression stored in Expression.
//   - RFC5545 RRULE string stored in Expression.
//   - fixed interval stored in IntervalNanos (or IntervalMs).
//
// Use the NewCronScheduleRule, NewRRuleScheduleRule and
// NewIntervalScheduleRule constructors, then Validate before persisting.
//
// Constructors:
//
//	domain.NewJobSchedule(name, payload, rule)
//	domain.NewCronScheduleRule("0 * * * *")
//	domain.NewRRuleScheduleRule("FREQ=DAILY;COUNT=10")
//	domain.NewIntervalScheduleRule(5 * time.Minute)
//
// # Thread Safety
//
// The types in this package are plain data structures. Concurrent
// mutation of a single Job or JobSchedule value is not safe and must be
// coordinated by the caller (for example by the scheduler and worker
// pool). The package-level entropy source used by ID generation is
// internally synchronized and safe for concurrent use.
package domain
