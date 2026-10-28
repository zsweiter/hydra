package storage

import (
	"context"
	"errors"
	"time"

	"github.com/zsweiter/hydra/domain"
)

// ErrStaleVersion is returned by SaveSchedule when the stored
// schedule has a Version greater than or equal to the one
// being saved.
var ErrStaleVersion = errors.New("stale schedule version")

// ScheduleQuery selects schedules for listing, newest update
// first. Empty Statuses and Queue match all.
type ScheduleQuery struct {
	Statuses []domain.ScheduleStatus
	Queue    domain.QueueName
	Limit    int
	Offset   int
}

// JobQuery selects jobs of a schedule for listing, newest
// update first, e.g. the history of its occurrences. Empty
// Statuses match all; a zero Before means now.
type JobQuery struct {
	ScheduleID string
	Statuses   []domain.JobStatus
	Before     time.Time
	Limit      int
}

// DueQuery selects the active schedules due up to Before,
// earliest NextRunAt first (unset first). Nil Shards match all
// shards (see domain.ScheduleShard).
type DueQuery struct {
	Before time.Time
	Shards []int
	Limit  int

	// After, when set, keeps only schedules due strictly after
	// it, leaving out unset NextRunAt: the scheduler extends its
	// window without reloading what it already holds.
	After time.Time

	// ChangedSince, when set, keeps only schedules saved after
	// it (UpdatedAt > ChangedSince). Instances sharing a storage
	// use it to pick up what others changed without reloading
	// their whole window.
	ChangedSince time.Time
}

// Claim is one occurrence claimed by ClaimOccurrences.
type Claim struct {
	ScheduleID string

	// Expected is the NextRunAt the claim must find stored, and
	// Next the one it stores (zero when the schedule ends).
	Expected time.Time
	Next     time.Time

	// Generated is how many occurrences the claim accounts for
	// towards MaxJobs. It may exceed len(Jobs): occurrences
	// delivered directly (see hydra.Queue.HandleDirect) are
	// claimed without storing a job.
	Generated int

	// Jobs are stored with the claim, atomically.
	Jobs []*domain.Job
}

// ClaimOne claims a single occurrence through ClaimOccurrences.
func ClaimOne(ctx context.Context, s ScheduleStore, scheduleID string, expected, next time.Time, jobs []*domain.Job) (bool, error) {
	ok, err := s.ClaimOccurrences(ctx, []Claim{{
		ScheduleID: scheduleID,
		Expected:   expected,
		Next:       next,
		Generated:  len(jobs),
		Jobs:       jobs,
	}})
	if err != nil {
		return false, err
	}

	return ok[0], nil
}

// LeasedJob represents a job that has been claimed
// by a worker with a lease.
type LeasedJob struct {
	Job *domain.Job
}

// FailAction indicates what action was taken after
// a job execution failure (either retry or dead-letter).
type FailAction int

const (
	FailActionRetry FailAction = iota
	FailActionDeadLetter
)

// FailResult contains the outcome of a failed job execution.
type FailResult struct {
	Action    FailAction
	NextRunAt time.Time
}

// DeadLetter holds a permanently failed job record.
type DeadLetter struct {
	ID       string
	Queue    domain.QueueName
	Payload  []byte
	Error    string
	FailedAt time.Time
	Attempts int
}

// Stats contains key metrics about the storage state.
type Stats struct {
	ReadyCount     map[domain.QueueName]int64
	ScheduledCount int64
	DLQCount       int64
	LeaseCount     int64
}

// ScheduleStore is what the scheduler needs: schedules, their
// occurrence claims, history and the membership of instances
// sharing the storage. hydra-scheduler uses only this.
//
// Implementations must be safe for concurrent use.
type ScheduleStore interface {
	// SaveSchedule persists a job schedule (create or update).
	//
	// When schedule.Version is non-zero and the stored schedule
	// has a Version greater than or equal to it, nothing is
	// written and ErrStaleVersion is returned.
	SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error

	// SaveSchedules persists several schedules in one round
	// trip, each with the semantics of SaveSchedule, and returns
	// one error per schedule (nil, or ErrStaleVersion). The
	// returned error reports a failure of the whole call;
	// schedules are not saved all or nothing.
	SaveSchedules(ctx context.Context, schedules []*domain.JobSchedule) ([]error, error)

	// DeleteSchedule removes a job schedule permanently.
	// Already-created jobs are unaffected.
	DeleteSchedule(ctx context.Context, scheduleID string) error

	// LoadDueSchedules returns the active schedules matching q,
	// without Data and Metadata: the scheduler keeps a time
	// window of schedules in memory and loads their payload only
	// when they fire (LoadSchedules). Implementations index
	// active schedules by NextRunAt and shard.
	LoadDueSchedules(ctx context.Context, q DueQuery) ([]*domain.JobSchedule, error)

	// LoadSchedules returns the schedules with the given IDs,
	// in one round trip. Missing IDs are left out.
	LoadSchedules(ctx context.Context, ids []string) ([]*domain.JobSchedule, error)

	// LoadSchedule returns a single schedule by ID.
	LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error)

	// UpdateScheduleStatus changes a schedule's status.
	UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error

	// ClaimOccurrences claims occurrences of active schedules.
	// Each claim is atomic on its own: it moves NextRunAt from
	// Expected to Next, sets LastRunAt, adds Generated to
	// GeneratedJobs and persists Jobs. A zero Next, or reaching
	// MaxJobs, marks the schedule finished. Claims are batched
	// to save round trips, not applied all or nothing.
	//
	// A claim reports false, changing nothing, when its schedule
	// is no longer active or NextRunAt is no longer Expected:
	// another instance sharing the storage claimed the
	// occurrence first. An unset NextRunAt is claimable.
	// Implementations compare times at the precision they
	// persist them with.
	ClaimOccurrences(ctx context.Context, claims []Claim) ([]bool, error)

	// ListSchedules returns schedules matching q, including
	// finished and cancelled ones kept as history.
	// Implementations index by status so history does not
	// slow down LoadDueSchedules.
	ListSchedules(ctx context.Context, q ScheduleQuery) ([]*domain.JobSchedule, error)

	// PurgeHistory deletes up to limit completed jobs and up to
	// limit finished, cancelled or archived schedules last
	// updated before the given time, and returns how many it
	// deleted. Callers repeat it while it returns limit or more.
	// Dead letters are kept until requeued.
	PurgeHistory(ctx context.Context, before time.Time, limit int) (int, error)

	// Heartbeat records instanceID as alive until now+ttl, or
	// removes it when ttl <= 0, and returns the IDs of all live
	// instances sorted. Instances use it to split the shards.
	Heartbeat(ctx context.Context, instanceID string, ttl time.Duration) ([]string, error)

	// Close releases any resources held by the storage.
	Close() error
}

// JobStore is what the job queue needs: jobs, their leases,
// retries and dead letters, and queue pauses.
//
// Implementations must be safe for concurrent use.
type JobStore interface {
	// SaveJob persists a job. If RunAt <= now the job
	// is immediately available for claiming; otherwise
	// it is scheduled for future promotion.
	SaveJob(ctx context.Context, job *domain.Job) error

	// RemoveJob deletes a pending or scheduled job by ID.
	// Returns an error if the job is currently processing.
	RemoveJob(ctx context.Context, jobID string) error

	// ClaimJobs claims up to n pending jobs for the given
	// queue, assigning them a lease with the specified TTL.
	//
	// Returns the claimed jobs or an empty slice if no
	// pending jobs are available.
	ClaimJobs(ctx context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]LeasedJob, error)

	// AckJobs marks jobs as completed and releases their
	// leases, in one round trip. Completed jobs are kept as
	// history (see ListJobs) until PurgeHistory removes them.
	AckJobs(ctx context.Context, jobIDs []string) error

	// ExtendLease pushes the lease of a job being processed to
	// now+ttl, so long-running jobs are not recovered as
	// orphans while still running.
	ExtendLease(ctx context.Context, jobID string, ttl time.Duration) error

	// FailJob handles a job execution failure.
	//
	// If the job is retryable, it calculates the backoff
	// delay and reschedules the job. If max attempts have
	// been exceeded, the job moves to the dead-letter queue.
	FailJob(ctx context.Context, job *domain.Job, failErr error) (FailResult, error)

	// ReadyQueues returns a list of queue names that have
	// at least one ready (pending) job.
	ReadyQueues(ctx context.Context) ([]domain.QueueName, error)

	// PromoteScheduled moves due scheduled jobs (those
	// whose RunAt <= now) into the ready queue, up to the
	// specified limit.
	//
	// Returns the number of jobs promoted.
	PromoteScheduled(ctx context.Context, now time.Time, limit int) (int, error)

	// RecoverOrphaned finds jobs whose leases have expired
	// and returns them to the pending state (ready queue).
	//
	// Returns the number of jobs recovered.
	RecoverOrphaned(ctx context.Context, before time.Time) (int, error)

	// ListJobs returns the jobs of a schedule matching q.
	ListJobs(ctx context.Context, q JobQuery) ([]*domain.Job, error)

	// SetQueuePaused persists whether a queue is paused, so
	// every instance sharing the storage honours it.
	SetQueuePaused(ctx context.Context, queue domain.QueueName, paused bool) error

	// PausedQueues returns the queues currently paused.
	PausedQueues(ctx context.Context) ([]domain.QueueName, error)

	// ListDLQ returns dead-letter entries, optionally
	// filtered by queue.
	ListDLQ(ctx context.Context, queue domain.QueueName) ([]DeadLetter, error)

	// RequeueDLQ moves a dead-letter entry back to the
	// ready queue for reprocessing.
	RequeueDLQ(ctx context.Context, jobID string) error

	// NextScheduledAt returns the RunAt of the earliest
	// due scheduled job. Returns zero time if no scheduled
	// jobs exist.
	NextScheduledAt(ctx context.Context) (time.Time, error)

	// Stats returns key metrics about the storage state.
	Stats(ctx context.Context) (Stats, error)
}

// Storage is the system of record for hydra: the scheduler's
// schedules and the queue's jobs. All runtime state can be
// reconstructed from it. Implementations may leverage
// backend-specific features (e.g. SELECT FOR UPDATE SKIP LOCKED
// in PostgreSQL, Lua scripts in Redis) to minimise round trips.
type Storage interface {
	ScheduleStore
	JobStore
}

// SaveOne saves a single schedule through SaveSchedules.
func SaveOne(ctx context.Context, s ScheduleStore, schedule *domain.JobSchedule) error {
	errs, err := s.SaveSchedules(ctx, []*domain.JobSchedule{schedule})
	if err != nil {
		return err
	}

	return errs[0]
}
