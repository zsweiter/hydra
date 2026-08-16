package storage

import (
	"context"
	"errors"
	"time"

	"github.com/zsweiter/hydra/domain"
)

var (
	// ErrNotFound indicates that the requested entity does not exist.
	ErrNotFound = errors.New("storage: not found")

	// ErrAlreadyExists indicates that an entity with the same identity
	// already exists in storage.
	ErrAlreadyExists = errors.New("storage: already exists")

	// ErrConflict indicates that the requested operation conflicts with
	// the current state of the stored entity.
	ErrConflict = errors.New("storage: conflict")

	// ErrInvalid indicates that the requested operation contains invalid
	// or unsupported data.
	ErrInvalid = errors.New("storage: invalid")

	// ErrClosed indicates that the storage is no longer available for
	// read or write operations.
	ErrClosed = errors.New("storage: closed")

	// ErrTimeout indicates that the storage operation could not complete
	// within the allowed time.
	ErrTimeout = errors.New("storage: timeout")

	// ErrUnavailable indicates that the storage backend is temporarily
	// unavailable.
	ErrUnavailable = errors.New("storage: unavailable")
)

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

// Storage is the system of record for hydra.
//
// All runtime state can be reconstructed from a Storage
// implementation. Implementations may leverage backend-specific
// features (e.g. SELECT FOR UPDATE SKIP LOCKED in PostgreSQL,
// WATCH/MULTI/EXEC in Redis) to minimise round trips.
//
// Implementations must be safe for concurrent use.
type Storage interface {
	// SaveJob persists a job.
	//
	// The caller decides the job's delivery status and sets it on the
	// job (see domain.Job.IsDue): StatusScheduled jobs are held for
	// later promotion by PromoteScheduled, while any other status is
	// placed in the ready queue and persisted verbatim. A status other
	// than StatusPending or StatusScheduled is not claimable.
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

	// AckJob marks a job as completed and releases its lease.
	AckJob(ctx context.Context, jobID string) error

	// FailJob handles a job execution failure.
	//
	// The retry or dead-letter outcome is described by decision,
	// which the caller computes from the domain retry policy (see
	// domain.ComputeFailDecision). Storage only persists the result,
	// atomically where the backend allows it.
	FailJob(ctx context.Context, job *domain.Job, failErr error, decision domain.FailDecision) (*FailResult, error)

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

	// SaveSchedule persists a job schedule (create or update).
	SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error

	// DeleteSchedule removes a job schedule permanently.
	// Already-created jobs are unaffected.
	DeleteSchedule(ctx context.Context, scheduleID string) error

	// LoadActiveSchedules returns all schedules whose status
	// is ScheduleActive or SchedulePaused.
	LoadActiveSchedules(ctx context.Context) ([]*domain.JobSchedule, error)

	// UpdateScheduleStatus changes a schedule's status.
	UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error

	// LoadSchedule returns a single schedule by ID.
	LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error)

	// IncrementGeneratedJobs increments the GeneratedJobs
	// counter for a schedule. Returns the new count.
	IncrementGeneratedJobs(ctx context.Context, scheduleID string) (int, error)

	// UpdateNextRunAt updates a schedule's LastRunAt and
	// NextRunAt fields after a job has been generated.
	UpdateNextRunAt(ctx context.Context, scheduleID string, nextRun time.Time, lastRun time.Time) error

	// ListDLQ returns dead-letter entries, optionally
	// filtered by queue.
	ListDLQ(ctx context.Context, queue domain.QueueName) ([]DeadLetter, error)

	// RequeueDLQ moves a dead-letter entry back to the
	// ready queue for reprocessing.
	RequeueDLQ(ctx context.Context, jobID string) error

	// DrainQueue removes all pending and scheduled jobs from the
	// specified queue. Jobs that are currently being processed are
	// not interrupted.
	//
	// Returns the number of jobs removed.
	DrainQueue(ctx context.Context, queue domain.QueueName) (int, error)

	// NextScheduledAt returns the RunAt of the earliest
	// due scheduled job. Returns zero time if no scheduled
	// jobs exist.
	NextScheduledAt(ctx context.Context) (time.Time, error)

	// Stats returns key metrics about the storage state.
	Stats(ctx context.Context) (Stats, error)

	// Close releases any resources held by the storage.
	Close() error
}
