package hydra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/dispatcher"
	"github.com/zsweiter/hydra/internal/scheduler"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/internal/worker"
	"github.com/zsweiter/hydra/pkg/logger"
	"github.com/zsweiter/hydra/storage"
)

// ──────────────────────────────────────────────
//  Public types
// ──────────────────────────────────────────────

type (
	Job            = domain.Job
	JobSchedule    = domain.JobSchedule
	QueueName      = domain.QueueName
	ScheduleRule   = domain.ScheduleRule
	ScheduleType   = domain.ScheduleType
	BackoffPolicy  = domain.BackoffPolicy
	BackoffType    = domain.BackoffType
	JobStatus      = domain.JobStatus
	ScheduleStatus = domain.ScheduleStatus
	MisfirePolicy  = domain.MisfirePolicy
)

// ──────────────────────────────────────────────
//  Job status
// ──────────────────────────────────────────────

const (
	StatusPending    = domain.StatusPending
	StatusProcessing = domain.StatusProcessing
	StatusCompleted  = domain.StatusCompleted
	StatusFailed     = domain.StatusFailed
	StatusDeadLetter = domain.StatusDeadLetter
)

// ──────────────────────────────────────────────
//  Schedule status
// ──────────────────────────────────────────────

const (
	ScheduleActive   = domain.ScheduleActive
	SchedulePaused   = domain.SchedulePaused
	ScheduleFinished = domain.ScheduleFinished
)

// ──────────────────────────────────────────────
//  Misfire policy
// ──────────────────────────────────────────────

const (
	MisfireSkip    = domain.MisfireSkip
	MisfireRunOnce = domain.MisfireRunOnce
	MisfireCatchUp = domain.MisfireCatchUp
)

// ──────────────────────────────────────────────
//  Backoff
// ──────────────────────────────────────────────

const (
	BackoffFixed       = domain.BackoffFixed
	BackoffExponential = domain.BackoffExponential
)

// ──────────────────────────────────────────────
//  Schedule type
// ──────────────────────────────────────────────

const (
	ScheduleTypeCron     = domain.ScheduleCron
	ScheduleTypeRRule    = domain.ScheduleRRule
	ScheduleTypeInterval = domain.ScheduleInterval
)

// ──────────────────────────────────────────────
//  Constructors
// ──────────────────────────────────────────────

// NewJob creates a new Job with a cryptographically secure, automatically
// generated ID.
//
// NewJob panics if the system cannot provide a secure source of entropy.
// This is an extremely rare edge case and generally indicates a serious
// system-level failure.
func NewJob(name string, data []byte, metadata map[string]string) Job {
	return domain.NewJob(domain.MustNewID().String(), name, data, metadata)
}

// NewJobWithID creates a new Job with the given ID.
func NewJobWithID(id, name string, data []byte, metadata map[string]string) Job {
	return domain.NewJob(id, name, data, metadata)
}

// NewJobSchedule creates a new recurring job schedule with a cryptographically
// secure, automatically generated ID.
//
// NewJobSchedule panics if the system cannot provide a secure source of
// entropy. This is an extremely rare edge case and generally indicates a
// serious system-level failure.
func NewJobSchedule(name string, data []byte, rule ScheduleRule) JobSchedule {
	return domain.NewJobSchedule(domain.MustNewID().String(), name, data, rule)
}

// NewJobScheduleWithID creates a new recurring job schedule with the given ID.
func NewJobScheduleWithID(id, name string, data []byte, rule ScheduleRule) JobSchedule {
	return domain.NewJobSchedule(id, name, data, rule)
}

// CronRule creates a cron-based recurrence rule.
func CronRule(expr string) ScheduleRule {
	return domain.NewCronScheduleRule(expr)
}

// RRuleRule creates an RFC 5545 RRULE recurrence rule.
func RRuleRule(expr string) ScheduleRule {
	return domain.NewRRuleScheduleRule(expr)
}

// IntervalRule creates a fixed-interval recurrence rule.
func IntervalRule(d time.Duration) ScheduleRule {
	return domain.NewIntervalScheduleRule(d)
}

// DefaultBackoff returns the default exponential backoff policy
// (100ms base, 10s max).
func DefaultBackoff() BackoffPolicy {
	return domain.DefaultBackoff()
}

// Worker is a function that processes a job.
type Worker = func(ctx context.Context, job *Job) error

type Processor interface {
	Handle(ctx context.Context, job *Job) error
}

// Config holds all configuration for a hydra instance.
type Config struct {
	MaxRetries int

	// Config for elastic worker pool
	MinWorkers       int
	MaxWorkers       int
	ScaleUpThreshold float64 // channel occupancy (0.0–1.0) to trigger scale-up; default 0.25
	ScaleDownIdle    time.Duration

	BatchSize int
	LeaseTTL  time.Duration

	Logger  logger.Logger
	Storage storage.Storage
}

// Producer defines the public API for submitting and managing jobs.
//
// Implementations are responsible for persisting jobs and applying the
// queue's delivery semantics, including job state transitions.
type Producer interface {
	// Submit adds a new job to the specified queue.
	//
	// The job is persisted and becomes eligible for processing according
	// to the queue's configuration. Implementations should return an error
	// if the job cannot be accepted or persisted.
	Submit(
		ctx context.Context,
		queue QueueName,
		job Job,
	) error

	// RemoveJob removes the specified job from the queue.
	//
	// Removing a job that is currently being processed must not silently
	// interrupt its execution. Implementations should return an error when
	// the job does not exist or cannot be removed.
	RemoveJob(
		ctx context.Context,
		queue QueueName,
		jobID string,
	) error

	// RetryJob requeues a failed job so that it can be processed again.
	//
	// The job is transitioned back to a pending state and becomes eligible
	// for processing according to the queue's normal dispatch rules.
	RetryJob(
		ctx context.Context,
		queue QueueName,
		jobID string,
	) error
}

// Scheduler defines the public API for managing recurring job schedules.
//
// A schedule produces jobs according to its configured recurrence and is
// associated with a specific queue.
type Scheduler interface {
	// UpsertSchedule creates a new schedule or updates an existing schedule
	// identified by its schedule ID.
	//
	// Updating a schedule replaces its scheduling configuration while
	// preserving its identity within the queue.
	UpsertSchedule(
		ctx context.Context,
		queue QueueName,
		schedule JobSchedule,
	) error

	// RemoveSchedule permanently removes a recurring schedule.
	//
	// Removing a schedule prevents it from producing any future jobs but
	// does not affect jobs that have already been created by the schedule.
	RemoveSchedule(
		ctx context.Context,
		queue QueueName,
		scheduleID string,
	) error

	// PauseSchedule temporarily suspends a recurring schedule.
	//
	// A paused schedule remains registered and retains its configuration,
	// but does not produce new jobs until it is resumed.
	PauseSchedule(
		ctx context.Context,
		queue QueueName,
		scheduleID string,
	) error

	// ResumeSchedule resumes a previously paused schedule.
	//
	// The schedule continues producing jobs according to its configured
	// recurrence without creating a new schedule identity.
	ResumeSchedule(
		ctx context.Context,
		queue QueueName,
		scheduleID string,
	) error
}

// Consumer defines the public API for registering job processors.
//
// A consumer associates a processor with a queue. Once the queue system
// is running, eligible jobs from that queue can be dispatched to the
// registered processor.
type Consumer interface {
	// Process registers a processor for the specified queue.
	//
	// The processor is responsible for executing jobs delivered from the
	// queue. Registering a processor does not start the queue system;
	// processing begins when the Queue is started.
	Process(queue QueueName, processor Processor)
}

// Queue is the main public entry point for Hydra.
//
// It combines job production, scheduling, consumption, and queue lifecycle
// management. Internally, it coordinates the scheduler, dispatcher, and
// worker pool while exposing a single API to applications.
type Queue interface {
	Producer
	Scheduler
	Consumer

	// Start starts the queue system and begins dispatching eligible jobs
	// to registered processors.
	//
	// Start blocks until the context is cancelled or the queue system
	// encounters a fatal error.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the queue system.
	//
	// The implementation should stop accepting new work, allow in-flight
	// processing to finish when possible, and release the resources owned
	// by the queue system.
	Stop(ctx context.Context) error

	// Pause temporarily stops processing new jobs from the specified queue.
	//
	// Jobs that are already being processed are not interrupted. Pending
	// jobs remain in the queue and become eligible for processing again
	// when the queue is resumed.
	Pause(ctx context.Context, queue QueueName) error

	// Resume resumes processing jobs from a previously paused queue.
	//
	// Pending jobs become eligible for dispatch according to the queue's
	// normal processing and scheduling rules.
	Resume(ctx context.Context, queue QueueName) error

	// Drain removes pending jobs from the specified queue.
	//
	// Jobs that are already being processed are not interrupted. Draining
	// a queue does not remove schedules associated with it.
	Drain(ctx context.Context, queue QueueName) error

	// Stats returns a point-in-time snapshot of the all queues.
	//
	// The snapshot may include information such as pending, processing,
	// completed, failed, and scheduled jobs, depending on the configured
	// storage and metrics implementation.
	Stats(ctx context.Context) (StatsSnapshot, error)
}

type queue struct {
	storage    storage.Storage
	scheduler  *scheduler.Scheduler
	dispatcher *dispatcher.Dispatcher
	pool       *worker.Pool
	registry   *worker.Registry
	isRunning  atomic.Bool
	paused     sync.Map
	stopFunc   context.CancelFunc
	done       chan struct{}
	cfg        Config
	wg         sync.WaitGroup
}

//go:inline
func (q *queue) IsRunning() bool {
	return q.isRunning.Load()
}

// New creates a new hydra queue instance.
func New(cfg Config) *queue {
	if cfg.Storage == nil {
		cfg.Storage = memory.New()
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.DefaultLogger
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.MinWorkers <= 0 {
		cfg.MinWorkers = 1
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = 16
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.ScaleDownIdle <= 0 {
		cfg.ScaleDownIdle = 10 * time.Second
	}
	if cfg.ScaleUpThreshold <= 0 {
		cfg.ScaleUpThreshold = 0.25
	}

	registry := worker.NewRegistry()

	disp := dispatcher.New(cfg.Storage, nil, cfg.BatchSize, cfg.LeaseTTL)

	pool := worker.NewPool(registry, cfg.Storage, worker.Config{
		MinWorkers:       cfg.MinWorkers,
		MaxWorkers:       cfg.MaxWorkers,
		LeaseTTL:         cfg.LeaseTTL,
		ScaleDownIdle:    cfg.ScaleDownIdle,
		ScaleUpThreshold: cfg.ScaleUpThreshold,
		OnRetry:          func(_ time.Time) { disp.Signal() },
	})

	disp.SetJobChan(pool.JobChan())

	sched := scheduler.New(cfg.Storage, func() { disp.Signal() })

	return &queue{
		storage:    cfg.Storage,
		scheduler:  sched,
		dispatcher: disp,
		pool:       pool,
		registry:   registry,
		cfg:        cfg,
		done:       make(chan struct{}),
	}
}

func (q *queue) Submit(ctx context.Context, queueName QueueName, job Job) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	job.Queue = queueName
	now := time.Now().UTC()
	if job.RunAt.IsZero() {
		job.RunAt = now
	}
	if job.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return fmt.Errorf("generate id: %w", err)
		}
		job.ID = id.String()
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = q.cfg.MaxRetries
	}
	if job.Backoff.Type == 0 && job.Backoff.BaseMs == 0 {
		job.Backoff = DefaultBackoff()
	}
	if job.IsDue(now) {
		job.Status = StatusPending
	} else {
		job.Status = domain.StatusScheduled
	}

	if err := q.storage.SaveJob(ctx, &job); err != nil {
		return fmt.Errorf("persist job: %w", err)
	}

	q.dispatcher.Signal()
	return nil
}

func (q *queue) UpsertSchedule(ctx context.Context, queueName QueueName, schedule JobSchedule) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	schedule.Queue = queueName
	if schedule.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return fmt.Errorf("generate id: %w", err)
		}
		schedule.ID = id.String()
	}

	if err := schedule.Rule.Validate(); err != nil {
		return fmt.Errorf("validate rule: %w", err)
	}

	if err := q.storage.SaveSchedule(ctx, &schedule); err != nil {
		return fmt.Errorf("persist schedule: %w", err)
	}

	q.scheduler.Notify(scheduler.Event{
		Type:       scheduler.EventScheduleAdded,
		ScheduleID: schedule.ID,
	})
	q.scheduler.Signal()

	return nil
}

func (q *queue) RemoveSchedule(ctx context.Context, queueName QueueName, scheduleID string) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	if err := q.storage.DeleteSchedule(ctx, scheduleID); err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}

	q.scheduler.Notify(scheduler.Event{
		Type:       scheduler.EventScheduleRemoved,
		ScheduleID: scheduleID,
	})
	q.scheduler.Signal()

	return nil
}

func (q *queue) RemoveJob(ctx context.Context, queueName QueueName, jobID string) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	return q.storage.RemoveJob(ctx, jobID)
}

func (q *queue) RetryJob(ctx context.Context, queueName QueueName, jobID string) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	if err := q.storage.RequeueDLQ(ctx, jobID); err != nil {
		return fmt.Errorf("requeue job: %w", err)
	}

	q.dispatcher.Signal()
	return nil
}

func (q *queue) PauseSchedule(ctx context.Context, queueName QueueName, scheduleID string) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	if err := q.storage.UpdateScheduleStatus(ctx, scheduleID, SchedulePaused); err != nil {
		return fmt.Errorf("pause schedule: %w", err)
	}

	q.scheduler.Notify(scheduler.Event{
		Type:       scheduler.EventSchedulePaused,
		ScheduleID: scheduleID,
	})
	q.scheduler.Signal()

	return nil
}

func (q *queue) ResumeSchedule(ctx context.Context, queueName QueueName, scheduleID string) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	if err := q.storage.UpdateScheduleStatus(ctx, scheduleID, ScheduleActive); err != nil {
		return fmt.Errorf("resume schedule: %w", err)
	}

	q.scheduler.Notify(scheduler.Event{
		Type:       scheduler.EventScheduleResumed,
		ScheduleID: scheduleID,
	})
	q.scheduler.Signal()

	return nil
}

func (q *queue) Handle(queueName QueueName, worker Worker) {
	q.registry.Register(queueName, worker)
}

func (q *queue) Process(queueName QueueName, processor Processor) {
	q.registry.Register(queueName, processor.Handle)
}

func (q *queue) Pause(_ context.Context, queueName QueueName) error {
	q.paused.Store(queueName, true)
	q.dispatcher.PauseQueue(queueName)
	return nil
}

func (q *queue) Resume(_ context.Context, queueName QueueName) error {
	q.paused.Store(queueName, false)
	q.dispatcher.ResumeQueue(queueName)
	q.dispatcher.Signal()
	return nil
}

func (q *queue) Drain(ctx context.Context, queueName QueueName) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	if _, err := q.storage.DrainQueue(ctx, queueName); err != nil {
		return fmt.Errorf("drain queue: %w", err)
	}

	return nil
}

func (q *queue) Start(ctx context.Context) error {
	if q.IsRunning() {
		return ErrQueueAlreadyRunning
	}

	ctx, cancel := context.WithCancel(ctx)
	q.stopFunc = cancel

	recovered, err := q.storage.RecoverOrphaned(ctx, time.Now().UTC())
	if err != nil {
		cancel()
		return fmt.Errorf("recover orphans: %w", err)
	}
	if recovered > 0 {
		q.dispatcher.Signal()
	}

	if err := q.scheduler.Load(ctx); err != nil {
		cancel()
		return fmt.Errorf("load schedules: %w", err)
	}

	if err := q.pool.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("start pool: %w", err)
	}

	q.wg.Go(func() {
		if err := q.dispatcher.Run(ctx); err != nil && err != context.Canceled {
			q.cfg.Logger.Error(err, logger.Fields{"component": "dispatcher"})
		}
	})

	q.wg.Go(func() {
		if err := q.scheduler.Run(ctx); err != nil && err != context.Canceled {
			q.cfg.Logger.Error(err, logger.Fields{"component": "scheduler"})
		}
	})

	q.isRunning.Store(true)
	close(q.done)
	return nil
}

func (q *queue) Stop(ctx context.Context) error {
	if !q.IsRunning() {
		return ErrQueueNotRunning
	}

	q.isRunning.Store(false)

	if err := q.pool.Stop(ctx); err != nil {
		return fmt.Errorf("stop pool: %w", err)
	}

	q.stopFunc()

	q.wg.Wait()

	if err := q.storage.Close(); err != nil {
		return fmt.Errorf("close storage: %w", err)
	}

	return nil
}

func (q *queue) Stats(ctx context.Context) (StatsSnapshot, error) {
	s, err := q.storage.Stats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}

	return StatsSnapshot{
		ReadyCount:     s.ReadyCount,
		ScheduledCount: s.ScheduledCount,
		DLQCount:       s.DLQCount,
		ActiveWorkers:  q.pool.ActiveWorkers(),
	}, nil
}

var _ Queue = (*queue)(nil)

// StatsSnapshot contains a point-in-time view of queue metrics.
type StatsSnapshot struct {
	ReadyCount     map[QueueName]int64
	ScheduledCount int64
	DLQCount       int64
	ActiveWorkers  uint64
}
