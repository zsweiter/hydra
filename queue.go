package hydra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/internal/dispatcher"
	"github.com/zsweiter/hydra/internal/domain"
	"github.com/zsweiter/hydra/internal/scheduler"
	"github.com/zsweiter/hydra/internal/storage"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/internal/worker"
	"github.com/zsweiter/hydra/pkg/logger"
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

// NewJob creates a new Job with the given identity, name,
// payload and metadata. If id is empty, an ULID is generated
// automatically when the job is submitted.
func NewJob(id, name string, data []byte, metadata map[string]string) Job {
	return domain.NewJob(id, name, data, metadata)
}

// NewJobSchedule creates a new recurring job schedule.
// If id is empty, the ID is generated automatically on Upsert.
func NewJobSchedule(id, name string, data []byte, rule ScheduleRule) JobSchedule {
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

// Config holds all configuration for a hydra instance.
type Config struct {
	MaxRetries int

	MinWorkers    int
	MaxWorkers    int
	ScaleDownIdle time.Duration

	BatchSize int
	LeaseTTL  time.Duration

	Logger  logger.Logger
	Storage storage.Storage
}

// Producer is the public API for submitting and managing jobs.
type Producer interface {
	Submit(
		ctx context.Context,
		queue QueueName,
		job Job,
	) error

	RemoveJob(
		ctx context.Context,
		queue QueueName,
		jobID string,
	) error

	UpsertSchedule(
		ctx context.Context,
		queue QueueName,
		schedule JobSchedule,
	) error

	RemoveSchedule(
		ctx context.Context,
		queue QueueName,
		scheduleID string,
	) error

	PauseSchedule(
		ctx context.Context,
		queue QueueName,
		scheduleID string,
	) error

	ResumeSchedule(
		ctx context.Context,
		queue QueueName,
		scheduleID string,
	) error
}

// Consumer is the public API for registering workers.
type Consumer interface {
	Handle(queue QueueName, worker Worker)
}

// Queue is the single public entry point for hydra.
//
// It coordinates the Scheduler, Dispatcher, and Worker Pool.
type Queue interface {
	Producer
	Consumer

	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	Pause(ctx context.Context, queue QueueName) error
	Resume(ctx context.Context, queue QueueName) error

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

	registry := worker.NewRegistry()

	disp := dispatcher.New(cfg.Storage, nil, cfg.BatchSize, cfg.LeaseTTL)

	pool := worker.NewPool(registry, cfg.Storage, worker.Config{
		MinWorkers:    cfg.MinWorkers,
		MaxWorkers:    cfg.MaxWorkers,
		LeaseTTL:      cfg.LeaseTTL,
		ScaleDownIdle: cfg.ScaleDownIdle,
		OnRetry:       func(_ time.Time) { disp.Signal() },
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
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
	}

	job.Queue = queueName
	if job.RunAt.IsZero() {
		job.RunAt = time.Now().UTC()
	}
	if job.ID == "" {
		id, err := domain.New()
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

	if err := q.storage.SaveJob(ctx, &job); err != nil {
		return fmt.Errorf("persist job: %w", err)
	}

	q.dispatcher.Signal()
	return nil
}

func (q *queue) UpsertSchedule(ctx context.Context, queueName QueueName, schedule JobSchedule) error {
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
	}

	schedule.Queue = queueName
	if schedule.ID == "" {
		id, err := domain.New()
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
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
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
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
	}

	return q.storage.RemoveJob(ctx, jobID)
}

func (q *queue) PauseSchedule(ctx context.Context, queueName QueueName, scheduleID string) error {
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
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
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
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

func (q *queue) Start(ctx context.Context) error {
	if q.isRunning.Load() {
		return fmt.Errorf("queue is already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	q.stopFunc = cancel

	recovered, err := q.storage.RecoverOrphaned(ctx, time.Now())
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

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		if err := q.dispatcher.Run(ctx); err != nil && err != context.Canceled {
			q.cfg.Logger.Error(err, logger.Fields{"component": "dispatcher"})
		}
	}()

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		if err := q.scheduler.Run(ctx); err != nil && err != context.Canceled {
			q.cfg.Logger.Error(err, logger.Fields{"component": "scheduler"})
		}
	}()

	q.isRunning.Store(true)
	close(q.done)
	return nil
}

func (q *queue) Stop(ctx context.Context) error {
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
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
