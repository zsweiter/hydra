package hydra

import (
	"context"
	"errors"
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

	// ScheduleCancelled marks a removed schedule kept as history
	// (and as a tombstone for versioned upserts).
	ScheduleCancelled = domain.ScheduleCancelled

	// ScheduleArchived marks a past revision of a schedule,
	// kept as history under its own ID after a replacement.
	ScheduleArchived = domain.ScheduleArchived
)

// ErrStaleVersion is returned by UpsertSchedule when the stored
// schedule has a Version greater than or equal to the given
// one; the upsert had no effect.
var ErrStaleVersion = storage.ErrStaleVersion

type (
	// ScheduleQuery selects schedules for ListSchedules.
	ScheduleQuery = storage.ScheduleQuery

	// JobQuery selects the jobs of a schedule for ListJobs.
	JobQuery = storage.JobQuery

	// DeadLetter is a job that exhausted its retries.
	DeadLetter = storage.DeadLetter
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
	ScheduleTypeOnce     = domain.ScheduleOnce
)

// MetadataScheduledFor is the job metadata key holding the
// time a schedule-generated job was due.
const MetadataScheduledFor = domain.MetadataScheduledFor

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

// OnceRule creates a rule that fires a single time at at.
func OnceRule(at time.Time) ScheduleRule {
	return domain.NewOnceScheduleRule(at)
}

// IntervalRule creates a fixed-interval recurrence rule.
func IntervalRule(d time.Duration) ScheduleRule {
	return domain.NewIntervalScheduleRule(d)
}

// ValidateRule checks that a rule is well formed and that its
// expression parses, so it can be rejected before it is stored.
func ValidateRule(rule ScheduleRule) error {
	_, err := scheduler.Compile(rule)
	return err
}

// DefaultBackoff returns the default exponential backoff policy
// (100ms base, 10s max).
func DefaultBackoff() BackoffPolicy {
	return domain.DefaultBackoff()
}

// Worker is a function that processes a job.
type Worker = func(ctx context.Context, job *Job) error

// DirectWorker delivers the jobs of due schedule occurrences
// before they are stored (see Consumer.HandleDirect). It
// returns one error per job, or nil when all were delivered.
type DirectWorker = func(ctx context.Context, jobs []*Job) []error

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

	// SyncInterval enables running several instances against a
	// shared storage (e.g. Redis). Every interval each instance
	// reloads schedules changed by others, recovers jobs whose
	// lease expired (a crashed instance) and checks for jobs
	// submitted elsewhere. Zero, the default, is for a single
	// instance and keeps the queue fully idle.
	SyncInterval time.Duration

	// CatchUpBatch caps the occurrences a catch-up schedule
	// generates per claim after falling behind (e.g. downtime).
	// The rest are replayed in order, batch by batch, time
	// sliced with on-time occurrences; schedules with a lower
	// Priority value catch up first. Default 100.
	CatchUpBatch int

	// ScheduleWindow is how far ahead schedules are loaded into
	// memory, and ScheduleWindowLimit how many at most per load.
	// The window is reloaded before it runs out, so memory
	// grows with the schedules due soon, not with all of them.
	// Defaults 1m and 10000.
	ScheduleWindow      time.Duration
	ScheduleWindowLimit int

	// FireBatch is how many due schedules fire together: their
	// payloads are loaded in one round trip and their claims
	// written in ClaimConcurrency concurrent ones. Defaults 512
	// and 4.
	FireBatch        int
	ClaimConcurrency int

	// HistoryTTL deletes completed jobs, and finished, cancelled
	// and archived schedules, once they are older than it. Zero,
	// the default, keeps history forever.
	HistoryTTL time.Duration

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

	// DeleteSchedule removes a schedule record for good, unlike
	// RemoveSchedule, which keeps it cancelled as history. Jobs
	// already generated are unaffected.
	DeleteSchedule(
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
	Process(queue QueueName, processor Processor)

	// HandleDirect delivers the jobs schedules generate for
	// queue as soon as they are due, in batches, before they
	// are stored: a delivered occurrence is only recorded in
	// its schedule's progress, with no job, no history and no
	// worker round trip. Jobs it fails to deliver are stored
	// and processed by the queue's Worker (retries, dead
	// letters), which must be registered too.
	//
	// An occurrence may be delivered twice when its claim fails
	// after delivery or instances overlap, so delivery must be
	// idempotent, e.g. keyed by ScheduleID and
	// MetadataScheduledFor. Call it before Start.
	HandleDirect(queue QueueName, worker DirectWorker)
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

	Inspector
}

// Inspector reads schedules, their history and the
// dead-letter queue, and requeues dead letters. It works
// without Start, e.g. from an admin command.
type Inspector interface {
	GetSchedule(ctx context.Context, scheduleID string) (*JobSchedule, error)

	// ListSchedules includes finished and cancelled schedules,
	// kept as history.
	ListSchedules(ctx context.Context, query ScheduleQuery) ([]*JobSchedule, error)

	// ListJobs returns the jobs generated by a schedule.
	ListJobs(ctx context.Context, query JobQuery) ([]*Job, error)

	ListDLQ(ctx context.Context, queue QueueName) ([]DeadLetter, error)
	RequeueDLQ(ctx context.Context, jobID string) error
}

type queue struct {
	direct     sync.Map // QueueName -> DirectWorker
	storage    storage.Storage
	scheduler  *scheduler.Scheduler
	dispatcher *dispatcher.Dispatcher
	pool       *worker.Pool
	registry   *worker.Registry
	isRunning  atomic.Bool
	stopFunc   context.CancelFunc
	done       chan struct{}
	cfg        Config
	wg         sync.WaitGroup

	// instanceID identifies this instance among those sharing
	// the storage, which split the schedule shards.
	instanceID string
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
	sched.SetMaxAttempts(cfg.MaxRetries)
	sched.SetCatchUpBatch(cfg.CatchUpBatch)
	sched.SetLogger(cfg.Logger)
	sched.SetWindow(cfg.ScheduleWindow, cfg.ScheduleWindowLimit)
	sched.SetFireBatch(cfg.FireBatch, cfg.ClaimConcurrency)

	instanceID, _ := domain.New()

	q := &queue{
		instanceID: instanceID.String(),
		storage:    cfg.Storage,
		scheduler:  sched,
		dispatcher: disp,
		pool:       pool,
		registry:   registry,
		cfg:        cfg,
		done:       make(chan struct{}),
	}
	sched.SetDirect(func(name domain.QueueName) scheduler.DirectFunc {
		if w, ok := q.direct.Load(name); ok {
			return w.(DirectWorker)
		}
		return nil
	})
	return q
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

	// A cancelled schedule is a tombstone: it has no rule to
	// run, only a Version that older upserts cannot beat. An
	// archived one is history.
	if schedule.Status != ScheduleCancelled && schedule.Status != ScheduleArchived {
		if err := q.prepareSchedule(ctx, &schedule); err != nil {
			return err
		}
	}

	if err := q.storage.SaveSchedule(ctx, &schedule); err != nil {
		if errors.Is(err, storage.ErrStaleVersion) {
			return err
		}
		return fmt.Errorf("persist schedule: %w", err)
	}

	event := scheduler.EventScheduleAdded
	switch schedule.Status {
	case SchedulePaused:
		event = scheduler.EventSchedulePaused
	case ScheduleFinished, ScheduleCancelled, ScheduleArchived:
		event = scheduler.EventScheduleRemoved
	}
	q.scheduler.Notify(scheduler.Event{Type: event, ScheduleID: schedule.ID})

	return nil
}

// prepareSchedule validates the rule and sets the schedule's
// progress before it is saved (see scheduler.Prepare).
func (q *queue) prepareSchedule(ctx context.Context, schedule *JobSchedule) error {
	prev, err := q.storage.LoadSchedule(ctx, schedule.ID)
	if err != nil {
		prev = nil
	}
	return scheduler.Prepare(schedule, prev, time.Now())
}

// RemoveSchedule cancels a schedule. The record is kept with
// status ScheduleCancelled as history; jobs already generated
// are unaffected.
func (q *queue) RemoveSchedule(ctx context.Context, queueName QueueName, scheduleID string) error {
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
	}

	if err := q.storage.UpdateScheduleStatus(ctx, scheduleID, ScheduleCancelled); err != nil {
		return fmt.Errorf("cancel schedule: %w", err)
	}

	q.scheduler.Notify(scheduler.Event{
		Type:       scheduler.EventScheduleRemoved,
		ScheduleID: scheduleID,
	})

	return nil
}

func (q *queue) DeleteSchedule(ctx context.Context, queueName QueueName, scheduleID string) error {
	if err := q.storage.DeleteSchedule(ctx, scheduleID); err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}

	q.scheduler.Notify(scheduler.Event{
		Type:       scheduler.EventScheduleRemoved,
		ScheduleID: scheduleID,
	})
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

// ResumeSchedule reactivates a paused schedule from its next
// occurrence after now: occurrences due while it was paused
// are skipped, whatever its misfire policy.
func (q *queue) ResumeSchedule(ctx context.Context, queueName QueueName, scheduleID string) error {
	if !q.isRunning.Load() {
		return fmt.Errorf("queue is not running")
	}

	sch, err := q.storage.LoadSchedule(ctx, scheduleID)
	if err != nil {
		return fmt.Errorf("resume schedule: %w", err)
	}
	if sch.Status != SchedulePaused {
		return nil
	}
	if err := ResumeFrom(sch, time.Now()); err != nil {
		return fmt.Errorf("resume schedule: %w", err)
	}
	if err := q.storage.SaveSchedule(ctx, sch); err != nil {
		return fmt.Errorf("resume schedule: %w", err)
	}

	q.scheduler.Notify(scheduler.Event{
		Type:       scheduler.EventScheduleResumed,
		ScheduleID: scheduleID,
	})
	q.scheduler.Signal()

	return nil
}

// ResumeFrom makes sch active again from its first occurrence
// after now, or finished when it has none left.
func ResumeFrom(sch *JobSchedule, now time.Time) error {
	compiled, err := scheduler.Compile(sch.Rule)
	if err != nil {
		return err
	}
	sch.Status = ScheduleActive
	sch.NextRunAt = nil
	if next, ok := compiled.Next(now.UTC()); ok {
		sch.NextRunAt = &next
	} else {
		sch.Status = ScheduleFinished
	}
	return nil
}

func (q *queue) Handle(queueName QueueName, worker Worker) {
	q.registry.Register(queueName, worker)
}

func (q *queue) Process(queueName QueueName, processor Processor) {
	q.registry.Register(queueName, processor.Handle)
}

func (q *queue) HandleDirect(queueName QueueName, worker DirectWorker) {
	q.direct.Store(queueName, worker)
}

// Pause stops jobs of a queue from being claimed, on every
// instance sharing the storage.
func (q *queue) Pause(ctx context.Context, queueName QueueName) error {
	if err := q.storage.SetQueuePaused(ctx, queueName, true); err != nil {
		return fmt.Errorf("pause queue: %w", err)
	}
	return q.refreshPausedQueues(ctx)
}

func (q *queue) Resume(ctx context.Context, queueName QueueName) error {
	if err := q.storage.SetQueuePaused(ctx, queueName, false); err != nil {
		return fmt.Errorf("resume queue: %w", err)
	}
	if err := q.refreshPausedQueues(ctx); err != nil {
		return err
	}
	q.dispatcher.Signal()
	return nil
}

func (q *queue) refreshPausedQueues(ctx context.Context) error {
	paused, err := q.storage.PausedQueues(ctx)
	if err != nil {
		return fmt.Errorf("load paused queues: %w", err)
	}
	q.dispatcher.SetPausedQueues(paused)
	return nil
}

// ──────────────────────────────────────────────
//  Inspection
// ──────────────────────────────────────────────

func (q *queue) GetSchedule(ctx context.Context, scheduleID string) (*JobSchedule, error) {
	return q.storage.LoadSchedule(ctx, scheduleID)
}

func (q *queue) ListSchedules(ctx context.Context, query ScheduleQuery) ([]*JobSchedule, error) {
	return q.storage.ListSchedules(ctx, query)
}

func (q *queue) ListJobs(ctx context.Context, query JobQuery) ([]*Job, error) {
	return q.storage.ListJobs(ctx, query)
}

func (q *queue) ListDLQ(ctx context.Context, queueName QueueName) ([]DeadLetter, error) {
	return q.storage.ListDLQ(ctx, queueName)
}

func (q *queue) RequeueDLQ(ctx context.Context, jobID string) error {
	if err := q.storage.RequeueDLQ(ctx, jobID); err != nil {
		return err
	}
	q.dispatcher.Signal()
	return nil
}

func (q *queue) Start(ctx context.Context) error {
	if q.isRunning.Load() {
		return fmt.Errorf("queue is already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	q.stopFunc = cancel

	if err := q.refreshPausedQueues(ctx); err != nil {
		cancel()
		return err
	}

	recovered, err := q.storage.RecoverOrphaned(ctx, time.Now())
	if err != nil {
		cancel()
		return fmt.Errorf("recover orphans: %w", err)
	}
	if recovered > 0 {
		q.dispatcher.Signal()
	}

	if q.cfg.SyncInterval > 0 {
		if err := q.joinCluster(ctx); err != nil {
			cancel()
			return err
		}
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

	if q.cfg.SyncInterval > 0 {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.syncLoop(ctx)
		}()
	}

	if q.cfg.HistoryTTL > 0 {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.purgeLoop(ctx)
		}()
	}

	q.isRunning.Store(true)
	close(q.done)
	return nil
}

// syncLoop keeps an instance in step with others sharing the
// storage. Claims stay correct without it; it only ensures
// this instance learns about work it was not signalled for.
func (q *queue) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(q.cfg.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := q.joinCluster(ctx); err != nil {
			q.cfg.Logger.Error(err, logger.Fields{"component": "sync"})
		}
		q.scheduler.Resync()

		if err := q.refreshPausedQueues(ctx); err != nil {
			q.cfg.Logger.Error(err, logger.Fields{"component": "sync"})
		}

		if _, err := q.storage.RecoverOrphaned(ctx, time.Now()); err != nil {
			q.cfg.Logger.Error(fmt.Errorf("recover orphaned jobs: %w", err), logger.Fields{"component": "sync"})
		}

		// Jobs submitted or recovered elsewhere do not signal
		// this dispatcher.
		q.dispatcher.Signal()
	}
}

// purgeBatch is how many records a purge deletes per round
// trip, so it never holds the storage for long.
const purgeBatch = 1000

// purgeLoop deletes history older than HistoryTTL, in small
// batches. Every instance runs it; deletions are idempotent.
func (q *queue) purgeLoop(ctx context.Context) {
	every := min(max(q.cfg.HistoryTTL/10, time.Second), 10*time.Minute)
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		before := time.Now().Add(-q.cfg.HistoryTTL)
		for ctx.Err() == nil {
			n, err := q.storage.PurgeHistory(ctx, before, purgeBatch)
			if err != nil {
				q.cfg.Logger.Error(fmt.Errorf("purge history: %w", err), logger.Fields{"component": "purge"})
				break
			}
			if n < purgeBatch {
				break
			}
		}
	}
}

// joinCluster renews this instance's membership and takes its
// share of the schedule shards. While membership changes, two
// instances may briefly own a shard; the atomic claim keeps
// each occurrence unique, and a shard owned by nobody is taken
// on the next sync.
func (q *queue) joinCluster(ctx context.Context) error {
	members, err := q.storage.Heartbeat(ctx, q.instanceID, 3*q.cfg.SyncInterval)
	if err != nil {
		return fmt.Errorf("cluster heartbeat: %w", err)
	}
	q.scheduler.SetShards(domain.OwnedShards(q.instanceID, members))
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

	if q.cfg.SyncInterval > 0 {
		// Leave at once so the others take over its shards on
		// their next sync, instead of after the heartbeat TTL.
		if _, err := q.storage.Heartbeat(ctx, q.instanceID, 0); err != nil {
			q.cfg.Logger.Error(fmt.Errorf("leave cluster: %w", err))
		}
	}

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

	sched := q.scheduler.Stats()
	return StatsSnapshot{
		ReadyCount:      s.ReadyCount,
		ScheduledCount:  s.ScheduledCount,
		DLQCount:        s.DLQCount,
		ActiveWorkers:   q.pool.ActiveWorkers(),
		SchedulesLoaded: sched.Loaded,
		CatchUpBacklog:  sched.Backlog,
		OwnedShards:     sched.Shards,

		OccurrencesClaimed:   sched.Claimed,
		ClaimsLost:           sched.Lost,
		OccurrencesDelivered: sched.Delivered,
		OccurrencesOutboxed:  sched.Outboxed,
	}, nil
}

var _ Queue = (*queue)(nil)

// StatsSnapshot contains a point-in-time view of queue metrics.
type StatsSnapshot struct {
	ReadyCount     map[QueueName]int64
	ScheduledCount int64
	DLQCount       int64
	ActiveWorkers  uint64

	// Scheduler state of this instance: schedules held in the
	// time window, catch-up schedules still behind, and shards
	// owned among the instances sharing the storage.
	SchedulesLoaded int
	CatchUpBacklog  int
	OwnedShards     int

	// Cumulative scheduler counts of this instance: occurrences
	// claimed, claims another instance won, occurrences
	// delivered directly, and occurrences stored as jobs after
	// a direct delivery failed (see Consumer.HandleDirect).
	OccurrencesClaimed   int64
	ClaimsLost           int64
	OccurrencesDelivered int64
	OccurrencesOutboxed  int64
}
