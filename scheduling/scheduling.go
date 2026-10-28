// Package scheduling runs hydra's scheduler on its own, without
// the job queue: schedules are stored, and each occurrence is
// handed to a delivery function when due and claimed only once
// delivered. Nothing but the schedules is stored.
//
// hydra-scheduler is built on it. Applications that also want
// jobs, workers and retries use the hydra package instead.
package scheduling

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/scheduler"
	"github.com/zsweiter/hydra/pkg/logger"
	"github.com/zsweiter/hydra/storage"
)

// DeliverFunc delivers due occurrences, in batches, and returns
// one error per occurrence (nil when all were delivered). An
// occurrence that fails is delivered again, with exponential
// backoff, and its schedule does not advance until it succeeds.
//
// An occurrence may be delivered more than once (its claim
// failed after delivery, or two instances fired it while shards
// moved), so delivery must be idempotent per occurrence: key it
// by the job's ScheduleID and RunAt, when it was due. The jobs
// are never stored: they have no ID, and their Metadata is the
// schedule's, shared, so it must not be modified.
type DeliverFunc = func(ctx context.Context, occurrences []*domain.Job) []error

// Config configures a Service. Zero values take the defaults
// documented on each field.
type Config struct {
	Store   storage.ScheduleStore // required
	Deliver DeliverFunc           // required
	Logger  logger.Logger

	// SyncInterval enables several instances sharing Store:
	// every interval each renews its membership, takes its share
	// of the shards and loads the schedules others changed.
	// Zero is for a single instance.
	SyncInterval time.Duration

	CatchUpBatch     int           // occurrences a catch-up schedule replays per claim; default 100
	Window           time.Duration // how far ahead schedules are held in memory; default 1m
	WindowLimit      int           // schedules loaded at most per window; default 10000
	FireBatch        int           // due schedules fired together; default 512
	ClaimConcurrency int           // concurrent claim round trips per batch; default 4

	// HistoryTTL deletes ended, cancelled and archived schedules
	// older than it. Zero keeps them.
	HistoryTTL time.Duration
}

// Service is a running scheduler.
type Service struct {
	cfg        Config
	sched      *scheduler.Scheduler
	instanceID string

	// Failed heartbeats and purges; the scheduler counts its own.
	storeErrors atomic.Int64
	purged      atomic.Int64

	stop context.CancelFunc
	wg   sync.WaitGroup
}

// New creates a Service; Start runs it.
func New(cfg Config) (*Service, error) {
	if cfg.Store == nil || cfg.Deliver == nil {
		return nil, errors.New("scheduling: Store and Deliver are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.DefaultLogger
	}

	sched := scheduler.New(cfg.Store, nil)
	sched.SetOutbox(false)
	sched.SetDirect(func(domain.QueueName) scheduler.DirectFunc { return cfg.Deliver })
	sched.SetCatchUpBatch(cfg.CatchUpBatch)
	sched.SetWindow(cfg.Window, cfg.WindowLimit)
	sched.SetFireBatch(cfg.FireBatch, cfg.ClaimConcurrency)
	sched.SetLogger(cfg.Logger)

	id, err := domain.New()
	if err != nil {
		return nil, err
	}

	return &Service{cfg: cfg, sched: sched, instanceID: id.String()}, nil
}

// Start loads the schedules and runs the scheduler until Stop
// or ctx ends.
func (s *Service) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.stop = cancel

	if s.cfg.SyncInterval > 0 {
		if err := s.join(ctx); err != nil {
			cancel()
			return err
		}
	}
	if err := s.sched.Load(ctx); err != nil {
		cancel()
		return fmt.Errorf("load schedules: %w", err)
	}
	st := s.sched.Stats()
	s.cfg.Logger.Info("schedules loaded", logger.Fields{"loaded": st.Loaded, "backlog": st.Backlog, "shards": st.Shards})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.sched.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.cfg.Logger.Error(err, logger.Fields{"component": "scheduler"})
		}
	}()

	if s.cfg.SyncInterval > 0 {
		s.every(ctx, s.cfg.SyncInterval, func() {
			if err := s.join(ctx); err != nil {
				s.storeErrors.Add(1)
				s.cfg.Logger.Error(err, logger.Fields{"component": "cluster"})
			}
			s.sched.Resync()
		})
	}

	if s.cfg.HistoryTTL > 0 {
		s.every(ctx, min(max(s.cfg.HistoryTTL/10, time.Second), 10*time.Minute), func() { s.purge(ctx) })
	}
	return nil
}

// Stop stops the scheduler and leaves the cluster at once, so
// the other instances take over its shards on their next sync.
// It does not close the store.
func (s *Service) Stop(ctx context.Context) error {
	if s.stop == nil {
		return nil
	}

	s.stop()
	s.wg.Wait()
	if s.cfg.SyncInterval > 0 {
		if _, err := s.cfg.Store.Heartbeat(ctx, s.instanceID, 0); err != nil {
			return fmt.Errorf("leave cluster: %w", err)
		}
	}

	return nil
}

func (s *Service) every(ctx context.Context, interval time.Duration, fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fn()
			}
		}
	}()
}

// join renews this instance's membership and takes its share of
// the shards.
func (s *Service) join(ctx context.Context) error {
	members, err := s.cfg.Store.Heartbeat(ctx, s.instanceID, 3*s.cfg.SyncInterval)
	if err != nil {
		return fmt.Errorf("cluster heartbeat: %w", err)
	}

	// Logged on change only: members joining or leaving move
	// shards, and with them the schedules this instance fires.
	if shards := domain.OwnedShards(s.instanceID, members); s.sched.SetShards(shards) {
		s.cfg.Logger.Info("shards rebalanced", logger.Fields{
			"component": "cluster", "instance": s.instanceID, "members": len(members), "shards": len(shards),
		})
	}
	return nil
}

// purgeBatch is how many records a purge deletes per round trip.
const purgeBatch = 1000

func (s *Service) purge(ctx context.Context) {
	before := time.Now().Add(-s.cfg.HistoryTTL)
	total := 0
	defer func() {
		s.purged.Add(int64(total))
		if total > 0 {
			s.cfg.Logger.Info("history purged", logger.Fields{"component": "purge", "deleted": total})
		}
	}()
	for ctx.Err() == nil {
		n, err := s.cfg.Store.PurgeHistory(ctx, before, purgeBatch)
		total += n
		if err != nil {
			s.storeErrors.Add(1)
			s.cfg.Logger.Error(fmt.Errorf("purge history: %w", err), logger.Fields{"component": "purge"})
			return
		}
		if n < purgeBatch {
			return
		}
	}
}

// Prepare validates the rule of schedule and sets its progress
// before it is saved: an unchanged rule keeps the progress of
// prev (the stored schedule, or nil), a new one gets its first
// occurrence. Save does not call it, so callers that decide
// what to save (versions, revisions) prepare first.
func Prepare(schedule, prev *domain.JobSchedule, now time.Time) error {
	return scheduler.Prepare(schedule, prev, now)
}

// FirstAfter returns the first occurrence of rule after t, as
// Resume needs it.
func FirstAfter(rule domain.ScheduleRule, t time.Time) (time.Time, bool, error) {
	c, err := scheduler.Compile(rule)
	if err != nil {
		return time.Time{}, false, err
	}

	next, ok := c.Next(t.UTC())
	return next, ok, nil
}

// Validate checks that a rule is well formed and parses.
func Validate(rule domain.ScheduleRule) error {
	_, err := scheduler.Compile(rule)
	return err
}

// Save stores prepared schedules in one round trip and makes
// the scheduler pick them up. It returns one error per schedule
// (nil, or storage.ErrStaleVersion when a newer version is
// stored); the error reports a failure of the whole call.
func (s *Service) Save(ctx context.Context, schedules []*domain.JobSchedule) ([]error, error) {
	errs, err := s.cfg.Store.SaveSchedules(ctx, schedules)
	if err != nil {
		return nil, err
	}

	for i, sch := range schedules {
		if errs[i] == nil {
			s.notify(sch)
		}
	}

	return errs, nil
}

func (s *Service) notify(sch *domain.JobSchedule) {
	event := scheduler.EventScheduleAdded
	switch sch.Status {
	case domain.SchedulePaused:
		event = scheduler.EventSchedulePaused
	case domain.ScheduleFinished, domain.ScheduleCancelled, domain.ScheduleArchived:
		event = scheduler.EventScheduleRemoved
	}

	s.sched.Notify(scheduler.Event{Type: event, ScheduleID: sch.ID})
}

// Load returns the stored schedules with the given IDs; missing
// ones are left out.
func (s *Service) Load(ctx context.Context, ids []string) ([]*domain.JobSchedule, error) {
	return s.cfg.Store.LoadSchedules(ctx, ids)
}

// Delete removes schedules for good.
func (s *Service) Delete(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if err := s.cfg.Store.DeleteSchedule(ctx, id); err != nil {
			return err
		}
		s.sched.Notify(scheduler.Event{Type: scheduler.EventScheduleRemoved, ScheduleID: id})
	}

	return nil
}

// Stats is a point-in-time view of the service: the
// scheduler's figures (schedules in memory, catch-up backlog,
// shards owned, occurrences claimed, claims lost to other
// instances, deliveries retried, schedules stalled, store
// errors) and the history records purged.
type Stats struct {
	scheduler.Stats
	Purged int64
}

// Stats returns the current figures. Safe to call from any
// goroutine.
func (s *Service) Stats() Stats {
	st := Stats{Stats: s.sched.Stats(), Purged: s.purged.Load()}
	st.StoreErrors += s.storeErrors.Load()
	return st
}
