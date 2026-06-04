package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/zsweiter/hydra/internal/domain"
	"github.com/zsweiter/hydra/internal/storage"
)

// SignalFunc is a lightweight notification callback used
// to wake up another actor (e.g. the Dispatcher).
type SignalFunc func()

// EventType describes what kind of schedule change occurred.
type EventType int

const (
	EventScheduleAdded EventType = iota
	EventScheduleRemoved
	EventSchedulePaused
	EventScheduleResumed
)

// Event carries a schedule change notification to the Scheduler.
type Event struct {
	Type       EventType
	ScheduleID string
}

// Scheduler is an idle-first actor that transforms schedules
// into executable jobs.
//
// It maintains a temporal index (ScheduleHeap) and a cache of
// CompiledSchedule values to avoid reparsing recurrence rules.
//
// The Scheduler never executes jobs directly. It signals the
// Dispatcher after generating new jobs.
type Scheduler struct {
	storage storage.Storage
	signal  SignalFunc

	heap     *ScheduleHeap
	compiled map[string]domain.CompiledSchedule
	events   chan Event
	sigChan  chan struct{}
	mu       sync.Mutex

	// stopped tracks schedule IDs that have been paused or
	// removed but whose heap entry may still be in-flight
	// (already popped by tick). When set, tick will skip
	// reinsertion.
	stopped map[string]bool

	logf func(format string, args ...any)
}

// New creates a new Scheduler.
//
// The signal callback is invoked whenever the Scheduler
// generates new jobs, so the Dispatcher can be woken up.
func New(s storage.Storage, signal SignalFunc) *Scheduler {
	return &Scheduler{
		storage:  s,
		signal:   signal,
		heap:     &ScheduleHeap{},
		compiled: make(map[string]domain.CompiledSchedule),
		events:   make(chan Event, 64),
		sigChan:  make(chan struct{}, 1),
		stopped:  make(map[string]bool),
		logf:     func(string, ...any) {},
	}
}

// Signal wakes the Scheduler to re-evaluate its heap.
// This is called by the Queue when schedules change.
func (s *Scheduler) Signal() {
	select {
	case s.sigChan <- struct{}{}:
	default:
	}
}

// Notify sends a schedule event to the Scheduler.
func (s *Scheduler) Notify(event Event) {
	select {
	case s.events <- event:
	default:
	}
}

// Load initialises the Scheduler from storage.
// Must be called before Run.
func (s *Scheduler) Load(ctx context.Context) error {
	schedules, err := s.storage.LoadActiveSchedules(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sch := range schedules {
		c, err := Compile(sch.Rule)
		if err != nil {
			continue
		}
		s.compiled[sch.ID] = c

		if sch.Status == domain.ScheduleActive {
			entry := HeapEntryFromSchedule(sch, c)
			if entry != nil {
				heap.Push(s.heap, entry)
			}
		}
	}

	return nil
}

// Run starts the scheduler loop. Blocks until the context
// is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	for {
		s.mu.Lock()
		next := s.heap.Peek()
		s.mu.Unlock()

		if next == nil {
			if err := s.waitForEvent(ctx); err != nil {
				return err
			}
			continue
		}

		wait := time.Duration(next.NextRunNanos-time.Now().UnixNano()) * time.Nanosecond
		if wait < 0 {
			wait = 0
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()

		case event := <-s.events:
			timer.Stop()
			s.handleEvent(ctx, event)

		case <-s.sigChan:
			timer.Stop()

		case <-timer.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) waitForEvent(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case event := <-s.events:
		s.handleEvent(ctx, event)
	case <-s.sigChan:
	}
	return nil
}

func (s *Scheduler) tick(ctx context.Context) {
	s.mu.Lock()
	if s.heap.Len() == 0 {
		s.mu.Unlock()
		return
	}

	entry := heap.Pop(s.heap).(*HeapEntry)
	s.mu.Unlock()

	c, ok := s.compiled[entry.ScheduleID]
	if !ok {
		return
	}

	now := time.Now().UTC()
	entryTime := time.Unix(0, entry.NextRunNanos)

	var jobs []*domain.Job

	switch entry.MisfirePolicy {
	case domain.MisfireCatchUp:
		genTime := entryTime
		for {
			if genTime.After(now) {
				break
			}
			job := s.buildJob(entry, genTime)
			jobs = append(jobs, job)

			next, ok := c.Next(genTime)
			if !ok {
				break
			}
			genTime = next
		}

	case domain.MisfireRunOnce:
		if entryTime.Before(now.Add(-time.Minute)) {
			job := s.buildJob(entry, now)
			jobs = append(jobs, job)
		} else {
			job := s.buildJob(entry, entryTime)
			jobs = append(jobs, job)
		}

	default:
		job := s.buildJob(entry, entryTime)
		jobs = append(jobs, job)
	}

	for _, job := range jobs {
		if err := s.storage.SaveJob(ctx, job); err != nil {
			continue
		}
	}

	// Update GeneratedJobs counter
	for range jobs {
		s.storage.IncrementGeneratedJobs(ctx, entry.ScheduleID)
	}

	// Check if schedule was paused or removed while we were
	// generating jobs. If so, drop the entry.
	s.mu.Lock()
	if s.stopped[entry.ScheduleID] {
		delete(s.stopped, entry.ScheduleID)
		s.mu.Unlock()
		if s.signal != nil && len(jobs) > 0 {
			s.signal()
		}
		return
	}
	s.mu.Unlock()

	// Check if schedule should finish (MaxJobs, EndAt)
	schedule, err := s.storage.LoadSchedule(ctx, entry.ScheduleID)
	if err == nil && schedule.ShouldFinish() {
		if s.signal != nil && len(jobs) > 0 {
			s.signal()
		}
		return
	}

	// Calculate next occurrence from now, not from the
	// job's RunAt time. This prevents busy-looping when
	// the timer fired late (e.g. sub-ms intervals on a
	// ms-precision timer).
	next, ok := c.Next(now)
	if !ok {
		return
	}

	entry.NextRunNanos = next.UnixNano()

	if err := s.storage.UpdateNextRunAt(ctx, entry.ScheduleID, next, now); err != nil {
		return
	}

	s.mu.Lock()
	heap.Push(s.heap, entry)
	s.mu.Unlock()

	if s.signal != nil && len(jobs) > 0 {
		s.signal()
	}
}

func (s *Scheduler) buildJob(entry *HeapEntry, runAt time.Time) *domain.Job {
	id, _ := domain.New()
	return &domain.Job{
		ID:         id.String(),
		Name:       entry.JobName,
		Queue:      entry.Queue,
		Data:       entry.Data,
		Metadata:   entry.Metadata,
		Priority:   entry.Priority,
		RunAt:      runAt,
		Status:     domain.StatusPending,
		ScheduleID: &entry.ScheduleID,
	}
}

func (s *Scheduler) handleEvent(ctx context.Context, event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event.Type {
	case EventScheduleAdded:
		delete(s.stopped, event.ScheduleID)
		s.loadAndInsertSchedule(ctx, event.ScheduleID)

	case EventScheduleResumed:
		delete(s.stopped, event.ScheduleID)
		s.loadAndInsertSchedule(ctx, event.ScheduleID)

	case EventScheduleRemoved:
		removed := s.heap.RemoveByScheduleID(event.ScheduleID)
		if removed == nil {
			s.stopped[event.ScheduleID] = true
		}
		delete(s.compiled, event.ScheduleID)

	case EventSchedulePaused:
		removed := s.heap.RemoveByScheduleID(event.ScheduleID)
		if removed == nil {
			s.stopped[event.ScheduleID] = true
		}
	}
}

func (s *Scheduler) loadAndInsertSchedule(ctx context.Context, scheduleID string) {
	schedules, err := s.storage.LoadActiveSchedules(ctx)
	if err != nil {
		return
	}

	var sch *domain.JobSchedule
	for _, sc := range schedules {
		if sc.ID == scheduleID {
			sch = sc
			break
		}
	}
	if sch == nil {
		return
	}

	c, err := Compile(sch.Rule)
	if err != nil {
		return
	}
	s.compiled[sch.ID] = c

	// Remove old entry if exists
	s.heap.RemoveByScheduleID(scheduleID)

	if sch.Status == domain.ScheduleActive {
		entry := HeapEntryFromSchedule(sch, c)
		if entry != nil {
			heap.Push(s.heap, entry)
		}
	}
}
