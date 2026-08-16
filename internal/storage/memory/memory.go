package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

type leaseEntry struct {
	job       *domain.Job
	workerID  string
	expiresAt time.Time
}

// Storage is an in-memory Storage implementation for
// development and testing. It is safe for concurrent use.
type Storage struct {
	mu sync.RWMutex

	ready     map[domain.QueueName][]*domain.Job
	scheduled []*domain.Job
	leases    map[string]leaseEntry
	schedules map[string]*domain.JobSchedule
	dlq       []storage.DeadLetter
}

// New creates a new in-memory storage.
func New() *Storage {
	return &Storage{
		ready:     make(map[domain.QueueName][]*domain.Job),
		leases:    make(map[string]leaseEntry),
		schedules: make(map[string]*domain.JobSchedule),
	}
}

func (s *Storage) SaveJob(_ context.Context, job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready == nil {
		s.ready = make(map[domain.QueueName][]*domain.Job)
	}

	stored := cloneJob(job)
	if job.Status == domain.StatusScheduled {
		s.insertScheduled(stored)
		return nil
	}

	s.ready[job.Queue] = append(s.ready[job.Queue], stored)
	return nil
}

func (s *Storage) RemoveJob(_ context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, leased := s.leases[jobID]; leased {
		return fmt.Errorf("%w: job is processing: %s", storage.ErrConflict, jobID)
	}

	for queue, jobs := range s.ready {
		for i, j := range jobs {
			if j.ID == jobID {
				s.ready[queue] = append(jobs[:i], jobs[i+1:]...)
				if len(s.ready[queue]) == 0 {
					delete(s.ready, queue)
				}
				return nil
			}
		}
	}

	for i, j := range s.scheduled {
		if j.ID == jobID {
			s.scheduled = append(s.scheduled[:i], s.scheduled[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("%w: %s", storage.ErrNotFound, jobID)
}

func (s *Storage) ClaimJobs(_ context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]storage.LeasedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ready := s.ready[queue]
	if len(ready) == 0 {
		return nil, nil
	}

	batch := min(n, len(ready))

	jobs := make([]*domain.Job, batch)
	copy(jobs, ready[:batch])

	if batch == len(ready) {
		delete(s.ready, queue)
	} else {
		s.ready[queue] = ready[batch:]
	}

	result := make([]storage.LeasedJob, batch)
	now := time.Now().UTC()
	for i, job := range jobs {
		leased := cloneJob(job)
		leased.Status = domain.StatusProcessing
		leased.UpdatedAt = now
		s.leases[job.ID] = leaseEntry{
			job:       cloneJob(leased),
			workerID:  workerID,
			expiresAt: now.Add(ttl),
		}
		result[i] = storage.LeasedJob{Job: leased}
	}

	return result, nil
}

func (s *Storage) AckJob(_ context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.leases, jobID)
	return nil
}

func (s *Storage) FailJob(_ context.Context, job *domain.Job, failErr error, decision domain.FailDecision) (*storage.FailResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.leases, job.ID)

	stored := cloneJob(job)
	stored.UpdatedAt = time.Now().UTC()

	if decision.Retry {
		stored.Status = domain.StatusPending
		stored.RunAt = decision.NextRunAt
		s.insertScheduled(stored)

		return &storage.FailResult{
			Action:    storage.FailActionRetry,
			NextRunAt: decision.NextRunAt,
		}, nil
	}

	stored.Status = domain.StatusDeadLetter
	s.dlq = append(s.dlq, storage.DeadLetter{
		ID:       stored.ID,
		Queue:    stored.Queue,
		Payload:  stored.Data,
		Error:    failErr.Error(),
		FailedAt: stored.UpdatedAt,
		Attempts: stored.Attempts,
	})

	return &storage.FailResult{Action: storage.FailActionDeadLetter}, nil
}

func (s *Storage) PromoteScheduled(_ context.Context, now time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.scheduled) == 0 {
		return 0, nil
	}

	cut := 0
	for i, job := range s.scheduled {
		if !job.RunAt.After(now) {
			job.Status = domain.StatusPending
			job.UpdatedAt = now
			s.ready[job.Queue] = append(s.ready[job.Queue], job)
			cut = i + 1
			if cut >= limit {
				break
			}
		} else {
			break
		}
	}

	if cut > 0 {
		s.scheduled = s.scheduled[cut:]
	}

	return cut, nil
}

func (s *Storage) RecoverOrphaned(_ context.Context, before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	recovered := 0
	for id, entry := range s.leases {
		if entry.expiresAt.Before(before) {
			delete(s.leases, id)
			entry.job.Status = domain.StatusPending
			entry.job.UpdatedAt = time.Now().UTC()
			s.ready[entry.job.Queue] = append(s.ready[entry.job.Queue], entry.job)
			recovered++
		}
	}
	return recovered, nil
}

func (s *Storage) SaveSchedule(_ context.Context, schedule *domain.JobSchedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := cloneSchedule(schedule)
	stored.UpdatedAt = time.Now().UTC()
	s.schedules[stored.ID] = stored
	return nil
}

func (s *Storage) DeleteSchedule(_ context.Context, scheduleID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.schedules, scheduleID)
	return nil
}

func (s *Storage) LoadActiveSchedules(_ context.Context) ([]*domain.JobSchedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*domain.JobSchedule
	for _, sch := range s.schedules {
		if sch.Status == domain.ScheduleActive || sch.Status == domain.SchedulePaused {
			result = append(result, cloneSchedule(sch))
		}
	}
	return result, nil
}

func (s *Storage) LoadSchedule(_ context.Context, scheduleID string) (*domain.JobSchedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sch, ok := s.schedules[scheduleID]
	if !ok {
		return nil, fmt.Errorf("%w: schedule not found: %s", storage.ErrNotFound, scheduleID)
	}
	return cloneSchedule(sch), nil
}

func (s *Storage) IncrementGeneratedJobs(_ context.Context, scheduleID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sch, ok := s.schedules[scheduleID]
	if !ok {
		return 0, fmt.Errorf("%w: schedule not found: %s", storage.ErrNotFound, scheduleID)
	}
	sch.GeneratedJobs++
	sch.UpdatedAt = time.Now().UTC()
	return sch.GeneratedJobs, nil
}

func (s *Storage) UpdateScheduleStatus(_ context.Context, scheduleID string, status domain.ScheduleStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sch, ok := s.schedules[scheduleID]
	if !ok {
		return fmt.Errorf("%w: schedule not found: %s", storage.ErrNotFound, scheduleID)
	}
	sch.Status = status
	sch.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Storage) UpdateNextRunAt(_ context.Context, scheduleID string, nextRun time.Time, lastRun time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sch, ok := s.schedules[scheduleID]
	if !ok {
		return fmt.Errorf("%w: schedule not found: %s", storage.ErrNotFound, scheduleID)
	}
	sch.LastRunAt = &lastRun
	sch.NextRunAt = &nextRun
	sch.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Storage) ReadyQueues(_ context.Context) ([]domain.QueueName, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]domain.QueueName, 0, len(s.ready))
	for q := range s.ready {
		if len(s.ready[q]) > 0 {
			result = append(result, q)
		}
	}
	return result, nil
}

func (s *Storage) ListDLQ(_ context.Context, queue domain.QueueName) ([]storage.DeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if queue == "" {
		result := make([]storage.DeadLetter, len(s.dlq))
		copy(result, s.dlq)
		return result, nil
	}
	var result []storage.DeadLetter
	for _, dl := range s.dlq {
		if dl.Queue == queue {
			result = append(result, dl)
		}
	}
	return result, nil
}

func (s *Storage) RequeueDLQ(_ context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, dl := range s.dlq {
		if dl.ID == jobID {
			s.dlq = append(s.dlq[:i], s.dlq[i+1:]...)
			job := &domain.Job{
				ID:       dl.ID,
				Queue:    dl.Queue,
				Data:     dl.Payload,
				RunAt:    time.Now().UTC(),
				Status:   domain.StatusPending,
				Attempts: 0,
			}
			s.ready[job.Queue] = append(s.ready[job.Queue], job)
			return nil
		}
	}
	return fmt.Errorf("%w: dead-letter entry not found: %s", storage.ErrNotFound, jobID)
}

func (s *Storage) NextScheduledAt(_ context.Context) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.scheduled) == 0 {
		return time.Time{}, nil
	}
	return s.scheduled[0].RunAt, nil
}

func (s *Storage) DrainQueue(_ context.Context, queue domain.QueueName) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	drained := 0

	if ready := s.ready[queue]; len(ready) > 0 {
		drained += len(ready)
		delete(s.ready, queue)
	}

	filtered := s.scheduled[:0]
	for _, job := range s.scheduled {
		if job.Queue == queue {
			drained++
			continue
		}
		filtered = append(filtered, job)
	}
	s.scheduled = filtered

	return drained, nil
}

func (s *Storage) Stats(_ context.Context) (storage.Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	readyCount := make(map[domain.QueueName]int64, len(s.ready))
	for q, jobs := range s.ready {
		readyCount[q] = int64(len(jobs))
	}
	return storage.Stats{
		ReadyCount:     readyCount,
		ScheduledCount: int64(len(s.scheduled)),
		DLQCount:       int64(len(s.dlq)),
		LeaseCount:     int64(len(s.leases)),
	}, nil
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = nil
	s.scheduled = nil
	s.leases = nil
	s.schedules = nil
	s.dlq = nil
	return nil
}

func (s *Storage) insertScheduled(job *domain.Job) {
	idx := sort.Search(len(s.scheduled), func(i int) bool {
		return s.scheduled[i].RunAt.After(job.RunAt)
	})
	s.scheduled = append(s.scheduled, nil)
	copy(s.scheduled[idx+1:], s.scheduled[idx:])
	s.scheduled[idx] = job
}

func cloneJob(job *domain.Job) *domain.Job {
	c := *job
	if job.Data != nil {
		c.Data = append([]byte(nil), job.Data...)
	}
	if job.Metadata != nil {
		c.Metadata = make(map[string]string, len(job.Metadata))
		for k, v := range job.Metadata {
			c.Metadata[k] = v
		}
	}
	if job.ScheduleID != nil {
		id := *job.ScheduleID
		c.ScheduleID = &id
	}
	return &c
}

func cloneSchedule(schedule *domain.JobSchedule) *domain.JobSchedule {
	c := *schedule
	if schedule.Data != nil {
		c.Data = append([]byte(nil), schedule.Data...)
	}
	if schedule.Metadata != nil {
		c.Metadata = make(map[string]string, len(schedule.Metadata))
		for k, v := range schedule.Metadata {
			c.Metadata[k] = v
		}
	}
	if schedule.LastRunAt != nil {
		t := *schedule.LastRunAt
		c.LastRunAt = &t
	}
	if schedule.NextRunAt != nil {
		t := *schedule.NextRunAt
		c.NextRunAt = &t
	}
	return &c
}
