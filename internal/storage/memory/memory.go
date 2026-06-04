package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zsweiter/hydra/internal/domain"
	"github.com/zsweiter/hydra/internal/storage"
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
	if job.RunAt.After(time.Now()) {
		s.insertScheduled(job)
		return nil
	}
	s.ready[job.Queue] = append(s.ready[job.Queue], job)
	return nil
}

func (s *Storage) RemoveJob(_ context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, leased := s.leases[jobID]; leased {
		return fmt.Errorf("cannot remove processing job: %s", jobID)
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

	return fmt.Errorf("job not found: %s", jobID)
}

func (s *Storage) ClaimJobs(_ context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]storage.LeasedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ready := s.ready[queue]
	if len(ready) == 0 {
		return nil, nil
	}

	batch := n
	if batch > len(ready) {
		batch = len(ready)
	}

	jobs := make([]*domain.Job, batch)
	copy(jobs, ready[:batch])

	if batch == len(ready) {
		delete(s.ready, queue)
	} else {
		s.ready[queue] = ready[batch:]
	}

	result := make([]storage.LeasedJob, batch)
	now := time.Now()
	for i, job := range jobs {
		job.Status = domain.StatusProcessing
		job.UpdatedAt = now
		s.leases[job.ID] = leaseEntry{
			job:       job,
			workerID:  workerID,
			expiresAt: now.Add(ttl),
		}
		result[i] = storage.LeasedJob{Job: job}
	}

	return result, nil
}

func (s *Storage) AckJob(_ context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.leases, jobID)
	return nil
}

func (s *Storage) FailJob(_ context.Context, job *domain.Job, failErr error) (storage.FailResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.leases, job.ID)

	job.Attempts++
	job.UpdatedAt = time.Now().UTC()

	if job.IsRetryable() {
		delay := job.Backoff.NextDelay(job.Attempts)
		nextRun := time.Now().Add(delay)
		job.RunAt = nextRun
		job.Status = domain.StatusPending
		s.insertScheduled(job)
		return storage.FailResult{
			Action:    storage.FailActionRetry,
			NextRunAt: nextRun,
		}, nil
	}

	job.Status = domain.StatusDeadLetter
	s.dlq = append(s.dlq, storage.DeadLetter{
		ID:       job.ID,
		Queue:    job.Queue,
		Payload:  job.Data,
		Error:    failErr.Error(),
		FailedAt: time.Now(),
		Attempts: job.Attempts,
	})
	return storage.FailResult{Action: storage.FailActionDeadLetter}, nil
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
	schedule.UpdatedAt = time.Now().UTC()
	s.schedules[schedule.ID] = schedule
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
			copy := *sch
			result = append(result, &copy)
		}
	}
	return result, nil
}

func (s *Storage) LoadSchedule(_ context.Context, scheduleID string) (*domain.JobSchedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sch, ok := s.schedules[scheduleID]
	if !ok {
		return nil, fmt.Errorf("schedule not found: %s", scheduleID)
	}
	copy := *sch
	return &copy, nil
}

func (s *Storage) IncrementGeneratedJobs(_ context.Context, scheduleID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sch, ok := s.schedules[scheduleID]
	if !ok {
		return 0, fmt.Errorf("schedule not found: %s", scheduleID)
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
		return fmt.Errorf("schedule not found: %s", scheduleID)
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
		return fmt.Errorf("schedule not found: %s", scheduleID)
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
				RunAt:    time.Now(),
				Status:   domain.StatusPending,
				Attempts: 0,
			}
			s.ready[job.Queue] = append(s.ready[job.Queue], job)
			return nil
		}
	}
	return fmt.Errorf("dead-letter entry not found: %s", jobID)
}

func (s *Storage) NextScheduledAt(_ context.Context) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.scheduled) == 0 {
		return time.Time{}, nil
	}
	return s.scheduled[0].RunAt, nil
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
