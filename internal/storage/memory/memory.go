package memory

import (
	"context"
	"fmt"
	"slices"
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
	paused    map[domain.QueueName]bool
	instances map[string]time.Time

	// jobs indexes every job by ID, including completed and
	// dead-lettered ones, for history and requeueing.
	jobs map[string]*domain.Job
}

var _ storage.Storage = (*Storage)(nil)

// New creates a new in-memory storage.
func New() *Storage {
	return &Storage{
		ready:     make(map[domain.QueueName][]*domain.Job),
		leases:    make(map[string]leaseEntry),
		schedules: make(map[string]*domain.JobSchedule),
		paused:    make(map[domain.QueueName]bool),
		instances: make(map[string]time.Time),
		jobs:      make(map[string]*domain.Job),
	}
}

func (s *Storage) SaveJob(_ context.Context, job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.saveJobLocked(job)
	return nil
}

func (s *Storage) saveJobLocked(job *domain.Job) {
	if s.ready == nil {
		s.ready = make(map[domain.QueueName][]*domain.Job)
	}
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	s.jobs[job.ID] = job
	if job.RunAt.After(time.Now()) {
		s.insertScheduled(job)
		return
	}
	s.ready[job.Queue] = append(s.ready[job.Queue], job)
}

func (s *Storage) ClaimOccurrences(_ context.Context, claims []storage.Claim) ([]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]bool, len(claims))
	for i, c := range claims {
		result[i] = s.claimLocked(c)
	}
	return result, nil
}

func (s *Storage) claimLocked(c storage.Claim) bool {
	sch, ok := s.schedules[c.ScheduleID]
	// An unset NextRunAt (schedule saved before it was ever
	// computed) is claimable once; the claim then sets it.
	if !ok || sch.Status != domain.ScheduleActive ||
		(sch.NextRunAt != nil && !sch.NextRunAt.Equal(c.Expected)) {
		return false
	}

	now := time.Now().UTC()
	sch.LastRunAt = &now
	sch.GeneratedJobs += c.Generated
	if c.Next.IsZero() {
		sch.NextRunAt = nil
	} else {
		next := c.Next
		sch.NextRunAt = &next
	}
	if c.Next.IsZero() || (sch.MaxJobs > 0 && sch.GeneratedJobs >= sch.MaxJobs) {
		sch.Status = domain.ScheduleFinished
		sch.UpdatedAt = now
	}

	for _, job := range c.Jobs {
		s.saveJobLocked(job)
	}
	return true
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

	batch := min(n, len(ready))

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

func (s *Storage) AckJobs(_ context.Context, jobIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range jobIDs {
		delete(s.leases, id)
		if job, ok := s.jobs[id]; ok {
			job.UpdateStatus(domain.StatusCompleted)
		}
	}

	return nil
}

func (s *Storage) ExtendLease(_ context.Context, jobID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.leases[jobID]
	if !ok {
		return fmt.Errorf("lease not found: %s", jobID)
	}

	entry.expiresAt = time.Now().Add(ttl)
	s.leases[jobID] = entry
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

func (s *Storage) SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error {
	return storage.SaveOne(ctx, s, schedule)
}

func (s *Storage) SaveSchedules(_ context.Context, schedules []*domain.JobSchedule) ([]error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	errs := make([]error, len(schedules))
	for i, schedule := range schedules {
		if prev, ok := s.schedules[schedule.ID]; ok && schedule.Version > 0 && prev.Version >= schedule.Version {
			errs[i] = storage.ErrStaleVersion
			continue
		}

		schedule.UpdatedAt = time.Now().UTC()
		if schedule.CreatedAt.IsZero() {
			schedule.CreatedAt = schedule.UpdatedAt
		}
		stored := *schedule
		s.schedules[schedule.ID] = &stored
	}

	return errs, nil
}

func (s *Storage) DeleteSchedule(_ context.Context, scheduleID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.schedules, scheduleID)
	return nil
}

func (s *Storage) LoadDueSchedules(_ context.Context, q storage.DueQuery) ([]*domain.JobSchedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*domain.JobSchedule
	for _, sch := range s.schedules {
		if sch.Status != domain.ScheduleActive ||
			(sch.NextRunAt != nil && sch.NextRunAt.After(q.Before)) ||
			(!q.After.IsZero() && (sch.NextRunAt == nil || !sch.NextRunAt.After(q.After))) ||
			(!q.ChangedSince.IsZero() && !sch.UpdatedAt.After(q.ChangedSince)) ||
			(q.Shards != nil && !slices.Contains(q.Shards, domain.ScheduleShard(sch.ID))) {
			continue
		}
		copy := *sch
		copy.Data, copy.Metadata = nil, nil
		result = append(result, &copy)
	}

	sort.Slice(result, func(i, j int) bool {
		a, b := result[i].NextRunAt, result[j].NextRunAt
		switch {
		case a == nil || b == nil:
			return a == nil && b != nil
		case !a.Equal(*b):
			return a.Before(*b)
		default:
			return result[i].ID < result[j].ID
		}
	})
	return page(result, 0, q.Limit), nil
}

func (s *Storage) LoadSchedules(_ context.Context, ids []string) ([]*domain.JobSchedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*domain.JobSchedule, 0, len(ids))
	for _, id := range ids {
		if sch, ok := s.schedules[id]; ok {
			copy := *sch
			result = append(result, &copy)
		}
	}
	return result, nil
}

func (s *Storage) PurgeHistory(_ context.Context, before time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs, schedules := 0, 0
	for id, job := range s.jobs {
		if jobs == limit {
			break
		}
		if job.Status == domain.StatusCompleted && job.UpdatedAt.Before(before) {
			delete(s.jobs, id)
			jobs++
		}
	}
	for id, sch := range s.schedules {
		if schedules == limit {
			break
		}
		if isHistory(sch.Status) && sch.UpdatedAt.Before(before) {
			delete(s.schedules, id)
			schedules++
		}
	}
	return jobs + schedules, nil
}

func isHistory(st domain.ScheduleStatus) bool {
	return st == domain.ScheduleFinished || st == domain.ScheduleCancelled || st == domain.ScheduleArchived
}

func (s *Storage) Heartbeat(_ context.Context, instanceID string, ttl time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if ttl > 0 {
		s.instances[instanceID] = now.Add(ttl)
	} else {
		delete(s.instances, instanceID)
	}

	live := make([]string, 0, len(s.instances))
	for id, expires := range s.instances {
		if expires.After(now) {
			live = append(live, id)
		} else {
			delete(s.instances, id)
		}
	}
	slices.Sort(live)
	return live, nil
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

			// Requeue the original job so its metadata survives.
			job, ok := s.jobs[dl.ID]
			if !ok {
				job = &domain.Job{ID: dl.ID, Queue: dl.Queue, Data: dl.Payload}
				s.jobs[job.ID] = job
			}
			job.Attempts = 0
			job.RunAt = time.Now()
			job.UpdateStatus(domain.StatusPending)
			s.ready[job.Queue] = append(s.ready[job.Queue], job)
			return nil
		}
	}
	return fmt.Errorf("dead-letter entry not found: %s", jobID)
}

func (s *Storage) ListSchedules(_ context.Context, q storage.ScheduleQuery) ([]*domain.JobSchedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []*domain.JobSchedule
	for _, sch := range s.schedules {
		if (q.Queue == "" || sch.Queue == q.Queue) &&
			(len(q.Statuses) == 0 || slices.Contains(q.Statuses, sch.Status)) {
			copy := *sch
			matched = append(matched, &copy)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].UpdatedAt.Equal(matched[j].UpdatedAt) {
			return matched[i].ID < matched[j].ID
		}
		return matched[i].UpdatedAt.After(matched[j].UpdatedAt)
	})
	return page(matched, q.Offset, q.Limit), nil
}

func (s *Storage) ListJobs(_ context.Context, q storage.JobQuery) ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	before := q.Before
	if before.IsZero() {
		before = time.Now().Add(time.Millisecond)
	}

	var matched []*domain.Job
	for _, job := range s.jobs {
		if job.ScheduleID != nil && *job.ScheduleID == q.ScheduleID &&
			job.UpdatedAt.Before(before) &&
			(len(q.Statuses) == 0 || slices.Contains(q.Statuses, job.Status)) {
			copy := *job
			matched = append(matched, &copy)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].UpdatedAt.After(matched[j].UpdatedAt) })
	return page(matched, 0, q.Limit), nil
}

func (s *Storage) SetQueuePaused(_ context.Context, queue domain.QueueName, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if paused {
		s.paused[queue] = true
	} else {
		delete(s.paused, queue)
	}
	return nil
}

func (s *Storage) PausedQueues(_ context.Context) ([]domain.QueueName, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]domain.QueueName, 0, len(s.paused))
	for q := range s.paused {
		result = append(result, q)
	}
	return result, nil
}

func page[T any](items []T, offset, limit int) []T {
	if offset >= len(items) {
		return nil
	}
	items = items[offset:]
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return items
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
