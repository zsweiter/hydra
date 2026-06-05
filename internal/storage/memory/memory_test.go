package memory

import (
	"context"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
)

func TestSaveAndClaimJob(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{
		ID:    "job-1",
		Name:  "test",
		Queue: "emails",
		Data:  []byte("hello"),
	}

	if err := s.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	leased, err := s.ClaimJobs(ctx, "emails", "worker-1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected 1 leased job, got %d", len(leased))
	}
	if leased[0].Job.ID != "job-1" {
		t.Fatalf("expected job-1, got %s", leased[0].Job.ID)
	}
}

func TestClaimEmptyQueue(t *testing.T) {
	s := New()
	ctx := context.Background()

	leased, err := s.ClaimJobs(ctx, "emails", "worker-1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 0 {
		t.Fatalf("expected 0 jobs, got %d", len(leased))
	}
}

func TestAckJob(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{ID: "job-1", Queue: "emails"}
	s.SaveJob(ctx, job)

	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if err := s.AckJob(ctx, leased[0].Job.ID); err != nil {
		t.Fatal(err)
	}
}

func TestFailJobRetry(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{
		ID:          "job-1",
		Queue:       "emails",
		MaxAttempts: 3,
		Backoff:     domain.DefaultBackoff(),
	}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	result, err := s.FailJob(ctx, leased[0].Job, assertErr{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != 0 {
		t.Fatalf("expected retry action, got %d", result.Action)
	}
}

func TestFailJobDeadLetter(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{
		ID:          "job-1",
		Queue:       "emails",
		MaxAttempts: 1,
		Attempts:    1,
		Backoff:     domain.DefaultBackoff(),
	}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	result, err := s.FailJob(ctx, leased[0].Job, assertErr{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != 1 {
		t.Fatalf("expected dead-letter action, got %d", result.Action)
	}

	dlq, _ := s.ListDLQ(ctx, "emails")
	if len(dlq) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlq))
	}
}

func TestPromoteScheduled(t *testing.T) {
	s := New()
	ctx := context.Background()

	due := time.Now().Add(-time.Second)

	// Create a job with RunAt in the future so it goes to scheduled
	job := &domain.Job{ID: "due-soon", Queue: "emails", RunAt: time.Now().Add(time.Hour)}
	s.SaveJob(ctx, job)

	stats, _ := s.Stats(ctx)
	if stats.ScheduledCount != 1 {
		t.Fatalf("expected 1 scheduled, got %d", stats.ScheduledCount)
	}

	// Hack: make it due by claiming it and failing (to put it in scheduled with past time)
	// Instead, directly use the fact that PromoteScheduled checks RunAt <= now
	s.mu.Lock()
	s.scheduled[0].RunAt = due
	s.mu.Unlock()

	promoted, err := s.PromoteScheduled(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 1 {
		t.Fatalf("expected 1 promoted, got %d", promoted)
	}

	// Verify it's now in ready queue
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if len(leased) != 1 {
		t.Fatalf("expected 1 ready job, got %d", len(leased))
	}
}

func TestRecoverOrphaned(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{ID: "orphan", Queue: "emails"}
	s.SaveJob(ctx, job)
	s.ClaimJobs(ctx, "emails", "w1", 10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	recovered, err := s.RecoverOrphaned(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered, got %d", recovered)
	}
}

func TestSaveAndLoadSchedule(t *testing.T) {
	s := New()
	ctx := context.Background()

	sch := &domain.JobSchedule{
		ID:     "sched-1",
		Name:   "test-schedule",
		Queue:  "emails",
		Status: domain.ScheduleActive,
		Rule:   domain.NewIntervalScheduleRule(time.Minute),
	}
	if err := s.SaveSchedule(ctx, sch); err != nil {
		t.Fatal(err)
	}

	schedules, err := s.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(schedules))
	}
}

func TestDeleteSchedule(t *testing.T) {
	s := New()
	ctx := context.Background()

	sch := &domain.JobSchedule{ID: "sched-1", Status: domain.ScheduleActive}
	s.SaveSchedule(ctx, sch)
	s.DeleteSchedule(ctx, "sched-1")

	schedules, _ := s.LoadActiveSchedules(ctx)
	if len(schedules) != 0 {
		t.Fatalf("expected 0 schedules after delete")
	}
}

func TestStats(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.SaveJob(ctx, &domain.Job{ID: "j1", Queue: "emails"})
	s.SaveJob(ctx, &domain.Job{ID: "j2", Queue: "emails", RunAt: time.Now().Add(time.Hour)})

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ReadyCount["emails"] != 1 {
		t.Fatalf("expected 1 ready, got %d", stats.ReadyCount["emails"])
	}
	if stats.ScheduledCount != 1 {
		t.Fatalf("expected 1 scheduled, got %d", stats.ScheduledCount)
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "test error" }
