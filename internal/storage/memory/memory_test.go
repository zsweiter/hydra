package memory

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

func TestRemoveJobErrors(t *testing.T) {
	s := New()
	ctx := context.Background()

	if err := s.RemoveJob(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: "j1", Queue: "q"})
	s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	if err := s.RemoveJob(ctx, "j1"); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestScheduleErrors(t *testing.T) {
	s := New()
	ctx := context.Background()

	if _, err := s.LoadSchedule(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if err := s.UpdateScheduleStatus(ctx, "missing", domain.SchedulePaused); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if err := s.RequeueDLQ(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSaveAndClaimJob(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{Status: domain.StatusPending,
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

	job := &domain.Job{Status: domain.StatusPending, ID: "job-1", Queue: "emails"}
	s.SaveJob(ctx, job)

	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if err := s.AckJob(ctx, leased[0].Job.ID); err != nil {
		t.Fatal(err)
	}
}

func TestFailJobRetry(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{Status: domain.StatusPending,
		ID:          "job-1",
		Queue:       "emails",
		MaxAttempts: 3,
		Backoff:     domain.DefaultBackoff(),
	}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	decision := domain.ComputeFailDecision(leased[0].Job, time.Now().UTC())
	result, err := s.FailJob(ctx, leased[0].Job, assertErr{}, decision)
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

	job := &domain.Job{Status: domain.StatusPending,
		ID:          "job-1",
		Queue:       "emails",
		MaxAttempts: 1,
		Attempts:    1,
		Backoff:     domain.DefaultBackoff(),
	}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	decision := domain.ComputeFailDecision(leased[0].Job, time.Now().UTC())
	result, err := s.FailJob(ctx, leased[0].Job, assertErr{}, decision)
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

	due := time.Now().UTC().Add(-time.Second)

	// Create a job with RunAt in the future so it goes to scheduled
	job := &domain.Job{ID: "due-soon", Queue: "emails", RunAt: time.Now().UTC().Add(time.Hour), Status: domain.StatusScheduled}
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

	promoted, err := s.PromoteScheduled(ctx, time.Now().UTC(), 10)
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

	job := &domain.Job{Status: domain.StatusPending, ID: "orphan", Queue: "emails"}
	s.SaveJob(ctx, job)
	s.ClaimJobs(ctx, "emails", "w1", 10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	recovered, err := s.RecoverOrphaned(ctx, time.Now().UTC())
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

	s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: "j1", Queue: "emails"})
	s.SaveJob(ctx, &domain.Job{ID: "j2", Queue: "emails", RunAt: time.Now().UTC().Add(time.Hour), Status: domain.StatusScheduled})

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

// Hydra is a time-driven scheduler: every timestamp the storage
// produces must be in UTC so scheduling math stays timezone-stable.

func assertUTCLocation(t *testing.T, field string, ts time.Time) {
	t.Helper()

	if ts.Location() != time.UTC {
		t.Fatalf("%s location = %v, want UTC", field, ts.Location())
	}
}

func TestClaimedJobTimestampsAreUTC(t *testing.T) {
	s := New()
	ctx := context.Background()

	job := &domain.Job{Status: domain.StatusPending, ID: "job-1", Queue: "emails", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	if err := s.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	leased, err := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected 1 leased job, got %d", len(leased))
	}

	assertUTCLocation(t, "job.UpdatedAt after claim", leased[0].Job.UpdatedAt)
}

func TestRetryAndDLQTimesAreUTC(t *testing.T) {
	ctx := context.Background()

	t.Run("retry", func(t *testing.T) {
		s := New()
		job := &domain.Job{Status: domain.StatusPending, ID: "job-1", Queue: "emails", MaxAttempts: 3, Backoff: domain.DefaultBackoff()}
		s.SaveJob(ctx, job)
		leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

		decision := domain.ComputeFailDecision(leased[0].Job, time.Now().UTC())
		result, err := s.FailJob(ctx, leased[0].Job, assertErr{}, decision)
		if err != nil {
			t.Fatal(err)
		}
		if result.Action != storage.FailActionRetry {
			t.Fatalf("expected retry action, got %d", result.Action)
		}

		assertUTCLocation(t, "FailResult.NextRunAt", result.NextRunAt)
		assertUTCLocation(t, "job.RunAt after retry", leased[0].Job.RunAt)
	})

	t.Run("dead letter", func(t *testing.T) {
		s := New()
		job := &domain.Job{Status: domain.StatusPending, ID: "job-2", Queue: "emails", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
		s.SaveJob(ctx, job)
		leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

		decision := domain.ComputeFailDecision(leased[0].Job, time.Now().UTC())
		result, err := s.FailJob(ctx, leased[0].Job, assertErr{}, decision)
		if err != nil {
			t.Fatal(err)
		}
		if result.Action != storage.FailActionDeadLetter {
			t.Fatalf("expected dead-letter action, got %d", result.Action)
		}

		dlq, _ := s.ListDLQ(ctx, "emails")
		if len(dlq) != 1 {
			t.Fatalf("expected 1 DLQ entry, got %d", len(dlq))
		}

		assertUTCLocation(t, "DLQ FailedAt", dlq[0].FailedAt)
	})
}

func TestDrainQueue(t *testing.T) {
	s := New()
	ctx := context.Background()

	now := time.Now().UTC()
	future := now.Add(time.Hour)

	for i := 0; i < 3; i++ {
		if err := s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: fmt.Sprintf("ready-%d", i), Queue: "q1", RunAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := s.SaveJob(ctx, &domain.Job{ID: fmt.Sprintf("sched-%d", i), Queue: "q1", RunAt: future, Status: domain.StatusScheduled}); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: "other-ready", Queue: "q2", RunAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveJob(ctx, &domain.Job{ID: "other-sched", Queue: "q2", RunAt: future, Status: domain.StatusScheduled}); err != nil {
		t.Fatal(err)
	}

	n, err := s.DrainQueue(ctx, "q1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("expected 5 jobs drained, got %d", n)
	}

	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.ReadyCount["q1"]; got != 0 {
		t.Fatalf("expected q1 ready drained, got %d", got)
	}
	if got := st.ReadyCount["q2"]; got != 1 {
		t.Fatalf("expected q2 untouched, got %d", got)
	}
	if got := st.ScheduledCount; got != 1 {
		t.Fatalf("expected only q2 scheduled remaining, got %d", got)
	}
}

func TestSaveJobStatusIsAuthoritative(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.SaveJob(ctx, &domain.Job{ID: "deferred", Queue: "emails", RunAt: time.Now().UTC(), Status: domain.StatusScheduled})
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if len(leased) != 0 {
		t.Fatal("scheduled job claimed before promotion")
	}

	s.SaveJob(ctx, &domain.Job{ID: "eager", Queue: "emails", RunAt: time.Now().UTC().Add(time.Hour), Status: domain.StatusPending})
	leased, _ = s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if len(leased) != 1 || leased[0].Job.ID != "eager" {
		t.Fatal("pending job not claimable")
	}
}
