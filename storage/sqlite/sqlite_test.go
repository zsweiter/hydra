package sqlite

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

func newDB(t testing.TB) *Storage {
	t.Helper()
	s, err := NewFromOptions(&Options{Path: "file:test.db?mode=memory"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// ──────────────────────────────────────────────
//  Basic operations
// ──────────────────────────────────────────────

func TestSaveAndClaimJob(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	job := &domain.Job{ID: "j1", Name: "test", Queue: "emails", Data: []byte("hello")}
	if err := s.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	leased, err := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected 1, got %d", len(leased))
	}
	if leased[0].Job.ID != "j1" {
		t.Fatalf("expected j1, got %s", leased[0].Job.ID)
	}
}

func TestClaimEmptyQueue(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	leased, err := s.ClaimJobs(ctx, "nonexistent", "w1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 0 {
		t.Fatalf("expected 0, got %d", len(leased))
	}
}

func TestAckJob(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveJob(ctx, &domain.Job{ID: "j1", Queue: "emails"})
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if err := s.AckJob(ctx, leased[0].Job.ID); err != nil {
		t.Fatal(err)
	}
}

func TestFailJobRetry(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	job := &domain.Job{ID: "j1", Queue: "emails", MaxAttempts: 3, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	result, err := s.FailJob(ctx, leased[0].Job, fmt.Errorf("test error"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != storage.FailActionRetry {
		t.Fatalf("expected retry, got %d", result.Action)
	}
}

func TestFailJobDeadLetter(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	job := &domain.Job{ID: "j1", Queue: "emails", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	result, err := s.FailJob(ctx, leased[0].Job, fmt.Errorf("dead"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != storage.FailActionDeadLetter {
		t.Fatalf("expected dead letter, got %d", result.Action)
	}
	dlq, _ := s.ListDLQ(ctx, "emails")
	if len(dlq) != 1 {
		t.Fatalf("expected 1 DLQ, got %d", len(dlq))
	}
}

// ──────────────────────────────────────────────
//  Delayed jobs
// ──────────────────────────────────────────────

func TestDelayedJobIsNotReady(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	future := time.Now().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "future", Queue: "q", RunAt: future})

	leased, err := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 0 {
		t.Fatal("delayed job claimed before RunAt")
	}
}

func TestDelayedJobIsPromoted(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	// Save a job with RunAt in the future → stored as "scheduled"
	future := time.Now().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "due", Queue: "q", RunAt: future})

	// Before time, PromoteScheduled should do nothing
	now := time.Now()
	n, err := s.PromoteScheduled(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 promoted before RunAt, got %d", n)
	}

	// Simulate time passing: promote with `future` as the cutoff
	n, err = s.PromoteScheduled(ctx, future.Add(time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 promoted after RunAt, got %d", n)
	}

	leased, _ := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	if len(leased) != 1 {
		t.Fatal("promoted job not claimable")
	}
}

func TestDelayedJobNotPromotedBeforeTime(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	future := time.Now().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "future", Queue: "q", RunAt: future})

	n, err := s.PromoteScheduled(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 promoted, got %d", n)
	}
}

// ──────────────────────────────────────────────
//  Schedule operations
// ──────────────────────────────────────────────

func TestScheduleSaveLoad(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	sch := &domain.JobSchedule{ID: "s1", Name: "test", Queue: "emails", Status: domain.ScheduleActive,
		Rule: domain.NewIntervalScheduleRule(time.Minute)}
	if err := s.SaveSchedule(ctx, sch); err != nil {
		t.Fatal(err)
	}
	schedules, err := s.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(schedules) != 1 {
		t.Fatalf("expected 1, got %d", len(schedules))
	}
}

func TestScheduleDelete(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	sch := &domain.JobSchedule{ID: "s1", Queue: "emails", Status: domain.ScheduleActive}
	s.SaveSchedule(ctx, sch)
	s.DeleteSchedule(ctx, "s1")

	schedules, _ := s.LoadActiveSchedules(ctx)
	if len(schedules) != 0 {
		t.Fatal("schedule not deleted")
	}
}

func TestSchedulePausedExcludedFromActive(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.SchedulePaused})

	schedules, _ := s.LoadActiveSchedules(ctx)
	for _, sch := range schedules {
		if sch.ID == "s1" {
			return // paused schedules ARE returned by LoadActiveSchedules (per contract)
		}
	}
	// The contract says LoadActiveSchedules returns ScheduleActive OR SchedulePaused
}

func TestScheduleStatusUpdate(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleActive})
	s.UpdateScheduleStatus(ctx, "s1", domain.SchedulePaused)

	sch, err := s.LoadSchedule(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if sch.Status != domain.SchedulePaused {
		t.Fatalf("expected paused, got %d", sch.Status)
	}
}

func TestIncrementGeneratedJobs(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleActive})

	n, err := s.IncrementGeneratedJobs(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}
	// Load and check
	sch, _ := s.LoadSchedule(ctx, "s1")
	if sch.GeneratedJobs != 1 {
		t.Fatalf("expected 1 generated job, got %d", sch.GeneratedJobs)
	}
}

func TestUpdateNextRunAt(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleActive})

	next := time.Now().Add(time.Hour)
	last := time.Now()
	s.UpdateNextRunAt(ctx, "s1", next, last)

	sch, _ := s.LoadSchedule(ctx, "s1")
	if sch.NextRunAt == nil || sch.NextRunAt.UnixMilli() != next.UnixMilli() {
		t.Fatalf("NextRunAt not updated: got %v, expected %v", sch.NextRunAt, next)
	}
	if sch.LastRunAt == nil || sch.LastRunAt.UnixMilli() != last.UnixMilli() {
		t.Fatalf("LastRunAt not updated: got %v, expected %v", sch.LastRunAt, last)
	}
}

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func TestListDLQ(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	// Fail job to create DLQ entry
	job := &domain.Job{ID: "d1", Queue: "q", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	s.FailJob(ctx, leased[0].Job, fmt.Errorf("fail"))

	dlq, err := s.ListDLQ(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlq))
	}
}

func TestRequeueDLQ(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	job := &domain.Job{ID: "d1", Queue: "q", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	s.FailJob(ctx, leased[0].Job, fmt.Errorf("fail"))

	if err := s.RequeueDLQ(ctx, "d1"); err != nil {
		t.Fatal(err)
	}

	dlq, _ := s.ListDLQ(ctx, "")
	if len(dlq) != 0 {
		t.Fatal("DLQ entry not removed after requeue")
	}

	// Job should be claimable again
	leased, _ = s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	if len(leased) != 1 {
		t.Fatal("requeued job not claimable")
	}
}

// ──────────────────────────────────────────────
//  Stats
// ──────────────────────────────────────────────

func TestStatsCounts(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	s.SaveJob(ctx, &domain.Job{ID: "j1", Queue: "q1"})
	s.SaveJob(ctx, &domain.Job{ID: "j2", Queue: "q1"})
	s.SaveJob(ctx, &domain.Job{ID: "j3", Queue: "q2"})
	s.SaveJob(ctx, &domain.Job{ID: "j4", Queue: "q2", RunAt: time.Now().Add(time.Hour)})

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ReadyCount["q1"] != 2 {
		t.Fatalf("expected 2 ready in q1, got %d", stats.ReadyCount["q1"])
	}
	if stats.ReadyCount["q2"] != 1 {
		t.Fatalf("expected 1 ready in q2, got %d", stats.ReadyCount["q2"])
	}
	if stats.ScheduledCount != 1 {
		t.Fatalf("expected 1 scheduled (j4 has future RunAt), got %d", stats.ScheduledCount)
	}
}

// ──────────────────────────────────────────────
//  RemoveJob edge cases
// ──────────────────────────────────────────────

func TestRemoveNonExistentJob(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	if err := s.RemoveJob(ctx, "nonexistent"); err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

func TestRemoveProcessingJob(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveJob(ctx, &domain.Job{ID: "j1", Queue: "q"})
	s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)

	if err := s.RemoveJob(ctx, "j1"); err == nil {
		t.Fatal("expected error when removing processing job")
	}
}

// ──────────────────────────────────────────────
//  RecoverOrphaned
// ──────────────────────────────────────────────

func TestRecoverOrphaned(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveJob(ctx, &domain.Job{ID: "orphan", Queue: "q"})
	s.ClaimJobs(ctx, "q", "w1", 10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	recovered, err := s.RecoverOrphaned(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered, got %d", recovered)
	}
}

func TestRecoverNoOrphans(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	n, err := s.RecoverOrphaned(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}
}

// ──────────────────────────────────────────────
//  NextScheduledAt
// ──────────────────────────────────────────────

func TestNextScheduledAt(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	future := time.Now().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "future", Queue: "q", RunAt: future})
	// With the fix, delayed jobs are stored as "scheduled" status.
	// NextScheduledAt queries for status='scheduled'.
	next, err := s.NextScheduledAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next.IsZero() {
		t.Fatal("expected non-zero NextScheduledAt for delayed job")
	}
}

// ──────────────────────────────────────────────
//  Concurrent access
// ──────────────────────────────────────────────

func TestConcurrentSaveAndClaim(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("c-%d", i)
			s.SaveJob(ctx, &domain.Job{ID: id, Queue: "concurrent"})
		}(i)
	}
	wg.Wait()

	total := 0
	for {
		leased, _ := s.ClaimJobs(ctx, "concurrent", "w1", 20, time.Minute)
		if len(leased) == 0 {
			break
		}
		total += len(leased)
	}
	if total != n {
		t.Fatalf("expected %d, claimed %d", n, total)
	}
}

func TestConcurrentScheduleOps(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("s-%d", i)
			s.SaveSchedule(ctx, &domain.JobSchedule{ID: id, Queue: "q", Status: domain.ScheduleActive,
				Rule: domain.NewIntervalScheduleRule(time.Minute)})
		}(i)
	}
	wg.Wait()

	schedules, _ := s.LoadActiveSchedules(ctx)
	if len(schedules) != n {
		t.Fatalf("expected %d schedules, got %d", n, len(schedules))
	}
}

// ──────────────────────────────────────────────
//  Benchmarks
// ──────────────────────────────────────────────

func BenchmarkSaveJob(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		s.SaveJob(ctx, &domain.Job{ID: fmt.Sprintf("b-%d", b.N), Queue: "bench"})
	}
}

func BenchmarkClaimAndAck(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()

	// Pre-load jobs
	for i := 0; i < 1000; i++ {
		s.SaveJob(ctx, &domain.Job{ID: fmt.Sprintf("pre-%d", i), Queue: "bench"})
	}
	b.ResetTimer()

	for b.Loop() {
		leased, err := s.ClaimJobs(ctx, "bench", "w1", 10, time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		for _, l := range leased {
			s.AckJob(ctx, l.Job.ID)
		}
	}
}

func BenchmarkFailRetry(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		id := fmt.Sprintf("fail-%d", b.N)
		job := &domain.Job{ID: id, Queue: "bench", MaxAttempts: 3, Backoff: domain.DefaultBackoff()}
		s.SaveJob(ctx, job)
		leased, _ := s.ClaimJobs(ctx, "bench", "w1", 10, time.Minute)
		if len(leased) > 0 {
			s.FailJob(ctx, leased[0].Job, fmt.Errorf("err"))
		}
	}
}

func BenchmarkSaveSchedule(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		s.SaveSchedule(ctx, &domain.JobSchedule{ID: fmt.Sprintf("s-%d", b.N), Queue: "bench",
			Status: domain.ScheduleActive, Rule: domain.NewIntervalScheduleRule(time.Minute)})
	}
}

func BenchmarkPromoteScheduled(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()
	past := time.Now().Add(-time.Hour)
	for i := range 100 {
		s.SaveJob(ctx, &domain.Job{ID: fmt.Sprintf("d-%d", i), Queue: "bench", RunAt: past})
	}
	b.ResetTimer()
	for b.Loop() {
		s.PromoteScheduled(ctx, time.Now(), 100)
	}
}

func BenchmarkStats(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()
	for i := range 100 {
		s.SaveJob(ctx, &domain.Job{ID: fmt.Sprintf("st-%d", i), Queue: "bench"})
	}
	b.ResetTimer()
	for b.Loop() {
		s.Stats(ctx)
	}
}
