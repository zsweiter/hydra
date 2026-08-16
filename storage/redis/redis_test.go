package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

func testClient(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 9})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", addr, err)
	}
	t.Cleanup(func() { rdb.FlushDB(context.Background()); rdb.Close() })
	return rdb
}

func newDB(t testing.TB) *Storage {
	t.Helper()
	return Use(testClient(t))
}

func TestRemoveJobSentinels(t *testing.T) {
	s := newDB(t)
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

func TestLoadScheduleNotFound(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	if _, err := s.LoadSchedule(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestRequeueDLQNotFound(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	if err := s.RequeueDLQ(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func failJob(t testing.TB, s *Storage, ctx context.Context, job *domain.Job, failErr error) (*storage.FailResult, error) {
	t.Helper()
	decision := domain.ComputeFailDecision(job, time.Now().UTC())
	return s.FailJob(ctx, job, failErr, decision)
}

// ──────────────────────────────────────────────
//  Basic operations
// ──────────────────────────────────────────────

func TestSaveAndClaimJob(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	job := &domain.Job{Status: domain.StatusPending, ID: "j1", Name: "test", Queue: "emails", Data: []byte("hello")}
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
	s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: "j1", Queue: "emails"})
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if err := s.AckJob(ctx, leased[0].Job.ID); err != nil {
		t.Fatal(err)
	}

	status, err := s.rdb.HGet(ctx, jobKey("j1"), "status").Result()
	if err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("expected job marked completed, got %q", status)
	}

	released, err := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(released) != 0 {
		t.Fatalf("completed job claimed again, got %d", len(released))
	}
}

func TestFailJobRetry(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	job := &domain.Job{Status: domain.StatusPending, ID: "j1", Queue: "emails", MaxAttempts: 3, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	result, err := failJob(t, s, ctx, leased[0].Job, fmt.Errorf("test error"))
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
	job := &domain.Job{Status: domain.StatusPending, ID: "j1", Queue: "emails", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "emails", "w1", 10, time.Minute)

	result, err := failJob(t, s, ctx, leased[0].Job, fmt.Errorf("dead"))
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

	future := time.Now().UTC().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "future", Queue: "q", RunAt: future, Status: domain.StatusScheduled})

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

	future := time.Now().UTC().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "due", Queue: "q", RunAt: future, Status: domain.StatusScheduled})

	n, err := s.PromoteScheduled(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 promoted before RunAt, got %d", n)
	}

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

	future := time.Now().UTC().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "future", Queue: "q", RunAt: future, Status: domain.StatusScheduled})

	n, err := s.PromoteScheduled(ctx, time.Now().UTC(), 10)
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
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleActive})
	s.DeleteSchedule(ctx, "s1")

	schedules, _ := s.LoadActiveSchedules(ctx)
	if len(schedules) != 0 {
		t.Fatal("schedule not deleted")
	}
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

func TestScheduleEnumsPersistedAsText(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	sch := &domain.JobSchedule{
		ID:            "s1",
		Queue:         "q",
		Status:        domain.SchedulePaused,
		MisfirePolicy: domain.MisfireCatchUp,
		Rule:          domain.NewIntervalScheduleRule(time.Minute),
	}
	if err := s.SaveSchedule(ctx, sch); err != nil {
		t.Fatal(err)
	}

	status, err := s.rdb.HGet(ctx, scheduleKey("s1"), "status").Result()
	if err != nil {
		t.Fatal(err)
	}
	if status != "paused" {
		t.Fatalf("status persisted as %q, want %q", status, "paused")
	}

	misfire, err := s.rdb.HGet(ctx, scheduleKey("s1"), "misfire_policy").Result()
	if err != nil {
		t.Fatal(err)
	}
	if misfire != "catch_up" {
		t.Fatalf("misfire_policy persisted as %q, want %q", misfire, "catch_up")
	}
}

func TestJobBackoffTypePersistedAsText(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	job := &domain.Job{Status: domain.StatusPending,
		ID:    "j1",
		Queue: "q",
		Backoff: domain.BackoffPolicy{
			Type:   domain.BackoffExponential,
			BaseMs: 100,
			MaxMs:  1000,
		},
	}
	if err := s.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	bt, err := s.rdb.HGet(ctx, jobKey("j1"), "backoff_type").Result()
	if err != nil {
		t.Fatal(err)
	}
	if bt != "exponential" {
		t.Fatalf("backoff_type persisted as %q, want %q", bt, "exponential")
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
}

func TestUpdateNextRunAt(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleActive})

	next := time.Now().UTC().Add(time.Hour)
	last := time.Now().UTC()
	s.UpdateNextRunAt(ctx, "s1", next, last)

	sch, _ := s.LoadSchedule(ctx, "s1")
	if sch.NextRunAt == nil || sch.NextRunAt.UnixMilli() != next.UnixMilli() {
		t.Fatalf("NextRunAt not updated: got %v, expected %v", sch.NextRunAt, next)
	}
}

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func TestListDLQ(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	job := &domain.Job{Status: domain.StatusPending, ID: "d1", Queue: "q", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	failJob(t, s, ctx, leased[0].Job, fmt.Errorf("fail"))

	dlq, err := s.ListDLQ(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 1 {
		t.Fatalf("expected 1 DLQ, got %d", len(dlq))
	}
}

func TestRequeueDLQ(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	job := &domain.Job{Status: domain.StatusPending, ID: "d1", Queue: "q", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	failJob(t, s, ctx, leased[0].Job, fmt.Errorf("fail"))

	if err := s.RequeueDLQ(ctx, "d1"); err != nil {
		t.Fatal(err)
	}
	dlq, _ := s.ListDLQ(ctx, "")
	if len(dlq) != 0 {
		t.Fatal("DLQ not removed after requeue")
	}
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
	s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: "j1", Queue: "q1"})
	s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: "j2", Queue: "q1"})

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ReadyCount["q1"] != 2 {
		t.Fatalf("expected 2 ready, got %d", stats.ReadyCount["q1"])
	}
}

// ──────────────────────────────────────────────
//  RecoverOrphaned
// ──────────────────────────────────────────────

func TestRecoverOrphaned(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: "orphan", Queue: "q"})
	s.ClaimJobs(ctx, "q", "w1", 10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	recovered, err := s.RecoverOrphaned(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1, got %d", recovered)
	}
}

func TestRecoverNoOrphans(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()
	n, err := s.RecoverOrphaned(ctx, time.Now().UTC())
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
	future := time.Now().UTC().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "future", Queue: "q", RunAt: future, Status: domain.StatusScheduled})

	next, err := s.NextScheduledAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next.IsZero() {
		t.Fatal("expected non-zero NextScheduledAt")
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
			s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: id, Queue: "concurrent"})
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
	for i := range n {
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
		t.Fatalf("expected %d, got %d", n, len(schedules))
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
		s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: fmt.Sprintf("b-%d", b.N), Queue: "bench"})
	}
}

func BenchmarkClaimAndAck(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()
	for i := range 1000 {
		s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: fmt.Sprintf("pre-%d", i), Queue: "bench"})
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
		job := &domain.Job{Status: domain.StatusPending, ID: id, Queue: "bench", MaxAttempts: 3, Backoff: domain.DefaultBackoff()}
		s.SaveJob(ctx, job)
		leased, _ := s.ClaimJobs(ctx, "bench", "w1", 10, time.Minute)
		if len(leased) > 0 {
			failJob(b, s, ctx, leased[0].Job, fmt.Errorf("err"))
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
	past := time.Now().UTC().Add(-time.Hour)
	for i := range 100 {
		s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: fmt.Sprintf("d-%d", i), Queue: "bench", RunAt: past})
	}
	b.ResetTimer()
	for b.Loop() {
		s.PromoteScheduled(ctx, time.Now().UTC(), 100)
	}
}

func BenchmarkStats(b *testing.B) {
	s := newDB(b)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		s.SaveJob(ctx, &domain.Job{Status: domain.StatusPending, ID: fmt.Sprintf("st-%d", i), Queue: "bench"})
	}
	b.ResetTimer()
	for b.Loop() {
		s.Stats(ctx)
	}
}

// Hydra is a time-driven scheduler: timestamps reconstructed from
// storage must be UTC so scheduling math stays timezone-stable.

func assertUTCLocation(t *testing.T, field string, ts time.Time) {
	t.Helper()

	if ts.Location() != time.UTC {
		t.Fatalf("%s location = %v, want UTC", field, ts.Location())
	}
}

func TestNextScheduledAtIsUTC(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	runAt := time.Now().UTC().Add(time.Hour)
	if err := s.SaveJob(ctx, &domain.Job{ID: "future", Queue: "q", RunAt: runAt, Status: domain.StatusScheduled}); err != nil {
		t.Fatal(err)
	}

	next, err := s.NextScheduledAt(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if next.IsZero() {
		t.Fatal("expected a scheduled job")
	}

	assertUTCLocation(t, "NextScheduledAt", next)
}

func TestLoadScheduleTimestampsAreUTC(t *testing.T) {
	s := newDB(t)
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

	loaded, err := s.LoadSchedule(ctx, "sched-1")
	if err != nil {
		t.Fatal(err)
	}

	assertUTCLocation(t, "CreatedAt", loaded.CreatedAt)
	assertUTCLocation(t, "UpdatedAt", loaded.UpdatedAt)
}

func TestDrainQueue(t *testing.T) {
	s := newDB(t)
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
	s := newDB(t)
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
