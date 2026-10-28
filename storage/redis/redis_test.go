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
	// Tests flush their database, so they only run against a
	// server named explicitly (make test-integration does).
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
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
	if err := s.AckJobs(ctx, []string{leased[0].Job.ID}); err != nil {
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

	future := time.Now().Add(time.Hour)
	s.SaveJob(ctx, &domain.Job{ID: "due", Queue: "q", RunAt: future})

	n, err := s.PromoteScheduled(ctx, time.Now(), 10)
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
	schedules, err := s.ListSchedules(ctx, storage.ScheduleQuery{Statuses: []domain.ScheduleStatus{domain.ScheduleActive, domain.SchedulePaused}})
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

	schedules, _ := s.ListSchedules(ctx, storage.ScheduleQuery{Statuses: []domain.ScheduleStatus{domain.ScheduleActive, domain.SchedulePaused}})
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

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func TestListDLQ(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	job := &domain.Job{ID: "d1", Queue: "q", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	s.FailJob(ctx, leased[0].Job, fmt.Errorf("fail"))

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
	job := &domain.Job{ID: "d1", Queue: "q", MaxAttempts: 1, Attempts: 1, Backoff: domain.DefaultBackoff()}
	s.SaveJob(ctx, job)
	leased, _ := s.ClaimJobs(ctx, "q", "w1", 10, time.Minute)
	s.FailJob(ctx, leased[0].Job, fmt.Errorf("fail"))

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
	s.SaveJob(ctx, &domain.Job{ID: "j1", Queue: "q1"})
	s.SaveJob(ctx, &domain.Job{ID: "j2", Queue: "q1"})

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
	s.SaveJob(ctx, &domain.Job{ID: "orphan", Queue: "q"})
	s.ClaimJobs(ctx, "q", "w1", 10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	recovered, err := s.RecoverOrphaned(ctx, time.Now())
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

	schedules, _ := s.ListSchedules(ctx, storage.ScheduleQuery{Statuses: []domain.ScheduleStatus{domain.ScheduleActive, domain.SchedulePaused}})
	if len(schedules) != n {
		t.Fatalf("expected %d, got %d", n, len(schedules))
	}
}

func TestClaimOccurrence(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	first := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	second := first.Add(time.Hour)
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleActive, MaxJobs: 2, NextRunAt: &first})

	job := func(id string) []*domain.Job {
		return []*domain.Job{{ID: id, Queue: "q", RunAt: first, MaxAttempts: 3}}
	}

	ok, err := storage.ClaimOne(ctx, s, "s1", first, second, job("j1"))
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}

	ok, err = storage.ClaimOne(ctx, s, "s1", first, second, job("j1-dup"))
	if err != nil || ok {
		t.Fatalf("stale claim must lose: ok=%v err=%v", ok, err)
	}

	sch, _ := s.LoadSchedule(ctx, "s1")
	if sch.NextRunAt == nil || sch.NextRunAt.UnixMilli() != second.UnixMilli() || sch.GeneratedJobs != 1 {
		t.Fatalf("after claim: next=%v generated=%d", sch.NextRunAt, sch.GeneratedJobs)
	}

	stats, _ := s.Stats(ctx)
	if stats.ReadyCount["q"] != 1 {
		t.Fatalf("expected the claimed job only, ready=%d", stats.ReadyCount["q"])
	}

	ok, err = storage.ClaimOne(ctx, s, "s1", second, second.Add(time.Hour), job("j2"))
	if err != nil || !ok {
		t.Fatalf("second claim: ok=%v err=%v", ok, err)
	}
	sch, _ = s.LoadSchedule(ctx, "s1")
	if sch.Status != domain.ScheduleFinished {
		t.Fatalf("MaxJobs reached, status = %v", sch.Status)
	}
}

func TestClaimOccurrence_Concurrent(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	due := time.Now().Truncate(time.Millisecond)
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleActive, NextRunAt: &due})

	var wins sync.WaitGroup
	var mu sync.Mutex
	won := 0
	for i := range 10 {
		wins.Add(1)
		go func() {
			defer wins.Done()
			jobs := []*domain.Job{{ID: fmt.Sprintf("j%d", i), Queue: "q", RunAt: due}}
			ok, err := storage.ClaimOne(ctx, s, "s1", due, due.Add(time.Minute), jobs)
			if err != nil {
				t.Error(err)
			}
			if ok {
				mu.Lock()
				won++
				mu.Unlock()
			}
		}()
	}
	wins.Wait()

	if won != 1 {
		t.Fatalf("%d instances claimed the same occurrence, want 1", won)
	}
}

func TestSaveSchedule_RejectsStaleVersion(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	v5 := &domain.JobSchedule{ID: "s1", Queue: "q", Name: "v5", Version: 5}
	if err := s.SaveSchedule(ctx, v5); err != nil {
		t.Fatal(err)
	}

	for _, v := range []int64{5, 3} {
		old := &domain.JobSchedule{ID: "s1", Queue: "q", Name: "old", Version: v}
		if err := s.SaveSchedule(ctx, old); !errors.Is(err, storage.ErrStaleVersion) {
			t.Fatalf("version %d over 5: err = %v, want ErrStaleVersion", v, err)
		}
	}

	if err := s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Name: "v6", Version: 6}); err != nil {
		t.Fatal(err)
	}
	// Unversioned writes are never rejected.
	if err := s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Name: "plain"}); err != nil {
		t.Fatal(err)
	}

	sch, _ := s.LoadSchedule(ctx, "s1")
	if sch.Name != "plain" {
		t.Fatalf("name = %q", sch.Name)
	}
}

func TestClaimOccurrence_RequiresActiveSchedule(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	due := time.Now().Truncate(time.Millisecond)
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "s1", Queue: "q", Status: domain.ScheduleCancelled, NextRunAt: &due})

	ok, err := storage.ClaimOne(ctx, s, "s1", due, due.Add(time.Minute), nil)
	if err != nil || ok {
		t.Fatalf("cancelled schedule claimed: ok=%v err=%v", ok, err)
	}
}

func TestListSchedules_History(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "active", Queue: "q", Status: domain.ScheduleActive})
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "done", Queue: "q", Status: domain.ScheduleActive})
	s.SaveSchedule(ctx, &domain.JobSchedule{ID: "gone", Queue: "other", Status: domain.ScheduleCancelled})
	s.UpdateScheduleStatus(ctx, "done", domain.ScheduleFinished)

	active, _ := s.ListSchedules(ctx, storage.ScheduleQuery{Statuses: []domain.ScheduleStatus{domain.ScheduleActive, domain.SchedulePaused}})
	if len(active) != 1 || active[0].ID != "active" {
		t.Fatalf("active schedules = %v, want only 'active'", ids(active))
	}

	history, err := s.ListSchedules(ctx, storage.ScheduleQuery{
		Statuses: []domain.ScheduleStatus{domain.ScheduleFinished, domain.ScheduleCancelled},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(history); len(got) != 2 {
		t.Fatalf("history = %v, want done and gone", got)
	}

	byQueue, _ := s.ListSchedules(ctx, storage.ScheduleQuery{Queue: "q"})
	if got := ids(byQueue); len(got) != 2 {
		t.Fatalf("queue q = %v, want active and done", got)
	}

	limited, _ := s.ListSchedules(ctx, storage.ScheduleQuery{Limit: 1, Offset: 1})
	if len(limited) != 1 {
		t.Fatalf("page of 1 returned %d", len(limited))
	}
}

func TestAckJob_KeepsHistory(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	sid := "s1"
	for _, id := range []string{"j1", "j2"} {
		s.SaveJob(ctx, &domain.Job{ID: id, Queue: "q", RunAt: time.Now().Add(-time.Second), ScheduleID: &sid, MaxAttempts: 1})
	}
	claimed, _ := s.ClaimJobs(ctx, "q", "w", 2, time.Minute)
	if len(claimed) != 2 {
		t.Fatalf("claimed %d", len(claimed))
	}
	for _, c := range claimed {
		if c.Job.ID == "j1" {
			s.AckJobs(ctx, []string{c.Job.ID})
		} else {
			s.FailJob(ctx, c.Job, fmt.Errorf("boom"))
		}
	}

	jobs, err := s.ListJobs(ctx, storage.JobQuery{ScheduleID: sid})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("history has %d jobs, want 2", len(jobs))
	}

	completed, _ := s.ListJobs(ctx, storage.JobQuery{ScheduleID: sid, Statuses: []domain.JobStatus{domain.StatusCompleted}})
	if len(completed) != 1 || completed[0].ID != "j1" {
		t.Fatalf("completed = %v", completed)
	}

	stats, _ := s.Stats(ctx)
	if stats.LeaseCount != 0 {
		t.Fatalf("leases left after ack/fail: %d", stats.LeaseCount)
	}
}

func TestExtendLease(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	s.SaveJob(ctx, &domain.Job{ID: "j1", Queue: "q", RunAt: time.Now().Add(-time.Second)})
	s.ClaimJobs(ctx, "q", "w", 1, 50*time.Millisecond)

	if err := s.ExtendLease(ctx, "j1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.RecoverOrphaned(ctx, time.Now().Add(time.Second)); n != 0 {
		t.Fatalf("extended lease recovered as orphan")
	}
	if n, _ := s.RecoverOrphaned(ctx, time.Now().Add(2*time.Minute)); n != 1 {
		t.Fatalf("expired lease not recovered")
	}
	if err := s.ExtendLease(ctx, "j1", time.Minute); err == nil {
		t.Fatal("extending a released lease must fail")
	}
}

func TestPausedQueues(t *testing.T) {
	s := newDB(t)
	ctx := context.Background()

	s.SetQueuePaused(ctx, "a", true)
	s.SetQueuePaused(ctx, "b", true)
	s.SetQueuePaused(ctx, "a", false)

	paused, err := s.PausedQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(paused) != 1 || paused[0] != "b" {
		t.Fatalf("paused = %v, want [b]", paused)
	}
}

func ids(schedules []*domain.JobSchedule) []string {
	var out []string
	for _, s := range schedules {
		out = append(out, s.ID)
	}
	return out
}
