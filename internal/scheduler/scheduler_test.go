package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/storage/memory"
)

func runScheduler(t *testing.T, store *memory.Storage) context.CancelFunc {
	t.Helper()
	s := New(store, nil)
	if err := s.Load(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	return cancel
}

// A schedule that reaches MaxJobs must be persisted as
// finished, so a restart does not generate more jobs.
func TestScheduler_FinishedScheduleSurvivesRestart(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	sch := domain.NewJobSchedule("s1", "s1", nil, domain.NewIntervalScheduleRule(20*time.Millisecond))
	sch.Queue = "q"
	sch.MaxJobs = 2
	if err := store.SaveSchedule(ctx, &sch); err != nil {
		t.Fatal(err)
	}

	stop := runScheduler(t, store)
	time.Sleep(200 * time.Millisecond)
	stop()

	got, err := store.LoadSchedule(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.ScheduleFinished {
		t.Fatalf("status = %v, want finished", got.Status)
	}

	stop = runScheduler(t, store)
	time.Sleep(100 * time.Millisecond)
	stop()

	got, _ = store.LoadSchedule(ctx, "s1")
	if got.GeneratedJobs != 2 {
		t.Fatalf("generated %d jobs, want 2", got.GeneratedJobs)
	}
}

// An occurrence missed while the process was down must reach
// the misfire policy instead of being dropped on restart.
func TestScheduler_MissedOccurrenceRunsOnceAfterRestart(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	missed := time.Now().Add(-10 * time.Minute)
	sch := domain.NewJobSchedule("s2", "s2", nil, domain.NewCronScheduleRule("0 0 1 1 *"))
	sch.Queue = "q"
	sch.MisfirePolicy = domain.MisfireRunOnce
	sch.NextRunAt = &missed
	if err := store.SaveSchedule(ctx, &sch); err != nil {
		t.Fatal(err)
	}

	stop := runScheduler(t, store)
	time.Sleep(100 * time.Millisecond)
	stop()

	got, _ := store.LoadSchedule(ctx, "s2")
	if got.GeneratedJobs != 1 {
		t.Fatalf("generated %d jobs, want 1 for the missed occurrence", got.GeneratedJobs)
	}
}

func TestScheduler_SkipDropsOccurrenceMissedDuringDowntime(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	missed := time.Now().Add(-10 * time.Minute)
	sch := domain.NewJobSchedule("s3", "s3", nil, domain.NewCronScheduleRule("0 0 1 1 *"))
	sch.Queue = "q"
	sch.MisfirePolicy = domain.MisfireSkip
	sch.NextRunAt = &missed
	if err := store.SaveSchedule(ctx, &sch); err != nil {
		t.Fatal(err)
	}

	stop := runScheduler(t, store)
	time.Sleep(100 * time.Millisecond)
	stop()

	got, _ := store.LoadSchedule(ctx, "s3")
	if got.GeneratedJobs != 0 {
		t.Fatalf("generated %d jobs, want 0 with MisfireSkip", got.GeneratedJobs)
	}
}
