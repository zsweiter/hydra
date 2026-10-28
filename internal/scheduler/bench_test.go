package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/storage"
)

// BenchmarkHeap measures one pop and push on a heap holding n
// entries: the cost of moving one schedule to its next
// occurrence, whatever the window size.
func BenchmarkHeap(b *testing.B) {
	for _, n := range []int{10_000, 1_000_000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			h := &ScheduleHeap{}
			base := time.Now().UnixNano()
			for i := range n {
				heap.Push(h, &HeapEntry{ScheduleID: strconv.Itoa(i), NextRunNanos: base + rand.Int64N(int64(time.Hour))})
			}
			b.ResetTimer()
			for b.Loop() {
				e := heap.Pop(h).(*HeapEntry)
				e.NextRunNanos += int64(time.Hour)
				heap.Push(h, e)
			}
		})
	}
}

// BenchmarkWindowMemory reports the memory one schedule takes
// in the window: its entry, compiled cron rule and index.
func BenchmarkWindowMemory(b *testing.B) {
	const n = 100_000
	for b.Loop() {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)

		s := New(memory.New(), nil)
		next := time.Now().Add(time.Minute)
		for i := range n {
			sch := domain.NewJobSchedule(fmt.Sprintf("reminders.daily|user-%07d", i), "reminders.daily", nil,
				domain.NewCronScheduleRule("0 9 * * *").WithTimezone("America/Santiago"))
			sch.NextRunAt = &next
			if e := s.entryFor(&sch, nil); e != nil {
				s.pushLocked(e, false)
			}
		}

		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/n, "B/schedule")
		runtime.KeepAlive(s)
	}
}

// BenchmarkFire measures the scheduler firing a peak: every
// schedule due at the same instant (e.g. "0 9 * * *"),
// delivered directly, against the memory store. It isolates
// the scheduler from storage latency. HYDRA_LOADTEST=n sets
// the number of schedules (default 100000).
func BenchmarkFire(b *testing.B) {
	n := 100_000
	if v, err := strconv.Atoi(os.Getenv("HYDRA_LOADTEST")); err == nil && v > 0 {
		n = v
	}
	// outbox is the job queue's mode (failed deliveries become
	// jobs); direct is hydra-scheduler's (nothing is stored).
	b.Run("outbox", func(b *testing.B) { benchFire(b, n, true) })
	b.Run("direct", func(b *testing.B) { benchFire(b, n, false) })
}

func benchFire(b *testing.B, n int, outbox bool) {
	for b.Loop() {
		b.StopTimer()
		store := memory.New()
		ctx := context.Background()
		due := time.Now().Add(-time.Second).Truncate(time.Millisecond)
		for i := range n {
			sch := domain.NewJobSchedule(fmt.Sprintf("reminders.daily|user-%07d", i), "reminders.daily",
				[]byte(`{"text":"take your medication"}`), domain.NewCronScheduleRule("0 9 * * *"))
			sch.Queue = "q"
			sch.NextRunAt = &due
			if err := store.SaveSchedule(ctx, &sch); err != nil {
				b.Fatal(err)
			}
		}

		var delivered atomic.Int64
		s := New(store, nil)
		s.SetOutbox(outbox)
		s.SetWindow(time.Minute, n)
		s.SetDirect(func(domain.QueueName) DirectFunc {
			return func(_ context.Context, jobs []*domain.Job) []error {
				delivered.Add(int64(len(jobs)))
				return nil
			}
		})
		b.StartTimer()

		start := time.Now()
		if err := s.Load(ctx); err != nil {
			b.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); s.Run(runCtx) }()
		for delivered.Load() < int64(n) {
			time.Sleep(time.Millisecond)
		}
		elapsed := time.Since(start)
		cancel()
		<-done

		b.ReportMetric(float64(n)/elapsed.Seconds(), "occurrences/s")
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/peak")
	}
}

// claimCounter counts the claims a store receives.
type claimCounter struct {
	storage.Storage
	calls atomic.Int64
}

func (c *claimCounter) ClaimOccurrences(ctx context.Context, claims []storage.Claim) ([]bool, error) {
	c.calls.Add(1)
	return c.Storage.ClaimOccurrences(ctx, claims)
}

// A peak is claimed in batches, not one round trip per
// occurrence.
func TestFire_BatchesPeak(t *testing.T) {
	store := &claimCounter{Storage: memory.New()}
	ctx := context.Background()
	due := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	for i := range 2000 {
		sch := domain.NewJobSchedule(fmt.Sprintf("s%04d", i), "s", nil, domain.NewCronScheduleRule("0 9 * * *"))
		sch.Queue = "q"
		sch.NextRunAt = &due
		if err := store.SaveSchedule(ctx, &sch); err != nil {
			t.Fatal(err)
		}
	}

	var delivered atomic.Int64
	startScheduler(t, store, func(s *Scheduler) {
		s.SetFireBatch(500, 4)
		s.SetDirect(func(domain.QueueName) DirectFunc {
			return func(_ context.Context, jobs []*domain.Job) []error {
				delivered.Add(int64(len(jobs)))
				return nil
			}
		})
	})
	waitFor(t, 5*time.Second, "peak delivered", func() bool { return delivered.Load() == 2000 })

	// 2000 schedules in batches of 500 over 4 round trips each.
	if calls := store.calls.Load(); calls > 16+4 {
		t.Fatalf("%d claim round trips for 2000 occurrences, want them batched (about 16)", calls)
	}
	jobs, _ := store.ListJobs(ctx, storage.JobQuery{ScheduleID: "s0001"})
	if len(jobs) != 0 {
		t.Fatalf("%d jobs stored for a delivered occurrence", len(jobs))
	}
}

// A failed direct delivery is stored as a job, and a claim
// whose write fails is retried without delivering again.
func TestFire_DirectFailureAndClaimRetry(t *testing.T) {
	store := &flakyClaims{Storage: memory.New()}
	store.fail.Store(1)
	ctx := context.Background()
	due := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	for _, id := range []string{"ok", "bad"} {
		sch := domain.NewJobSchedule(id, id, nil, domain.NewCronScheduleRule("0 9 * * *"))
		sch.Queue = "q"
		sch.NextRunAt = &due
		if err := store.SaveSchedule(ctx, &sch); err != nil {
			t.Fatal(err)
		}
	}

	var deliveries atomic.Int64
	startScheduler(t, store, func(s *Scheduler) {
		s.SetDirect(func(domain.QueueName) DirectFunc {
			return func(_ context.Context, jobs []*domain.Job) []error {
				deliveries.Add(int64(len(jobs)))
				errs := make([]error, len(jobs))
				for i, j := range jobs {
					if *j.ScheduleID == "bad" {
						errs[i] = fmt.Errorf("broker down")
					}
				}
				return errs
			}
		})
	})

	waitFor(t, 5*time.Second, "both claimed", func() bool {
		a, _ := store.LoadSchedule(ctx, "ok")
		b, _ := store.LoadSchedule(ctx, "bad")
		return a.GeneratedJobs == 1 && b.GeneratedJobs == 1
	})

	if n := deliveries.Load(); n != 2 {
		t.Fatalf("%d deliveries, want 2: the claim retry must not deliver again", n)
	}
	if jobs, _ := store.ListJobs(ctx, storage.JobQuery{ScheduleID: "bad"}); len(jobs) != 1 {
		t.Fatalf("failed delivery stored %d jobs, want 1 for the outbox", len(jobs))
	}
	if jobs, _ := store.ListJobs(ctx, storage.JobQuery{ScheduleID: "ok"}); len(jobs) != 0 {
		t.Fatalf("delivered occurrence stored %d jobs, want 0", len(jobs))
	}
}

// flakyClaims fails the first fail claim batches.
type flakyClaims struct {
	storage.Storage
	fail atomic.Int64
}

func (f *flakyClaims) ClaimOccurrences(ctx context.Context, claims []storage.Claim) ([]bool, error) {
	if f.fail.Add(-1) >= 0 {
		return nil, fmt.Errorf("store unavailable")
	}
	return f.Storage.ClaimOccurrences(ctx, claims)
}

// Without the outbox, a failed delivery is not claimed: the
// same occurrence is delivered again, with backoff, and the
// schedule advances once it succeeds. Nothing is stored but the
// schedule.
func TestFire_NoOutboxRetriesUntilDelivered(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	due := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	sch := domain.NewJobSchedule("s", "s", []byte("x"), domain.NewCronScheduleRule("0 9 * * *"))
	sch.Queue = "q"
	sch.NextRunAt = &due
	if err := store.SaveSchedule(ctx, &sch); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var seen []string // job id @ scheduled_for of every attempt
	startScheduler(t, store, func(s *Scheduler) {
		s.SetOutbox(false)
		s.SetDirect(func(domain.QueueName) DirectFunc {
			return func(_ context.Context, jobs []*domain.Job) []error {
				mu.Lock()
				defer mu.Unlock()
				seen = append(seen, jobs[0].ID+"@"+jobs[0].Metadata[domain.MetadataScheduledFor])
				if len(seen) < 3 {
					got, _ := store.LoadSchedule(ctx, "s")
					if !got.NextRunAt.Equal(due) {
						t.Errorf("schedule advanced before delivery succeeded")
					}
					return []error{fmt.Errorf("broker down")}
				}
				return nil
			}
		})
	})

	waitFor(t, 10*time.Second, "claimed after delivery", func() bool {
		got, _ := store.LoadSchedule(ctx, "s")
		return got.GeneratedJobs == 1
	})

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 || seen[0] != seen[1] || seen[1] != seen[2] {
		t.Fatalf("attempts %v, want the same occurrence three times", seen)
	}
	if jobs, _ := store.ListJobs(ctx, storage.JobQuery{ScheduleID: "s"}); len(jobs) != 0 {
		t.Fatalf("%d jobs stored without the outbox", len(jobs))
	}
}

// A schedule cancelled while its delivery is failing stops
// being retried.
func TestFire_NoOutboxStopsRetryingCancelled(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	due := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	sch := domain.NewJobSchedule("s", "s", nil, domain.NewCronScheduleRule("0 9 * * *"))
	sch.Queue = "q"
	sch.NextRunAt = &due
	if err := store.SaveSchedule(ctx, &sch); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int64
	startScheduler(t, store, func(s *Scheduler) {
		s.SetOutbox(false)
		s.SetDirect(func(domain.QueueName) DirectFunc {
			return func(_ context.Context, jobs []*domain.Job) []error {
				if attempts.Add(1) == 1 {
					store.UpdateScheduleStatus(ctx, "s", domain.ScheduleCancelled)
				}
				return []error{fmt.Errorf("broker down")}
			}
		})
	})

	time.Sleep(3500 * time.Millisecond)
	if n := attempts.Load(); n != 1 {
		t.Fatalf("%d delivery attempts, want 1: the cancelled schedule must not be retried", n)
	}
}
