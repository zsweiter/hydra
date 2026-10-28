package storagetest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/scheduler"
	"github.com/zsweiter/hydra/storage"
)

// BenchFactory returns an empty storage for one benchmark run,
// and registers its cleanup (b.Cleanup) itself.
type BenchFactory func(b *testing.B) storage.Storage

// Bench runs the storage benchmarks. Every operation works on
// fresh rows (no benchmark reuses one ID across iterations or
// runs its queue dry), and ns/op is per job or occurrence.
func Bench(b *testing.B, newStore BenchFactory) {
	run := func(name string, fn func(*testing.B, storage.Storage)) {
		b.Run(name, func(b *testing.B) { fn(b, newStore(b)) })
	}

	run("SaveJob", benchSaveJob)
	run("ClaimAck", benchClaimAck(1))
	run("ClaimAckBatch10", benchClaimAck(10))
	run("ClaimAckParallel8", benchClaimAckParallel)
	run("FailRetry", benchFailRetry)
	run("ClaimOccurrence", benchClaimOccurrence)
	run("ClaimOccurrencesBatch500", benchClaimBatch)
	run("LoadDueWindow", benchLoadDue)
	run("LoadSchedules500", benchLoadSchedules)
	run("FirePeakDirect", benchFirePeak(true))
	run("FirePeakOutbox", benchFirePeak(false))
}

func benchJob(prefix string, i int) *domain.Job {
	return &domain.Job{ID: fmt.Sprintf("%s-%d", prefix, i), Queue: "bench", Data: []byte(`{"n":1}`),
		MaxAttempts: 3, Backoff: domain.DefaultBackoff(), Metadata: map[string]string{}}
}

func fill(b *testing.B, s storage.Storage, prefix string, n int) {
	b.Helper()
	ctx := context.Background()
	for i := range n {
		if err := s.SaveJob(ctx, benchJob(prefix, i)); err != nil {
			b.Fatal(err)
		}
	}
}

// One durable write of a new job.
func benchSaveJob(b *testing.B, s storage.Storage) {
	ctx := context.Background()
	i := 0
	for b.Loop() {
		i++
		if err := s.SaveJob(ctx, benchJob("j", i)); err != nil {
			b.Fatal(err)
		}
	}
}

// Claim batch jobs and ack them together, as the dispatcher and
// the worker pool do.
func benchClaimAck(batch int) func(*testing.B, storage.Storage) {
	return func(b *testing.B, s storage.Storage) {
		ctx := context.Background()
		fill(b, s, "c", b.N+batch)
		b.ResetTimer()
		for done := 0; done < b.N; {
			leased, err := s.ClaimJobs(ctx, "bench", "w", batch, time.Minute)
			if err != nil || len(leased) == 0 {
				b.Fatalf("claimed %d: %v", len(leased), err)
			}
			ids := make([]string, len(leased))
			for i, l := range leased {
				ids[i] = l.Job.ID
			}
			if err := s.AckJobs(ctx, ids); err != nil {
				b.Fatal(err)
			}
			done += len(leased)
		}
	}
}

// 8 workers claiming batches of 10 from one queue.
func benchClaimAckParallel(b *testing.B, s storage.Storage) {
	ctx := context.Background()
	fill(b, s, "p", b.N+100)
	var left atomic.Int64
	left.Store(int64(b.N))
	b.ResetTimer()

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for left.Load() > 0 {
				leased, err := s.ClaimJobs(ctx, "bench", fmt.Sprint("w", w), 10, time.Minute)
				if err != nil {
					b.Error(err)
					return
				}
				ids := make([]string, len(leased))
				for i, l := range leased {
					ids[i] = l.Job.ID
				}
				if err := s.AckJobs(ctx, ids); err != nil {
					b.Error(err)
					return
				}
				left.Add(-int64(len(leased)))
			}
		}()
	}
	wg.Wait()
}

// Claim one job and fail it: the backoff write.
func benchFailRetry(b *testing.B, s storage.Storage) {
	ctx := context.Background()
	fill(b, s, "f", b.N)
	b.ResetTimer()
	for range b.N {
		leased, err := s.ClaimJobs(ctx, "bench", "w", 1, time.Minute)
		if err != nil || len(leased) != 1 {
			b.Fatalf("claimed %d: %v", len(leased), err)
		}
		if _, err := s.FailJob(ctx, leased[0].Job, fmt.Errorf("boom")); err != nil {
			b.Fatal(err)
		}
	}
}

// One occurrence claimed with its job, one round trip each.
func benchClaimOccurrence(b *testing.B, s storage.Storage) {
	ctx := context.Background()
	start := time.Now().Truncate(time.Millisecond)
	sch := domain.NewJobSchedule("occ", "occ", nil, domain.NewIntervalScheduleRule(time.Millisecond))
	sch.Queue = "bench"
	sch.NextRunAt = &start
	if err := s.SaveSchedule(ctx, &sch); err != nil {
		b.Fatal(err)
	}

	at := start
	i := 0
	for b.Loop() {
		i++
		next := at.Add(time.Millisecond)
		ok, err := storage.ClaimOne(ctx, s, "occ", at, next, []*domain.Job{benchJob("o", i)})
		if err != nil || !ok {
			b.Fatalf("claim %d: %v %v", i, ok, err)
		}
		at = next
	}
}

func saveDue(b *testing.B, s storage.Storage, prefix string, n int, at time.Time) []string {
	b.Helper()
	ctx := context.Background()
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("%s-%06d", prefix, i)
		sch := domain.NewJobSchedule(ids[i], "reminders", []byte(`{"text":"take your medication"}`),
			domain.NewCronScheduleRule("0 9 * * *"))
		sch.Queue = "bench"
		sch.NextRunAt = &at
		if err := s.SaveSchedule(ctx, &sch); err != nil {
			b.Fatal(err)
		}
	}
	return ids
}

// A peak: 500 schedules due together claimed in one call,
// delivered directly (no job stored). ns/op is per occurrence.
func benchClaimBatch(b *testing.B, s storage.Storage) {
	ctx := context.Background()
	const batch = 500
	due := time.Now().Truncate(time.Millisecond)
	ids := saveDue(b, s, "peak", batch, due)

	at := due
	b.ResetTimer()
	for done := 0; done < b.N; done += batch {
		// Every schedule moves together, so the last round
		// claims them all too; ns/op stays per occurrence for
		// b.N >= batch.
		next := at.Add(time.Millisecond)
		claims := make([]storage.Claim, batch)
		for i, id := range ids {
			claims[i] = storage.Claim{ScheduleID: id, Expected: at, Next: next, Generated: 1}
		}
		ok, err := s.ClaimOccurrences(ctx, claims)
		if err != nil {
			b.Fatal(err)
		}
		for i := range ok {
			if !ok[i] {
				b.Fatalf("claim %s lost", ids[i])
			}
		}
		at = next
	}
}

// Loading the scheduler window: 1000 due among 10000 schedules.
func benchLoadDue(b *testing.B, s storage.Storage) {
	ctx := context.Background()
	now := time.Now()
	for i := range 10_000 {
		next := now.Add(time.Duration(i) * 6 * time.Second)
		sch := domain.NewJobSchedule(fmt.Sprint("s", i), "s", []byte(`{"text":"payload"}`), domain.NewIntervalScheduleRule(time.Hour))
		sch.Queue = "bench"
		sch.NextRunAt = &next
		if err := s.SaveSchedule(ctx, &sch); err != nil {
			b.Fatal(err)
		}
	}
	before := now.Add(999 * 6 * time.Second)
	b.ResetTimer()
	for b.Loop() {
		got, err := s.LoadDueSchedules(ctx, storage.DueQuery{Before: before, Limit: 10_000})
		if err != nil || len(got) != 1000 {
			b.Fatalf("got %d: %v", len(got), err)
		}
	}
}

// Loading the payloads of a fire batch.
func benchLoadSchedules(b *testing.B, s storage.Storage) {
	ctx := context.Background()
	ids := saveDue(b, s, "load", 500, time.Now())
	b.ResetTimer()
	for b.Loop() {
		got, err := s.LoadSchedules(ctx, ids)
		if err != nil || len(got) != 500 {
			b.Fatalf("got %d: %v", len(got), err)
		}
	}
}

// The whole scheduler on this store: n schedules due at the
// same instant, fired until every occurrence is claimed. With
// direct delivery nothing but the claims is written; with the
// outbox every occurrence also stores a job. ns/op is per
// occurrence.
func benchFirePeak(direct bool) func(*testing.B, storage.Storage) {
	return func(b *testing.B, s storage.Storage) {
		ctx := context.Background()
		n := max(b.N, 1)
		saveDue(b, s, "fire", n, time.Now().Add(-time.Second).Truncate(time.Millisecond))

		var claimed atomic.Int64
		sched := scheduler.New(countingStore{s, &claimed}, nil)
		sched.SetWindow(time.Minute, n)
		if direct {
			sched.SetDirect(func(domain.QueueName) scheduler.DirectFunc {
				return func(context.Context, []*domain.Job) []error { return nil }
			})
		}

		b.ResetTimer()
		if err := sched.Load(ctx); err != nil {
			b.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); sched.Run(runCtx) }()

		deadline := time.Now().Add(5 * time.Minute)
		for claimed.Load() < int64(n) {
			if time.Now().After(deadline) {
				b.Fatalf("claimed %d of %d", claimed.Load(), n)
			}
			time.Sleep(time.Millisecond)
		}
		b.StopTimer()
		cancel()
		<-done
	}
}

// countingStore counts the occurrences claimed.
type countingStore struct {
	storage.Storage
	claimed *atomic.Int64
}

func (c countingStore) ClaimOccurrences(ctx context.Context, claims []storage.Claim) ([]bool, error) {
	ok, err := c.Storage.ClaimOccurrences(ctx, claims)
	for i := range ok {
		if ok[i] {
			c.claimed.Add(int64(claims[i].Generated))
		}
	}
	return ok, err
}
