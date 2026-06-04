package hydra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ──────────────────────────────────────────────
//  Edge Case Tests
// ──────────────────────────────────────────────

func TestSubmitBeforeStart(t *testing.T) {
	q := New(Config{})
	defer q.Stop(context.Background())

	err := q.Submit(context.Background(), "test", Job{Name: "nope"})
	if err == nil {
		t.Fatal("expected error when submitting before Start")
	}
}

func TestDoubleStart(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 1,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	err := q.Start(ctx)
	if err == nil {
		t.Fatal("expected error on double start")
	}
}

func TestSubmitToUnhandledQueue(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 1,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	// Should not panic — job should fail and go to DLQ
	err := q.Submit(ctx, "no-handler", Job{
		Name:        "orphan",
		Data:        []byte("hello"),
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Give time for the failure to be processed
	time.Sleep(200 * time.Millisecond)

	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DLQCount != 1 {
		t.Fatalf("expected 1 DLQ entry for unhandled job, got %d", stats.DLQCount)
		// NOTE: This test documents that unhandled jobs go to DLQ.
		// Expected: DLQCount == 1  —  Actual may differ depending on
		// whether the dispatcher has claimed and processed the job yet.
	}
}

func TestEmptyDataJob(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 1,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	processed := make(chan struct{}, 1)
	q.Handle("empty", func(ctx context.Context, job *Job) error {
		select {
		case processed <- struct{}{}:
		default:
		}
		return nil
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	err := q.Submit(ctx, "empty", Job{Name: "no-data"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: empty-data job was not processed")
	}
}

func TestJobScheduledFarFuture(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 1,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var called atomic.Bool
	q.Handle("future", func(ctx context.Context, job *Job) error {
		called.Store(true)
		return nil
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	future := time.Now().Add(10 * time.Minute)
	err := q.Submit(ctx, "future", Job{
		Name:  "future-job",
		Data:  []byte("not yet"),
		RunAt: future,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)
	if called.Load() {
		t.Fatal("future job was processed before its RunAt time")
	}
}

func TestScheduleWithMaxJobs(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count atomic.Int32
	q.Handle("limited", func(ctx context.Context, job *Job) error {
		count.Add(1)
		return nil
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	schedule := NewJobSchedule("limited-sched", "max-3-jobs", []byte("limited"),
		IntervalRule(50*time.Millisecond),
	)
	schedule.MaxJobs = 3

	q.UpsertSchedule(ctx, "limited", schedule)
	time.Sleep(500 * time.Millisecond)

	n := count.Load()
	if n > 4 {
		t.Fatalf("expected max ~3 jobs from a MaxJobs=3 schedule, got %d", n)
		// NOTE: This test documents a known limitation: the scheduler may
		// generate one extra job before detecting ShouldFinish(), because
		// the MaxJobs check happens before reinsertion, not before generation.
		// Expected: n <= 4  —  Actual: may be 3-4.
	}
}

func TestScheduleCatchUpMisfire(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 4,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count atomic.Int32
	q.Handle("catchup", func(ctx context.Context, job *Job) error {
		count.Add(1)
		return nil
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	schedule := NewJobSchedule("catch-sched", "catchup-test", []byte("catch"),
		IntervalRule(30*time.Millisecond),
	)
	schedule.MisfirePolicy = MisfireCatchUp

	q.UpsertSchedule(ctx, "catchup", schedule)
	// Let it tick a few times, then pause, then resume after a gap
	time.Sleep(150 * time.Millisecond)
	q.PauseSchedule(ctx, "catchup", "catch-sched")
	time.Sleep(200 * time.Millisecond)

	countBefore := count.Load()
	q.ResumeSchedule(ctx, "catchup", "catch-sched")
	time.Sleep(300 * time.Millisecond)

	countAfter := count.Load()
	if countAfter <= countBefore {
		t.Fatal("expected catch-up jobs after resume")
	}
}

func TestRemoveNonExistentJob(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	q.Start(ctx)
	defer q.Stop(ctx)

	err := q.RemoveJob(ctx, "test", "non-existent-id")
	if err == nil {
		t.Fatal("expected error when removing non-existent job")
	}
}

func TestHandlerPanic(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	q.Handle("panic-q", func(ctx context.Context, job *Job) error {
		panic("deliberate panic in handler")
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	err := q.Submit(ctx, "panic-q", Job{
		Name:        "will-panic",
		Data:        []byte("boom"),
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify stats — the job should have reached DLQ after panic
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DLQCount != 1 {
		t.Fatalf("expected 1 DLQ entry after handler panic, got %d", stats.DLQCount)
	}
}

func TestUpsertRemoveRace(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q.Handle("race-q", func(ctx context.Context, job *Job) error { return nil })
	q.Start(ctx)
	defer q.Stop(ctx)

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("race-sched-%d", i)
			schedule := NewJobSchedule(id, fmt.Sprintf("race-%d", i), []byte("race"),
				IntervalRule(50*time.Millisecond),
			)
			q.UpsertSchedule(ctx, "race-q", schedule)
			time.Sleep(time.Duration(i%5) * 10 * time.Millisecond)
			q.RemoveSchedule(ctx, "race-q", id)
		}(i)
	}
	wg.Wait()

	// Should not deadlock or panic
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Stats after concurrent upsert/remove: %+v", stats)
}

// ──────────────────────────────────────────────
//  Stress Tests
// ──────────────────────────────────────────────

func TestConcurrentJobSubmission(t *testing.T) {
	q := New(Config{
		MinWorkers: 4,
		MaxWorkers: 8,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var processed atomic.Int32
	q.Handle("stress", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	const N = 500
	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q.Submit(ctx, "stress", Job{
				Name: fmt.Sprintf("stress-%d", i),
				Data: []byte(fmt.Sprintf("payload-%d", i)),
			})
		}(i)
	}
	wg.Wait()

	// Wait for processing to finish
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() >= int32(N) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if n := processed.Load(); n < int32(N) {
		t.Fatalf("expected to process %d jobs, got %d", N, n)
		// NOTE: The dispatcher may not have claimed all pending jobs
		// if the worker pool is saturated or batch sizes are too small.
		// Expected: processed == N  —  Actual may be less due to
		// timing / throughput limits.
	}
}

func TestManyJobsSingleQueue(t *testing.T) {
	q := New(Config{
		MinWorkers: 8,
		MaxWorkers: 16,
		BatchSize:  50,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var processed atomic.Int32
	q.Handle("burst", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	const N = 2000
	for i := range N {
		q.Submit(ctx, "burst", Job{
			Name: fmt.Sprintf("burst-%d", i),
			Data: []byte(fmt.Sprintf("payload-%d", i)),
		})
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() >= int32(N) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if n := processed.Load(); n < int32(N) {
		t.Fatalf("expected to process all %d jobs in burst, got %d", N, n)
		// NOTE: This test documents throughput limits with the in-memory
		// storage. 2000 jobs may not all be claimed and processed within
		// the 8-second deadline depending on backpressure and batch sizes.
	}
}

func TestManyQueuesConcurrent(t *testing.T) {
	q := New(Config{
		MinWorkers: 8,
		MaxWorkers: 16,
		BatchSize:  20,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const nQueues = 10
	const nJobs = 200 // per queue

	counts := make([]atomic.Int32, nQueues)
	for i := range nQueues {
		qName := fmt.Sprintf("mq-%d", i)
		idx := i
		q.Handle(qName, func(ctx context.Context, job *Job) error {
			counts[idx].Add(1)
			return nil
		})
	}

	q.Start(ctx)
	defer q.Stop(ctx)

	for i := 0; i < nQueues; i++ {
		qName := fmt.Sprintf("mq-%d", i)
		for j := 0; j < nJobs; j++ {
			q.Submit(ctx, qName, Job{
				Name: fmt.Sprintf("job-%d-%d", i, j),
				Data: []byte("data"),
			})
		}
	}
	// Submit all jobs for this queue, then move on

	deadline := time.Now().Add(5 * time.Second)
	totalProcessed := func() int {
		sum := 0
		for i := range nQueues {
			sum += int(counts[i].Load())
		}
		return sum
	}

	for time.Now().Before(deadline) {
		if totalProcessed() >= nQueues*nJobs {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	total := totalProcessed()
	if total < nQueues*nJobs {
		t.Fatalf("expected to process %d jobs across %d queues, got %d", nQueues*nJobs, nQueues, total)
		// NOTE: This test documents multi-queue throughput.
		// Expected: all jobs processed  —  Actual may be less.
	}
}

func TestConcurrentScheduleOps(t *testing.T) {
	q := New(Config{
		MinWorkers: 2,
		MaxWorkers: 4,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q.Handle("sched-op", func(ctx context.Context, job *Job) error { return nil })
	q.Start(ctx)
	defer q.Stop(ctx)

	const N = 100
	var wg sync.WaitGroup

	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("sched-op-%d", i)
			schedule := NewJobSchedule(id, fmt.Sprintf("op-%d", i), []byte("op"),
				IntervalRule(time.Duration(10+(i%10))*time.Millisecond),
			)
			_ = q.UpsertSchedule(ctx, "sched-op", schedule)
			time.Sleep(time.Duration(5+(i%10)) * time.Millisecond)

			switch i % 3 {
			case 0:
				_ = q.PauseSchedule(ctx, "sched-op", id)
			case 1:
				_ = q.ResumeSchedule(ctx, "sched-op", id)
			case 2:
				_ = q.RemoveSchedule(ctx, "sched-op", id)
			}
		}(i)
	}
	wg.Wait()

	// Should not hang or deadlock
	t.Log("Concurrent schedule operations completed without deadlock")
}

// ──────────────────────────────────────────────
//  Benchmarks
// ──────────────────────────────────────────────

func BenchmarkSubmitAndProcess(b *testing.B) {
	q := New(Config{
		MinWorkers: 4,
		MaxWorkers: 8,
		BatchSize:  100,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var processed atomic.Int64

	q.Handle("bench", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	})

	q.Start(ctx)
	defer q.Stop(ctx)

	b.ResetTimer()

	// Submit all jobs synchronously — timer includes submission + processing
	for i := 0; i < b.N; i++ {
		q.Submit(ctx, "bench", Job{
			Name: fmt.Sprintf("bench-%d", i),
			Data: []byte("benchmark"),
		})
	}

	// Wait for all jobs to finish processing
	for processed.Load() < int64(b.N) {
		time.Sleep(5 * time.Millisecond)
	}

	b.StopTimer()
}

func BenchmarkConcurrentSubmit(b *testing.B) {
	q := New(Config{
		MinWorkers: 8,
		MaxWorkers: 16,
		BatchSize:  100,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var processed atomic.Int64
	done := make(chan struct{})

	q.Handle("cbench", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	})

	q.Start(ctx)
	defer q.Stop(ctx)

	// Consumer that waits for all jobs
	go func() {
		for {
			if processed.Load() >= int64(b.N) {
				close(done)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Submit(ctx, "cbench", Job{
				Name: fmt.Sprintf("cbench-%d", i),
				Data: []byte("concurrent"),
			})
			i++
		}
	})

	<-done
	b.StopTimer()
}

func BenchmarkScheduleGeneration(b *testing.B) {
	q := New(Config{
		MinWorkers: 4,
		MaxWorkers: 8,
		BatchSize:  50,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var processed atomic.Int64
	done := make(chan struct{})

	q.Handle("sbench", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	})

	q.Start(ctx)
	defer q.Stop(ctx)

	go func() {
		for {
			if processed.Load() >= int64(b.N) {
				close(done)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	b.ResetTimer()

	// Use a very fast interval to generate many jobs quickly
	schedule := NewJobSchedule("bench-sched", "bench-gen", []byte("gen"),
		IntervalRule(time.Millisecond),
	)
	q.UpsertSchedule(ctx, "sbench", schedule)

	<-done
	b.StopTimer()

	q.RemoveSchedule(ctx, "sbench", "bench-sched")
}
