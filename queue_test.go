package hydra

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zsweiter/hydra/storage"
)

func TestSubmitAndProcessJob(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var received atomic.Bool
	var mu sync.Mutex
	processed := make(chan struct{}, 1)

	q.Handle("emails", func(ctx context.Context, job *Job) error {
		received.Store(true)
		mu.Lock()
		defer mu.Unlock()
		select {
		case processed <- struct{}{}:
		default:
		}
		return nil
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	if err := q.Submit(ctx, "emails", Job{
		Name: "test-email",
		Data: []byte("hello"),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for job processing")
	}

	if !received.Load() {
		t.Fatal("worker was not invoked")
	}
}

func TestSubmitAndRetryOnFailure(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   100 * time.Millisecond,
		MaxRetries: 2,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var attempts atomic.Int32

	q.Handle("retry-queue", func(ctx context.Context, job *Job) error {
		attempts.Add(1)
		return assertErr{} // always fails
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	if err := q.Submit(ctx, "retry-queue", Job{
		Name: "retry-job",
		Data: []byte("will fail"),
		Backoff: BackoffPolicy{
			Type:   BackoffFixed,
			BaseMs: 10,
			MaxMs:  50,
		},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	if n := attempts.Load(); n < 2 {
		t.Fatalf("expected at least 2 attempts (initial + 1 retry), got %d", n)
	}
}

func TestScheduleInterval(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var count atomic.Int32

	q.Handle("ticker", func(ctx context.Context, job *Job) error {
		count.Add(1)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	schedule := NewJobSchedule("every-100ms", []byte("tick"),
		IntervalRule(100*time.Millisecond),
	)

	if err := q.UpsertSchedule(ctx, "ticker", schedule); err != nil {
		t.Fatal(err)
	}

	time.Sleep(600 * time.Millisecond)

	if n := count.Load(); n < 3 {
		t.Fatalf("expected at least 3 scheduled jobs, got %d", n)
	}
}

func TestPauseResumeSchedule(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count atomic.Int32

	q.Handle("pausable", func(ctx context.Context, job *Job) error {
		count.Add(1)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	schedule := NewJobScheduleWithID("sched-1", "every-50ms", []byte("tick"),
		IntervalRule(50*time.Millisecond),
	)

	q.UpsertSchedule(ctx, "pausable", schedule)

	time.Sleep(100 * time.Millisecond)
	countBefore := count.Load()
	if countBefore == 0 {
		t.Fatal("expected some jobs before pause")
	}

	// Small extra wait to avoid racing with the next tick
	time.Sleep(60 * time.Millisecond)
	q.PauseSchedule(ctx, "pausable", "sched-1")
	afterPause := count.Load()

	// Wait long enough that another tick would have fired if
	// pause didn't work
	time.Sleep(150 * time.Millisecond)
	countAfterWait := count.Load()

	if countAfterWait != afterPause {
		t.Fatalf("expected no new jobs after pause, before=%d after=%d", afterPause, countAfterWait)
	}

	q.ResumeSchedule(ctx, "pausable", "sched-1")
	time.Sleep(150 * time.Millisecond)

	if n := count.Load(); n <= countAfterWait {
		t.Fatal("expected new jobs after resume")
	}
}

func TestQueuePauseResume(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count atomic.Int32

	q.Handle("slow", func(ctx context.Context, job *Job) error {
		count.Add(1)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	// Submit some jobs
	for range 5 {
		q.Submit(ctx, "slow", Job{
			Name: "job",
			Data: []byte("data"),
		})
	}

	time.Sleep(500 * time.Millisecond)
	if count.Load() == 0 {
		t.Fatal("expected some jobs to process")
	}

	// Pause queue
	q.Pause(ctx, "slow")
	beforePause := count.Load()

	for range 5 {
		q.Submit(ctx, "slow", Job{
			Name: "job",
			Data: []byte("data"),
		})
	}

	time.Sleep(500 * time.Millisecond)
	if n := count.Load(); n != beforePause {
		t.Fatalf("expected no new processing during pause, before=%d after=%d", beforePause, n)
	}

	// Resume
	q.Resume(ctx, "slow")
	time.Sleep(500 * time.Millisecond)

	if n := count.Load(); n <= beforePause {
		t.Fatal("expected processing after resume")
	}
}

func TestRemoveSchedule(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count atomic.Int32

	q.Handle("rm-test", func(ctx context.Context, job *Job) error {
		count.Add(1)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	schedule := NewJobScheduleWithID("rm-sched", "every-50ms", []byte("tick"),
		IntervalRule(50*time.Millisecond),
	)

	q.UpsertSchedule(ctx, "rm-test", schedule)
	time.Sleep(100 * time.Millisecond)

	if count.Load() == 0 {
		t.Fatal("expected some jobs before removal")
	}

	// Wait for a known state before removing
	time.Sleep(60 * time.Millisecond)
	countBeforeRemove := count.Load()
	q.RemoveSchedule(ctx, "rm-test", "rm-sched")
	time.Sleep(200 * time.Millisecond)

	if n := count.Load(); n != countBeforeRemove {
		t.Fatalf("expected no new jobs after removal, before=%d after=%d", countBeforeRemove, n)
	}
}

func TestMultipleQueues(t *testing.T) {
	q := New(Config{
		MinWorkers: 2,
		MaxWorkers: 4,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var emails atomic.Int32
	var reports atomic.Int32

	q.Handle("emails", func(ctx context.Context, job *Job) error {
		emails.Add(1)
		return nil
	})

	q.Handle("reports", func(ctx context.Context, job *Job) error {
		reports.Add(1)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	q.Submit(ctx, "emails", Job{Name: "email-1", Data: []byte("e1")})
	q.Submit(ctx, "emails", Job{Name: "email-2", Data: []byte("e2")})
	q.Submit(ctx, "reports", Job{Name: "report-1", Data: []byte("r1")})

	time.Sleep(500 * time.Millisecond)

	if emails.Load() != 2 {
		t.Fatalf("expected 2 email jobs, got %d", emails.Load())
	}
	if reports.Load() != 1 {
		t.Fatalf("expected 1 report job, got %d", reports.Load())
	}
}

func TestStats(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q.Handle("stats-q", func(ctx context.Context, job *Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	for range 3 {
		q.Submit(ctx, "stats-q", Job{Name: "job", Data: []byte("data")})
	}

	time.Sleep(200 * time.Millisecond)

	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if stats.ActiveWorkers == 0 {
		t.Fatal("expected active workers")
	}
}

func TestDrain(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q.Handle("drain-queue", func(ctx context.Context, job *Job) error { return nil })

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	// Ready jobs on a paused queue stay pending.
	if err := q.Pause(ctx, "drain-paused"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := q.Submit(ctx, "drain-paused", Job{Name: "job"}); err != nil {
			t.Fatal(err)
		}
	}

	// Scheduled jobs are pending as well.
	future := time.Now().UTC().Add(10 * time.Minute)
	for range 2 {
		if err := q.Submit(ctx, "drain-queue", Job{Name: "job", RunAt: future}); err != nil {
			t.Fatal(err)
		}
	}

	if err := q.Drain(ctx, "drain-paused"); err != nil {
		t.Fatal(err)
	}
	if err := q.Drain(ctx, "drain-queue"); err != nil {
		t.Fatal(err)
	}

	st, err := q.storage.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := st.ReadyCount["drain-paused"]; n != 0 {
		t.Fatalf("expected drain-paused to be drained, got %d ready jobs", n)
	}
	if n := st.ReadyCount["drain-queue"]; n != 0 {
		t.Fatalf("expected drain-queue to be drained, got %d ready jobs", n)
	}
	if n := st.ScheduledCount; n != 0 {
		t.Fatalf("expected scheduled jobs to be drained, got %d", n)
	}
}

func TestRetryJob(t *testing.T) {
	q := New(Config{
		MinWorkers: 1,
		MaxWorkers: 2,
		BatchSize:  10,
		LeaseTTL:   100 * time.Millisecond,
		MaxRetries: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var attempts atomic.Int32

	q.Handle("retry-queue", func(ctx context.Context, job *Job) error {
		attempts.Add(1)
		return assertErr{} // always fails
	})

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer q.Stop(ctx)

	jobID := "retry-job-1"
	if err := q.Submit(ctx, "retry-queue", Job{
		ID:   jobID,
		Name: "fail",
		Backoff: BackoffPolicy{
			Type:   BackoffFixed,
			BaseMs: 10,
			MaxMs:  10,
		},
	}); err != nil {
		t.Fatal(err)
	}

	waitForDLQ(t, ctx, q.storage, "retry-queue")

	if err := q.RetryJob(ctx, "retry-queue", jobID); err != nil {
		t.Fatal(err)
	}

	before := attempts.Load()
	waitFor(t, ctx, func() bool { return attempts.Load() > before })

	if err := q.RetryJob(ctx, "retry-queue", "does-not-exist"); err == nil {
		t.Fatal("expected error retrying a job that is not in the DLQ")
	}
}

func TestSentinelErrors(t *testing.T) {
	q := New(Config{})

	if err := q.Submit(context.Background(), "q", Job{Name: "x"}); !errors.Is(err, ErrQueueNotRunning) {
		t.Fatalf("expected ErrQueueNotRunning, got %v", err)
	}
	if err := q.Drain(context.Background(), "q"); !errors.Is(err, ErrQueueNotRunning) {
		t.Fatalf("expected ErrQueueNotRunning, got %v", err)
	}
	if err := q.RetryJob(context.Background(), "q", "id"); !errors.Is(err, ErrQueueNotRunning) {
		t.Fatalf("expected ErrQueueNotRunning, got %v", err)
	}
	if err := q.Stop(context.Background()); !errors.Is(err, ErrQueueNotRunning) {
		t.Fatalf("expected ErrQueueNotRunning, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if err := q.Start(ctx); !errors.Is(err, ErrQueueAlreadyRunning) {
		t.Fatalf("expected ErrQueueAlreadyRunning, got %v", err)
	}

	if err := q.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func waitForDLQ(t *testing.T, ctx context.Context, s storage.Storage, queue QueueName) {
	t.Helper()
	waitFor(t, ctx, func() bool {
		dls, err := s.ListDLQ(ctx, queue)
		return err == nil && len(dls) > 0
	})
}

func waitFor(t *testing.T, ctx context.Context, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for condition: %v", ctx.Err())
		default:
		}
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

type assertErr struct{}

func (assertErr) Error() string { return "test error" }
