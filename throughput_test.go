package hydra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkThroughputJobs(b *testing.B) {
	sizes := []struct {
		min, max int
		batch    int
	}{
		{1, 2, 10},
		{4, 8, 50},
		{8, 16, 100},
		{16, 32, 200},
	}

	for _, s := range sizes {
		name := fmt.Sprintf("workers_%d-%d_batch_%d", s.min, s.max, s.batch)
		b.Run(name, func(b *testing.B) {
			q := New(Config{
				MinWorkers: s.min,
				MaxWorkers: s.max,
				BatchSize:  s.batch,
				LeaseTTL:   time.Minute,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			var processed atomic.Int64
			q.Handle("throughput", func(ctx context.Context, job *Job) error {
				processed.Add(1)
				return nil
			})
			q.Start(ctx)

			b.ResetTimer()

			// Submit N jobs and measure throughput
			for i := 0; i < b.N; i++ {
				q.Submit(ctx, "throughput", Job{
					Name: fmt.Sprintf("job-%d", i),
					Data: []byte("payload"),
				})
			}

			// Wait for all to complete
			for processed.Load() < int64(b.N) {
				time.Sleep(time.Millisecond)
			}

			b.StopTimer()
			q.Stop(ctx)

			elapsed := time.Duration(b.Elapsed().Nanoseconds())
			if elapsed > 0 {
				jobsPerSec := float64(b.N) / elapsed.Seconds()
				b.ReportMetric(jobsPerSec, "jobs/sec")
			}
		})
	}
}

func BenchmarkThroughputSchedule(b *testing.B) {
	sizes := []struct {
		minWorkers int
		maxWorkers int
		batch      int
		nSchedules int
	}{
		{4, 8, 50, 1},
		{4, 8, 50, 5},
		{8, 16, 100, 10},
		{16, 32, 200, 20},
	}

	for _, s := range sizes {
		name := fmt.Sprintf("schedules_%d_workers_%d-%d", s.nSchedules, s.minWorkers, s.maxWorkers)
		b.Run(name, func(b *testing.B) {
			q := New(Config{
				MinWorkers: s.minWorkers,
				MaxWorkers: s.maxWorkers,
				BatchSize:  s.batch,
				LeaseTTL:   time.Minute,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			var processed atomic.Int64
			q.Handle("sched-tp", func(ctx context.Context, job *Job) error {
				processed.Add(1)
				return nil
			})
			q.Start(ctx)

			// Create schedules with very fast interval
			for i := range s.nSchedules {
				sch := NewJobSchedule(fmt.Sprintf("tp-sched-%d", i), fmt.Sprintf("tp-%d", i), []byte("tp"),
					IntervalRule(time.Millisecond),
				)
				q.UpsertSchedule(ctx, "sched-tp", sch)
			}

			b.ResetTimer()

			// Wait until we've generated b.N jobs
			target := int64(b.N)
			for processed.Load() < target {
				time.Sleep(time.Millisecond)
			}

			b.StopTimer()

			// Clean up schedules
			for i := range s.nSchedules {
				q.RemoveSchedule(ctx, "sched-tp", fmt.Sprintf("tp-sched-%d", i))
			}
			q.Stop(ctx)

			elapsed := time.Duration(b.Elapsed().Nanoseconds())
			if elapsed > 0 {
				jobsPerSec := float64(b.N) / elapsed.Seconds()
				b.ReportMetric(jobsPerSec, "jobs/sec")
			}
		})
	}
}

func TestStressReport(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	q := New(Config{
		MinWorkers: 8,
		MaxWorkers: 16,
		BatchSize:  100,
		LeaseTTL:   time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var submittedDirect atomic.Int64
	var processedDirect atomic.Int64
	var processedSched atomic.Int64

	q.Handle("direct", func(ctx context.Context, job *Job) error {
		processedDirect.Add(1)
		return nil
	})
	q.Handle("sched", func(ctx context.Context, job *Job) error {
		processedSched.Add(1)
		return nil
	})
	q.Start(ctx)
	defer q.Stop(ctx)

	for i := range 5 {
		sch := NewJobSchedule(fmt.Sprintf("stress-sched-%d", i), "stress", []byte("stress"),
			IntervalRule(5*time.Millisecond),
		)
		q.UpsertSchedule(ctx, "sched", sch)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					q.Submit(ctx, "direct", Job{
						Name: fmt.Sprintf("direct-%d", id),
						Data: []byte("stress"),
					})
					submittedDirect.Add(1)
				}
			}
		}(g)
	}

	time.Sleep(10 * time.Second)
	close(done)
	wg.Wait()

	stats, _ := q.Stats(ctx)

	t.Logf("")
	t.Logf("═══════════════════════════════════════")
	t.Logf("  STRESS TEST - 10 segundos")
	t.Logf("═══════════════════════════════════════")
	t.Logf("")
	t.Logf("  DIRECT JOBS")
	t.Logf("    Submitted:  %d", submittedDirect.Load())
	t.Logf("    Processed:  %d", processedDirect.Load())
	dirRate := float64(processedDirect.Load()) / 10.0
	t.Logf("    Throughput: %.0f jobs/sec", dirRate)
	t.Logf("")
	t.Logf("  SCHEDULED JOBS")
	t.Logf("    Processed:  %d", processedSched.Load())
	schedRate := float64(processedSched.Load()) / 10.0
	t.Logf("    Throughput: %.0f jobs/sec", schedRate)
	t.Logf("")
	t.Logf("  TOTAL")
	t.Logf("    Combined:   %.0f jobs/sec", dirRate+schedRate)
	t.Logf("")
	t.Logf("  STORAGE STATS")
	t.Logf("    Ready:      %v", stats.ReadyCount)
	t.Logf("    Scheduled:  %d", stats.ScheduledCount)
	t.Logf("    DLQ:        %d", stats.DLQCount)
	t.Logf("    Workers:    %d", q.pool.ActiveWorkers())
	t.Logf("═══════════════════════════════════════")
}
