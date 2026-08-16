package hydra

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaxCPULoadDirect(t *testing.T) {
	if testing.Short() {
		t.Skip("skip max CPU test in short mode")
	}

	configs := []struct {
		name        string
		minW, maxW  int
		batch       int
		submitters  int
		payloadSize int
	}{
		{"baseline-4-8-50", 4, 8, 50, 4, 64},
		{"high-workers-32-64-100", 32, 64, 100, 8, 64},
		{"max-workers-64-128-200", 64, 128, 200, 16, 64},
		{"big-batch-16-32-500", 16, 32, 500, 8, 64},
		{"max-all-64-128-500", 64, 128, 500, 16, 64},
		{"payload-1k-16-32-100", 16, 32, 100, 8, 1024},
		{"payload-10k-16-32-50", 16, 32, 50, 4, 10240},
		{"ultra-submit-8-16-100", 8, 16, 100, 32, 64},
	}

	const testDuration = 5 * time.Second

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()

			q := New(Config{
				MinWorkers: cfg.minW,
				MaxWorkers: cfg.maxW,
				BatchSize:  cfg.batch,
				LeaseTTL:   time.Minute,
			})
			ctx, cancel := context.WithTimeout(context.Background(), testDuration+2*time.Second)
			defer cancel()

			var processed atomic.Int64
			var submitted atomic.Int64

			payload := make([]byte, cfg.payloadSize)
			for i := range payload {
				payload[i] = byte(i)
			}

			q.Handle("limit", func(ctx context.Context, job *Job) error {
				processed.Add(1)
				return nil
			})
			if err := q.Start(ctx); err != nil {
				t.Fatal(err)
			}

			var wg sync.WaitGroup
			done := make(chan struct{})

			start := time.Now().UTC()

			for g := 0; g < cfg.submitters; g++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					buf := make([]byte, cfg.payloadSize)
					copy(buf, payload)
					for {
						select {
						case <-done:
							return
						default:
							q.Submit(ctx, "limit", Job{
								Name: fmt.Sprintf("limit-%d-%d", id, submitted.Add(1)),
								Data: buf,
							})
						}
					}
				}(g)
			}

			time.Sleep(testDuration)
			close(done)
			wg.Wait()
			elapsed := time.Since(start)

			// Drain remaining
			time.Sleep(500 * time.Millisecond)
			stats, _ := q.Stats(ctx)
			q.Stop(ctx)

			procTotal := processed.Load()
			subTotal := submitted.Load()
			jobsSec := float64(procTotal) / elapsed.Seconds()
			backlog := int(subTotal - procTotal)

			readySum := 0
			for _, c := range stats.ReadyCount {
				readySum += int(c)
			}

			t.Logf("")
			t.Logf("── %s ──", cfg.name)
			t.Logf("  Workers:     %d-%d (batch %d)", cfg.minW, cfg.maxW, cfg.batch)
			t.Logf("  Submitters:  %d", cfg.submitters)
			t.Logf("  Payload:     %d bytes", cfg.payloadSize)
			t.Logf("  Submitted:   %d", subTotal)
			t.Logf("  Processed:   %d", procTotal)
			t.Logf("  Throughput:  %.0f jobs/sec", jobsSec)
			t.Logf("  Backlog:     %d (ready: %d)", backlog, readySum)
			t.Logf("  Goroutines:  %d", runtime.NumGoroutine())

			// Always report metric
			t.Logf("  RESULT:      %.0f jobs/sec", jobsSec)
		})
	}
}

func TestMaxCPULoadSchedules(t *testing.T) {
	if testing.Short() {
		t.Skip("skip max CPU test in short mode")
	}

	scheduleCounts := []int{1, 5, 10, 50, 100, 200}
	intervals := []time.Duration{time.Millisecond, 500 * time.Microsecond, 100 * time.Microsecond}

	const testDuration = 5 * time.Second

	for _, n := range scheduleCounts {
		for _, interval := range intervals {
			if n*int(interval) < 100*int(time.Microsecond) && interval < time.Millisecond {
				continue // skip impossible combos
			}

			name := fmt.Sprintf("sched_%d_interval_%d", n, interval/time.Microsecond)
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				q := New(Config{
					MinWorkers: 16,
					MaxWorkers: 32,
					BatchSize:  200,
					LeaseTTL:   time.Minute,
				})
				ctx, cancel := context.WithTimeout(context.Background(), testDuration+2*time.Second)
				defer cancel()

				var processed atomic.Int64
				q.Handle("sched-limit", func(ctx context.Context, job *Job) error {
					processed.Add(1)
					return nil
				})
				if err := q.Start(ctx); err != nil {
					t.Fatal(err)
				}

				for i := range n {
					sch := NewJobScheduleWithID(fmt.Sprintf("limit-sched-%d", i), "limit", []byte("limit"),
						IntervalRule(interval),
					)
					q.UpsertSchedule(ctx, "sched-limit", sch)
				}

				time.Sleep(testDuration)
				procTotal := processed.Load()

				// Cleanup
				for i := range n {
					q.RemoveSchedule(ctx, "sched-limit", fmt.Sprintf("limit-sched-%d", i))
				}
				stats, _ := q.Stats(ctx)
				q.Stop(ctx)

				jobsSec := float64(procTotal) / testDuration.Seconds()
				expectedMax := float64(n) / interval.Seconds()

				t.Logf("")
				t.Logf("── %s ──", name)
				t.Logf("  Schedules:   %d", n)
				t.Logf("  Interval:    %v", interval)
				t.Logf("  Theoretical: %.0f jobs/sec", expectedMax)
				t.Logf("  Actual:      %.0f jobs/sec", jobsSec)
				t.Logf("  Efficiency:  %.1f%%", jobsSec/expectedMax*100)
				t.Logf("  Goroutines:  %d", runtime.NumGoroutine())
				t.Logf("  DLQ:         %d", stats.DLQCount)

				if jobsSec > 0 {
					t.Logf("  RESULT:      %.0f jobs/sec (%.1f%%)", jobsSec, jobsSec/expectedMax*100)
				}
			})
		}
	}
}

func TestBottleneck(t *testing.T) {
	if testing.Short() {
		t.Skip("skip bottleneck test in short mode")
	}

	t.Run("id-gen", func(t *testing.T) {
		start := time.Now().UTC()
		n := 100000
		for i := range n {
			NewJob(fmt.Sprintf("id-%d", i), nil, nil)
		}
		elapsed := time.Since(start)
		rate := float64(n) / elapsed.Seconds()
		t.Logf("  ID gen:      %.0f ops/sec (%.0f ns/op)", rate, float64(elapsed.Nanoseconds())/float64(n))
	})

	t.Run("submit-only", func(t *testing.T) {
		q := New(Config{
			MinWorkers: 0,
			MaxWorkers: 0,
			BatchSize:  0,
			LeaseTTL:   time.Minute,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		q.Start(ctx)

		start := time.Now().UTC()
		n := 100000
		for i := range n {
			q.Submit(ctx, "bottleneck", Job{
				Name: fmt.Sprintf("bn-%d", i),
				Data: []byte("data"),
			})
		}
		elapsed := time.Since(start)
		q.Stop(ctx)

		rate := float64(n) / elapsed.Seconds()
		t.Logf("  Submit only: %.0f ops/sec (%.0f ns/op)", rate, float64(elapsed.Nanoseconds())/float64(n))
	})

	t.Run("submit-storage", func(t *testing.T) {
		q := New(Config{
			MinWorkers: 0,
			MaxWorkers: 0,
			BatchSize:  0,
			LeaseTTL:   time.Minute,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		q.Start(ctx)

		start := time.Now().UTC()
		n := 100000
		for i := range n {
			q.Submit(ctx, "bottleneck", Job{
				Name: fmt.Sprintf("bn-%d", i),
				Data: []byte("data"),
			})
		}
		elapsed := time.Since(start)
		q.Stop(ctx)

		rate := float64(n) / elapsed.Seconds()
		t.Logf("  Submit→Storage: %.0f ops/sec (%.0f ns/op)", rate, float64(elapsed.Nanoseconds())/float64(n))
	})

	t.Run("full-pipe", func(t *testing.T) {
		q := New(Config{
			MinWorkers: 8,
			MaxWorkers: 16,
			BatchSize:  100,
			LeaseTTL:   time.Minute,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var processed atomic.Int64
		q.Handle("bottleneck", func(ctx context.Context, job *Job) error {
			processed.Add(1)
			return nil
		})
		q.Start(ctx)

		start := time.Now().UTC()
		n := 50000
		for i := range n {
			q.Submit(ctx, "bottleneck", Job{
				Name: fmt.Sprintf("bn-%d", i),
				Data: []byte("data"),
			})
		}
		for processed.Load() < int64(n) {
			time.Sleep(time.Millisecond)
		}
		elapsed := time.Since(start)
		q.Stop(ctx)

		rate := float64(n) / elapsed.Seconds()
		t.Logf("  Full pipe:   %.0f jobs/sec", rate)
		t.Logf("  Compared to submit-only: %.1f%%", rate/380000*100)
	})
}

func TestMaxSchedulesThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode")
	}

	for n := 10; n <= 500; n *= 2 {
		n := n
		name := fmt.Sprintf("schedules_%d", n)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			q := New(Config{
				MinWorkers: 32,
				MaxWorkers: 64,
				BatchSize:  500,
				LeaseTTL:   time.Minute,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			var processed atomic.Int64
			q.Handle("max-sched", func(ctx context.Context, job *Job) error {
				processed.Add(1)
				return nil
			})
			if err := q.Start(ctx); err != nil {
				t.Fatal(err)
			}

			const interval = time.Millisecond
			expectedMax := float64(n) / interval.Seconds()

			for i := range n {
				sch := NewJobScheduleWithID(fmt.Sprintf("max-sched-%d", i), "max", []byte("max"),
					IntervalRule(interval),
				)
				q.UpsertSchedule(ctx, "max-sched", sch)
			}

			time.Sleep(3 * time.Second)
			procTotal := processed.Load()

			for i := range n {
				q.RemoveSchedule(ctx, "max-sched", fmt.Sprintf("max-sched-%d", i))
			}
			q.Stop(ctx)

			jobsSec := float64(procTotal) / 3.0
			efficiency := jobsSec / expectedMax * 100

			t.Logf("  Schedules:   %d", n)
			t.Logf("  Actual:      %.0f jobs/sec", jobsSec)
			t.Logf("  Efficiency:  %.1f%%", efficiency)

			if efficiency < 80 {
				t.Logf("  ⚠ SATURATED at %d schedules (%.1f%%)", n, efficiency)
			}
		})
	}
}
