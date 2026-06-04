package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	hydra "github.com/zsweiter/hydra"
)

func main() {
	q := hydra.New(hydra.Config{
		MinWorkers: 2,
		MaxWorkers: 4,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var intervalCount atomic.Int64
	var rruleCount atomic.Int64

	q.Handle("ticker", func(ctx context.Context, job *hydra.Job) error {
		n := intervalCount.Add(1)
		fmt.Printf("[interval #%d] %s\n", n, job.Name)
		return nil
	})

	q.Handle("weekly", func(ctx context.Context, job *hydra.Job) error {
		n := rruleCount.Add(1)
		fmt.Printf("[rrule #%d] %s at %s\n", n, job.Name, job.RunAt.Format(time.RFC3339))
		return nil
	})

	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer q.Stop(ctx)

	// Interval schedule
	intervalSched := hydra.NewJobSchedule("", "heartbeat", []byte("ping"),
		hydra.IntervalRule(200*time.Millisecond),
	)
	if err := q.UpsertSchedule(ctx, "ticker", intervalSched); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Interval schedule created (every 200ms)")

	// RRULE schedule (every minute for demo)
	rruleSched := hydra.NewJobSchedule("", "weekly-report", []byte("report"),
		hydra.RRuleRule("FREQ=SECONDLY;INTERVAL=1"),
	)
	rruleSched.MisfirePolicy = hydra.MisfireSkip
	if err := q.UpsertSchedule(ctx, "weekly", rruleSched); err != nil {
		log.Fatal(err)
	}
	fmt.Println("RRULE schedule created (every minute)")

	fmt.Println("\n--- Running schedules (3s) ---")
	time.Sleep(3 * time.Second)

	// Pause
	fmt.Println("\n--- Pausing interval schedule ---")
	q.PauseSchedule(ctx, "ticker", intervalSched.ID)
	time.Sleep(1 * time.Second)

	ivBefore := intervalCount.Load()
	time.Sleep(1 * time.Second)
	ivAfter := intervalCount.Load()

	if ivAfter == ivBefore {
		fmt.Println("✓ Interval schedule correctly paused")
	} else {
		fmt.Printf("⚠ Interval kept firing: %d → %d\n", ivBefore, ivAfter)
	}

	// Resume
	fmt.Println("\n--- Resuming interval schedule ---")
	q.ResumeSchedule(ctx, "ticker", intervalSched.ID)
	time.Sleep(1 * time.Second)

	fmt.Println("\n--- Cleaning up ---")
	q.RemoveSchedule(ctx, "ticker", intervalSched.ID)
	q.RemoveSchedule(ctx, "weekly", rruleSched.ID)

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Interval jobs: %d\n", intervalCount.Load())
	fmt.Printf("  RRULE jobs:    %d\n", rruleCount.Load())
}
