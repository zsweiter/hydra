package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	hydra "github.com/zsweiter/hydra"
	"github.com/zsweiter/hydra/storage/sqlite"
)

func main() {
	storage, err := sqlite.New("./datas.db")
	if err != nil {
		log.Fatal(err)

	}
	q := hydra.New(hydra.Config{
		MinWorkers:    8,
		MaxWorkers:    16,
		BatchSize:     50,
		LeaseTTL:      time.Minute,
		ScaleDownIdle: 30 * time.Second,
		Storage:       storage,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var counter atomic.Int64

	// Single handler for all queues
	q.Handle("default", func(ctx context.Context, job *hydra.Job) error {
		n := counter.Add(1)
		t := job.RunAt.Format("15:04:05.000")
		typ := "direct"
		if job.ScheduleID != nil {
			typ = "schedule"
		}
		fmt.Printf("[%d] [%s] %-8s queue=%-8s name=%s\n", n, t, typ, job.Queue, job.Name)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		os.Exit(1)
	}

	// ── 5 cron schedules ──
	cronExprs := []struct {
		id   string
		name string
		expr string
	}{
		{"cron-1", "every-minute", "* * * * *"},
		{"cron-2", "every-2min", "*/2 * * * *"},
		{"cron-3", "every-5min", "*/5 * * * *"},
		{"cron-4", "every-30min", "*/30 * * * *"},
		{"cron-5", "every-hour", "0 * * * *"},
	}
	for _, c := range cronExprs {
		sch := hydra.NewJobScheduleWithID(c.id, c.name, []byte(c.expr),
			hydra.CronRule(c.expr))
		sch.ID = c.id
		if err := q.UpsertSchedule(ctx, "default", sch); err != nil {
			fmt.Fprintf(os.Stderr, "cron %s: %v\n", c.id, err)
		}
	}

	// ── 5 interval schedules ──
	intervals := []struct {
		id   string
		name string
		d    time.Duration
	}{
		{"int-1", "every-2s", 2 * time.Second},
		{"int-2", "every-3s", 3 * time.Second},
		{"int-3", "every-5s", 5 * time.Second},
		{"int-4", "every-10s", 10 * time.Second},
		{"int-5", "every-15s", 15 * time.Second},
	}
	for _, iv := range intervals {
		sch := hydra.NewJobScheduleWithID(iv.id, iv.name, []byte(iv.d.String()),
			hydra.IntervalRule(iv.d))
		sch.ID = iv.id
		if err := q.UpsertSchedule(ctx, "default", sch); err != nil {
			fmt.Fprintf(os.Stderr, "interval %s: %v\n", iv.id, err)
		}
	}

	// ── 5 RRULE schedules ──
	rruleExprs := []struct {
		id   string
		name string
		expr string
	}{
		{"rrule-1", "every-1m", "FREQ=MINUTELY;INTERVAL=1"},
		{"rrule-2", "every-2m", "FREQ=MINUTELY;INTERVAL=2"},
		{"rrule-3", "every-5m", "FREQ=MINUTELY;INTERVAL=5"},
		{"rrule-4", "every-10m", "FREQ=MINUTELY;INTERVAL=10"},
		{"rrule-5", "every-1h", "FREQ=HOURLY;INTERVAL=1"},
	}
	for _, r := range rruleExprs {
		sch := hydra.NewJobScheduleWithID(r.id, r.name, []byte(r.expr),
			hydra.RRuleRule(r.expr))
		sch.ID = r.id
		if err := q.UpsertSchedule(ctx, "default", sch); err != nil {
			fmt.Fprintf(os.Stderr, "rrule %s: %v\n", r.id, err)
		}
	}

	// ── 10 direct jobs ──
	fmt.Println("\n--- Submitting 10 direct jobs ---")
	for i := 1; i <= 10; i++ {
		job := hydra.NewJob(fmt.Sprintf("direct-%d", i), []byte("direct"), nil)
		if err := q.Submit(ctx, "default", job); err != nil {
			fmt.Fprintf(os.Stderr, "direct submit: %v\n", err)
		}
	}

	fmt.Println("\n══════════════════════════════════════════")
	fmt.Println("  Daemon running. Press Ctrl+C to stop.")
	fmt.Println("  5 cron + 5 interval + 5 rrule + 10 direct")
	fmt.Println("══════════════════════════════════════════")

	// Block until SIGINT/SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\n--- Shutting down ---")
	q.Stop(ctx)
	fmt.Printf("Total jobs processed: %d\n", counter.Load())
}
