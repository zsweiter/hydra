package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zsweiter/hydra"
)

// shared lets several queues use one store; stopping one must
// not close it under the others.
type shared struct{ *Storage }

func (shared) Close() error { return nil }

// Two instances on one database split the schedules, and every
// occurrence is handled exactly once.
func TestCluster_EachOccurrenceOnce(t *testing.T) {
	s := newDB(t)

	var mu sync.Mutex
	seen := map[string]int{}
	var queues []hydra.Queue
	for range 2 {
		q := hydra.New(hydra.Config{Storage: shared{s}, SyncInterval: 100 * time.Millisecond})
		q.Handle("c", func(ctx context.Context, job *hydra.Job) error {
			mu.Lock()
			seen[*job.ScheduleID+"@"+job.Metadata[hydra.MetadataScheduledFor]]++
			mu.Unlock()
			return nil
		})
		if err := q.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer q.Stop(context.Background())
		queues = append(queues, q)
	}

	for i := range 20 {
		sch := hydra.NewJobSchedule(fmt.Sprintf("s%02d", i), "tick", nil, hydra.IntervalRule(50*time.Millisecond))
		sch.MaxJobs = 10
		if err := queues[i%2].UpsertSchedule(context.Background(), "c", sch); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d distinct occurrences handled, want 200", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for occ, n := range seen {
		if n != 1 {
			t.Errorf("%s handled %d times", occ, n)
		}
	}

	a, _ := queues[0].Stats(context.Background())
	b, _ := queues[1].Stats(context.Background())
	if a.OwnedShards+b.OwnedShards != 256 || a.OwnedShards == 0 || b.OwnedShards == 0 {
		t.Errorf("instances own %d + %d shards, want 256 split", a.OwnedShards, b.OwnedShards)
	}
}
