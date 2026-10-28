package hydra

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/storage"
)

// sharedStorage lets several queues use one store; stopping
// one instance must not close it under the others.
type sharedStorage struct {
	storage.Storage
}

func (sharedStorage) Close() error { return nil }

func startInstance(t *testing.T, store storage.Storage, handled *atomic.Int64) (Queue, context.CancelFunc) {
	t.Helper()
	q := New(Config{
		Storage:      sharedStorage{store},
		SyncInterval: 50 * time.Millisecond,
		LeaseTTL:     time.Second,
	})
	q.Handle("cluster", func(ctx context.Context, job *Job) error {
		handled.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return q, func() {
		q.Stop(context.Background())
		cancel()
	}
}

func TestCluster_OccurrenceGeneratedOnce(t *testing.T) {
	store := memory.New()
	var handled atomic.Int64

	q1, stop1 := startInstance(t, store, &handled)
	defer stop1()
	_, stop2 := startInstance(t, store, &handled)
	defer stop2()

	schedule := NewJobSchedule("every-100ms", "tick", nil, IntervalRule(100*time.Millisecond))
	schedule.MaxJobs = 5
	if err := q1.UpsertSchedule(context.Background(), "cluster", schedule); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	if n := handled.Load(); n != 5 {
		t.Fatalf("handled %d jobs across instances, want exactly 5", n)
	}
}

func TestCluster_SurvivingInstanceTakesOver(t *testing.T) {
	store := memory.New()
	var handled atomic.Int64

	q1, stop1 := startInstance(t, store, &handled)
	_, stop2 := startInstance(t, store, &handled)
	defer stop2()

	schedule := NewJobSchedule("failover", "tick", nil, IntervalRule(100*time.Millisecond))
	if err := q1.UpsertSchedule(context.Background(), "cluster", schedule); err != nil {
		t.Fatal(err)
	}

	time.Sleep(250 * time.Millisecond)
	stop1()
	before := handled.Load()

	time.Sleep(600 * time.Millisecond)
	if after := handled.Load(); after-before < 3 {
		t.Fatalf("only %d jobs after the first instance stopped, want the second to keep the schedule running", after-before)
	}
}

// Instances sharing a storage split the schedules: each holds
// only those of its shards, and together all of them.
func TestCluster_InstancesSplitSchedules(t *testing.T) {
	store := memory.New()
	var handled atomic.Int64

	q1, stop1 := startInstance(t, store, &handled)
	defer stop1()
	q2, stop2 := startInstance(t, store, &handled)
	defer stop2()

	for i := range 40 {
		schedule := NewJobSchedule(fmt.Sprintf("s%02d", i), "tick", nil, IntervalRule(100*time.Millisecond))
		if err := q1.UpsertSchedule(context.Background(), "cluster", schedule); err != nil {
			t.Fatal(err)
		}
	}

	// Both must hold their share at once: wait for it rather
	// than sample after a fixed delay, which a loaded machine
	// can make too short.
	var st1, st2 StatsSnapshot
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		st1, _ = q1.Stats(context.Background())
		st2, _ = q2.Stats(context.Background())
		split := st1.OwnedShards+st2.OwnedShards == domain.ShardCount &&
			st1.SchedulesLoaded > 0 && st2.SchedulesLoaded > 0 && st1.SchedulesLoaded+st2.SchedulesLoaded == 40
		if split || time.Now().After(deadline) {
			break
		}
	}
	if st1.OwnedShards+st2.OwnedShards != domain.ShardCount {
		t.Fatalf("instances own %d + %d shards, want %d in total", st1.OwnedShards, st2.OwnedShards, domain.ShardCount)
	}
	if st1.SchedulesLoaded == 0 || st2.SchedulesLoaded == 0 || st1.SchedulesLoaded+st2.SchedulesLoaded != 40 {
		t.Fatalf("instances hold %d + %d schedules, want 40 split between them", st1.SchedulesLoaded, st2.SchedulesLoaded)
	}
}
