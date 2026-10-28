package scheduler

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/storage"
)

// claimLog records the successful claims made on a store.
type claimLog struct {
	*memory.Storage
	mu     sync.Mutex
	claims []claimRecord
}

type claimRecord struct {
	scheduleID string
	jobs       int
	first      time.Time
}

func newClaimLog() *claimLog { return &claimLog{Storage: memory.New()} }

func (c *claimLog) ClaimOccurrences(ctx context.Context, claims []storage.Claim) ([]bool, error) {
	ok, err := c.Storage.ClaimOccurrences(ctx, claims)
	if err != nil {
		return ok, err
	}
	c.mu.Lock()
	for i, claim := range claims {
		if ok[i] && len(claim.Jobs) > 0 {
			c.claims = append(c.claims, claimRecord{scheduleID: claim.ScheduleID, jobs: len(claim.Jobs), first: claim.Jobs[0].RunAt})
		}
	}
	c.mu.Unlock()

	return ok, nil
}

func (c *claimLog) records(id string) []claimRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []claimRecord
	for _, r := range c.claims {
		if id == "" || r.scheduleID == id {
			out = append(out, r)
		}
	}

	return out
}

func startScheduler(t *testing.T, store storage.Storage, configure func(*Scheduler)) (*Scheduler, context.CancelFunc) {
	t.Helper()
	s := New(store, nil)
	if configure != nil {
		configure(s)
	}

	if err := s.Load(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	stop := func() {
		cancel()
		<-done
	}

	t.Cleanup(stop)
	return s, stop
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// saveBehind stores a catch-up schedule whose next occurrence
// was missed long ago.
func saveBehind(t *testing.T, store storage.Storage, id string, every time.Duration, missed int, priority int) time.Time {
	t.Helper()
	first := time.Now().Add(-time.Duration(missed) * every).Truncate(time.Millisecond)
	sch := domain.NewJobSchedule(id, id, nil, domain.NewIntervalScheduleRule(every))
	sch.Queue = "q"
	sch.MisfirePolicy = domain.MisfireCatchUp
	sch.Priority = priority
	sch.NextRunAt = &first
	if err := store.SaveSchedule(context.Background(), &sch); err != nil {
		t.Fatal(err)
	}

	return first
}

func scheduledFor(t *testing.T, store storage.Storage, id string) []time.Time {
	t.Helper()
	jobs, err := store.ListJobs(context.Background(), storage.JobQuery{ScheduleID: id})
	if err != nil {
		t.Fatal(err)
	}

	out := make([]time.Time, 0, len(jobs))
	for _, j := range jobs {
		at, err := time.Parse(time.RFC3339Nano, j.Metadata[domain.MetadataScheduledFor])
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, at)
	}

	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })
	return out
}

// assertContiguous checks that every occurrence from first on
// was generated exactly once.
func assertContiguous(t *testing.T, got []time.Time, first time.Time, every time.Duration) {
	t.Helper()
	for i, at := range got {
		if want := first.Add(time.Duration(i) * every); !at.Equal(want) {
			t.Fatalf("occurrence %d scheduled for %v, want %v (missing or duplicated)", i, at, want)
		}
	}
}

func TestCatchUp_ReplaysBacklogInOrderedBatches(t *testing.T) {
	store := newClaimLog()
	first := saveBehind(t, store, "s", time.Second, 1000, 0)

	startScheduler(t, store, func(s *Scheduler) { s.SetCatchUpBatch(100) })
	waitFor(t, 5*time.Second, "backlog replayed", func() bool {
		got, _ := store.LoadSchedule(context.Background(), "s")
		return got.NextRunAt.After(time.Now())
	})

	claims := store.records("s")
	if len(claims) < 10 {
		t.Fatalf("%d claims, want the 1000 missed occurrences split in batches of 100", len(claims))
	}

	for i, c := range claims {
		if c.jobs > 100 {
			t.Fatalf("claim %d generated %d jobs, want at most 100", i, c.jobs)
		}
		if i > 0 && !c.first.After(claims[i-1].first) {
			t.Fatalf("claim %d starts at %v, not after the previous batch", i, c.first)
		}
	}

	got := scheduledFor(t, store, "s")
	if len(got) < 1000 {
		t.Fatalf("%d occurrences, want at least 1000", len(got))
	}

	assertContiguous(t, got, first, time.Second)
}

// A backlog must not hold back schedules that are on time.
func TestCatchUp_TimeSlicedWithOnTimeOccurrences(t *testing.T) {
	store := newClaimLog()
	saveBehind(t, store, "backlog", time.Millisecond, 200_000, 0)

	onTime := domain.NewJobSchedule("on-time", "on-time", nil, domain.NewIntervalScheduleRule(20*time.Millisecond))
	onTime.Queue = "q"
	if err := store.SaveSchedule(context.Background(), &onTime); err != nil {
		t.Fatal(err)
	}

	startScheduler(t, store, func(s *Scheduler) { s.SetCatchUpBatch(10) })
	waitFor(t, 3*time.Second, "on-time occurrences", func() bool {
		return len(store.records("on-time")) >= 5
	})

	got, _ := store.LoadSchedule(context.Background(), "backlog")
	if !got.NextRunAt.Before(time.Now().Add(-time.Minute)) {
		t.Fatal("backlog finished before the check; make it larger")
	}
	if len(store.records("backlog")) == 0 {
		t.Fatal("backlog made no progress while on-time occurrences fired")
	}
}

// Among schedules behind, the more important one (lower
// Priority) catches up first.
func TestCatchUp_ImportantBacklogFirst(t *testing.T) {
	store := newClaimLog()
	saveBehind(t, store, "minor", time.Second, 500, 5)
	saveBehind(t, store, "major", time.Second, 500, 1)

	startScheduler(t, store, func(s *Scheduler) { s.SetCatchUpBatch(50) })
	waitFor(t, 5*time.Second, "both backlogs replayed", func() bool {
		a, _ := store.LoadSchedule(context.Background(), "minor")
		b, _ := store.LoadSchedule(context.Background(), "major")
		return a.NextRunAt.After(time.Now()) && b.NextRunAt.After(time.Now())
	})

	// Both start with one batch as due occurrences; after
	// that the backlog drains major entirely before minor.
	all := store.records("")
	lastMajor, firstMinorBacklog := -1, -1
	minorSeen := 0
	for i, c := range all {
		switch c.scheduleID {
		case "major":
			lastMajor = i
		case "minor":
			if minorSeen++; minorSeen == 2 && firstMinorBacklog < 0 {
				firstMinorBacklog = i
			}
		}
	}
	if firstMinorBacklog < lastMajor {
		t.Fatalf("minor backlog resumed at claim %d before major finished at %d", firstMinorBacklog, lastMajor)
	}
}

// A replay interrupted by a restart resumes where it stopped,
// without losing or repeating occurrences.
func TestCatchUp_ResumesAfterRestart(t *testing.T) {
	store := newClaimLog()
	first := saveBehind(t, store, "s", time.Millisecond, 50_000, 0)

	_, stop := startScheduler(t, store, func(s *Scheduler) { s.SetCatchUpBatch(10) })
	waitFor(t, 3*time.Second, "some batches", func() bool { return len(store.records("s")) >= 20 })
	stop()

	mid, _ := store.LoadSchedule(context.Background(), "s")
	if !mid.NextRunAt.Before(time.Now()) {
		t.Fatal("backlog finished before the restart; make it larger")
	}

	startScheduler(t, store, func(s *Scheduler) { s.SetCatchUpBatch(1000) })
	waitFor(t, 10*time.Second, "backlog replayed", func() bool {
		got, _ := store.LoadSchedule(context.Background(), "s")
		return got.NextRunAt.After(time.Now())
	})

	got := scheduledFor(t, store, "s")
	if len(got) < 50_000 {
		t.Fatalf("%d occurrences, want at least 50000", len(got))
	}
	assertContiguous(t, got, first, time.Millisecond)
}

func TestCatchUp_StopsAtMaxJobs(t *testing.T) {
	store := newClaimLog()
	first := time.Now().Add(-time.Minute)
	sch := domain.NewJobSchedule("s", "s", nil, domain.NewIntervalScheduleRule(time.Second))
	sch.Queue = "q"
	sch.MisfirePolicy = domain.MisfireCatchUp
	sch.MaxJobs = 5
	sch.NextRunAt = &first
	if err := store.SaveSchedule(context.Background(), &sch); err != nil {
		t.Fatal(err)
	}

	startScheduler(t, store, nil)
	waitFor(t, 2*time.Second, "schedule finished", func() bool {
		got, _ := store.LoadSchedule(context.Background(), "s")
		return got.Status == domain.ScheduleFinished
	})

	if got := scheduledFor(t, store, "s"); len(got) != 5 {
		t.Fatalf("%d occurrences, want MaxJobs = 5", len(got))
	}
}

// Only schedules due within the window are held in memory; the
// others are loaded as the window advances.
func TestWindow_LoadsOnlyDueSchedules(t *testing.T) {
	store := newClaimLog()
	ctx := context.Background()

	far := time.Now().Add(time.Hour)
	for i := range 100 {
		sch := domain.NewJobSchedule(string(rune('a'+i%26))+time.Duration(i).String(), "far", nil, domain.NewIntervalScheduleRule(time.Hour))
		sch.Queue = "q"
		sch.NextRunAt = &far
		if err := store.SaveSchedule(ctx, &sch); err != nil {
			t.Fatal(err)
		}
	}
	soon := time.Now().Add(150 * time.Millisecond)
	near := domain.NewJobSchedule("near", "near", nil, domain.NewIntervalScheduleRule(150*time.Millisecond))
	near.Queue = "q"
	near.NextRunAt = &soon
	if err := store.SaveSchedule(ctx, &near); err != nil {
		t.Fatal(err)
	}

	s, stop := startScheduler(t, store, func(s *Scheduler) { s.SetWindow(100*time.Millisecond, 0) })

	// "near" is outside the first window, so it is loaded by
	// a later one and still fires repeatedly.
	waitFor(t, 2*time.Second, "near fires from later windows", func() bool {
		return len(store.records("near")) >= 3
	})
	stop()

	s.mu.Lock()
	loaded := len(s.entries)
	s.mu.Unlock()
	if loaded > 1 {
		t.Fatalf("%d schedules in memory, want only those due within the window", loaded)
	}
	if n := len(store.records("")) - len(store.records("near")); n != 0 {
		t.Fatalf("%d far schedules fired", n)
	}
}

// When more schedules are due than a load may hold, the rest
// come with the following loads and none is skipped.
func TestWindow_TruncatedLoadsCatchUp(t *testing.T) {
	store := newClaimLog()
	ctx := context.Background()

	due := time.Now().Add(20 * time.Millisecond)
	for i := range 50 {
		sch := domain.NewJobSchedule(time.Duration(i+1).String(), "s", nil, domain.NewIntervalScheduleRule(time.Hour))
		sch.Queue = "q"
		sch.NextRunAt = &due
		if err := store.SaveSchedule(ctx, &sch); err != nil {
			t.Fatal(err)
		}
	}

	startScheduler(t, store, func(s *Scheduler) { s.SetWindow(time.Minute, 7) })
	waitFor(t, 3*time.Second, "all 50 fired", func() bool { return len(store.records("")) == 50 })
}

// Schedulers owning disjoint shards of a shared store each
// fire only their own schedules, and together all of them.
func TestShards_SplitSchedules(t *testing.T) {
	store := newClaimLog()
	ctx := context.Background()

	ids := make([]string, 40)
	for i := range ids {
		ids[i] = "s" + time.Duration(i).String()
		sch := domain.NewJobSchedule(ids[i], ids[i], nil, domain.NewIntervalScheduleRule(50*time.Millisecond))
		sch.Queue = "q"
		if err := store.SaveSchedule(ctx, &sch); err != nil {
			t.Fatal(err)
		}
	}

	members := []string{"a", "b"}
	var schedulers []*Scheduler
	for i := range 2 {
		s, _ := startScheduler(t, store, func(s *Scheduler) { s.SetShards(domain.OwnedShards(members[i], members)) })
		schedulers = append(schedulers, s)
	}

	waitFor(t, 3*time.Second, "every schedule fired", func() bool {
		for _, id := range ids {
			if len(store.records(id)) == 0 {
				return false
			}
		}
		return true
	})

	for i, s := range schedulers {
		s.mu.Lock()
		for id := range s.entries {
			if !slices.Contains(domain.OwnedShards(members[i], members), domain.ScheduleShard(id)) {
				t.Errorf("scheduler %d holds %s from a shard it does not own", i, id)
			}
		}
		s.mu.Unlock()
	}
}
