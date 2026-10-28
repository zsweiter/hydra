// Package storagetest checks that a storage.Storage
// implementation behaves as the scheduler expects. Each store
// runs it from its own tests:
//
//	func TestConformance(t *testing.T) {
//		storagetest.Run(t, func(t *testing.T) storage.Storage { return newDB(t) })
//	}
package storagetest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// Factory returns an empty storage for one test.
type Factory func(t *testing.T) storage.Storage

// Run runs every conformance test against stores from newStore.
func Run(t *testing.T, newStore Factory) {
	t.Run("LoadDueSchedules", func(t *testing.T) { testLoadDue(t, newStore(t)) })
	t.Run("LoadDueSchedulesAfter", func(t *testing.T) { testLoadDueAfter(t, newStore(t)) })
	t.Run("LoadDueSchedulesChangedSince", func(t *testing.T) { testLoadDueChangedSince(t, newStore(t)) })
	t.Run("LoadSchedules", func(t *testing.T) { testLoadSchedules(t, newStore(t)) })
	t.Run("SaveSchedules", func(t *testing.T) { testSaveSchedules(t, newStore(t)) })
	t.Run("ClaimOccurrences", func(t *testing.T) { testClaimOccurrences(t, newStore(t)) })
	t.Run("ClaimOccurrencesConcurrent", func(t *testing.T) { testClaimConcurrent(t, newStore(t)) })
	t.Run("AckJobs", func(t *testing.T) { testAckJobs(t, newStore(t)) })
	t.Run("PurgeHistory", func(t *testing.T) { testPurgeHistory(t, newStore(t)) })
	t.Run("ArchivedIsHistory", func(t *testing.T) { testArchived(t, newStore(t)) })
	t.Run("Heartbeat", func(t *testing.T) { testHeartbeat(t, newStore(t)) })
}

func ids(schedules []*domain.JobSchedule) []string {
	out := make([]string, len(schedules))
	for i, s := range schedules {
		out[i] = s.ID
	}
	return out
}

func save(t *testing.T, s storage.Storage, id string, next *time.Time, status domain.ScheduleStatus) *domain.JobSchedule {
	t.Helper()
	sch := domain.NewJobSchedule(id, id, []byte("payload-"+id), domain.NewIntervalScheduleRule(time.Minute))
	sch.Queue = "q"
	sch.NextRunAt = next
	sch.Status = status
	sch.Metadata = map[string]string{"k": id}
	if err := s.SaveSchedule(context.Background(), &sch); err != nil {
		t.Fatal(err)
	}
	return &sch
}

func job(id, schedule string) *domain.Job {
	return &domain.Job{
		ID: id, Name: "n", Queue: "q", Data: []byte("d"), RunAt: time.Now(),
		MaxAttempts: 3, Backoff: domain.DefaultBackoff(), ScheduleID: &schedule,
		Metadata: map[string]string{},
	}
}

func claimOne(t *testing.T, s storage.Storage, id string, expected, next time.Time, jobs ...*domain.Job) bool {
	t.Helper()
	ok, err := storage.ClaimOne(context.Background(), s, id, expected, next, jobs)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func testLoadDue(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }

	save(t, s, "late", at(-time.Hour), domain.ScheduleActive)
	save(t, s, "soon", at(10*time.Second), domain.ScheduleActive)
	save(t, s, "unset", nil, domain.ScheduleActive)
	save(t, s, "later", at(time.Hour), domain.ScheduleActive)
	save(t, s, "paused", at(-time.Minute), domain.SchedulePaused)
	save(t, s, "done", at(-time.Minute), domain.ScheduleFinished)

	got, err := s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"unset", "late", "soon"}; fmt.Sprint(ids(got)) != fmt.Sprint(want) {
		t.Fatalf("due = %v, want %v (active only, earliest first, unset first)", ids(got), want)
	}
	for _, sch := range got {
		if len(sch.Data) != 0 || len(sch.Metadata) != 0 {
			t.Fatalf("%s loaded with its payload; the window holds schedules without it", sch.ID)
		}
		if sch.Rule.Type != domain.ScheduleInterval || sch.Queue != "q" || sch.UpdatedAt.IsZero() {
			t.Fatalf("%s loaded without its rule, queue or revision: %+v", sch.ID, sch)
		}
	}

	got, _ = s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(time.Minute), Limit: 2})
	if len(got) != 2 {
		t.Fatalf("limit 2 returned %d", len(got))
	}

	// A claim moves the schedule in the index.
	if !claimOne(t, s, "late", now.Add(-time.Hour), now.Add(2*time.Hour)) {
		t.Fatal("claim of late failed")
	}
	// Pausing removes it; resuming puts it back.
	if err := s.UpdateScheduleStatus(ctx, "soon", domain.SchedulePaused); err != nil {
		t.Fatal(err)
	}
	got, _ = s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(time.Minute)})
	if want := []string{"unset"}; fmt.Sprint(ids(got)) != fmt.Sprint(want) {
		t.Fatalf("due after claim and pause = %v, want %v", ids(got), want)
	}
	if err := s.UpdateScheduleStatus(ctx, "soon", domain.ScheduleActive); err != nil {
		t.Fatal(err)
	}
	got, _ = s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(time.Minute)})
	if want := []string{"unset", "soon"}; fmt.Sprint(ids(got)) != fmt.Sprint(want) {
		t.Fatalf("due after resume = %v, want %v", ids(got), want)
	}

	// Shards partition the result.
	members := []string{"a", "b", "c"}
	all, _ := s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(3 * time.Hour)})
	var split int
	for _, m := range members {
		owned := domain.OwnedShards(m, members)
		part, err := s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(3 * time.Hour), Shards: owned})
		if err != nil {
			t.Fatal(err)
		}
		for _, sch := range part {
			if !slices.Contains(owned, domain.ScheduleShard(sch.ID)) {
				t.Fatalf("shard query of %s returned %s from shard %d", m, sch.ID, domain.ScheduleShard(sch.ID))
			}
		}
		split += len(part)
	}
	if split != len(all) {
		t.Fatalf("shards returned %d schedules in total, want %d", split, len(all))
	}
}

func testLoadDueAfter(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }

	save(t, s, "unset", nil, domain.ScheduleActive)
	save(t, s, "edge", at(time.Minute), domain.ScheduleActive)
	save(t, s, "in", at(90*time.Second), domain.ScheduleActive)
	save(t, s, "out", at(3*time.Minute), domain.ScheduleActive)

	got, err := s.LoadDueSchedules(ctx, storage.DueQuery{After: now.Add(time.Minute), Before: now.Add(2 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"in"}; fmt.Sprint(ids(got)) != fmt.Sprint(want) {
		t.Fatalf("due after = %v, want %v (strictly after, unset left out)", ids(got), want)
	}
}

func testLoadDueChangedSince(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }

	save(t, s, "old", at(time.Second), domain.ScheduleActive)
	time.Sleep(20 * time.Millisecond)
	since := time.Now()
	time.Sleep(20 * time.Millisecond)
	save(t, s, "new", at(time.Second), domain.ScheduleActive)
	save(t, s, "new-paused", at(time.Second), domain.SchedulePaused)

	got, err := s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(time.Minute), ChangedSince: since})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"new"}; fmt.Sprint(ids(got)) != fmt.Sprint(want) {
		t.Fatalf("changed since = %v, want %v", ids(got), want)
	}

	// Claims do not count as changes: they are the scheduler's own.
	if !claimOne(t, s, "old", now.Add(time.Second), now.Add(time.Minute)) {
		t.Fatal("claim failed")
	}
	got, _ = s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(2 * time.Minute), ChangedSince: since})
	if want := []string{"new"}; fmt.Sprint(ids(got)) != fmt.Sprint(want) {
		t.Fatalf("changed since after a claim = %v, want %v", ids(got), want)
	}
}

func testLoadSchedules(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	save(t, s, "a", nil, domain.ScheduleActive)
	save(t, s, "b", nil, domain.ScheduleCancelled)

	got, err := s.LoadSchedules(ctx, []string{"b", "missing", "a"})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].ID < got[j].ID })
	if fmt.Sprint(ids(got)) != "[a b]" {
		t.Fatalf("loaded %v, want [a b]", ids(got))
	}
	if string(got[0].Data) != "payload-a" || got[0].Metadata["k"] != "a" || got[1].Status != domain.ScheduleCancelled {
		t.Fatalf("loaded without payload or status: %+v %+v", got[0], got[1])
	}

	if got, err := s.LoadSchedules(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("empty load = %v, %v", got, err)
	}
}

func testSaveSchedules(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	next := time.Now().Add(time.Minute).Truncate(time.Millisecond)

	v := func(id string, version int64, status domain.ScheduleStatus) *domain.JobSchedule {
		sch := domain.NewJobSchedule(id, id, []byte("v"+fmt.Sprint(version)), domain.NewIntervalScheduleRule(time.Minute))
		sch.Queue = "q"
		sch.Version = version
		sch.Status = status
		sch.NextRunAt = &next
		return &sch
	}

	errs, err := s.SaveSchedules(ctx, []*domain.JobSchedule{v("a", 5, domain.ScheduleActive), v("b", 0, domain.SchedulePaused), v("c", 1, domain.ScheduleArchived)})
	if err != nil || len(errs) != 3 || errs[0] != nil || errs[1] != nil || errs[2] != nil {
		t.Fatalf("first batch: %v %v", errs, err)
	}

	// An older and an equal version are stale; a newer one and
	// an unversioned one win.
	errs, err = s.SaveSchedules(ctx, []*domain.JobSchedule{v("a", 4, domain.ScheduleActive), v("a", 5, domain.ScheduleActive), v("a", 6, domain.SchedulePaused), v("b", 0, domain.ScheduleActive)})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(errs[0], storage.ErrStaleVersion) || !errors.Is(errs[1], storage.ErrStaleVersion) || errs[2] != nil || errs[3] != nil {
		t.Fatalf("second batch errors = %v, want [stale stale nil nil]", errs)
	}

	a, _ := s.LoadSchedule(ctx, "a")
	if a.Version != 6 || a.Status != domain.SchedulePaused || string(a.Data) != "v6" {
		t.Fatalf("a = version %d status %v data %s, want the newest", a.Version, a.Status, a.Data)
	}

	// Indexes follow the status: only b is due now.
	due, _ := s.LoadDueSchedules(ctx, storage.DueQuery{Before: next.Add(time.Second)})
	if fmt.Sprint(ids(due)) != "[b]" {
		t.Fatalf("due = %v, want [b]", ids(due))
	}
	archived, _ := s.ListSchedules(ctx, storage.ScheduleQuery{Statuses: []domain.ScheduleStatus{domain.ScheduleArchived}})
	if fmt.Sprint(ids(archived)) != "[c]" {
		t.Fatalf("archived = %v, want [c]", ids(archived))
	}

	if errs, err := s.SaveSchedules(ctx, nil); err != nil || len(errs) != 0 {
		t.Fatalf("empty batch = %v, %v", errs, err)
	}
}

func testClaimOccurrences(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	next := now.Add(time.Minute)

	save(t, s, "a", &now, domain.ScheduleActive)
	save(t, s, "b", &now, domain.ScheduleActive)
	capped := save(t, s, "capped", &now, domain.ScheduleActive)
	capped.MaxJobs = 3
	if err := s.SaveSchedule(ctx, capped); err != nil {
		t.Fatal(err)
	}

	ok, err := s.ClaimOccurrences(ctx, []storage.Claim{
		{ScheduleID: "a", Expected: now, Next: next, Generated: 1, Jobs: []*domain.Job{job("ja", "a")}},
		{ScheduleID: "b", Expected: now.Add(-time.Second), Next: next, Generated: 1, Jobs: []*domain.Job{job("jb", "b")}},
		{ScheduleID: "missing", Expected: now, Next: next, Generated: 1},
		// Delivered directly: counted, but no job stored.
		{ScheduleID: "capped", Expected: now, Next: next, Generated: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ok) != "[true false false true]" {
		t.Fatalf("claims = %v, want [true false false true]", ok)
	}

	jobs, _ := s.ListJobs(ctx, storage.JobQuery{ScheduleID: "a"})
	if len(jobs) != 1 || jobs[0].ID != "ja" {
		t.Fatalf("jobs of a = %v, want the claimed one", jobs)
	}
	if jobs, _ := s.ListJobs(ctx, storage.JobQuery{ScheduleID: "b"}); len(jobs) != 0 {
		t.Fatalf("a lost claim stored %d jobs", len(jobs))
	}

	got, _ := s.LoadSchedule(ctx, "a")
	if got.NextRunAt == nil || !got.NextRunAt.Equal(next) || got.GeneratedJobs != 1 || got.LastRunAt == nil {
		t.Fatalf("a after claim: next %v generated %d last %v", got.NextRunAt, got.GeneratedJobs, got.LastRunAt)
	}
	got, _ = s.LoadSchedule(ctx, "capped")
	if got.Status != domain.ScheduleFinished || got.GeneratedJobs != 3 {
		t.Fatalf("capped after reaching MaxJobs: status %v generated %d", got.Status, got.GeneratedJobs)
	}
	if jobs, _ := s.ListJobs(ctx, storage.JobQuery{ScheduleID: "capped"}); len(jobs) != 0 {
		t.Fatalf("a direct claim stored %d jobs", len(jobs))
	}

	// The same occurrence cannot be claimed twice, and a zero
	// next finishes the schedule.
	if claimOne(t, s, "a", now, next) {
		t.Fatal("claimed the same occurrence twice")
	}
	if !claimOne(t, s, "a", next, time.Time{}) {
		t.Fatal("claim of the last occurrence failed")
	}
	got, _ = s.LoadSchedule(ctx, "a")
	if got.Status != domain.ScheduleFinished {
		t.Fatalf("status after the last occurrence = %v, want finished", got.Status)
	}
	due, _ := s.LoadDueSchedules(ctx, storage.DueQuery{Before: now.Add(time.Hour)})
	if slices.Contains(ids(due), "a") || slices.Contains(ids(due), "capped") {
		t.Fatalf("finished schedules still due: %v", ids(due))
	}

	if ok, err := s.ClaimOccurrences(ctx, nil); err != nil || len(ok) != 0 {
		t.Fatalf("empty claim = %v, %v", ok, err)
	}
}

func testClaimConcurrent(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	due := time.Now().Truncate(time.Millisecond)
	for i := range 20 {
		save(t, s, fmt.Sprintf("s%d", i), &due, domain.ScheduleActive)
	}

	var mu sync.Mutex
	won := make(map[string]int)
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claims := make([]storage.Claim, 20)
			for i := range claims {
				id := fmt.Sprintf("s%d", i)
				claims[i] = storage.Claim{ScheduleID: id, Expected: due, Next: due.Add(time.Minute), Generated: 1,
					Jobs: []*domain.Job{job(fmt.Sprintf("j%d-%d", w, i), id)}}
			}
			ok, err := s.ClaimOccurrences(ctx, claims)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for i, c := range claims {
				if ok[i] {
					won[c.ScheduleID]++
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	for i := range 20 {
		id := fmt.Sprintf("s%d", i)
		if won[id] != 1 {
			t.Errorf("%s claimed %d times, want once", id, won[id])
		}
		if jobs, _ := s.ListJobs(ctx, storage.JobQuery{ScheduleID: id}); len(jobs) != 1 {
			t.Errorf("%s has %d jobs, want 1", id, len(jobs))
		}
	}
}

func testAckJobs(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	for _, id := range []string{"j1", "j2", "j3"} {
		if err := s.SaveJob(ctx, job(id, "sch")); err != nil {
			t.Fatal(err)
		}
	}
	leased, err := s.ClaimJobs(ctx, "q", "w", 3, time.Minute)
	if err != nil || len(leased) != 3 {
		t.Fatalf("claimed %d, %v", len(leased), err)
	}

	if err := s.AckJobs(ctx, []string{"j1", "j3"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AckJobs(ctx, nil); err != nil {
		t.Fatal(err)
	}

	jobs, _ := s.ListJobs(ctx, storage.JobQuery{ScheduleID: "sch", Statuses: []domain.JobStatus{domain.StatusCompleted}})
	got := make([]string, len(jobs))
	for i, j := range jobs {
		got[i] = j.ID
	}
	sort.Strings(got)
	if fmt.Sprint(got) != "[j1 j3]" {
		t.Fatalf("completed = %v, want [j1 j3]", got)
	}

	// Acked jobs hold no lease: expiring leases recovers only j2.
	n, err := s.RecoverOrphaned(ctx, time.Now().Add(2*time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("recovered %d, %v; want only the unacked job", n, err)
	}
}

func testPurgeHistory(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	for _, id := range []string{"done1", "done2", "pending"} {
		if err := s.SaveJob(ctx, job(id, "sch")); err != nil {
			t.Fatal(err)
		}
	}
	leased, _ := s.ClaimJobs(ctx, "q", "w", 2, time.Minute)
	acked := []string{leased[0].Job.ID, leased[1].Job.ID}
	if err := s.AckJobs(ctx, acked); err != nil {
		t.Fatal(err)
	}

	save(t, s, "active", nil, domain.ScheduleActive)
	save(t, s, "paused", nil, domain.SchedulePaused)
	save(t, s, "finished", nil, domain.ScheduleFinished)
	save(t, s, "cancelled", nil, domain.ScheduleCancelled)
	save(t, s, "archived", nil, domain.ScheduleArchived)

	// Nothing is older than an hour ago.
	if n, err := s.PurgeHistory(ctx, time.Now().Add(-time.Hour), 100); err != nil || n != 0 {
		t.Fatalf("purged %d, %v; want nothing recent", n, err)
	}

	time.Sleep(5 * time.Millisecond)
	cutoff := time.Now()
	total := 0
	for {
		n, err := s.PurgeHistory(ctx, cutoff, 1)
		if err != nil {
			t.Fatal(err)
		}
		total += n
		if n < 1 {
			break
		}
	}
	if total != 5 {
		t.Fatalf("purged %d records, want 2 completed jobs and 3 history schedules", total)
	}

	left, _ := s.ListSchedules(ctx, storage.ScheduleQuery{})
	if got := ids(left); len(got) != 2 || !slices.Contains(got, "active") || !slices.Contains(got, "paused") {
		t.Fatalf("schedules left = %v, want active and paused", got)
	}
	jobs, _ := s.ListJobs(ctx, storage.JobQuery{ScheduleID: "sch"})
	if len(jobs) != 1 || jobs[0].Status == domain.StatusCompleted {
		t.Fatalf("jobs left = %d, want only the one not completed", len(jobs))
	}
	if got, _ := s.LoadSchedules(ctx, []string{"finished"}); len(got) != 0 {
		t.Fatal("a purged schedule can still be loaded")
	}
}

func testArchived(t *testing.T, s storage.Storage) {
	ctx := context.Background()
	save(t, s, "rev#1", nil, domain.ScheduleArchived)
	save(t, s, "live", nil, domain.ScheduleActive)

	got, err := s.ListSchedules(ctx, storage.ScheduleQuery{Statuses: []domain.ScheduleStatus{domain.ScheduleArchived}})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids(got)) != "[rev#1]" {
		t.Fatalf("archived = %v, want [rev#1]", ids(got))
	}
	due, _ := s.LoadDueSchedules(ctx, storage.DueQuery{Before: time.Now().Add(time.Hour)})
	if fmt.Sprint(ids(due)) != "[live]" {
		t.Fatalf("due = %v, want only live", ids(due))
	}
}

func testHeartbeat(t *testing.T, s storage.Storage) {
	ctx := context.Background()

	if _, err := s.Heartbeat(ctx, "b", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Heartbeat(ctx, "gone", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	live, err := s.Heartbeat(ctx, "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(live) != "[a b gone]" {
		t.Fatalf("live = %v, want [a b gone]", live)
	}

	time.Sleep(100 * time.Millisecond)
	live, _ = s.Heartbeat(ctx, "a", time.Minute)
	if fmt.Sprint(live) != "[a b]" {
		t.Fatalf("live = %v, want the expired instance gone", live)
	}

	live, _ = s.Heartbeat(ctx, "b", 0)
	if fmt.Sprint(live) != "[a]" {
		t.Fatalf("live = %v, want b to have left", live)
	}
}
