// Package scheduler transforms recurring schedules into
// executable jobs. It operates as an idle-first actor:
// it sleeps when no upcoming occurrences exist and is
// awakened through lightweight signals whenever a schedule
// is added, updated, or removed.
package scheduler

import (
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// HeapEntry holds what the Scheduler needs to order upcoming
// occurrences. The payload (Data, Metadata) is not kept: it is
// loaded when the schedule fires, so memory grows with the
// number of schedules in the window, not with their size.
type HeapEntry struct {
	ScheduleID    string
	JobName       string
	Queue         domain.QueueName
	Priority      int
	MisfirePolicy domain.MisfirePolicy

	// MaxJobs and GeneratedJobs mirror the schedule. A claim
	// only succeeds on the state the entry was built from, so
	// the count stays exact without reading the schedule back.
	MaxJobs       int
	GeneratedJobs int

	// UpdatedAt is the revision of the schedule the entry and
	// its compiled rule were built from.
	UpdatedAt time.Time
	compiled  domain.CompiledSchedule

	// NextRunNanos is the Unix nanosecond timestamp of
	// the next scheduled occurrence. Using nanoseconds
	// enables sub-millisecond schedule intervals.
	NextRunNanos int64

	// ClaimNanos is the occurrence persisted as the schedule's
	// NextRunAt, the value a claim must find in storage. It
	// differs from NextRunNanos only while a failed claim
	// waits to be retried.
	ClaimNanos int64

	// pending is a claim whose storage write failed. It is
	// retried as is: its occurrences were already delivered
	// (directly) or have job IDs, so a retry neither delivers
	// them again nor duplicates their jobs.
	pending *storage.Claim

	// undelivered are the jobs of pending not delivered yet
	// (without the outbox), and failures how many deliveries
	// failed in a row.
	undelivered []*domain.Job
	failures    int

	// Index is the position in the heap, maintained by
	// heap.Interface.
	Index int

	// inBacklog tells which heap of the Scheduler holds it.
	inBacklog bool
}

// finished reports whether the schedule reached MaxJobs.
func (e *HeapEntry) finished() bool {
	return e.MaxJobs > 0 && e.GeneratedJobs >= e.MaxJobs
}

// remaining returns how many jobs the schedule may still
// generate, or -1 when unlimited.
func (e *HeapEntry) remaining() int {
	if e.MaxJobs == 0 {
		return -1
	}
	return max(e.MaxJobs-e.GeneratedJobs, 0)
}

// refresh copies the fields of a newer revision of the
// schedule into the entry.
func (e *HeapEntry) refresh(sch *domain.JobSchedule, compiled domain.CompiledSchedule) {
	e.JobName = sch.Name
	e.Queue = sch.Queue
	e.Priority = sch.Priority
	e.MisfirePolicy = sch.MisfirePolicy
	e.MaxJobs = sch.MaxJobs
	e.GeneratedJobs = sch.GeneratedJobs
	e.UpdatedAt = sch.UpdatedAt
	e.compiled = compiled
}

type ScheduleHeap []*HeapEntry

func (h ScheduleHeap) Len() int { return len(h) }

func (h ScheduleHeap) Less(i, j int) bool {
	if h[i].NextRunNanos == h[j].NextRunNanos {
		return h[i].Priority < h[j].Priority
	}
	return h[i].NextRunNanos < h[j].NextRunNanos
}

func (h ScheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (h *ScheduleHeap) Push(x any) {
	n := len(*h)
	entry := x.(*HeapEntry)
	entry.Index = n
	*h = append(*h, entry)
}

func (h *ScheduleHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.Index = -1
	*h = old[0 : n-1]
	return entry
}

// BacklogHeap orders catch-up schedules still behind: the
// most important first (lowest Priority, as for jobs), then
// the one whose oldest missed occurrence is earliest, so equal
// schedules are replayed in time order, slice by slice.
type BacklogHeap struct{ ScheduleHeap }

func (h BacklogHeap) Less(i, j int) bool {
	a, b := h.ScheduleHeap[i], h.ScheduleHeap[j]
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}

	return a.NextRunNanos < b.NextRunNanos
}

// Peek returns the next entry without removing it, or nil
// if the heap is empty.
func (h *ScheduleHeap) Peek() *HeapEntry {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}

// HeapEntryFromSchedule creates a HeapEntry from a
// JobSchedule. A persisted NextRunAt is kept as is, even in
// the past, so occurrences missed while the process was down
// reach the misfire policy instead of being silently dropped.
// Otherwise the first occurrence is computed from now.
func HeapEntryFromSchedule(sch *domain.JobSchedule, compiled domain.CompiledSchedule) *HeapEntry {
	var next time.Time
	if sch.NextRunAt != nil && !sch.NextRunAt.IsZero() {
		next = *sch.NextRunAt
	} else {
		var ok bool
		next, ok = FirstRun(sch.Rule, compiled, time.Now().UTC())
		if !ok {
			return nil
		}
	}

	e := &HeapEntry{
		ScheduleID:   sch.ID,
		NextRunNanos: next.UnixNano(),
		ClaimNanos:   next.UnixNano(),
	}

	e.refresh(sch, compiled)
	return e
}
