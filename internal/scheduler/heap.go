// Package scheduler transforms recurring schedules into
// executable jobs. It operates as an idle-first actor:
// it sleeps when no upcoming occurrences exist and is
// awakened through lightweight signals whenever a schedule
// is added, updated, or removed.
package scheduler

import (
	"container/heap"
	"time"

	"github.com/zsweiter/hydra/internal/domain"
)

// HeapEntry holds the minimum information needed by the
// Scheduler to order upcoming occurrences and generate
// jobs without consulting storage.
type HeapEntry struct {
	ScheduleID    string
	JobName       string
	Queue         domain.QueueName
	Data          []byte
	Metadata      map[string]string
	Priority      int
	MisfirePolicy domain.MisfirePolicy

	// NextRunNanos is the Unix nanosecond timestamp of
	// the next scheduled occurrence. Using nanoseconds
	// enables sub-millisecond schedule intervals.
	NextRunNanos int64

	// Index is the position in the heap, maintained by
	// heap.Interface.
	Index int
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

// Peek returns the next entry without removing it, or nil
// if the heap is empty.
func (h *ScheduleHeap) Peek() *HeapEntry {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}

// RemoveByScheduleID finds and removes the entry with the
// given ScheduleID. Returns the removed entry or nil.
func (h *ScheduleHeap) RemoveByScheduleID(scheduleID string) *HeapEntry {
	for i, entry := range *h {
		if entry.ScheduleID == scheduleID {
			return heap.Remove(h, i).(*HeapEntry)
		}
	}
	return nil
}

// HeapEntryFromSchedule creates a HeapEntry from a
// JobSchedule by computing the first NextRunAt.
func HeapEntryFromSchedule(sch *domain.JobSchedule, compiled domain.CompiledSchedule) *HeapEntry {
	now := time.Now().UTC()
	next, ok := compiled.Next(now)
	if !ok {
		return nil
	}

	return &HeapEntry{
		ScheduleID:    sch.ID,
		JobName:       sch.Name,
		Queue:         sch.Queue,
		Data:          sch.Data,
		Metadata:      sch.Metadata,
		Priority:      sch.Priority,
		MisfirePolicy: sch.MisfirePolicy,
		NextRunNanos:  next.UnixNano(),
	}
}

// BuildHeap constructs a ScheduleHeap from a list of
// active schedules using the provided compiled map.
func BuildHeap(schedules []*domain.JobSchedule, compiled map[string]domain.CompiledSchedule) *ScheduleHeap {
	h := &ScheduleHeap{}
	heap.Init(h)

	for _, sch := range schedules {
		if sch.Status != domain.ScheduleActive {
			continue
		}

		c, ok := compiled[sch.ID]
		if !ok {
			continue
		}

		entry := HeapEntryFromSchedule(sch, c)
		if entry == nil {
			continue
		}

		heap.Push(h, entry)
	}

	return h
}
