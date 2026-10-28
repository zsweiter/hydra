package bridge

import (
	"sync/atomic"
	"time"
)

// Metrics counts what the bridge did since it started. The
// broker reports its own side (consumer lag, redeliveries,
// stream rates); these cover what only hydra knows.
type Metrics struct {
	// MessagesIgnored counts messages without recurrence
	// attributes: regular events sharing the stream.
	MessagesIgnored atomic.Int64

	// DefinitionsRegistered counts definitions stored (created
	// or replaced), DefinitionsReplaced those that archived a
	// previous revision.
	DefinitionsRegistered atomic.Int64
	DefinitionsReplaced   atomic.Int64

	// DefinitionsDuplicate counts definitions identical to the
	// stored one (retries, redeliveries), which change nothing.
	DefinitionsDuplicate atomic.Int64

	// DefinitionsConflict counts creates rejected because their
	// recurrenceid holds a different live definition.
	DefinitionsConflict atomic.Int64

	DefinitionsPaused    atomic.Int64
	DefinitionsResumed   atomic.Int64
	DefinitionsCancelled atomic.Int64
	DefinitionsDeleted   atomic.Int64

	// DefinitionsUnknown counts state changes discarded because
	// their schedule does not exist.
	DefinitionsUnknown atomic.Int64

	// DefinitionsStale counts changes ignored because a newer
	// version of the same schedule was already stored
	// (out-of-order delivery or redelivery).
	DefinitionsStale atomic.Int64

	// DefinitionsInvalid counts definitions that break the
	// contract, discarded.
	DefinitionsInvalid atomic.Int64

	OccurrencesPublished atomic.Int64
	PublishErrors        atomic.Int64

	// StoreErrors counts batches of definitions that could not
	// be loaded or saved; their messages are redelivered.
	StoreErrors atomic.Int64

	// PublishDelay is how late occurrences were published
	// compared to when they were due: the scheduler's latency,
	// whose tail an average would hide.
	PublishDelay Histogram
}

func (m *Metrics) observePublishDelay(d time.Duration) {
	m.OccurrencesPublished.Add(1)
	m.PublishDelay.observe(max(d, 0))
}

// DelayBuckets are the upper bounds of the PublishDelay
// buckets: from what a healthy scheduler shows (milliseconds) to
// what a backlog or an outage does (a minute).
var DelayBuckets = [...]time.Duration{
	5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2500 * time.Millisecond, 5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute,
}

// Histogram counts durations in DelayBuckets, lock free.
type Histogram struct {
	buckets [len(DelayBuckets)]atomic.Int64 // not cumulative
	count   atomic.Int64
	sum     atomic.Int64 // nanoseconds
}

func (h *Histogram) observe(d time.Duration) {
	for i, le := range DelayBuckets {
		if d <= le {
			h.buckets[i].Add(1)
			break
		}
	}
	h.count.Add(1)
	h.sum.Add(int64(d))
}

// Snapshot returns the cumulative count of each bucket (as
// Prometheus expects), the total count and the sum.
func (h *Histogram) Snapshot() (cumulative [len(DelayBuckets)]int64, count int64, sum time.Duration) {
	var acc int64
	for i := range h.buckets {
		acc += h.buckets[i].Load()
		cumulative[i] = acc
	}
	// Read last, count can trail a bucket observed meanwhile.
	return cumulative, max(h.count.Load(), acc), time.Duration(h.sum.Load())
}
