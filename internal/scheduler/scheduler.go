package scheduler

import (
	"container/heap"
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/pkg/logger"
	"github.com/zsweiter/hydra/storage"
)

// misfireGrace is how late an occurrence may fire and still
// count as on time rather than as a misfire.
const misfireGrace = time.Minute

// claimRetryDelay is how long the scheduler waits before
// retrying a claim or a window load that failed with a
// storage error.
const claimRetryDelay = time.Second

// syncOverlap widens each sync's ChangedSince, so a change
// saved by an instance whose clock runs behind is not missed.
const syncOverlap = 5 * time.Second

// fullReloadEvery is how many window extensions run between
// two full reloads. Extensions only add what entered the
// window; the full reload is a safety net that also drops
// what changed unnoticed.
const fullReloadEvery = 20

// Defaults of the scheduler settings.
const (
	DefaultWindow       = time.Minute
	DefaultWindowLimit  = 10_000
	DefaultCatchUpBatch = 100
	DefaultFireBatch    = 512
	DefaultConcurrency  = 4
)

// backlogEvery is how many due occurrences may fire in a row
// while a catch-up backlog waits. Due occurrences go first, so
// a backlog delays them by at most one slice every
// backlogEvery, and a busy schedule set cannot starve the
// backlog either.
const backlogEvery = 8

// SignalFunc is a lightweight notification callback used
// to wake up another actor (e.g. the Dispatcher).
type SignalFunc func()

// DirectFunc delivers the jobs of due occurrences directly,
// before they are claimed, and returns one error per job (nil
// for all delivered). Delivered occurrences are claimed
// without storing a job; failed ones are stored with the
// claim and go through the job queue (retries, dead letters).
//
// A delivered occurrence whose claim then fails may be
// delivered again, by this instance or another, so delivery
// must be idempotent per job (e.g. keyed by the schedule and
// MetadataScheduledFor).
type DirectFunc func(ctx context.Context, jobs []*domain.Job) []error

// EventType describes what kind of schedule change occurred.
type EventType int

const (
	EventScheduleAdded EventType = iota
	EventScheduleRemoved
	EventSchedulePaused
	EventScheduleResumed
)

// Event carries a schedule change notification to the Scheduler.
type Event struct {
	Type       EventType
	ScheduleID string
}

// Scheduler is an idle-first actor that transforms schedules
// into executable jobs.
//
// It keeps in memory only the schedules due within a time
// window (see SetWindow), ordered in a ScheduleHeap, without
// their payload. The window is extended from storage before it
// runs out, so memory stays bounded by the window, not by the
// schedule count or size.
//
// Due schedules fire in batches: their payloads are loaded in
// one round trip, their occurrences are optionally delivered
// directly (SetDirect), and the claims are written in a few
// concurrent round trips (SetFireBatch).
//
// Catch-up schedules that fell behind (e.g. after downtime)
// are replayed in batches from a separate BacklogHeap, time
// sliced with the on-time occurrences (see SetCatchUpBatch).
//
// Everything but Signal, Notify, Resync, SetShards and Stats
// runs on the goroutine of Run.
type Scheduler struct {
	storage storage.ScheduleStore
	signal  SignalFunc
	direct  func(domain.QueueName) DirectFunc

	// mu guards the heaps, entries and shards.
	mu      sync.Mutex
	heap    *ScheduleHeap
	backlog *BacklogHeap
	entries map[string]*HeapEntry // by schedule ID, in either heap
	// firing holds the IDs of the batch being fired, out of the
	// heaps until they are pushed back or forgotten.
	firing   map[string]struct{}
	shards   []int // owned shards, nil for all
	shardSet *[domain.ShardCount]bool

	// horizon is how far the loaded window reaches: every
	// owned active schedule due by then is in a heap. An
	// occurrence past it is dropped and loaded again later.
	horizon  time.Time
	refillAt time.Time
	// truncated is set when the last load hit windowLimit; the
	// window then reloads in full as soon as the heap drains.
	truncated  bool
	extensions int
	dueStreak  int
	// lastSync is when the window last caught up with storage;
	// a sync loads what changed since.
	lastSync time.Time

	sigChan    chan struct{}
	resyncChan chan struct{}
	reloadChan chan struct{}

	// events is an unbounded queue drained by Run: a burst of
	// schedule changes must never be dropped.
	eventsMu sync.Mutex
	events   []Event

	// maxAttempts is applied to generated jobs, which would
	// otherwise be dead-lettered on their first failure.
	maxAttempts  int
	catchUpBatch int
	window       time.Duration
	windowLimit  int
	fireBatch    int
	concurrency  int

	// outbox stores the occurrences a DirectFunc failed to
	// deliver as jobs. Without it they are delivered again,
	// with backoff, and claimed only once all are delivered.
	outbox bool

	stats counters

	// alloc batches the allocations of the batch being fired.
	alloc *batchAlloc

	log logger.Logger
}

// counters are cumulative figures of the scheduler.
type counters struct {
	claimed   atomic.Int64
	lost      atomic.Int64
	delivered atomic.Int64
	outboxed  atomic.Int64
	retried   atomic.Int64
	stalled   atomic.Int64 // a gauge: schedules failing for stallAfter attempts

	storeErrors atomic.Int64
}

// New creates a new Scheduler.
//
// The signal callback is invoked whenever the Scheduler
// stores new jobs, so the Dispatcher can be woken up.
func New(s storage.ScheduleStore, signal SignalFunc) *Scheduler {
	return &Scheduler{
		storage: s,
		signal:  signal,
		direct:  func(domain.QueueName) DirectFunc { return nil },
		heap:    &ScheduleHeap{},
		backlog: &BacklogHeap{},
		entries: make(map[string]*HeapEntry),
		firing:  make(map[string]struct{}),

		sigChan:    make(chan struct{}, 1),
		resyncChan: make(chan struct{}, 1),
		reloadChan: make(chan struct{}, 1),

		catchUpBatch: DefaultCatchUpBatch,
		window:       DefaultWindow,
		windowLimit:  DefaultWindowLimit,
		fireBatch:    DefaultFireBatch,
		concurrency:  DefaultConcurrency,
		outbox:       true,
		log:          logger.Nop(),
	}
}

// SetMaxAttempts sets the retry budget of generated jobs.
// Must be called before Run.
func (s *Scheduler) SetMaxAttempts(n int) {
	s.maxAttempts = n
}

// SetLogger sets where the scheduler logs: storage errors,
// stalled and recovered deliveries, and rules that no longer
// compile. Must be called before Run.
func (s *Scheduler) SetLogger(log logger.Logger) {
	if log != nil {
		s.log = log.With(logger.Fields{"component": "scheduler"})
	}
}

// storeError counts and logs a failed storage operation.
func (s *Scheduler) storeError(op string, err error, fields ...logger.Fields) {
	s.stats.storeErrors.Add(1)
	s.log.Error(err, append([]logger.Fields{{"op": op}}, fields...)...)
}

// SetDirect sets how the jobs of each queue are delivered
// directly (nil for none, the default: jobs are stored). Must
// be called before Run.
func (s *Scheduler) SetDirect(lookup func(domain.QueueName) DirectFunc) {
	if lookup != nil {
		s.direct = lookup
	}
}

// SetOutbox sets what happens to occurrences a DirectFunc
// fails to deliver. With the outbox (the default) they are
// claimed with their jobs stored, and the job queue retries
// them. Without it nothing but the schedules is stored: the
// occurrence is not claimed, its schedule does not advance,
// and the same jobs are delivered again with exponential
// backoff until they all succeed. Must be called before Run.
func (s *Scheduler) SetOutbox(enabled bool) {
	s.outbox = enabled
}

// SetCatchUpBatch caps the occurrences a catch-up schedule
// generates per claim. A schedule further behind keeps its
// place in the backlog and continues on its next slice, in
// order, so a long downtime never turns into one huge write.
// Must be called before Run.
func (s *Scheduler) SetCatchUpBatch(n int) {
	if n > 0 {
		s.catchUpBatch = n
	}
}

// SetWindow sets how far ahead schedules are loaded into
// memory, and how many at most per load. Must be called
// before Load.
func (s *Scheduler) SetWindow(window time.Duration, limit int) {
	if window > 0 {
		s.window = window
	}
	if limit > 0 {
		s.windowLimit = limit
	}
}

// SetFireBatch sets how many due schedules fire together, and
// over how many concurrent storage round trips their claims
// are split. Must be called before Run.
func (s *Scheduler) SetFireBatch(size, concurrency int) {
	if size > 0 {
		s.fireBatch = size
	}
	if concurrency > 0 {
		s.concurrency = concurrency
	}
}

// SetShards restricts the scheduler to the schedules of the
// given shards (nil for all) and reloads its window if they
// changed, which it reports. Instances sharing a storage split
// the shards between them.
func (s *Scheduler) SetShards(shards []int) bool {
	var set *[domain.ShardCount]bool
	if shards != nil {
		set = new([domain.ShardCount]bool)
		for _, sh := range shards {
			set[sh] = true
		}
	}

	s.mu.Lock()
	changed := s.shardSet != set && (s.shardSet == nil || set == nil || *s.shardSet != *set)
	s.shards = shards
	s.shardSet = set
	s.mu.Unlock()

	if changed {
		select {
		case s.reloadChan <- struct{}{}:
		default:
		}
	}
	return changed
}

// Stats is a point-in-time view of the scheduler.
type Stats struct {
	Loaded  int // schedules held in memory, backlog and those firing included
	Backlog int // catch-up schedules still behind
	Shards  int // shards owned

	// Cumulative counts: occurrences claimed, claims lost to
	// another instance, occurrences delivered directly, and
	// occurrences stored as jobs after a direct delivery failed.
	Claimed   int64
	Lost      int64
	Delivered int64
	Outboxed  int64

	// Without the outbox: deliveries retried, and schedules
	// whose delivery has failed stallAfter times in a row.
	Retried int64
	Stalled int64

	// StoreErrors counts failed storage operations (loads,
	// syncs, claims); each is retried.
	StoreErrors int64
}

// Stats returns the current scheduler figures. Safe to call
// from any goroutine.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	shards := len(s.shards)
	if s.shards == nil {
		shards = domain.ShardCount
	}
	return Stats{
		Loaded:    len(s.entries) + len(s.firing),
		Backlog:   s.backlog.Len(),
		Shards:    shards,
		Claimed:   s.stats.claimed.Load(),
		Lost:      s.stats.lost.Load(),
		Delivered: s.stats.delivered.Load(),
		Outboxed:  s.stats.outboxed.Load(),
		Retried:   s.stats.retried.Load(),
		Stalled:   s.stats.stalled.Load(),

		StoreErrors: s.stats.storeErrors.Load(),
	}
}

// Signal wakes the Scheduler to re-evaluate its heap.
// This is called by the Queue when schedules change.
func (s *Scheduler) Signal() {
	select {
	case s.sigChan <- struct{}{}:
	default:
		// drop duplicate signal
	}
}

// Notify queues a schedule event for the Scheduler and wakes
// it up. It never blocks and never drops events.
func (s *Scheduler) Notify(event Event) {
	s.eventsMu.Lock()
	s.events = append(s.events, event)
	s.eventsMu.Unlock()
	s.Signal()
}

// Resync asks the scheduler to load the schedules of its
// window changed since the last sync. Instances sharing a
// storage call it periodically to pick up schedules added or
// changed by others; removals are noticed when they would
// fire.
func (s *Scheduler) Resync() {
	select {
	case s.resyncChan <- struct{}{}:
	default:
	}
}

// Load initialises the Scheduler from storage.
// Must be called before Run.
func (s *Scheduler) Load(ctx context.Context) error {
	return s.reload(ctx)
}

// Run starts the scheduler loop. Blocks until the context
// is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.drainEvents(ctx)

		select {
		case <-s.reloadChan:
			s.reload(ctx)
			continue
		case <-s.resyncChan:
			s.sync(ctx)
			continue
		default:
		}

		now := time.Now()
		if s.truncated && s.idle() {
			s.reload(ctx)
			continue
		}
		if !now.Before(s.refillAt) {
			s.extend(ctx)
			continue
		}

		if batch := s.nextBatch(now); len(batch) > 0 {
			s.fire(ctx, batch)
			s.mu.Lock()
			clear(s.firing)
			s.mu.Unlock()
			continue
		}

		wait := s.refillAt.Sub(now)
		s.mu.Lock()
		if top := s.heap.Peek(); top != nil {
			wait = min(wait, time.Duration(top.NextRunNanos-now.UnixNano()))
		}
		s.mu.Unlock()

		timer := time.NewTimer(max(wait, 0))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.sigChan:
			timer.Stop()
		case <-s.reloadChan:
			timer.Stop()
			s.reload(ctx)
		case <-s.resyncChan:
			timer.Stop()
			s.sync(ctx)
		case <-timer.C:
		}
	}
}

func (s *Scheduler) drainEvents(ctx context.Context) {
	s.eventsMu.Lock()
	events := s.events
	s.events = nil
	s.eventsMu.Unlock()

	for _, event := range events {
		s.handleEvent(ctx, event)
	}
}

// idle reports whether nothing is left to fire in the window.
func (s *Scheduler) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heap.Len() == 0 && s.backlog.Len() == 0
}

// nextBatch pops the entries to fire now, up to the fire
// batch: due occurrences, and catch-up backlog slices when
// nothing is due or after backlogEvery due occurrences in a
// row. A catch-up entry weighs as many occurrences as it may
// replay, so a batch never grows into a huge write, and a
// batch only takes backlog slices of one priority, so a less
// important backlog waits for the more important ones.
func (s *Scheduler) nextBatch(now time.Time) []*HeapEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	var batch []*HeapEntry
	weight := 0
	late := now.Add(-misfireGrace).UnixNano()
	backlogPriority, tookBacklog := 0, false
	for weight < s.fireBatch {
		fromBacklog := s.backlogTurnLocked(now)
		if fromBacklog && tookBacklog && s.backlog.Peek().Priority != backlogPriority {
			if !s.dueLocked(now) {
				break
			}
			fromBacklog = false
		}

		var entry *HeapEntry
		if fromBacklog {
			s.dueStreak = 0
			entry = heap.Pop(s.backlog).(*HeapEntry)
			backlogPriority, tookBacklog = entry.Priority, true
		} else if s.dueLocked(now) {
			s.dueStreak++
			entry = heap.Pop(s.heap).(*HeapEntry)
		} else {
			break
		}
		delete(s.entries, entry.ScheduleID)
		s.firing[entry.ScheduleID] = struct{}{}

		batch = append(batch, entry)
		if entry.MisfirePolicy == domain.MisfireCatchUp && entry.ClaimNanos < late {
			weight += s.catchUpBatch
		} else {
			weight++
		}
	}
	return batch
}

func (s *Scheduler) dueLocked(now time.Time) bool {
	return s.heap.Len() > 0 && s.heap.Peek().NextRunNanos <= now.UnixNano()
}

// backlogTurnLocked reports whether the next entry comes from
// the backlog: when nothing is due, or after backlogEvery due
// occurrences in a row.
func (s *Scheduler) backlogTurnLocked(now time.Time) bool {
	return s.backlog.Len() > 0 && (!s.dueLocked(now) || s.dueStreak >= backlogEvery)
}

// firing is an entry being fired and the claim it makes.
type firing struct {
	entry  *HeapEntry
	claim  storage.Claim
	behind bool
	// direct marks the jobs handed to a DirectFunc, and failed
	// those it could not deliver.
	direct []*domain.Job
	failed []bool
	// hold keeps the claim back: without the outbox, some of
	// its occurrences were not delivered.
	hold bool
}

// batchAlloc allocates the firings and jobs of one batch
// together, instead of one by one: most occurrences make one
// firing and one job. Nothing is reused, so jobs kept for a
// retry, or stored, stay valid.
// Each slice is made on first use, sized for the batch.
type batchAlloc struct {
	n       int
	firings []firing
	jobs    []domain.Job
	ptrs    []*domain.Job
}

func newBatchAlloc(n int) *batchAlloc {
	return &batchAlloc{n: n}
}

// firing and job copy their argument: returning its address
// would make it escape, a heap allocation on every call.
func (a *batchAlloc) firing(f firing) *firing {
	if a != nil && a.firings == nil {
		a.firings = make([]firing, 0, a.n)
	}
	if a == nil || len(a.firings) == cap(a.firings) {
		p := new(firing)
		*p = f
		return p
	}
	a.firings = append(a.firings, f)
	return &a.firings[len(a.firings)-1]
}

func (a *batchAlloc) job(j domain.Job) *domain.Job {
	if a != nil && a.jobs == nil {
		a.jobs = make([]domain.Job, 0, a.n)
	}
	if a == nil || len(a.jobs) == cap(a.jobs) {
		p := new(domain.Job)
		*p = j
		return p
	}
	a.jobs = append(a.jobs, j)
	return &a.jobs[len(a.jobs)-1]
}

// one returns a slice holding job alone; appending to it
// copies, so it never overwrites the next one's.
func (a *batchAlloc) one(job *domain.Job) []*domain.Job {
	if a != nil && a.ptrs == nil {
		a.ptrs = make([]*domain.Job, 0, a.n)
	}
	if a == nil || len(a.ptrs) == cap(a.ptrs) {
		return []*domain.Job{job}
	}
	a.ptrs = append(a.ptrs, job)
	n := len(a.ptrs)
	return a.ptrs[n-1 : n : n]
}

// fire claims the occurrences of a batch of due entries.
func (s *Scheduler) fire(ctx context.Context, batch []*HeapEntry) {
	now := time.Now().UTC()
	s.alloc = newBatchAlloc(len(batch))
	defer func() { s.alloc = nil }()

	var firings []*firing
	var load []*HeapEntry
	for _, entry := range batch {
		// A claim whose write failed, or whose delivery is
		// unfinished, is retried as it was. One waiting for a
		// delivery is checked against storage first, so a
		// schedule removed or claimed meanwhile stops retrying.
		if entry.pending == nil || entry.undelivered != nil {
			load = append(load, entry)
		}
	}

	var loaded map[string]*domain.JobSchedule
	if len(load) > 0 {
		var err error
		if loaded, err = s.loadByID(ctx, load); err != nil {
			s.storeError("load due schedules", err, logger.Fields{"schedules": len(load)})
			for _, entry := range batch {
				s.retry(entry, now)
			}
			return
		}
	}

	for _, entry := range batch {
		sch := loaded[entry.ScheduleID]
		switch {
		case entry.pending == nil:
			if f := s.prepare(entry, sch, now); f != nil {
				firings = append(firings, f)
			}
		case entry.undelivered != nil && !stillDue(sch, entry):
			s.recovered(entry)
			entry.pending = nil
			if sch == nil {
				s.forget(entry.ScheduleID)
			} else {
				s.adopt(sch)
			}
		default:
			firings = append(firings, s.alloc.firing(firing{entry: entry, claim: *entry.pending, behind: entry.inBacklog}))
		}
	}
	if len(firings) == 0 {
		return
	}

	s.deliver(ctx, firings)

	// Claims held back wait for their undelivered occurrences,
	// with backoff; the rest are written.
	held := firings[:0:0]
	ready := firings[:0:0]
	for _, f := range firings {
		if f.hold {
			held = append(held, f)
		} else {
			ready = append(ready, f)
		}
	}
	for _, f := range held {
		claim := f.claim
		f.entry.pending = &claim
		s.backoff(f.entry, now)
	}
	firings = ready
	if len(firings) == 0 {
		return
	}

	results, errs := s.claim(ctx, firings)

	var lost []string
	stored := false
	var claimErr error
	failed := 0
	for i, f := range firings {
		entry := f.entry
		switch {
		case errs[i] != nil:
			// Logged once per batch: a store outage fails every
			// claim of it with the same error.
			claimErr = errs[i]
			failed++
			// Retry later, still claiming the same occurrence.
			claim := f.claim
			entry.pending = &claim
			s.retry(entry, now)

		case !results[i]:
			// Another instance got there first, or the schedule
			// changed: follow the state persisted.
			entry.pending = nil
			s.recovered(entry)
			s.stats.lost.Add(1)
			lost = append(lost, entry.ScheduleID)

		default:
			entry.pending = nil
			s.recovered(entry)
			s.stats.claimed.Add(int64(f.claim.Generated))
			stored = stored || len(f.claim.Jobs) > 0
			s.advance(entry, f.claim.Next, f.claim.Generated, f.behind)
		}
	}
	if claimErr != nil {
		s.storeError("claim occurrences", claimErr, logger.Fields{"schedules": failed})
	}

	if stored && s.signal != nil {
		s.signal()
	}
	if len(lost) > 0 {
		s.follow(ctx, lost)
	}
}

// stillDue reports whether sch is active and still expects the
// occurrence entry is about to claim.
func stillDue(sch *domain.JobSchedule, entry *HeapEntry) bool {
	return sch != nil && sch.Status == domain.ScheduleActive &&
		(sch.NextRunAt == nil || sch.NextRunAt.UnixMilli() == time.Unix(0, entry.ClaimNanos).UnixMilli())
}

// loadByID loads the schedules of entries, payload included.
func (s *Scheduler) loadByID(ctx context.Context, entries []*HeapEntry) (map[string]*domain.JobSchedule, error) {
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ScheduleID
	}
	schedules, err := s.storage.LoadSchedules(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*domain.JobSchedule, len(schedules))
	for _, sch := range schedules {
		byID[sch.ID] = sch
	}
	return byID, nil
}

// prepare checks a due entry against its stored schedule and
// builds its occurrences. It returns nil when the entry must
// not fire: the schedule was removed, paused or finished, or
// its occurrence was claimed or moved meanwhile.
func (s *Scheduler) prepare(entry *HeapEntry, sch *domain.JobSchedule, now time.Time) *firing {
	if sch == nil || sch.Status != domain.ScheduleActive || sch.ShouldFinish() {
		s.forget(entry.ScheduleID)
		return nil
	}

	// Stores persist milliseconds: compare at that precision.
	if sch.NextRunAt != nil && sch.NextRunAt.UnixMilli() != time.Unix(0, entry.ClaimNanos).UnixMilli() {
		s.adopt(sch)
		return nil
	}

	if !sch.UpdatedAt.Equal(entry.UpdatedAt) || entry.compiled == nil {
		c, ok := s.compile(sch)
		if !ok {
			s.forget(entry.ScheduleID)
			return nil
		}
		entry.refresh(sch, c)
	}
	entry.GeneratedJobs = sch.GeneratedJobs

	claimTime := time.Unix(0, entry.ClaimNanos)
	jobs, next, behind := s.occurrences(entry, sch, claimTime, now)

	return s.alloc.firing(firing{
		entry:  entry,
		behind: behind,
		claim: storage.Claim{
			ScheduleID: entry.ScheduleID,
			Expected:   claimTime,
			Next:       next,
			Generated:  len(jobs),
			Jobs:       jobs,
		},
	})
}

// deliver hands the jobs of queues delivered directly to their
// DirectFunc, one call per queue. With the outbox, each claim
// keeps only the jobs that could not be delivered; without it,
// a claim with undelivered jobs is held back (see SetOutbox).
func (s *Scheduler) deliver(ctx context.Context, firings []*firing) {
	type pos struct{ f, j int }
	byQueue := make(map[domain.QueueName][]pos)
	for fi, f := range firings {
		if s.direct(f.entry.Queue) == nil {
			continue
		}
		switch {
		case f.entry.undelivered != nil:
			// A retry delivers what is left, as it was built.
			f.direct = f.entry.undelivered
		case f.entry.pending != nil:
			continue
		default:
			f.direct = f.claim.Jobs
		}
		for ji := range f.direct {
			byQueue[f.entry.Queue] = append(byQueue[f.entry.Queue], pos{fi, ji})
		}
	}

	for queue, positions := range byQueue {
		jobs := make([]*domain.Job, len(positions))
		for i, p := range positions {
			jobs[i] = firings[p.f].direct[p.j]
		}

		errs := s.direct(queue)(ctx, jobs)

		for i, p := range positions {
			f := firings[p.f]
			if f.failed == nil {
				f.failed = make([]bool, len(f.direct))
			}
			f.failed[p.j] = i < len(errs) && errs[i] != nil
		}
	}

	for _, f := range firings {
		if f.failed == nil {
			continue
		}
		var failed []*domain.Job
		for j, job := range f.direct {
			if f.failed[j] {
				failed = append(failed, job)
			}
		}
		s.stats.delivered.Add(int64(len(f.direct) - len(failed)))

		if s.outbox {
			s.stats.outboxed.Add(int64(len(failed)))
			f.claim.Jobs = failed
			continue
		}
		f.claim.Jobs = nil
		if len(failed) > 0 {
			f.hold = true
			f.entry.undelivered = failed
		} else {
			f.entry.undelivered = nil
		}
	}
}

// claim writes the claims of firings, split over up to
// concurrency round trips.
func (s *Scheduler) claim(ctx context.Context, firings []*firing) ([]bool, []error) {
	n := len(firings)
	results := make([]bool, n)
	errs := make([]error, n)

	chunks := min(s.concurrency, n)
	size := (n + chunks - 1) / chunks

	var wg sync.WaitGroup
	for lo := 0; lo < n; lo += size {
		hi := min(lo+size, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			claims := make([]storage.Claim, hi-lo)
			for i := range claims {
				claims[i] = firings[lo+i].claim
			}
			ok, err := s.storage.ClaimOccurrences(ctx, claims)
			for i := range claims {
				if err != nil {
					errs[lo+i] = err
				} else {
					results[lo+i] = ok[i]
				}
			}
		}()
	}
	wg.Wait()

	return results, errs
}

// stallAfter is how many failed deliveries in a row make a
// schedule count as stalled.
const stallAfter = 10

// maxRetryDelay caps the backoff of failed deliveries.
const maxRetryDelay = time.Minute

// backoff puts an entry whose delivery failed back to fire
// again after an exponential delay, still delivering the same
// occurrences.
func (s *Scheduler) backoff(entry *HeapEntry, now time.Time) {
	entry.failures++
	s.stats.retried.Add(1)
	if entry.failures == stallAfter {
		s.stats.stalled.Add(1)
		s.log.Warn("delivery stalled, retrying every minute", logger.Fields{"schedule": entry.ScheduleID, "failures": entry.failures})
	} else if entry.failures < stallAfter {
		s.log.Debug("delivery failed, retrying", logger.Fields{"schedule": entry.ScheduleID, "failures": entry.failures})
	}
	delay := maxRetryDelay
	if entry.failures < 7 {
		delay = min(claimRetryDelay<<(entry.failures-1), maxRetryDelay)
	}
	entry.NextRunNanos = now.Add(delay).UnixNano()
	s.push(entry, entry.inBacklog)
}

// recovered resets the failure count of an entry delivered.
func (s *Scheduler) recovered(entry *HeapEntry) {
	if entry.failures >= stallAfter {
		s.stats.stalled.Add(-1)
		s.log.Info("delivery recovered", logger.Fields{"schedule": entry.ScheduleID, "failures": entry.failures})
	}
	entry.failures = 0
	entry.undelivered = nil
}

// retry puts an entry back to fire again after a storage
// error, still claiming the same occurrence.
func (s *Scheduler) retry(entry *HeapEntry, now time.Time) {
	entry.NextRunNanos = now.Add(claimRetryDelay).UnixNano()
	s.push(entry, entry.inBacklog)
}

// advance moves a claimed entry to its next occurrence.
func (s *Scheduler) advance(entry *HeapEntry, next time.Time, generated int, behind bool) {
	entry.GeneratedJobs += generated
	if next.IsZero() || entry.finished() {
		s.forget(entry.ScheduleID)
		return
	}

	entry.NextRunNanos = next.UnixNano()
	entry.ClaimNanos = entry.NextRunNanos

	switch {
	case behind:
		s.push(entry, true)
	case next.After(s.horizon) || !s.owns(entry.ScheduleID):
		// Loaded again when the window reaches it, by the
		// instance owning its shard.
		s.forget(entry.ScheduleID)
	default:
		s.push(entry, false)
	}
}

// occurrences builds the jobs of the occurrence due at
// claimTime under the entry's misfire policy, and returns the
// occurrence to claim next (zero when the schedule ends).
//
// A catch-up generates at most catchUpBatch jobs, in order.
// When occurrences up to now remain, behind is true and next
// is the first of them, persisted so the replay resumes there,
// on this instance or another, even after a restart.
func (s *Scheduler) occurrences(entry *HeapEntry, sch *domain.JobSchedule, claimTime, now time.Time) (jobs []*domain.Job, next time.Time, behind bool) {
	c := entry.compiled

	switch entry.MisfirePolicy {
	case domain.MisfireCatchUp:
		limit := s.catchUpBatch
		if r := entry.remaining(); r >= 0 {
			limit = min(limit, r)
		}
		t := claimTime
		for !t.After(now) {
			if len(jobs) == limit {
				return jobs, t, true
			}
			jobs = append(jobs, s.buildJob(entry, sch, t))
			n, ok := c.Next(t)
			if !ok {
				return jobs, time.Time{}, false
			}
			t = n
		}
		return jobs, t, false

	case domain.MisfireRunOnce:
		if claimTime.Before(now.Add(-misfireGrace)) {
			jobs = s.alloc.one(s.buildJob(entry, sch, now))
		} else {
			jobs = s.alloc.one(s.buildJob(entry, sch, claimTime))
		}

	default:
		// MisfireSkip: a late timer within the grace window
		// still fires; an occurrence missed during downtime
		// does not.
		if !claimTime.Before(now.Add(-misfireGrace)) {
			jobs = s.alloc.one(s.buildJob(entry, sch, claimTime))
		}
	}

	// Calculate next occurrence from now, not from the
	// job's RunAt time. This prevents busy-looping when
	// the timer fired late (e.g. sub-ms intervals on a
	// ms-precision timer).
	next, ok := c.Next(now)
	if !ok {
		return jobs, time.Time{}, false
	}
	return jobs, next, false
}

// push inserts entry into the heap or the backlog, replacing
// any entry of the same schedule so reloads and lost claims
// never duplicate it.
func (s *Scheduler) push(entry *HeapEntry, backlog bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushLocked(entry, backlog)
}

func (s *Scheduler) pushLocked(entry *HeapEntry, backlog bool) {
	s.removeLocked(entry.ScheduleID)

	entry.inBacklog = backlog
	if backlog {
		heap.Push(s.backlog, entry)
	} else {
		heap.Push(s.heap, entry)
	}
	s.entries[entry.ScheduleID] = entry
}

func (s *Scheduler) removeLocked(scheduleID string) {
	delete(s.firing, scheduleID)
	entry, ok := s.entries[scheduleID]
	if !ok {
		return
	}
	delete(s.entries, scheduleID)
	if entry.inBacklog {
		heap.Remove(s.backlog, entry.Index)
	} else {
		heap.Remove(s.heap, entry.Index)
	}
}

// forget drops a schedule from memory until it is loaded again.
func (s *Scheduler) forget(scheduleID string) {
	s.mu.Lock()
	s.removeLocked(scheduleID)
	s.mu.Unlock()
}

func (s *Scheduler) owns(scheduleID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ownsLocked(scheduleID)
}

func (s *Scheduler) ownsLocked(scheduleID string) bool {
	return s.shardSet == nil || s.shardSet[domain.ScheduleShard(scheduleID)]
}

func (s *Scheduler) compile(sch *domain.JobSchedule) (domain.CompiledSchedule, bool) {
	c, err := Compile(sch.Rule)
	if err != nil {
		// A stored rule that no longer parses (e.g. a zone
		// removed from tzdata): the schedule is left alone.
		s.log.Warn("skipping schedule with invalid rule", logger.Fields{"schedule": sch.ID, "error": err.Error()})
		return nil, false
	}
	return c, true
}

// entryFor builds the entry of a loaded schedule, reusing the
// compiled rule of old while the schedule is unchanged.
func (s *Scheduler) entryFor(sch *domain.JobSchedule, old *HeapEntry) *HeapEntry {
	if sch.ShouldFinish() {
		return nil
	}
	var c domain.CompiledSchedule
	if old != nil && old.compiled != nil && old.UpdatedAt.Equal(sch.UpdatedAt) {
		c = old.compiled
	} else {
		var ok bool
		if c, ok = s.compile(sch); !ok {
			return nil
		}
	}
	return HeapEntryFromSchedule(sch, c)
}

func (s *Scheduler) shardFilter() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shards
}

// reload replaces the heaps with the owned active schedules
// due within the window, and plans the next load.
func (s *Scheduler) reload(ctx context.Context) error {
	now := time.Now().UTC()
	until := now.Add(s.window)
	schedules, err := s.storage.LoadDueSchedules(ctx, storage.DueQuery{
		Before: until,
		Shards: s.shardFilter(),
		Limit:  s.windowLimit,
	})
	if err != nil {
		s.storeError("load window", err)
		s.refillAt = now.Add(claimRetryDelay)
		return err
	}

	// More schedules are due than fit: the window ends at the
	// last one loaded, and the rest come with the next load.
	s.truncated = len(schedules) >= s.windowLimit
	if s.truncated {
		until = now
		if last := schedules[len(schedules)-1].NextRunAt; last != nil {
			until = *last
		}
	}

	s.mu.Lock()
	old := s.entries
	s.mu.Unlock()

	h := &ScheduleHeap{}
	entries := make(map[string]*HeapEntry, len(schedules))
	for _, sch := range schedules {
		if entry := s.entryFor(sch, old[sch.ID]); entry != nil {
			heap.Push(h, entry)
			entries[sch.ID] = entry
		}
	}

	s.mu.Lock()
	s.heap = h
	s.backlog = &BacklogHeap{}
	s.entries = entries
	s.mu.Unlock()

	s.horizon = until
	s.lastSync = now
	s.extensions = 0
	s.planRefill(now, until)
	return nil
}

// extend loads the schedules that entered the window since
// the last load, without reloading those already in it. A
// truncated window, or every fullReloadEvery extensions, is
// reloaded in full instead.
func (s *Scheduler) extend(ctx context.Context) error {
	if s.truncated || s.extensions >= fullReloadEvery {
		return s.reload(ctx)
	}

	now := time.Now().UTC()
	until := now.Add(s.window)
	schedules, err := s.storage.LoadDueSchedules(ctx, storage.DueQuery{
		After:  s.horizon,
		Before: until,
		Shards: s.shardFilter(),
		Limit:  s.windowLimit,
	})
	if err != nil {
		s.storeError("load window", err)
		s.refillAt = now.Add(claimRetryDelay)
		return err
	}
	if len(schedules) >= s.windowLimit {
		return s.reload(ctx)
	}

	s.mu.Lock()
	for _, sch := range schedules {
		if _, ok := s.entries[sch.ID]; ok {
			continue
		}
		if entry := s.entryFor(sch, nil); entry != nil {
			s.pushLocked(entry, false)
		}
	}
	s.mu.Unlock()

	s.horizon = until
	s.extensions++
	s.planRefill(now, until)
	return nil
}

func (s *Scheduler) planRefill(now, until time.Time) {
	s.refillAt = now.Add(s.window / 2)
	if s.truncated {
		// Reload by the end of the truncated window, or sooner
		// once the heap drains (see Run), but never in a loop.
		if limit := now.Add(claimRetryDelay); until.Before(limit) {
			until = limit
		}
		if until.Before(s.refillAt) {
			s.refillAt = until
		}
	}
}

// sync loads the schedules of the window saved since the last
// sync (by other instances) and replaces their entries.
func (s *Scheduler) sync(ctx context.Context) error {
	now := time.Now().UTC()
	schedules, err := s.storage.LoadDueSchedules(ctx, storage.DueQuery{
		Before:       s.horizon,
		Shards:       s.shardFilter(),
		Limit:        s.windowLimit,
		ChangedSince: s.lastSync.Add(-syncOverlap),
	})
	if err != nil {
		s.storeError("sync window", err)
		return err
	}
	if len(schedules) >= s.windowLimit {
		return s.reload(ctx)
	}

	s.mu.Lock()
	for _, sch := range schedules {
		old := s.entries[sch.ID]
		if old != nil && old.UpdatedAt.Equal(sch.UpdatedAt) {
			continue
		}
		if entry := s.entryFor(sch, old); entry != nil {
			s.pushLocked(entry, false)
		}
	}
	s.mu.Unlock()

	s.lastSync = now
	return nil
}

// follow reloads schedules whose claim was lost and puts them
// back at their persisted state.
func (s *Scheduler) follow(ctx context.Context, ids []string) {
	schedules, err := s.storage.LoadSchedules(ctx, ids)
	if err != nil {
		s.storeError("load lost claims", err, logger.Fields{"schedules": len(ids)})
		return
	}
	found := make(map[string]bool, len(schedules))
	for _, sch := range schedules {
		found[sch.ID] = true
		s.adopt(sch)
	}
	for _, id := range ids {
		if !found[id] {
			s.forget(id)
		}
	}
}

// loadEntry rebuilds the entry of a schedule from storage.
func (s *Scheduler) loadEntry(ctx context.Context, scheduleID string) {
	sch, err := s.storage.LoadSchedule(ctx, scheduleID)
	if err != nil {
		s.forget(scheduleID)
		return
	}
	s.adopt(sch)
}

// adopt puts a schedule in the window at its persisted state,
// or drops it if it is no longer active. A schedule outside
// the window is left to a later load. It is inserted even from
// a shard owned elsewhere, so a new schedule fires without
// waiting for its owner to load it; the claim keeps the
// occurrence unique and the owner takes over after it.
func (s *Scheduler) adopt(sch *domain.JobSchedule) {
	if sch.Status != domain.ScheduleActive {
		s.forget(sch.ID)
		return
	}

	s.mu.Lock()
	old := s.entries[sch.ID]
	s.mu.Unlock()

	entry := s.entryFor(sch, old)
	if entry == nil || entry.NextRunNanos > s.horizon.UnixNano() {
		s.forget(sch.ID)
		return
	}
	s.push(entry, false)
}

func (s *Scheduler) buildJob(entry *HeapEntry, sch *domain.JobSchedule, runAt time.Time) *domain.Job {
	if !s.outbox && s.direct(sch.Queue) != nil {
		// Delivered directly and never stored: no id, and the
		// schedule's metadata shared, read-only. RunAt is when
		// the occurrence was due.
		return s.alloc.job(domain.Job{
			Name:       sch.Name,
			Queue:      sch.Queue,
			Data:       sch.Data,
			Metadata:   sch.Metadata,
			Priority:   entry.Priority,
			RunAt:      runAt,
			Status:     domain.StatusPending,
			ScheduleID: &entry.ScheduleID,
		})
	}

	id, _ := domain.New()

	metadata := make(map[string]string, len(sch.Metadata)+1)
	maps.Copy(metadata, sch.Metadata)
	metadata[domain.MetadataScheduledFor] = runAt.UTC().Format(time.RFC3339Nano)

	return &domain.Job{
		ID:         id.String(),
		Name:       sch.Name,
		Queue:      sch.Queue,
		Data:       sch.Data,
		Metadata:   metadata,
		Priority:   entry.Priority,
		RunAt:      runAt,
		Status:     domain.StatusPending,
		ScheduleID: &entry.ScheduleID,

		MaxAttempts: s.maxAttempts,
		Backoff:     domain.DefaultBackoff(),
	}
}

func (s *Scheduler) handleEvent(ctx context.Context, event Event) {
	switch event.Type {
	case EventScheduleAdded, EventScheduleResumed:
		s.loadEntry(ctx, event.ScheduleID)
	case EventScheduleRemoved, EventSchedulePaused:
		s.forget(event.ScheduleID)
	}
}
