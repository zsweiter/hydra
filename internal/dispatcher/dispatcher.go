package dispatcher

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// Dispatcher claims jobs from storage and feeds them into
// the worker pool's job channel.
//
// It batches claims to reduce storage round trips and
// skips paused queues.
type Dispatcher struct {
	storage   storage.Storage
	jobChan   chan<- *domain.Job
	signal    chan struct{}
	batchSize int
	leaseTTL  time.Duration
	workerID  string
	paused    atomic.Pointer[map[domain.QueueName]struct{}]
	stopped   atomic.Bool
}

// New creates a new Dispatcher.
//
// jobChan is the channel where claimed jobs are sent for
// the Worker Pool to process. It can be nil and set later
// with SetJobChan.
func New(s storage.Storage, jobChan chan<- *domain.Job, batchSize int, leaseTTL time.Duration) *Dispatcher {
	return &Dispatcher{
		storage:   s,
		jobChan:   jobChan,
		signal:    make(chan struct{}, 1),
		batchSize: batchSize,
		leaseTTL:  leaseTTL,
		workerID:  "dispatcher-" + newID(),
	}
}

// SetJobChan sets the output channel for claimed jobs.
// Must be called before Run.
func (d *Dispatcher) SetJobChan(jobChan chan<- *domain.Job) {
	d.jobChan = jobChan
}

// Signal wakes the Dispatcher to claim and dispatch jobs.
func (d *Dispatcher) Signal() {
	if d.stopped.Load() {
		return
	}
	select {
	case d.signal <- struct{}{}:
	default:
	}
}

// SetPausedQueues replaces the set of queues the Dispatcher
// must not claim jobs from.
func (d *Dispatcher) SetPausedQueues(queues []domain.QueueName) {
	set := make(map[domain.QueueName]struct{}, len(queues))
	for _, q := range queues {
		set[q] = struct{}{}
	}

	d.paused.Store(&set)
}

func (d *Dispatcher) isPaused(queue domain.QueueName) bool {
	set := d.paused.Load()
	if set == nil {
		return false
	}

	_, ok := (*set)[queue]
	return ok
}

// Run starts the dispatcher loop. Blocks until the context
// is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		d.drain(ctx)

		nextScheduled, err := d.storage.NextScheduledAt(ctx)
		if err != nil || nextScheduled.IsZero() {
			select {
			case <-ctx.Done():
				d.stopped.Store(true)
				return ctx.Err()
			case <-d.signal:
			}
			continue
		}

		wait := max(time.Until(nextScheduled), 0)

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			d.stopped.Store(true)
			return ctx.Err()

		case <-d.signal:
			timer.Stop()

		case <-timer.C:
		}
	}
}

func (d *Dispatcher) drain(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if _, err := d.storage.PromoteScheduled(ctx, time.Now(), d.batchSize); err != nil {
			return
		}

		claimed, err := d.claimFromQueues(ctx)
		if err != nil {
			return
		}

		if len(claimed) == 0 {
			return
		}

		for _, leased := range claimed {
			select {
			case d.jobChan <- leased.Job:
			case <-ctx.Done():
				return
			}
		}

		if len(claimed) < d.batchSize {
			return
		}
	}
}

func (d *Dispatcher) claimFromQueues(ctx context.Context) ([]storage.LeasedJob, error) {
	queues, err := d.storage.ReadyQueues(ctx)
	if err != nil {
		return nil, err
	}

	var cap int
	cap = min(len(queues), d.batchSize)
	all := make([]storage.LeasedJob, 0, cap)

	for _, q := range queues {
		if d.isPaused(q) {
			continue
		}

		remaining := d.batchSize - len(all)
		if remaining <= 0 {
			break
		}

		leased, err := d.storage.ClaimJobs(ctx, q, d.workerID, remaining, d.leaseTTL)
		if err != nil {
			return all, nil
		}

		all = append(all, leased...)
	}

	return all, nil
}

// newID makes lease owners unique across instances; a
// timestamp alone collides when two start in the same second.
func newID() string {
	id, err := domain.New()
	if err != nil {
		return time.Now().Format("150405.000000000")
	}

	return id.String()
}
