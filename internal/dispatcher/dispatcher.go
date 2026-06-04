package dispatcher

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/internal/domain"
	"github.com/zsweiter/hydra/internal/storage"
)

// queueHash computes a 6-bit hash (0-63) from a queue name
// for use in the paused-queues bitmask.
func queueHash(name domain.QueueName) uint64 {
	var h uint64
	for i := 0; i < len(name); i++ {
		h = h*31 + uint64(name[i])
	}
	return h & 63
}

// Dispatcher claims jobs from storage and feeds them into
// the worker pool's job channel.
//
// It batches claims to reduce storage round trips and
// respects queue pause states via a bitmask for up to 64
// queues.
type Dispatcher struct {
	storage   storage.Storage
	jobChan   chan<- *domain.Job
	signal    chan struct{}
	batchSize int
	leaseTTL  time.Duration
	workerID  string
	paused    atomic.Uint64
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
		workerID:  "dispatcher-" + time.Now().Format("150405"),
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

// PauseQueue prevents the Dispatcher from claiming jobs for
// a specific queue. Queue names are hashed; collisions result
// in both queues being paused (safe, max 64 queues).
func (d *Dispatcher) PauseQueue(queue domain.QueueName) {
	mask := uint64(1) << queueHash(queue)
	d.paused.Or(mask)
}

// ResumeQueue re-enables job claiming for a specific queue.
func (d *Dispatcher) ResumeQueue(queue domain.QueueName) {
	mask := ^(uint64(1) << queueHash(queue))
	d.paused.And(mask)
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
	pausedMask := d.paused.Load()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if _, err := d.storage.PromoteScheduled(ctx, time.Now(), d.batchSize); err != nil {
			return
		}

		claimed, err := d.claimFromQueues(ctx, pausedMask)
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

func (d *Dispatcher) claimFromQueues(ctx context.Context, pausedMask uint64) ([]storage.LeasedJob, error) {
	queues, err := d.storage.ReadyQueues(ctx)
	if err != nil {
		return nil, err
	}

	var cap int
	cap = min(len(queues), d.batchSize)
	all := make([]storage.LeasedJob, 0, cap)

	for _, q := range queues {
		if pausedMask&(uint64(1)<<queueHash(q)) != 0 {
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
