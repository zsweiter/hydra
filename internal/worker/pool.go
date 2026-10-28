package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// Pool is an elastic worker pool that dynamically adjusts
// the number of active workers based on the job backlog.
//
// It receives jobs from the Dispatcher through a buffered
// channel and executes the registered handler for each job's
// queue.
//
// Scaling is signal-based: after processing a job, a worker
// checks for backlog and spawns additional temporary workers
// as needed. No timer ticker, no polling — zero CPU when idle.
type Pool struct {
	registry         *Registry
	storage          storage.Storage
	jobChan          chan *domain.Job
	activeWorkers    atomic.Int32
	minWorkers       int
	maxWorkers       int
	batchSize        int
	leaseTTL         time.Duration
	scaleDownIdle    time.Duration
	scaleUpThreshold float64
	bufCap           int
	onRetry          func(nextRunAt time.Time)
	stopChan         chan struct{}
	wg               sync.WaitGroup

	// acks feeds the acker, which acknowledges completed jobs
	// in batches.
	acks      chan string
	ackerDone chan struct{}
}

// ackBatch caps how many jobs one AckJobs call acknowledges.
const ackBatch = 256

// Config holds configuration for the worker pool.
type Config struct {
	MinWorkers    int
	MaxWorkers    int
	BatchSize     int
	LeaseTTL      time.Duration
	ScaleDownIdle time.Duration

	// ScaleUpThreshold is the channel occupancy (0.0–1.0) that
	// triggers spawning a temporary worker. Default 0.25.
	ScaleUpThreshold float64

	// OnRetry is called when a job fails and is rescheduled
	// for retry. The nextRunAt is the time the job will be
	// eligible for claiming again.
	OnRetry func(nextRunAt time.Time)
}

// NewPool creates an elastic worker pool.
func NewPool(registry *Registry, s storage.Storage, cfg Config) *Pool {
	if cfg.MinWorkers <= 0 {
		cfg.MinWorkers = 1
	}
	if cfg.MaxWorkers < cfg.MinWorkers {
		cfg.MaxWorkers = cfg.MinWorkers
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.ScaleDownIdle <= 0 {
		cfg.ScaleDownIdle = 10 * time.Second
	}
	if cfg.ScaleUpThreshold <= 0 {
		cfg.ScaleUpThreshold = 0.25
	}

	chanSize := max(cfg.MaxWorkers*4, 16)

	return &Pool{
		registry:         registry,
		storage:          s,
		jobChan:          make(chan *domain.Job, chanSize),
		minWorkers:       cfg.MinWorkers,
		maxWorkers:       cfg.MaxWorkers,
		leaseTTL:         cfg.LeaseTTL,
		scaleDownIdle:    cfg.ScaleDownIdle,
		scaleUpThreshold: cfg.ScaleUpThreshold,
		bufCap:           chanSize,
		onRetry:          cfg.OnRetry,
		stopChan:         make(chan struct{}),
		acks:             make(chan string, ackBatch*4),
		ackerDone:        make(chan struct{}),
	}
}

// Start launches the pool's permanent workers.
func (p *Pool) Start(_ context.Context) error {
	go p.runAcker()
	for i := 0; i < p.minWorkers; i++ {
		p.spawnWorker(true)
	}

	return nil
}

// Stop gracefully shuts down the pool, once the jobs already
// processed are acknowledged.
func (p *Pool) Stop(_ context.Context) error {
	close(p.stopChan)
	p.wg.Wait()
	close(p.acks)
	<-p.ackerDone
	return nil
}

// runAcker acknowledges completed jobs. It takes every ack
// waiting, up to ackBatch, per AckJobs call: one at a time
// when idle, so a lone job is not delayed, and in batches
// under load, so acks cost a fraction of a round trip each.
// A failed ack leaves the lease to expire; the job is then
// processed again (at least once).
func (p *Pool) runAcker() {
	defer close(p.ackerDone)
	batch := make([]string, 0, ackBatch)
	for id := range p.acks {
		batch = append(batch[:0], id)
	drain:
		for len(batch) < ackBatch {
			select {
			case id, ok := <-p.acks:
				if !ok {
					break drain
				}
				batch = append(batch, id)
			default:
				break drain
			}
		}
		p.storage.AckJobs(context.Background(), batch)
	}
}

// JobChan returns the channel where the Dispatcher sends jobs.
func (p *Pool) JobChan() chan<- *domain.Job {
	return p.jobChan
}

// ActiveWorkers returns the current number of active workers.
func (p *Pool) ActiveWorkers() uint64 {
	return uint64(p.activeWorkers.Load())
}

func (p *Pool) spawnWorker(permanent bool) {
	p.activeWorkers.Add(1)
	p.wg.Add(1)

	go func() {
		defer p.activeWorkers.Add(-1)
		defer p.wg.Done()
		p.runWorker(permanent)
	}()
}

func (p *Pool) runWorker(permanent bool) {
	var idleTimer *time.Timer
	var idleC <-chan time.Time

	if !permanent {
		idleTimer = time.NewTimer(p.scaleDownIdle)
		idleC = idleTimer.C
		defer idleTimer.Stop()
	}

	for {
		if !permanent && idleC != nil {
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(p.scaleDownIdle)
		}

		select {
		case job := <-p.jobChan:
			p.processJob(job)
			p.maybeScale()

		case <-idleC:
			return

		case <-p.stopChan:
			return
		}
	}
}

func (p *Pool) processJob(job *domain.Job) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic in handler: %v", r)
			p.storage.FailJob(context.Background(), job, err)
		}
	}()

	ctx := context.Background()

	worker := p.registry.Next(job.Queue)
	if worker == nil {
		result, _ := p.storage.FailJob(ctx, job, errNoHandler)
		if result.Action == storage.FailActionRetry && p.onRetry != nil {
			p.onRetry(result.NextRunAt)
		}
		return
	}

	stopHeartbeat := p.heartbeat(job.ID)
	err := worker(ctx, job)
	stopHeartbeat()

	if err == nil {
		p.acks <- job.ID
	} else {
		result, _ := p.storage.FailJob(ctx, job, err)
		if result.Action == storage.FailActionRetry && p.onRetry != nil {
			p.onRetry(result.NextRunAt)
		}
	}
}

// heartbeat extends the lease of a running job every third of
// LeaseTTL, so a job that outlives the TTL is not recovered as
// an orphan and run twice. The returned func stops it.
func (p *Pool) heartbeat(jobID string) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(p.leaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				p.storage.ExtendLease(context.Background(), jobID, p.leaseTTL)
			}
		}
	}()

	return func() {
		close(done)
		<-stopped
	}
}

// maybeScale spawns a temporary worker when channel occupancy
// exceeds ScaleUpThreshold. Called by workers after processing
// a job. Workers self-destruct after ScaleDownIdle of inactivity.
func (p *Pool) maybeScale() {
	active := p.activeWorkers.Load()
	if active >= int32(p.maxWorkers) {
		return
	}

	occupancy := float64(len(p.jobChan)) / float64(p.bufCap)
	if occupancy < p.scaleUpThreshold {
		return
	}

	// Spawn a temporary worker that will self-destruct
	// after scaleDownIdle of inactivity.
	p.spawnWorker(false)
}

var errNoHandler = &noHandlerError{}

type noHandlerError struct{}

func (e *noHandlerError) Error() string {
	return "no handler registered for queue"
}
