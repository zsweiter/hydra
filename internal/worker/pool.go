package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zsweiter/hydra/internal/domain"
	"github.com/zsweiter/hydra/internal/storage"
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
	registry      *Registry
	storage       storage.Storage
	jobChan       chan *domain.Job
	activeWorkers atomic.Int32
	minWorkers    int
	maxWorkers    int
	batchSize     int
	leaseTTL      time.Duration
	scaleDownIdle time.Duration
	onRetry       func(nextRunAt time.Time)
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

// Config holds configuration for the worker pool.
type Config struct {
	MinWorkers    int
	MaxWorkers    int
	BatchSize     int
	LeaseTTL      time.Duration
	ScaleDownIdle time.Duration

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

	chanSize := max(cfg.MaxWorkers*4, 16)

	return &Pool{
		registry:      registry,
		storage:       s,
		jobChan:       make(chan *domain.Job, chanSize),
		minWorkers:    cfg.MinWorkers,
		maxWorkers:    cfg.MaxWorkers,
		leaseTTL:      cfg.LeaseTTL,
		scaleDownIdle: cfg.ScaleDownIdle,
		onRetry:       cfg.OnRetry,
		stopChan:      make(chan struct{}),
	}
}

// Start launches the pool's permanent workers.
func (p *Pool) Start(_ context.Context) error {
	for i := 0; i < p.minWorkers; i++ {
		p.spawnWorker(false)
	}

	return nil
}

// Stop gracefully shuts down the pool.
func (p *Pool) Stop(_ context.Context) error {
	close(p.stopChan)
	p.wg.Wait()
	return nil
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

	err := worker(ctx, job)
	if err == nil {
		p.storage.AckJob(ctx, job.ID)
	} else {
		result, _ := p.storage.FailJob(ctx, job, err)
		if result.Action == storage.FailActionRetry && p.onRetry != nil {
			p.onRetry(result.NextRunAt)
		}
	}
}

// maybeScale spawns a temporary worker if there's backlog
// and we haven't reached maxWorkers. Called by workers after
// processing a job.
func (p *Pool) maybeScale() {
	active := p.activeWorkers.Load()
	if active >= int32(p.maxWorkers) {
		return
	}

	backlog := len(p.jobChan)
	if backlog == 0 {
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
