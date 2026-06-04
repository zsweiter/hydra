package worker

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/zsweiter/hydra/internal/domain"
)

// Worker is a function that processes a job.
type Worker func(ctx context.Context, job *domain.Job) error

// handlerGroup holds multiple workers for a single queue
// and distributes jobs in round-robin fashion.
type handlerGroup struct {
	workers []Worker
	counter atomic.Uint64
}

// Registry maps queue names to their registered workers.
//
// Workers are registered using Queue.Handle() and can be
// looked up by the Worker Pool when a job arrives.
//
// The hot-path lookup (Next) uses sync.Map.Load which is
// optimised for read-heavy workloads.
type Registry struct {
	handlers sync.Map
}

// NewRegistry creates a new worker registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a worker for the given queue.
// Multiple workers can be registered for the same queue;
// jobs are distributed round-robin.
func (r *Registry) Register(queue domain.QueueName, worker Worker) {
	v, _ := r.handlers.LoadOrStore(queue, &handlerGroup{})
	hg := v.(*handlerGroup)
	hg.workers = append(hg.workers, worker)
}

// Next returns the next worker for the given queue using
// round-robin selection. Returns nil if no worker is
// registered for the queue.
func (r *Registry) Next(queue domain.QueueName) Worker {
	v, ok := r.handlers.Load(queue)
	if !ok {
		return nil
	}

	hg := v.(*handlerGroup)
	if len(hg.workers) == 0 {
		return nil
	}

	idx := hg.counter.Add(1) - 1
	return hg.workers[idx%uint64(len(hg.workers))]
}
