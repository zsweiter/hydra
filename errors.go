package hydra

import "errors"

var (
	// ErrQueueNotRunning is returned when an operation requires
	// the queue to be running but it is not.
	ErrQueueNotRunning = errors.New("queue is not running")

	// ErrQueueAlreadyRunning is returned when Start is called
	// on a queue that is already running.
	ErrQueueAlreadyRunning = errors.New("queue is already running")
)
