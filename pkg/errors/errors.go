package errors

import (
	"errors"
)

type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string { return e.Err.Error() }

func Retryable(err error) error { return RetryableError{Err: err} }

func IsRetryable(err error) bool {
	var retryable RetryableError
	return errors.As(err, &retryable)
}

type NonRetryableError struct {
	Err error
}

func (e NonRetryableError) Error() string { return e.Err.Error() }

func NonRetryable(err error) error { return NonRetryableError{Err: err} }

func IsNonRetryable(err error) bool {
	var nonRetryable NonRetryableError
	return errors.As(err, &nonRetryable)
}
