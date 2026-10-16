package durable

import (
	"errors"
	"fmt"
)

var (
	ErrNoActivity        = errors.New("no activity is ready")
	ErrLeaseLost         = errors.New("activity lease is no longer valid")
	ErrUnknownWorkflow   = errors.New("workflow definition is not registered")
	ErrNotFound          = errors.New("execution or step not found")
	ErrInvalidInput      = errors.New("invalid input")
	ErrPayloadTooLarge   = errors.New("JSON payload exceeds limit")
	ErrResultUnavailable = errors.New("step result is not available")
	ErrTypeMismatch      = errors.New("activity type does not match persisted contract")
)

// HandlerPanicError records an abnormal handler exit as a task failure. Panics
// in goroutines created by application handlers remain the application's concern.
type HandlerPanicError struct{ Value any }

func (e *HandlerPanicError) Error() string {
	return fmt.Sprintf("activity handler panicked: %v", e.Value)
}

func (e *HandlerPanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
