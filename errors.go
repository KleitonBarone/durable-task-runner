package durable

import (
	"errors"
	"fmt"
)

var (
	ErrNoActivity          = errors.New("no activity is ready")
	ErrLeaseLost           = errors.New("activity lease is no longer valid")
	ErrUnknownWorkflow     = errors.New("workflow definition is not registered")
	ErrNotFound            = errors.New("execution or step not found")
	ErrInvalidInput        = errors.New("invalid input")
	ErrPayloadTooLarge     = errors.New("JSON payload exceeds limit")
	ErrResultUnavailable   = errors.New("step result is not available")
	ErrTypeMismatch        = errors.New("activity type does not match persisted contract")
	ErrStartConflict       = errors.New("start key already used with different input")
	ErrWorkerBusy          = errors.New("worker instance is already running")
	ErrHandlerUnresponsive = errors.New("activity handler did not stop within shutdown grace period")
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

// TaskError means a handler failure was successfully recorded. RunOnce exposes
// it to callers; Run keeps processing other executions. Unwrap preserves causes.
type TaskError struct {
	WorkflowID string
	ActivityID string
	AttemptID  string
	Cause      error
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("activity %s failed: %v", e.ActivityID, e.Cause)
}
func (e *TaskError) Unwrap() error { return e.Cause }
