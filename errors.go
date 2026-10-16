package durable

import "errors"

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
