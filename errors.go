package durable

import "errors"

var (
	ErrNoActivity      = errors.New("no activity is ready")
	ErrLeaseLost       = errors.New("activity lease is no longer valid")
	ErrUnknownWorkflow = errors.New("workflow definition is not registered")
)
