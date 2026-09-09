package durable

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type WorkerConfig struct {
	ID                string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
}

type Worker struct {
	engine   *Engine
	config   WorkerConfig
	handlers map[string]Handler
}

func NewWorker(engine *Engine, config WorkerConfig, registrations ...HandlerRegistration) (*Worker, error) {
	if engine == nil {
		return nil, fmt.Errorf("engine is required")
	}
	if config.ID == "" {
		return nil, fmt.Errorf("worker id is required")
	}
	if config.LeaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}
	if config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.LeaseDuration {
		return nil, fmt.Errorf("heartbeat interval must be positive and shorter than the lease duration")
	}
	if config.PollInterval <= 0 {
		return nil, fmt.Errorf("poll interval must be positive")
	}
	handlers := make(map[string]Handler, len(registrations))
	for _, registration := range registrations {
		if registration.Name == "" || registration.Handler == nil {
			return nil, fmt.Errorf("handler name and function are required")
		}
		if _, exists := handlers[registration.Name]; exists {
			return nil, fmt.Errorf("handler %q is registered twice", registration.Name)
		}
		handlers[registration.Name] = registration.Handler
	}
	return &Worker{engine: engine, config: config, handlers: handlers}, nil
}

// RunOnce claims at most one activity and waits for its handler to finish. It
// heartbeats while the handler runs and fences completion if ownership is lost.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	claim, err := w.engine.ClaimActivity(ctx, w.config.ID, w.config.LeaseDuration)
	if errors.Is(err, ErrNoActivity) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	handler, ok := w.handlers[claim.ActivityName]
	if !ok {
		handlerErr := fmt.Errorf("worker %q has no handler for activity %q", w.config.ID, claim.ActivityName)
		if err := w.engine.FailActivity(ctx, claim, handlerErr); err != nil {
			return true, errors.Join(handlerErr, err)
		}
		return true, handlerErr
	}

	handlerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		output []byte
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		output, err := handler(handlerCtx, claim.ActivityInput)
		resultCh <- result{output: output, err: err}
	}()

	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		case handlerResult := <-resultCh:
			if handlerResult.err != nil {
				if err := w.engine.FailActivity(ctx, claim, handlerResult.err); err != nil {
					return true, errors.Join(handlerResult.err, err)
				}
				return true, handlerResult.err
			}
			if err := w.engine.CompleteActivity(ctx, claim, handlerResult.output); err != nil {
				return true, err
			}
			return true, nil
		case <-ticker.C:
			if _, err := w.engine.Heartbeat(ctx, claim, w.config.LeaseDuration); err != nil {
				cancel()
				return true, err
			}
		}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()
	for {
		worked, err := w.RunOnce(ctx)
		if err != nil {
			return err
		}
		if worked {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
