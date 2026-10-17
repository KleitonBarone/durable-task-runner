package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type WorkerConfig struct {
	ID                  string
	LeaseDuration       time.Duration
	HeartbeatInterval   time.Duration
	PollInterval        time.Duration
	DatabaseTimeout     time.Duration
	ShutdownGracePeriod time.Duration
}

type Worker struct {
	engine        *Engine
	config        WorkerConfig
	handlers      map[string]Handler
	capabilities  map[string]string
	loopActive    atomic.Bool
	claimActive   atomic.Bool
	handlerActive atomic.Bool
	health        runtimeHealth
}

func NewWorker(engine *Engine, config WorkerConfig, registrations ...HandlerRegistration) (*Worker, error) {
	if engine == nil {
		return nil, fmt.Errorf("engine is required")
	}
	if strings.TrimSpace(config.ID) == "" {
		return nil, fmt.Errorf("worker id is required")
	}
	if config.LeaseDuration == 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = config.LeaseDuration / 3
	}
	if config.PollInterval == 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.DatabaseTimeout == 0 {
		config.DatabaseTimeout = min(5*time.Second, config.LeaseDuration/3)
	}
	if config.ShutdownGracePeriod == 0 {
		config.ShutdownGracePeriod = 5 * time.Second
	}
	if config.LeaseDuration < 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}
	if config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.LeaseDuration {
		return nil, fmt.Errorf("heartbeat interval must be positive and shorter than the lease duration")
	}
	if config.PollInterval <= 0 {
		return nil, fmt.Errorf("poll interval must be positive")
	}
	if config.DatabaseTimeout <= 0 || config.DatabaseTimeout >= config.LeaseDuration-config.HeartbeatInterval {
		return nil, fmt.Errorf("database timeout must be positive and leave heartbeat headroom within the lease")
	}
	if config.ShutdownGracePeriod < 0 {
		return nil, fmt.Errorf("shutdown grace period must be positive")
	}
	if len(registrations) == 0 {
		return nil, fmt.Errorf("at least one handler is required")
	}
	handlers := make(map[string]Handler, len(registrations))
	contracts := make(map[string]string, len(registrations))
	for _, registration := range registrations {
		if registration.Name == "" || registration.Handler == nil {
			return nil, fmt.Errorf("handler name and function are required")
		}
		if _, exists := handlers[registration.Name]; exists {
			return nil, fmt.Errorf("handler %q is registered twice", registration.Name)
		}
		handlers[registration.Name] = registration.Handler
		contracts[registration.Name] = registration.contract
	}
	if err := engine.registry.bindActivities(contracts); err != nil {
		return nil, err
	}
	return &Worker{engine: engine, config: config, handlers: handlers, capabilities: contracts}, nil
}

// RunOnce claims at most one activity and waits for its handler to finish. It
// heartbeats while the handler runs and fences completion if ownership is lost.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	if w.loopActive.Load() {
		return false, ErrWorkerBusy
	}
	return w.runOnce(ctx)
}

func (w *Worker) runOnce(ctx context.Context) (bool, error) {
	if !w.claimActive.CompareAndSwap(false, true) {
		return false, ErrWorkerBusy
	}
	defer w.claimActive.Store(false)
	if w.handlerActive.Load() {
		return false, ErrHandlerUnresponsive
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	claimCtx, cancelClaim := context.WithTimeout(ctx, w.config.DatabaseTimeout)
	claim, err := w.engine.claimActivity(claimCtx, w.config.ID, w.config.LeaseDuration, w.capabilities)
	cancelClaim()
	if errors.Is(err, ErrNoActivity) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if w.loopActive.Load() {
		w.health.observe(w.engine.clock, nil, 0)
	}
	handler, ok := w.handlers[claim.ActivityName]
	if !ok {
		return true, fmt.Errorf("worker %q claimed an unsupported activity %q", w.config.ID, claim.ActivityName)
	}

	handlerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	handlerCtx = context.WithValue(handlerCtx, activityContextKey{}, activityContext{engine: w.engine, claim: claim})
	type result struct {
		output []byte
		err    error
	}
	resultCh := make(chan result, 1)
	w.handlerActive.Store(true)
	go func() {
		handlerResult := result{err: &HandlerPanicError{Value: "handler exited without returning"}}
		defer func() {
			if value := recover(); value != nil {
				handlerResult.err = &HandlerPanicError{Value: value}
			}
			w.handlerActive.Store(false)
			resultCh <- handlerResult
		}()
		handlerResult.output, handlerResult.err = handler(handlerCtx, claim.ActivityInput)
	}()

	ticker := newTicker(w.engine.clock, w.config.HeartbeatInterval)
	defer ticker.Stop()
	deadline := newTicker(w.engine.clock, claim.AttemptDeadline.Sub(claim.ClaimedAt))
	defer deadline.Stop()
	stopHandler := func(cause error) error {
		cancel()
		grace := newTicker(w.engine.clock, w.config.ShutdownGracePeriod)
		defer grace.Stop()
		select {
		case <-resultCh:
			return cause
		case <-grace.Ticks():
			return errors.Join(cause, ErrHandlerUnresponsive)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return true, stopHandler(ctx.Err())
		case handlerResult := <-resultCh:
			if err := ctx.Err(); err != nil {
				return true, err
			}
			if handlerResult.err != nil {
				return true, w.recordFailure(ctx, claim, handlerResult.err)
			}
			ackCtx, cancelAck := context.WithTimeout(ctx, w.config.DatabaseTimeout)
			err := w.engine.CompleteActivity(ackCtx, claim, handlerResult.output)
			cancelAck()
			if errors.Is(err, ErrInvalidInput) {
				return true, w.recordFailure(ctx, claim, err)
			}
			if err != nil {
				return true, err
			}
			return true, nil
		case <-ticker.Ticks():
			beatCtx, cancelBeat := context.WithTimeout(ctx, w.config.DatabaseTimeout)
			_, err := w.engine.Heartbeat(beatCtx, claim, w.config.LeaseDuration)
			cancelBeat()
			if err != nil {
				return true, stopHandler(err)
			}
			if w.loopActive.Load() {
				w.health.observe(w.engine.clock, nil, 0)
			}
		case <-deadline.Ticks():
			cancel()
			controlCtx, cancelControl := context.WithTimeout(ctx, w.config.DatabaseTimeout)
			_, err := w.engine.Heartbeat(controlCtx, claim, w.config.LeaseDuration)
			cancelControl()
			if err != nil {
				return true, stopHandler(err)
			}
		}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if !w.loopActive.CompareAndSwap(false, true) {
		return ErrWorkerBusy
	}
	defer w.loopActive.Store(false)
	return runtimeLoop(ctx, w.engine.clock, w.config.PollInterval, &w.health, w.runOnce)
}

func (w *Worker) recordFailure(ctx context.Context, claim Claim, cause error) error {
	ackCtx, cancel := context.WithTimeout(ctx, w.config.DatabaseTimeout)
	defer cancel()
	if err := w.engine.FailActivity(ackCtx, claim, cause); err != nil {
		return fmt.Errorf("record activity failure (%v): %w", cause, err)
	}
	return &TaskError{WorkflowID: claim.WorkflowID, ActivityID: claim.ActivityID, AttemptID: claim.AttemptID, Cause: cause}
}

func (w *Worker) Health() RuntimeHealth { return w.health.snapshot() }
