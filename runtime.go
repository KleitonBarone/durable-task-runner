package durable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// RuntimeHealth describes the local Run loop, not fleet-wide readiness.
// LastError contains runtime errors, never a recorded handler failure.
type RuntimeHealth struct {
	Running             bool
	Healthy             bool
	ConsecutiveFailures int
	LastError           error
	LastSuccessAt       time.Time
}

type runtimeHealth struct {
	mu     sync.RWMutex
	status RuntimeHealth
}

func (h *runtimeHealth) snapshot() RuntimeHealth {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.status
}

func (h *runtimeHealth) begin() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status = RuntimeHealth{Running: true}
}

func (h *runtimeHealth) observe(clock Clock, err error, failures int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status.Healthy, h.status.LastError, h.status.ConsecutiveFailures = err == nil, err, failures
	if err == nil {
		h.status.LastSuccessAt = clock.Now()
	}
}

func (h *runtimeHealth) finish(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status.Running, h.status.Healthy, h.status.LastError = false, false, err
}

// transientDatabaseError deliberately excludes SQL/configuration errors. A
// failed acknowledgement is never replayed in memory: lease recovery owns it.
func transientDatabaseError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03" || pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	var connectionErr *pgconn.ConnectError
	var networkErr net.Error
	return errors.As(err, &connectionErr) || errors.As(err, &networkErr) || pgconn.Timeout(err) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func runtimeLoop(ctx context.Context, clock Clock, poll time.Duration, health *runtimeHealth, step func(context.Context) (bool, error), onError ...func(error)) (result error) {
	health.begin()
	defer func() { health.finish(result) }()
	ticker := newTicker(clock, poll)
	defer ticker.Stop()
	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		worked, err := step(ctx)
		var taskErr *TaskError
		if errors.As(err, &taskErr) || (!errors.Is(err, ErrHandlerUnresponsive) && errors.Is(err, ErrLeaseLost)) {
			err = nil
		}
		if err != nil {
			failures = health.snapshot().ConsecutiveFailures + 1
			health.observe(clock, err, failures)
			if ctx.Err() != nil {
				return errors.Join(ctx.Err(), err)
			}
			for _, observe := range onError {
				observe(err)
			}
			if errors.Is(err, ErrHandlerUnresponsive) || !transientDatabaseError(err) || failures > 5 {
				return err
			}
			backoff := newTicker(clock, 100*time.Millisecond*time.Duration(1<<(failures-1)))
			select {
			case <-ctx.Done():
				backoff.Stop()
				return ctx.Err()
			case <-backoff.Ticks():
				backoff.Stop()
			}
			continue
		}
		failures = 0
		health.observe(clock, nil, 0)
		if worked {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.Ticks():
		}
	}
}

// WorkerConfig controls local ownership and shutdown. Zero durations select
// defaults; DatabaseTimeout plus HeartbeatInterval must fit inside LeaseDuration.
type WorkerConfig struct {
	ID                  string
	LeaseDuration       time.Duration
	HeartbeatInterval   time.Duration
	PollInterval        time.Duration
	DatabaseTimeout     time.Duration
	ShutdownGracePeriod time.Duration
}

// Worker runs one compatible handler at a time. Use multiple instances for concurrency.
type Worker struct {
	engine        *Engine
	config        WorkerConfig
	handlers      map[string]Handler
	capabilities  map[string]capability
	loopActive    atomic.Bool
	claimActive   atomic.Bool
	handlerActive atomic.Bool
	health        runtimeHealth
}

// NewWorker requires a nonblank ID and at least one handler. Defaults are a 30 s
// lease, heartbeat at one third of it, 100 ms idle poll, and 5 s shutdown grace.
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
	capabilities := make(map[string]capability, len(registrations))
	for _, registration := range registrations {
		if registration.Name == "" || registration.Handler == nil {
			return nil, fmt.Errorf("handler name and function are required")
		}
		if !registration.explicitVersion {
			if registration.Version == "" {
				registration.Version = defaultVersion
			}
			if registration.InputSchema == "" {
				registration.InputSchema = legacySchema
			}
			if registration.OutputSchema == "" {
				registration.OutputSchema = legacySchema
			}
		}
		c := capability{registration.Name, registration.Version, registration.contract, registration.InputSchema, registration.OutputSchema}
		if err := c.validate(); err != nil {
			return nil, err
		}
		key := activityKey(c.name, c.version)
		if _, exists := handlers[key]; exists {
			return nil, fmt.Errorf("handler %q is registered twice", registration.Name)
		}
		handlers[key] = registration.Handler
		contracts[key] = c.signature()
		capabilities[key] = c
	}
	if err := engine.registry.bindActivities(contracts); err != nil {
		return nil, err
	}
	return &Worker{engine: engine, config: config, handlers: handlers, capabilities: capabilities}, nil
}

// RunOnce claims at most one activity and waits for its handler to finish. It
// heartbeats while the handler runs and fences completion if ownership is lost.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	if w.loopActive.Load() {
		return false, ErrWorkerBusy
	}
	return w.runOnce(ctx)
}

func (w *Worker) runOnce(ctx context.Context) (didWork bool, runErr error) {
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
	defer func() {
		var taskErr *TaskError
		if !errors.As(runErr, &taskErr) && errors.Is(runErr, ErrLeaseLost) {
			w.engine.observe(ctx, Observation{Type: "lease.lost", At: w.engine.clock.Now(), WorkflowID: claim.WorkflowID, ActivityID: claim.ActivityID, AttemptID: claim.AttemptID, WorkerID: claim.WorkerID, StepKey: claim.StepKey, Version: claim.ActivityVersion, Kind: claim.Kind, Outcome: "lost"})
		}
	}()
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if w.loopActive.Load() {
		w.health.observe(w.engine.clock, nil, 0)
	}
	handler, ok := w.handlers[activityKey(claim.ActivityName, claim.ActivityVersion)]
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
		started := time.Now()
		handlerResult := result{err: &HandlerPanicError{Value: "handler exited without returning"}}
		defer func() {
			if value := recover(); value != nil {
				handlerResult.err = &HandlerPanicError{Value: value}
			}
			w.handlerActive.Store(false)
			w.engine.observe(handlerCtx, Observation{Type: "handler.finished", At: w.engine.clock.Now(), WorkflowID: claim.WorkflowID, ActivityID: claim.ActivityID, AttemptID: claim.AttemptID, WorkerID: claim.WorkerID, StepKey: claim.StepKey, Version: claim.ActivityVersion, Kind: claim.Kind, Outcome: handlerOutcome(handlerResult.err), Duration: time.Since(started)})
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
			// Local timers can run ahead of the database clock. Confirm the
			// durable deadline before cancelling; otherwise a cooperative
			// handler's context error could be recorded as an ordinary failure.
			controlCtx, cancelControl := context.WithTimeout(ctx, w.config.DatabaseTimeout)
			_, err := w.engine.Heartbeat(controlCtx, claim, w.config.LeaseDuration)
			cancelControl()
			if err != nil {
				return true, stopHandler(err)
			}
		}
	}
}

// Run continues after recorded task failures and cooperative lease loss. Runtime
// failures use bounded reconnects. Cancellation requests handler shutdown within
// the grace period; an unresponsive handler prevents another invocation.
func (w *Worker) Run(ctx context.Context) error {
	if !w.loopActive.CompareAndSwap(false, true) {
		return ErrWorkerBusy
	}
	defer w.loopActive.Store(false)
	return runtimeLoop(ctx, w.engine.clock, w.config.PollInterval, &w.health, w.runOnce, func(err error) { w.engine.runtimeError(ctx, err) })
}

func (w *Worker) recordFailure(ctx context.Context, claim Claim, cause error) error {
	ackCtx, cancel := context.WithTimeout(ctx, w.config.DatabaseTimeout)
	defer cancel()
	if err := w.engine.FailActivity(ackCtx, claim, cause); err != nil {
		return fmt.Errorf("record activity failure (%v): %w", cause, err)
	}
	return &TaskError{WorkflowID: claim.WorkflowID, ActivityID: claim.ActivityID, AttemptID: claim.AttemptID, Cause: cause}
}

// Health describes this instance's Run loop. RunOnce does not maintain loop health.
func (w *Worker) Health() RuntimeHealth { return w.health.snapshot() }

// Scheduler recovers expired ownership and drives persisted control transitions.
type Scheduler struct {
	engine       *Engine
	pollInterval time.Duration
	batchSize    int
	active       atomic.Bool
	health       runtimeHealth
}

// NewScheduler defaults to a one-second poll and batches of 100 workflows.
func NewScheduler(engine *Engine, pollInterval time.Duration, batchSize int) (*Scheduler, error) {
	if engine == nil {
		return nil, fmt.Errorf("engine is required")
	}
	if pollInterval == 0 {
		pollInterval = time.Second
	}
	if batchSize == 0 {
		batchSize = 100
	}
	if pollInterval < 0 {
		return nil, fmt.Errorf("scheduler poll interval must be positive")
	}
	if batchSize < 0 {
		return nil, fmt.Errorf("scheduler batch size must be positive")
	}
	return &Scheduler{engine: engine, pollInterval: pollInterval, batchSize: batchSize}, nil
}

// RunOnce performs one bounded reconciliation with a five-second database timeout.
func (s *Scheduler) RunOnce(ctx context.Context) (int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.engine.RecoverAbandoned(queryCtx, s.batchSize)
}

// Run polls until cancellation, a fatal error, or exhaustion of bounded reconnects.
func (s *Scheduler) Run(ctx context.Context) error {
	if !s.active.CompareAndSwap(false, true) {
		return fmt.Errorf("scheduler instance is already running")
	}
	defer s.active.Store(false)
	return runtimeLoop(ctx, s.engine.clock, s.pollInterval, &s.health, func(ctx context.Context) (bool, error) { _, err := s.RunOnce(ctx); return false, err }, func(err error) { s.engine.runtimeError(ctx, err) })
}

// Health describes this instance's Run loop, not fleet-wide scheduler availability.
func (s *Scheduler) Health() RuntimeHealth { return s.health.snapshot() }

func capabilityArrays(capabilities map[string]capability) ([]string, []string, []string, []string, []string) {
	names, contracts := make([]string, 0, len(capabilities)), make([]string, 0, len(capabilities))
	versions, inputSchemas, outputSchemas := make([]string, 0, len(capabilities)), make([]string, 0, len(capabilities)), make([]string, 0, len(capabilities))
	for _, c := range capabilities {
		names = append(names, c.name)
		contracts = append(contracts, c.contract)
		versions = append(versions, c.version)
		inputSchemas = append(inputSchemas, c.inputSchema)
		outputSchemas = append(outputSchemas, c.outputSchema)
	}
	return names, versions, contracts, inputSchemas, outputSchemas
}

type UnsupportedActivity struct {
	Name         string
	Version      string
	InputSchema  string
	OutputSchema string
	Contract     string
	ReadyCount   int64
}

// UnsupportedActivities reports up to 100 ready activity types this worker
// cannot claim. It does not classify blocked steps or claim global worker health.
func (w *Worker) UnsupportedActivities(ctx context.Context) ([]UnsupportedActivity, error) {
	if err := w.engine.CheckSchema(ctx); err != nil {
		return nil, err
	}
	names, versions, contracts, inputSchemas, outputSchemas := capabilityArrays(w.capabilities)
	rows, err := w.engine.pool.Query(ctx, `SELECT a.name,a.version,a.input_schema,a.output_schema, a.contract, count(*) FROM activities a JOIN workflow_executions w ON w.id = a.workflow_id WHERE a.state = 'ready' AND ((w.state='running' AND a.kind='forward') OR (w.state='compensating' AND a.kind='compensation')) AND (a.name,a.version, a.contract,a.input_schema,a.output_schema) NOT IN (SELECT * FROM unnest($1::text[], $2::text[],$3::text[],$4::text[],$5::text[])) GROUP BY a.name,a.version,a.input_schema,a.output_schema, a.contract ORDER BY a.name,a.version, a.contract LIMIT 100`, names, versions, contracts, inputSchemas, outputSchemas)
	if err != nil {
		return nil, fmt.Errorf("read unsupported activities: %w", err)
	}
	defer rows.Close()
	var unsupported []UnsupportedActivity
	for rows.Next() {
		var activity UnsupportedActivity
		if err := rows.Scan(&activity.Name, &activity.Version, &activity.InputSchema, &activity.OutputSchema, &activity.Contract, &activity.ReadyCount); err != nil {
			return nil, fmt.Errorf("scan unsupported activity: %w", err)
		}
		unsupported = append(unsupported, activity)
	}
	return unsupported, rows.Err()
}
