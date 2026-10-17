package durable

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
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
