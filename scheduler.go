package durable

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type Scheduler struct {
	engine       *Engine
	pollInterval time.Duration
	batchSize    int
	active       atomic.Bool
	health       runtimeHealth
}

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

func (s *Scheduler) RunOnce(ctx context.Context) (int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.engine.RecoverAbandoned(queryCtx, s.batchSize)
}

func (s *Scheduler) Run(ctx context.Context) error {
	if !s.active.CompareAndSwap(false, true) {
		return fmt.Errorf("scheduler instance is already running")
	}
	defer s.active.Store(false)
	return runtimeLoop(ctx, s.engine.clock, s.pollInterval, &s.health, func(ctx context.Context) (bool, error) { _, err := s.RunOnce(ctx); return false, err }, func(err error) { s.engine.runtimeError(ctx, err) })
}

func (s *Scheduler) Health() RuntimeHealth { return s.health.snapshot() }
