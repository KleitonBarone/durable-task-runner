package durable

import (
	"context"
	"fmt"
	"time"
)

type Scheduler struct {
	engine       *Engine
	pollInterval time.Duration
	batchSize    int
}

func NewScheduler(engine *Engine, pollInterval time.Duration, batchSize int) (*Scheduler, error) {
	if engine == nil {
		return nil, fmt.Errorf("engine is required")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("scheduler poll interval must be positive")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("scheduler batch size must be positive")
	}
	return &Scheduler{engine: engine, pollInterval: pollInterval, batchSize: batchSize}, nil
}

func (s *Scheduler) RunOnce(ctx context.Context) (int, error) {
	return s.engine.RecoverAbandoned(ctx, s.batchSize)
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		if _, err := s.RunOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
