//go:build integration

package benchmarks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func seedBenchmark(b *testing.B, count int) (*benchmarkEngine, []durable.WorkflowExecution) {
	b.Helper()
	engine, definition := newBenchmarkEngine(b)
	executions := make([]durable.WorkflowExecution, 0, count)
	for range count {
		execution, err := durable.Start(b.Context(), engine.Engine, definition, integrationInput{})
		if err != nil {
			b.Fatal(err)
		}
		executions = append(executions, execution)
	}
	return engine, executions
}

func BenchmarkClaimActivity(b *testing.B) {
	engine, _ := seedBenchmark(b, b.N)
	b.ResetTimer()
	for range b.N {
		if _, err := engine.ClaimActivity(b.Context(), "benchmark", time.Minute); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClaimContention(b *testing.B) {
	engine, _ := seedBenchmark(b, b.N)
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
	defer cancel()
	var next, empty atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	b.ResetTimer()
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(b.N) {
				for {
					_, err := engine.ClaimActivity(ctx, fmt.Sprintf("benchmark-%d", i), time.Minute)
					if errors.Is(err, durable.ErrNoActivity) {
						empty.Add(1)
						continue
					}
					if err != nil {
						errs <- err
						return
					}
					break
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	close(errs)
	for err := range errs {
		b.Fatal(err)
	}
	b.ReportMetric(float64(empty.Load())/float64(b.N), "empty/op")
	var claims int
	if err := engine.pool.QueryRow(b.Context(), `SELECT count(*) FROM leases WHERE state='active'`).Scan(&claims); err != nil || claims != b.N {
		b.Fatalf("claims=%d want=%d: %v", claims, b.N, err)
	}
}

func BenchmarkRecoveryBatch100(b *testing.B) {
	engine, _ := seedBenchmark(b, b.N*100)
	for range b.N * 100 {
		if _, err := engine.ClaimActivity(b.Context(), "disappeared", time.Minute); err != nil {
			b.Fatal(err)
		}
	}
	// Failure injection excludes waiting from the timing while preserving the
	// production PostgreSQL clock path inside recovery.
	if _, err := engine.pool.Exec(b.Context(), `UPDATE leases SET expires_at=clock_timestamp()-interval '1 microsecond' WHERE state='active'`); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		if count, err := engine.RecoverAbandoned(b.Context(), 100); err != nil || count != 100 {
			b.Fatalf("recovered=%d: %v", count, err)
		}
	}
	b.ReportMetric(100, "activities/op")
}

func BenchmarkTimelinePage(b *testing.B) {
	for _, size := range []int{100, 10000} {
		b.Run(fmt.Sprintf("events%d", size), func(b *testing.B) {
			engine, executions := seedBenchmark(b, 1)
			if _, err := engine.pool.Exec(b.Context(), `INSERT INTO execution_history(workflow_id,event_type,details,recorded_at) SELECT $1,'benchmark.sample','{}',clock_timestamp() FROM generate_series(1,$2::integer)`, executions[0].ID, size); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				page, err := engine.ListTimeline(b.Context(), executions[0].ID, durable.PageRequest{Limit: 100})
				if err != nil || len(page.Items) != 100 {
					b.Fatalf("timeline size=%d: %v", len(page.Items), err)
				}
			}
		})
	}
}
