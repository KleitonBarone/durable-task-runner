//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func TestBoundedLoadRecoversLostAcknowledgementsWithSchedulerOverlap(t *testing.T) {
	const jobs, crashes = 200, 20
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	clock := durable.NewManualClock(time.Unix(1000, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	first := durable.DefineActivity[int, int]("load-effect")
	last := durable.DefineActivity[int, int]("load-result")
	definition := durable.Define("bounded-load", func(in int) ([]durable.ActivitySpec, error) {
		a, err := first.Step("effect", in)
		if err != nil {
			return nil, err
		}
		b, err := last.Step("result", in)
		return []durable.ActivitySpec{a, b}, err
	})
	if err := durable.Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	var executions []durable.WorkflowExecution
	for i := range jobs {
		execution, err := durable.Start(ctx, engine.Engine, definition, i, durable.WithStartKey(fmt.Sprintf("load/%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		executions = append(executions, execution)
	}
	effects := map[string]int{}
	active := map[string]bool{}
	var mu sync.Mutex
	var deliveries, finished atomic.Int64
	for range crashes {
		claim, err := engine.ClaimActivity(ctx, "disappeared", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var input int
		if err := json.Unmarshal(claim.ActivityInput, &input); err != nil {
			t.Fatal(err)
		}
		effects["durable:"+claim.ActivityID] = input
		deliveries.Add(1)
	}
	clock.Advance(time.Second)
	errCh := make(chan error, 6)
	var wg sync.WaitGroup
	start := make(chan struct{})
	var recovered atomic.Int64
	for range 2 {
		scheduler, err := durable.NewScheduler(engine.Engine, time.Second, 100)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			count, err := scheduler.RunOnce(ctx)
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			recovered.Add(int64(count))
		}()
	}
	for i := range 4 {
		worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: fmt.Sprintf("load-%d", i)}, first.Handle(func(ctx context.Context, input int) (int, error) {
			metadata, _ := durable.Metadata(ctx)
			mu.Lock()
			if active[metadata.ActivityID] {
				mu.Unlock()
				return 0, errors.New("two live handlers for one activity")
			}
			active[metadata.ActivityID] = true
			mu.Unlock()
			defer func() { mu.Lock(); delete(active, metadata.ActivityID); mu.Unlock() }()
			runtime.Gosched()
			mu.Lock()
			defer mu.Unlock()
			deliveries.Add(1)
			if output, exists := effects[metadata.EffectKey]; exists {
				return output, nil
			}
			effects[metadata.EffectKey] = input
			return input, nil
		}), last.Handle(func(ctx context.Context, input int) (int, error) {
			output, err := first.Result(ctx, "effect")
			if err != nil {
				return 0, err
			}
			if output != input {
				return 0, errors.New("prior output changed")
			}
			finished.Add(1)
			return output + 1, nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for finished.Load() < jobs {
				worked, err := worker.RunOnce(ctx)
				if err != nil {
					errCh <- err
					cancel()
					return
				}
				if !worked {
					runtime.Gosched()
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if recovered.Load() != crashes || finished.Load() != jobs || deliveries.Load() != jobs+crashes || len(effects) != jobs {
		t.Fatalf("load outcomes: recovered=%d finished=%d deliveries=%d effects=%d", recovered.Load(), finished.Load(), deliveries.Load(), len(effects))
	}
	var completed, activities, abandoned, attempts, leases int
	if err := engine.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM workflow_executions WHERE state='completed'),(SELECT count(*) FROM activities WHERE state='completed'),(SELECT count(*) FROM attempts WHERE state='abandoned'),(SELECT count(*) FROM attempts),(SELECT count(*) FROM leases WHERE state='active')`).Scan(&completed, &activities, &abandoned, &attempts, &leases); err != nil {
		t.Fatal(err)
	}
	if completed != jobs || activities != jobs*2 || abandoned != crashes || attempts != jobs*2+crashes || leases != 0 {
		t.Fatalf("stuck or inconsistent load: workflows=%d activities=%d abandoned=%d attempts=%d active leases=%d", completed, activities, abandoned, attempts, leases)
	}
	for _, i := range []int{0, jobs - 1} {
		if output, err := last.WorkflowResult(ctx, engine.Engine, executions[i].ID); err != nil || output != i+1 {
			t.Fatalf("job %d result=%d: %v", i, output, err)
		}
	}
	page, err := engine.ListWorkflows(ctx, durable.WorkflowFilter{DefinitionName: definition.Name}, durable.PageRequest{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	count := len(page.Items)
	for page.NextCursor != "" {
		page, err = engine.ListWorkflows(ctx, durable.WorkflowFilter{DefinitionName: definition.Name}, durable.PageRequest{Limit: 50, Cursor: page.NextCursor})
		if err != nil {
			t.Fatal(err)
		}
		count += len(page.Items)
	}
	if count != jobs {
		t.Fatalf("load inspection count=%d", count)
	}
	t.Logf("%d jobs, 4 workers, 2 briefly overlapping scheduler calls: %d deliveries, %d effects, %d abandoned attempts, no active leases", jobs, deliveries.Load(), len(effects), abandoned)
}
