//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func TestConcurrentStartsDeduplicateAcrossReconnectAndCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	engine, definition := newIntegrationEngine(t, durable.SystemClock{}, 1)
	const callers = 20
	results := make(chan durable.WorkflowExecution, callers)
	errorsCh := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			execution, err := durable.Start(ctx, engine.Engine, definition, integrationInput{Value: "same"}, durable.WithStartKey("request-42"))
			results <- execution
			errorsCh <- err
		}()
	}
	group.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id string
	for result := range results {
		if id == "" {
			id = result.ID
		}
		if result.ID != id {
			t.Fatalf("duplicate execution: %s != %s", result.ID, id)
		}
	}
	if _, err := durable.Start(ctx, engine.Engine, definition, integrationInput{Value: "conflict"}, durable.WithStartKey("request-42")); !errors.Is(err, durable.ErrStartConflict) {
		t.Fatalf("conflict = %v", err)
	}
	claim, err := engine.ClaimActivity(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(ctx, claim, []byte(`"done"`)); err != nil {
		t.Fatal(err)
	}
	registry := durable.NewRegistry()
	if err := durable.Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	reconnected, err := newTestEngine(engine.pool, registry, durable.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := durable.Start(ctx, reconnected.Engine, definition, integrationInput{Value: "same"}, durable.WithStartKey("request-42"))
	if err != nil || result.ID != id || result.State != durable.WorkflowCompleted || string(result.Output) != `"done"` {
		t.Fatalf("duplicate after reconnect: %#v %v", result, err)
	}
	var workflows, activities, starts int
	err = engine.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM workflow_executions), (SELECT count(*) FROM activities), (SELECT count(*) FROM execution_history WHERE event_type = 'workflow.started')`).Scan(&workflows, &activities, &starts)
	if err != nil || workflows != 1 || activities != 1 || starts != 1 {
		t.Fatalf("duplicate persistence: %d %d %d %v", workflows, activities, starts, err)
	}
	for range 2 {
		if _, err := durable.Start(ctx, engine.Engine, definition, integrationInput{}); err != nil {
			t.Fatal(err)
		}
	}
	other := definition
	other.Name = "other"
	if err := durable.Register(registry, other); err != nil {
		t.Fatal(err)
	}
	if result, err := durable.Start(ctx, reconnected.Engine, other, integrationInput{}, durable.WithStartKey("request-42")); err != nil || result.ID == id {
		t.Fatalf("definition scope: %#v %v", result, err)
	}
}

func TestStartKeyUsesJSONBEquality(t *testing.T) {
	engine, _ := newIntegrationEngine(t, durable.SystemClock{}, 1)
	definition := durable.Define("json", func(input map[string]any) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("json", input)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	first, err := durable.Start(t.Context(), engine.Engine, definition, map[string]any{"a": 1, "b": "two"}, durable.WithStartKey("json"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := durable.Start(t.Context(), engine.Engine, definition, map[string]any{"b": "two", "a": 1.0}, durable.WithStartKey("json"))
	if err != nil || first.ID != second.ID {
		t.Fatalf("JSON equality: %#v %v", second, err)
	}
}
