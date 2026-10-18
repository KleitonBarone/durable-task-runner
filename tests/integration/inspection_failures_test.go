//go:build integration

package integration

import (
	"errors"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func TestInspectionPreservesFailureWithoutErrorText(t *testing.T) {
	engine, definition := newIntegrationEngine(t, durable.NewManualClock(time.Unix(1000, 0)), 1)
	execution, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, errors.New("")); err != nil {
		t.Fatal(err)
	}
	page, err := engine.ListWorkflows(t.Context(), durable.WorkflowFilter{ID: execution.ID}, durable.PageRequest{Sensitive: true})
	if err != nil || len(page.Items) != 1 || page.Items[0].State != durable.WorkflowFailed || !page.Items[0].HasError || page.Items[0].LastError != "" {
		t.Fatalf("empty error lost its failure indicator: %#v %v", page, err)
	}
}
