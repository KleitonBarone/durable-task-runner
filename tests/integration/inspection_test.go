//go:build integration

package integration

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func TestInspectionPagesBoundFilterAndRedact(t *testing.T) {
	clock := durable.NewManualClock(time.Unix(1000, 0))
	engine, definition := newIntegrationEngine(t, clock, 2)
	first, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{Value: "private-payload"}, durable.WithStartKey("private-start-key"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, errors.New("private-error")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second * 10)
	second, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{Value: "private-payload"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Cancel(t.Context(), second.ID, "private-actor", "private-reason"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second * 10)
	third, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	filter := durable.WorkflowFilter{DefinitionName: definition.Name, DefinitionVersion: "1"}
	page, err := engine.ListWorkflows(t.Context(), filter, durable.PageRequest{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != first.ID || page.NextCursor == "" {
		t.Fatalf("first page: %#v %v", page, err)
	}
	if page.Items[0].Age != time.Second*20 || !page.Items[0].HasError {
		t.Fatalf("diagnostic: %#v", page.Items[0])
	}
	assertRedacted := func(value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "private-") || strings.Contains(string(raw), claim.LeaseToken) || strings.Contains(string(raw), "durable:"+claim.ActivityID) {
			t.Fatalf("inspection leaked private data: %s", raw)
		}
	}
	assertRedacted(page)
	late, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{page.Items[0].ID}
	for page.NextCursor != "" {
		page, err = engine.ListWorkflows(t.Context(), filter, durable.PageRequest{Limit: 1, Cursor: page.NextCursor})
		if err != nil {
			t.Fatal(err)
		}
		assertRedacted(page)
		for _, item := range page.Items {
			ids = append(ids, item.ID)
		}
	}
	if len(ids) != 3 || ids[1] != second.ID || ids[2] != third.ID {
		t.Fatalf("new insert crossed upper bound: %v late=%s", ids, late.ID)
	}
	failed, err := engine.ListWorkflows(t.Context(), durable.WorkflowFilter{State: durable.WorkflowFailed, TerminalReason: "handler_error"}, durable.PageRequest{Sensitive: true})
	if err != nil || len(failed.Items) != 1 || failed.Items[0].LastError != "private-error" {
		t.Fatalf("sensitive failure: %#v %v", failed, err)
	}
	cancelled, err := engine.ListWorkflows(t.Context(), durable.WorkflowFilter{ID: second.ID}, durable.PageRequest{Sensitive: true})
	if err != nil || len(cancelled.Items) != 1 || cancelled.Items[0].CancelReason != "private-reason" {
		t.Fatalf("cancellation: %#v %v", cancelled, err)
	}
	activities, err := engine.ListActivities(t.Context(), first.ID, durable.PageRequest{Limit: 1})
	if err != nil || len(activities.Items) != 1 || activities.NextCursor == "" || activities.Items[0].Input != nil {
		t.Fatalf("activity page: %#v %v", activities, err)
	}
	assertRedacted(activities)
	attempts, err := engine.ListAttempts(t.Context(), claim.ActivityID, durable.PageRequest{})
	if err != nil || len(attempts.Items) != 1 || attempts.Items[0].State != durable.AttemptFailed {
		t.Fatalf("attempts: %#v %v", attempts, err)
	}
	assertRedacted(attempts)
	attempts, err = engine.ListAttempts(t.Context(), claim.ActivityID, durable.PageRequest{Sensitive: true})
	if err != nil || attempts.Items[0].Error != "private-error" {
		t.Fatalf("attempt error: %#v %v", attempts, err)
	}
	leases, err := engine.ListLeases(t.Context(), claim.ActivityID, durable.PageRequest{Sensitive: true})
	if err != nil || len(leases.Items) != 1 || leases.Items[0].Token != "" {
		t.Fatalf("lease token exposed: %#v %v", leases, err)
	}
	assertRedacted(leases)
	timeline, err := engine.ListTimeline(t.Context(), first.ID, durable.PageRequest{})
	if err != nil || len(timeline.Items) == 0 {
		t.Fatal(err)
	}
	assertRedacted(timeline)
	timeline, err = engine.ListTimeline(t.Context(), first.ID, durable.PageRequest{Sensitive: true})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(timeline)
	if !strings.Contains(string(raw), "private-error") {
		t.Fatal("sensitive history omitted failure")
	}
	for _, request := range []durable.PageRequest{{Limit: -1}, {Limit: 501}, {Cursor: "invalid"}, {Cursor: activities.NextCursor, Sensitive: true}} {
		if _, err := engine.ListWorkflows(t.Context(), filter, request); !errors.Is(err, durable.ErrInvalidInput) {
			t.Fatalf("invalid page accepted: %#v %v", request, err)
		}
	}
}

func TestTimelineCursorBoundsConcurrentProgressAndDeletedRows(t *testing.T) {
	clock := durable.NewManualClock(time.Unix(1000, 0))
	engine, definition := newIntegrationEngine(t, clock, 1)
	execution, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := engine.ListTimeline(t.Context(), execution.ID, durable.PageRequest{Limit: 1})
	if err != nil || page.NextCursor == "" {
		t.Fatalf("timeline first page: %#v %v", page, err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	next, err := engine.ListTimeline(t.Context(), execution.ID, durable.PageRequest{Limit: 1, Cursor: page.NextCursor})
	if err != nil || len(next.Items) != 1 || next.Items[0].Type != "activity.scheduled" || next.NextCursor != "" {
		t.Fatalf("later progress entered earlier scan: %#v %v", next, err)
	}
	newScan, err := engine.ListTimeline(t.Context(), execution.ID, durable.PageRequest{})
	if err != nil || len(newScan.Items) != 5 {
		t.Fatalf("new scan: %#v %v", newScan, err)
	}
	clock.Advance(time.Second)
	if _, err := engine.PurgeTerminal(t.Context(), durable.RetentionPolicy{CompletedBefore: clock.Now()}); err != nil {
		t.Fatal(err)
	}
	deleted, err := engine.ListTimeline(t.Context(), execution.ID, durable.PageRequest{Cursor: page.NextCursor})
	if err != nil || len(deleted.Items) != 0 || deleted.NextCursor != "" {
		t.Fatalf("deleted scope: %#v %v", deleted, err)
	}
}

func TestInspectionExplainsCleanupFailureAfterCancellation(t *testing.T) {
	engine, _, execution := compensationExecution(t, durable.RetryPolicy{MaxAttempts: 1})
	completeForwardSteps(t, engine, 2)
	if _, err := engine.Cancel(t.Context(), execution.ID, "operator", "private-reason"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	cleanup, err := engine.ClaimActivity(t.Context(), "cleanup", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), cleanup, errors.New("private-cleanup-error")); err != nil {
		t.Fatal(err)
	}
	page, err := engine.ListWorkflows(t.Context(), durable.WorkflowFilter{CompensationState: durable.CompensationFailed}, durable.PageRequest{})
	if err != nil || len(page.Items) != 1 || !page.Items[0].HasError || page.Items[0].OriginalState != durable.WorkflowCancelled || page.Items[0].CompensationError != "" || page.Items[0].State != durable.WorkflowCompensationFailed {
		t.Fatalf("cleanup diagnosis: %#v %v", page, err)
	}
	page, err = engine.ListWorkflows(t.Context(), durable.WorkflowFilter{ID: execution.ID}, durable.PageRequest{Sensitive: true})
	if err != nil || page.Items[0].CompensationError != "private-cleanup-error" || page.Items[0].LastError != "cancelled" {
		t.Fatalf("separate original/cleanup causes: %#v %v", page, err)
	}
}
