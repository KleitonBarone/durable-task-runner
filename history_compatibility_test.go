package durable

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type historyFixture struct {
	Events       []TimelineEvent
	Workflow     WorkflowState
	Compensation CompensationState
	Activities   map[string]ActivityState
	Attempts     map[string]AttemptState
}

// This test-owned projection reads recorded events. It never executes a workflow
// definition or handler, and is not a public workflow replay API.
func TestRecordedHistoryCompatibility(t *testing.T) {
	paths, err := filepath.Glob("testdata/history/*.json")
	if err != nil || len(paths) != 5 {
		t.Fatalf("history fixtures missing: %v %v", paths, err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var fixture historyFixture
			if err := json.Unmarshal(raw, &fixture); err != nil {
				t.Fatal(err)
			}
			workflow, compensation, activities, attempts, err := projectHistory(fixture.Events)
			if err != nil {
				t.Fatal(err)
			}
			if workflow != fixture.Workflow || compensation != fixture.Compensation || !reflect.DeepEqual(activities, fixture.Activities) || !reflect.DeepEqual(attempts, fixture.Attempts) {
				t.Fatalf("history meaning changed: %s %s %#v %#v", workflow, compensation, activities, attempts)
			}
		})
	}
}

func projectHistory(events []TimelineEvent) (WorkflowState, CompensationState, map[string]ActivityState, map[string]AttemptState, error) {
	var workflow WorkflowState
	compensation := CompensationNone
	activities := map[string]ActivityState{}
	attempts := map[string]AttemptState{}
	setAttempt := func(event TimelineEvent, state AttemptState) error {
		if event.AttemptID == nil {
			return nil
		}
		key := *event.AttemptID
		if attempts[key] == state {
			return nil
		}
		if err := ValidateAttemptTransition(attempts[key], state); err != nil {
			return err
		}
		attempts[key] = state
		return nil
	}
	for i, event := range events {
		if event.Sequence != int64(i+1) {
			return workflow, compensation, activities, attempts, fmt.Errorf("history order at %d", i)
		}
		var err error
		switch event.Type {
		case "workflow.started":
			if workflow != "" {
				err = fmt.Errorf("duplicate workflow start")
			} else {
				workflow = WorkflowRunning
			}
		case "workflow.compensation_completed":
			compensation = CompensationCompleted
		case "workflow.compensation_failed":
			compensation = CompensationFailed
			fallthrough
		case "workflow.completed", "workflow.failed", "workflow.timed_out", "workflow.cancel_requested", "workflow.cancelled", "workflow.compensating":
			next := WorkflowState(strings.TrimPrefix(event.Type, "workflow."))
			err = ValidateWorkflowTransition(workflow, next)
			workflow = next
			if next == WorkflowCompensating {
				compensation = CompensationRunning
			}
		case "activity.scheduled", "compensation.scheduled":
			var details struct{ State ActivityState }
			err = json.Unmarshal(event.Details, &details)
			if event.ActivityID == nil || details.State == "" {
				err = fmt.Errorf("scheduled activity missing identity/state")
			} else {
				activities[*event.ActivityID] = details.State
			}
		case "attempt.abandoned", "attempt.cancelled", "attempt.timed_out":
			err = setAttempt(event, AttemptState(strings.TrimPrefix(event.Type, "attempt.")))
		default:
			if event.ActivityID == nil {
				err = fmt.Errorf("unknown event %q", event.Type)
				break
			}
			key := *event.ActivityID
			var next ActivityState
			switch event.Type {
			case "activity.claimed":
				next = ActivityRunning
				if event.AttemptID == nil {
					err = fmt.Errorf("claim without attempt")
				} else if attempts[*event.AttemptID] != "" {
					err = fmt.Errorf("duplicate attempt")
				} else {
					attempts[*event.AttemptID] = AttemptRunning
				}
			case "activity.ready", "activity.recovered":
				next = ActivityReady
			case "activity.retry_scheduled":
				var details struct {
					AvailableAt time.Time `json:"available_at"`
				}
				err = json.Unmarshal(event.Details, &details)
				next = ActivityReady
				if details.AvailableAt.After(event.RecordedAt) {
					next = ActivityWaiting
				}
				if event.AttemptID != nil && attempts[*event.AttemptID] == AttemptRunning {
					err = setAttempt(event, AttemptFailed)
				}
			case "activity.completed":
				next = ActivityCompleted
				err = setAttempt(event, AttemptCompleted)
			case "activity.failed":
				next = ActivityFailed
				if event.AttemptID != nil && attempts[*event.AttemptID] == AttemptRunning {
					err = setAttempt(event, AttemptFailed)
				}
			case "activity.cancelled":
				next = ActivityCancelled
			case "activity.timed_out":
				next = ActivityTimedOut
			default:
				err = fmt.Errorf("unknown event %q", event.Type)
			}
			if err == nil {
				err = ValidateActivityTransition(activities[key], next)
			}
			activities[key] = next
		}
		if err != nil {
			return workflow, compensation, activities, attempts, fmt.Errorf("event %d %s: %w", i+1, event.Type, err)
		}
	}
	return workflow, compensation, activities, attempts, nil
}
