//go:build integration

package durable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecordedHistoriesMatchExecutionState(t *testing.T) {
	for _, scenario := range []string{"normal", "recovery", "retry", "cancellation", "compensation"} {
		t.Run(scenario, func(t *testing.T) {
			clock := NewManualClock(time.Unix(1000, 0).UTC())
			engine, _ := newIntegrationEngine(t, clock, 1)
			definition := Define("history", func(string) ([]ActivitySpec, error) {
				cleanup, err := NewActivity("cleanup", "undo")
				if err != nil {
					return nil, err
				}
				first, err := NewActivity("first", "input", WithRetry(RetryPolicy{RetryErrors: true, InitialInterval: time.Second}), WithCompensation(cleanup))
				if err != nil {
					return nil, err
				}
				second, err := NewActivity("second", "input")
				return []ActivitySpec{first, second}, err
			})
			if err := Register(engine.registry, definition); err != nil {
				t.Fatal(err)
			}
			execution, err := Start(t.Context(), engine, definition, "input")
			if err != nil {
				t.Fatal(err)
			}
			first, err := engine.ClaimActivity(t.Context(), "worker", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "recovery":
				clock.Advance(time.Second)
				_, err = engine.RecoverAbandoned(t.Context(), 10)
			case "retry":
				err = engine.FailActivity(t.Context(), first, errors.New("temporary"))
			case "cancellation":
				_, err = engine.Cancel(t.Context(), execution.ID, "operator", "recorded request")
				if err == nil {
					_, err = engine.RecoverAbandoned(t.Context(), 10)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "recovery" || scenario == "retry" {
				clock.Advance(time.Second)
				if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
					t.Fatal(err)
				}
				first, err = engine.ClaimActivity(t.Context(), "replacement", time.Second)
				if err != nil {
					t.Fatal(err)
				}
			}
			if scenario != "cancellation" {
				if err := engine.CompleteActivity(t.Context(), first, []byte(`"first"`)); err != nil {
					t.Fatal(err)
				}
				second, err := engine.ClaimActivity(t.Context(), "worker", time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "compensation" {
					if err := engine.FailActivity(t.Context(), second, errors.New("original failure")); err != nil {
						t.Fatal(err)
					}
					cleanup, err := engine.ClaimActivity(t.Context(), "cleanup", time.Second)
					if err != nil {
						t.Fatal(err)
					}
					if err := engine.CompleteActivity(t.Context(), cleanup, []byte(`"undone"`)); err != nil {
						t.Fatal(err)
					}
				} else if err := engine.CompleteActivity(t.Context(), second, []byte(`"second"`)); err != nil {
					t.Fatal(err)
				}
			}
			execution, err = engine.Workflow(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			events, err := engine.Timeline(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			activities, err := engine.Activities(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			fixture := historyFixture{Workflow: execution.State, Compensation: execution.CompensationState, Activities: map[string]ActivityState{}, Attempts: map[string]AttemptState{}}
			aliases := map[string]string{}
			for i, activity := range activities {
				alias := fmt.Sprintf("activity-%d", i+1)
				aliases[activity.ID] = alias
				fixture.Activities[alias] = activity.State
				attempts, err := engine.Attempts(t.Context(), activity.ID)
				if err != nil {
					t.Fatal(err)
				}
				for _, attempt := range attempts {
					key := fmt.Sprintf("%s/attempt-%d", alias, attempt.Number)
					aliases[attempt.ID] = key
					fixture.Attempts[key] = attempt.State
				}
			}
			for i, event := range events {
				event.Sequence, event.WorkflowID = int64(i+1), "workflow"
				event.RecordedAt = event.RecordedAt.UTC()
				if event.ActivityID != nil {
					alias := aliases[*event.ActivityID]
					event.ActivityID = &alias
				}
				if event.AttemptID != nil {
					alias := aliases[*event.AttemptID]
					event.AttemptID = &alias
				}
				var details map[string]json.RawMessage
				if err := json.Unmarshal(event.Details, &details); err != nil {
					t.Fatal(err)
				}
				if parent := details["compensates_id"]; parent != nil {
					var id string
					if err := json.Unmarshal(parent, &id); err != nil {
						t.Fatal(err)
					}
					details["compensates_id"], _ = json.Marshal(aliases[id])
				}
				event.Details, err = json.Marshal(details)
				if err != nil {
					t.Fatal(err)
				}
				fixture.Events = append(fixture.Events, event)
			}
			path := filepath.Join("testdata", "history", scenario+".json")
			if os.Getenv("DURABLE_UPDATE_HISTORY_FIXTURES") == "1" {
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				raw, err := json.MarshalIndent(fixture, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(raw, '\n'), 0644); err != nil {
					t.Fatal(err)
				}
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var recorded historyFixture
			if err := json.Unmarshal(raw, &recorded); err != nil {
				t.Fatal(err)
			}
			if len(fixture.Events) != len(recorded.Events) {
				t.Fatal("recorded event sequence changed")
			}
			// Additive detail fields preserve old history interpretation. Existing
			// fields, event ordering, and projected outcomes must still agree.
			for i := range fixture.Events {
				var currentFields, recordedFields map[string]json.RawMessage
				if err := json.Unmarshal(fixture.Events[i].Details, &currentFields); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(recorded.Events[i].Details, &recordedFields); err != nil {
					t.Fatal(err)
				}
				for key := range currentFields {
					if _, exists := recordedFields[key]; !exists {
						delete(currentFields, key)
					}
				}
				fixture.Events[i].Details, err = json.Marshal(currentFields)
				if err != nil {
					t.Fatal(err)
				}
			}
			actualJSON, err := json.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			recordedJSON, err := json.Marshal(recorded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actualJSON, recordedJSON) {
				t.Fatal("recorded history changed; inspect compatibility before explicitly updating fixtures")
			}
		})
	}
}
