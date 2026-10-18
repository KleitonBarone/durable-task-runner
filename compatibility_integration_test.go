//go:build integration

package durable

import (
	"bytes"
	"context"
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

func TestStructContractRoutingAndLegacyDrain(t *testing.T) {
	for _, mode := range []string{"forward", "compensation", "legacy-forward", "legacy-compensation"} {
		t.Run(mode, func(t *testing.T) {
			legacy := mode == "legacy-forward" || mode == "legacy-compensation"
			cleanup := mode == "compensation" || mode == "legacy-compensation"
			engine, _ := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 1)
			activity := DefineActivity[embeddedContractInput, int]("effect")
			step, err := activity.Step("effect", embeddedContractInput{ContractPayload{42}})
			if err != nil {
				t.Fatal(err)
			}
			if legacy {
				// Freeze the contract emitted by v1.0.0, without rewriting a stored plan.
				step.contract = legacyStructContract
			}
			definition := Define("contract", func(string) ([]ActivitySpec, error) {
				if !cleanup {
					return []ActivitySpec{step}, nil
				}
				first, err := NewActivity("create", "created", WithCompensation(step))
				if err != nil {
					return nil, err
				}
				last, err := NewActivity("fail", "trigger")
				return []ActivitySpec{first, last}, err
			})
			if err := Register(engine.registry, definition); err != nil {
				t.Fatal(err)
			}
			execution, err := Start(t.Context(), engine, definition, "input")
			if err != nil {
				t.Fatal(err)
			}
			if cleanup {
				completeForwardSteps(t, engine, 1)
				failForward(t, engine)
			}
			wrong := DefineActivity[nestedContractInput, int]("effect")
			wrongWorker := newContractWorker(t, engine, "wrong", wrong.Handle(func(context.Context, nestedContractInput) (int, error) {
				t.Error("incompatible handler ran")
				return 0, nil
			}))
			if worked, err := wrongWorker.RunOnce(t.Context()); err != nil || worked {
				t.Fatalf("incompatible worker claimed work: %v %v", worked, err)
			}
			unsupported, err := wrongWorker.UnsupportedActivities(t.Context())
			if err != nil || len(unsupported) != 1 {
				t.Fatalf("unsupported contract diagnostic: %#v %v", unsupported, err)
			}
			registration := activity.Handle(func(_ context.Context, input embeddedContractInput) (int, error) {
				return input.Value, nil
			})
			if legacy {
				current := newContractWorker(t, engine, "current", registration)
				if worked, err := current.RunOnce(t.Context()); err != nil || worked {
					t.Fatalf("new worker claimed ambiguous v1 work: %v %v", worked, err)
				}
				// Simulate the original binary advertising its frozen v1 contract.
				registration.contract = legacyStructContract
			}
			activities, err := engine.Activities(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range activities {
				if item.Name == "effect" && (item.State != ActivityReady || item.AttemptCount != 0 || item.Contract != step.contract) {
					t.Fatalf("unsupported work was mutated: %#v", item)
				}
			}
			rightWorker := newContractWorker(t, engine, "original", registration)
			if worked, err := rightWorker.RunOnce(t.Context()); err != nil || !worked {
				t.Fatalf("matching worker: %v %v", worked, err)
			}
			activities, err = engine.Activities(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range activities {
				if item.Name == "effect" && (item.State != ActivityCompleted || item.AttemptCount != 1 || string(item.Output) != "42" || item.Contract != step.contract) {
					t.Fatalf("matching completion changed the contract or output: %#v", item)
				}
			}
			result, err := engine.Workflow(t.Context(), execution.ID)
			want := WorkflowCompleted
			if cleanup {
				want = WorkflowFailed
			}
			if err != nil || result.State != want {
				t.Fatalf("workflow after drain: %#v %v", result, err)
			}
			if !cleanup {
				value, err := activity.WorkflowResult(t.Context(), engine, execution.ID)
				if legacy {
					if !errors.Is(err, ErrTypeMismatch) || string(result.Output) != "42" {
						t.Fatalf("ambiguous typed result: %d %v; raw %s", value, err, result.Output)
					}
				} else if err != nil || value != 42 {
					t.Fatalf("matching typed result: %d %v", value, err)
				}
			}
		})
	}
}

func newContractWorker(t *testing.T, engine *Engine, id string, registration HandlerRegistration) *Worker {
	t.Helper()
	// Use a separate registry to exercise database routing across deployments.
	restarted, err := New(engine.pool, NewRegistry(), engine.clock)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(restarted, WorkerConfig{ID: id}, registration)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestVersionsCoexistAndRecoverStoredPlans(t *testing.T) {
	clock := NewManualClock(time.Unix(1, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	old := DefineActivity[string, string]("transform").WithVersion("1", "text/1", "text/1")
	newer := DefineActivity[int, int]("transform").WithVersion("2", "number/1", "number/1")
	v1 := Define("versioned", func(in string) ([]ActivitySpec, error) {
		step, err := old.Step("transform", in)
		return []ActivitySpec{step}, err
	}).WithVersion("1", "text/1")
	v2 := Define("versioned", func(in int) ([]ActivitySpec, error) {
		step, err := newer.Step("transform", in)
		return []ActivitySpec{step}, err
	}).WithVersion("2", "number/1")
	if err := Register(engine.registry, v1); err != nil {
		t.Fatal(err)
	}
	if err := Register(engine.registry, v2); err != nil {
		t.Fatal(err)
	}
	first, err := Start(t.Context(), engine, v1, "old", WithStartKey("same"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Start(t.Context(), engine, v2, 2, WithStartKey("same"))
	if err != nil || first.ID == second.ID {
		t.Fatalf("version-scoped starts: %#v %v", second, err)
	}
	duplicate, err := Start(t.Context(), engine, v1, "old", WithStartKey("same"))
	if err != nil || duplicate.ID != first.ID || duplicate.DefinitionVersion != "1" || duplicate.InputSchema != "text/1" {
		t.Fatalf("duplicate: %#v %v", duplicate, err)
	}
	crashed, err := engine.claimActivity(t.Context(), "old-crashed", time.Second, map[string]capability{activityKey("transform", "1"): {"transform", "1", old.contract(), "text/1", "text/1"}})
	if err != nil || crashed.WorkflowID != first.ID {
		t.Fatalf("old claim: %#v %v", crashed, err)
	}
	// A deployment registers changed definition code. Existing work must never call it.
	restarted, err := New(engine.pool, NewRegistry(), clock)
	if err != nil {
		t.Fatal(err)
	}
	changed := v1
	changed.Plan = func(string) ([]ActivitySpec, error) {
		t.Error("reran old definition")
		return nil, errors.New("removed plan")
	}
	if err := Register(restarted.registry, changed); err != nil {
		t.Fatal(err)
	}
	newWorker, err := NewWorker(restarted, WorkerConfig{ID: "new"}, newer.Handle(func(ctx context.Context, in int) (int, error) {
		metadata, _ := Metadata(ctx)
		if metadata.ActivityVersion != "2" || metadata.InputSchema != "number/1" {
			return 0, errors.New("wrong version metadata")
		}
		return in * 2, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := newWorker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("new worker: %v %v", worked, err)
	}
	clock.Advance(time.Second)
	if _, err := restarted.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	if worked, err := newWorker.RunOnce(t.Context()); err != nil || worked {
		t.Fatalf("new worker claimed old work: %v %v", worked, err)
	}
	unsupported, err := newWorker.UnsupportedActivities(t.Context())
	if err != nil || len(unsupported) != 1 || unsupported[0].Version != "1" || unsupported[0].InputSchema != "text/1" {
		t.Fatalf("diagnostic: %#v %v", unsupported, err)
	}
	oldWorker, err := NewWorker(restarted, WorkerConfig{ID: "both"}, old.Handle(func(_ context.Context, in string) (string, error) { return in + " done", nil }), newer.Handle(func(_ context.Context, in int) (int, error) { return in, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := oldWorker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("old handler: %v %v", worked, err)
	}
	if out, err := old.WorkflowResult(t.Context(), restarted, first.ID); err != nil || out != "old done" {
		t.Fatalf("old result: %q %v", out, err)
	}
	if out, err := newer.WorkflowResult(t.Context(), restarted, second.ID); err != nil || out != 4 {
		t.Fatalf("new result: %d %v", out, err)
	}
	if _, err := old.WithVersion("2", "text/1", "text/1").WorkflowResult(t.Context(), restarted, first.ID); !errors.Is(err, ErrTypeMismatch) {
		t.Fatal(err)
	}
	if err := restarted.CompleteActivity(t.Context(), crashed, []byte(`"stale"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	activities, _ := restarted.Activities(t.Context(), first.ID)
	if activities[0].AttemptCount != 2 || activities[0].Version != "1" || activities[0].OutputSchema != "text/1" {
		t.Fatal(activities)
	}
}

func TestSchemaIdentityFencesHandlersAndCleanup(t *testing.T) {
	engine, _ := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 1)
	create := DefineActivity[string, string]("schema-create").WithVersion("v2", "input/2", "output/2")
	undo := DefineActivity[string, string]("schema-undo").WithVersion("v3", "cleanup/3", "cleanup/3")
	definition := Define("schemas", func(in string) ([]ActivitySpec, error) {
		cleanup, err := undo.Step("undo", in)
		if err != nil {
			return nil, err
		}
		first, err := create.Step("first", in, WithCompensation(cleanup))
		if err != nil {
			return nil, err
		}
		last, err := create.Step("last", in)
		return []ActivitySpec{first, last}, err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(t.Context(), engine, definition, "input")
	if err != nil {
		t.Fatal(err)
	}
	other, _ := New(engine.pool, NewRegistry(), engine.clock)
	wrong, err := NewWorker(other, WorkerConfig{ID: "wrong-schema"}, create.WithVersion("v2", "input/1", "output/2").Handle(func(_ context.Context, in string) (string, error) { t.Error("incompatible schema ran"); return in, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := wrong.RunOnce(t.Context()); err != nil || worked {
		t.Fatalf("schema routing: %v %v", worked, err)
	}
	planned, _ := engine.Activities(t.Context(), execution.ID)
	if planned[0].AttemptCount != 0 {
		t.Fatal("mismatch consumed attempt")
	}
	completeForwardSteps(t, engine, 1)
	failForward(t, engine)
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || len(activities) != 3 || activities[2].Version != "v3" || activities[2].InputSchema != "cleanup/3" {
		t.Fatalf("cleanup identity: %#v %v", activities, err)
	}
	worker, err := NewWorker(other, WorkerConfig{ID: "cleanup"}, undo.Handle(func(ctx context.Context, in string) (string, error) {
		if _, err := create.WithVersion("v2", "input/1", "output/2").Result(ctx, "first"); !errors.Is(err, ErrTypeMismatch) {
			return "", errors.New("schema mismatch accepted")
		}
		return create.Result(ctx, "first")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("cleanup: %v %v", worked, err)
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.CompensationState != CompensationCompleted {
		t.Fatalf("outcome: %#v %v", state, err)
	}
}
