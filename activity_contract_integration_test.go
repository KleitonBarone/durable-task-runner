//go:build integration

package durable

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
