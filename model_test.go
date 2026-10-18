package durable

import (
	"context"
	"strings"
	"testing"
)

func TestStateTransitions(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
		check func() error
	}{
		{name: "workflow completes", valid: true, check: func() error { return ValidateWorkflowTransition(WorkflowRunning, WorkflowCompleted) }},
		{name: "completed workflow cannot restart", check: func() error { return ValidateWorkflowTransition(WorkflowCompleted, WorkflowRunning) }},
		{name: "workflow requests cancellation", valid: true, check: func() error { return ValidateWorkflowTransition(WorkflowRunning, WorkflowCancelRequested) }},
		{name: "cancellation settles", valid: true, check: func() error { return ValidateWorkflowTransition(WorkflowCancelRequested, WorkflowCancelled) }},
		{name: "cancellation cannot complete", check: func() error { return ValidateWorkflowTransition(WorkflowCancelRequested, WorkflowCompleted) }},
		{name: "cancelled workflow cannot restart", check: func() error { return ValidateWorkflowTransition(WorkflowCancelled, WorkflowRunning) }},
		{name: "running workflow begins cleanup", valid: true, check: func() error { return ValidateWorkflowTransition(WorkflowRunning, WorkflowCompensating) }},
		{name: "cleanup preserves failed outcome", valid: true, check: func() error { return ValidateWorkflowTransition(WorkflowCompensating, WorkflowFailed) }},
		{name: "cleanup cannot produce forward success", check: func() error { return ValidateWorkflowTransition(WorkflowCompensating, WorkflowCompleted) }},
		{name: "failed cleanup cannot restart", check: func() error { return ValidateWorkflowTransition(WorkflowCompensationFailed, WorkflowCompensating) }},
		{name: "blocked activity becomes ready", valid: true, check: func() error { return ValidateActivityTransition(ActivityBlocked, ActivityReady) }},
		{name: "ready activity cannot complete without a claim", check: func() error { return ValidateActivityTransition(ActivityReady, ActivityCompleted) }},
		{name: "abandoned activity becomes ready", valid: true, check: func() error { return ValidateActivityTransition(ActivityRunning, ActivityReady) }},
		{name: "completed activity cannot run again", check: func() error { return ValidateActivityTransition(ActivityCompleted, ActivityRunning) }},
		{name: "running attempt is abandoned", valid: true, check: func() error { return ValidateAttemptTransition(AttemptRunning, AttemptAbandoned) }},
		{name: "completed attempt cannot be abandoned", check: func() error { return ValidateAttemptTransition(AttemptCompleted, AttemptAbandoned) }},
		{name: "active lease expires", valid: true, check: func() error { return ValidateLeaseTransition(LeaseActive, LeaseExpired) }},
		{name: "expired lease cannot be released", check: func() error { return ValidateLeaseTransition(LeaseExpired, LeaseReleased) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.check()
			if test.valid && err != nil {
				t.Fatalf("expected valid transition: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("expected invalid transition to fail")
			}
		})
	}
}

func TestVersionIdentityValidation(t *testing.T) {
	for _, bad := range []string{"", " ", "nul\x00", "bad\xff", strings.Repeat("a", 201)} {
		t.Run(strings.ReplaceAll(bad, "\x00", "nul"), func(t *testing.T) {
			a := DefineActivity[string, string]("test").WithVersion(bad, "in", "out")
			if _, err := a.Step("key", "input"); err == nil {
				t.Fatal("invalid version accepted")
			}
			if err := Register(NewRegistry(), Define("test", func(string) ([]ActivitySpec, error) { return nil, nil }).WithVersion("1", bad)); err == nil {
				t.Fatal("invalid schema accepted")
			}
		})
	}
	a := DefineActivity[string, string]("test")
	step, err := a.Step("key", "input")
	registration := a.Handle(func(_ context.Context, in string) (string, error) { return in, nil })
	if err != nil || step.Version != "1" || step.InputSchema != "legacy" || registration.OutputSchema != "legacy" {
		t.Fatalf("legacy defaults: %#v %#v %v", step, registration, err)
	}
}
