package durable

import "testing"

func TestStateTransitions(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
		check func() error
	}{
		{name: "workflow completes", valid: true, check: func() error { return ValidateWorkflowTransition(WorkflowRunning, WorkflowCompleted) }},
		{name: "completed workflow cannot restart", check: func() error { return ValidateWorkflowTransition(WorkflowCompleted, WorkflowRunning) }},
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
