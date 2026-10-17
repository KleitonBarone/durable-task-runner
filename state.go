package durable

import "fmt"

func ValidateWorkflowTransition(from, to WorkflowState) error {
	allowed := map[WorkflowState]map[WorkflowState]bool{
		WorkflowRunning:         {WorkflowCompleted: true, WorkflowFailed: true, WorkflowTimedOut: true, WorkflowCancelRequested: true},
		WorkflowCancelRequested: {WorkflowCancelled: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("invalid workflow transition %q -> %q", from, to)
	}
	return nil
}

func ValidateActivityTransition(from, to ActivityState) error {
	allowed := map[ActivityState]map[ActivityState]bool{
		ActivityBlocked: {ActivityReady: true, ActivityTimedOut: true, ActivityCancelled: true},
		ActivityReady:   {ActivityRunning: true, ActivityFailed: true, ActivityTimedOut: true, ActivityCancelled: true},
		ActivityRunning: {ActivityReady: true, ActivityWaiting: true, ActivityCompleted: true, ActivityFailed: true, ActivityTimedOut: true, ActivityCancelled: true},
		ActivityWaiting: {ActivityReady: true, ActivityTimedOut: true, ActivityCancelled: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("invalid activity transition %q -> %q", from, to)
	}
	return nil
}

func ValidateAttemptTransition(from, to AttemptState) error {
	if from != AttemptRunning || (to != AttemptCompleted && to != AttemptFailed && to != AttemptAbandoned && to != AttemptTimedOut && to != AttemptCancelled) {
		return fmt.Errorf("invalid attempt transition %q -> %q", from, to)
	}
	return nil
}

func terminalWorkflow(state WorkflowState) bool {
	return state == WorkflowCompleted || state == WorkflowFailed || state == WorkflowTimedOut || state == WorkflowCancelled
}

func ValidateLeaseTransition(from, to LeaseState) error {
	if from != LeaseActive || (to != LeaseReleased && to != LeaseExpired) {
		return fmt.Errorf("invalid lease transition %q -> %q", from, to)
	}
	return nil
}
