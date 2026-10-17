package durable

import "fmt"

// ValidateWorkflowTransition rejects illegal transitions, including reopening terminal work.
func ValidateWorkflowTransition(from, to WorkflowState) error {
	allowed := map[WorkflowState]map[WorkflowState]bool{
		WorkflowRunning:         {WorkflowCompleted: true, WorkflowFailed: true, WorkflowTimedOut: true, WorkflowCancelRequested: true, WorkflowCompensating: true},
		WorkflowCancelRequested: {WorkflowCancelled: true, WorkflowCompensating: true},
		WorkflowCompensating:    {WorkflowFailed: true, WorkflowTimedOut: true, WorkflowCancelled: true, WorkflowCompensationFailed: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("invalid workflow transition %q -> %q", from, to)
	}
	return nil
}

// ValidateActivityTransition checks scheduling, retry, and terminal step transitions.
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

// ValidateAttemptTransition allows one running attempt to reach one terminal outcome.
func ValidateAttemptTransition(from, to AttemptState) error {
	if from != AttemptRunning || (to != AttemptCompleted && to != AttemptFailed && to != AttemptAbandoned && to != AttemptTimedOut && to != AttemptCancelled) {
		return fmt.Errorf("invalid attempt transition %q -> %q", from, to)
	}
	return nil
}

func terminalWorkflow(state WorkflowState) bool {
	return state == WorkflowCompleted || state == WorkflowFailed || state == WorkflowTimedOut || state == WorkflowCancelled || state == WorkflowCompensationFailed
}

func workingWorkflow(state WorkflowState) bool {
	return state == WorkflowRunning || state == WorkflowCompensating
}

// ValidateLeaseTransition permits active ownership to be released or expired once.
func ValidateLeaseTransition(from, to LeaseState) error {
	if from != LeaseActive || (to != LeaseReleased && to != LeaseExpired) {
		return fmt.Errorf("invalid lease transition %q -> %q", from, to)
	}
	return nil
}
