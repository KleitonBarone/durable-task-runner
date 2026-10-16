package durable

import "context"

// ActivityMetadata identifies this delivery and its logical effect. EffectKey
// stays stable across replacement attempts; AttemptID and AttemptNumber change.
type ActivityMetadata struct {
	WorkflowID    string
	ActivityID    string
	ActivityName  string
	StepKey       string
	WorkerID      string
	AttemptID     string
	AttemptNumber int
	EffectKey     string
}

func Metadata(ctx context.Context) (ActivityMetadata, bool) {
	current, ok := ctx.Value(activityContextKey{}).(activityContext)
	if !ok {
		return ActivityMetadata{}, false
	}
	claim := current.claim
	return ActivityMetadata{WorkflowID: claim.WorkflowID, ActivityID: claim.ActivityID,
		ActivityName: claim.ActivityName, StepKey: claim.StepKey, WorkerID: claim.WorkerID,
		AttemptID: claim.AttemptID, AttemptNumber: claim.AttemptNumber,
		EffectKey: "durable:" + claim.ActivityID}, true
}
