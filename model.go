package durable

import (
	"encoding/json"
	"time"
)

type WorkflowState string

const (
	WorkflowRunning   WorkflowState = "running"
	WorkflowCompleted WorkflowState = "completed"
	WorkflowFailed    WorkflowState = "failed"
)

type ActivityState string

const (
	ActivityBlocked   ActivityState = "blocked"
	ActivityReady     ActivityState = "ready"
	ActivityRunning   ActivityState = "running"
	ActivityCompleted ActivityState = "completed"
	ActivityFailed    ActivityState = "failed"
)

type AttemptState string

const (
	AttemptRunning   AttemptState = "running"
	AttemptCompleted AttemptState = "completed"
	AttemptFailed    AttemptState = "failed"
	AttemptAbandoned AttemptState = "abandoned"
)

type LeaseState string

const (
	LeaseActive   LeaseState = "active"
	LeaseReleased LeaseState = "released"
	LeaseExpired  LeaseState = "expired"
)

type WorkflowExecution struct {
	ID             string
	DefinitionName string
	Input          json.RawMessage
	Output         json.RawMessage
	State          WorkflowState
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}

type Activity struct {
	ID           string
	WorkflowID   string
	Position     int
	StepKey      string
	Name         string
	Input        json.RawMessage
	Output       json.RawMessage
	State        ActivityState
	AttemptCount int
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  *time.Time
}

type Attempt struct {
	ID         string
	ActivityID string
	Number     int
	WorkerID   string
	State      AttemptState
	Error      string
	StartedAt  time.Time
	EndedAt    *time.Time
}

type Lease struct {
	ID          string
	ActivityID  string
	AttemptID   string
	Token       string
	WorkerID    string
	State       LeaseState
	AcquiredAt  time.Time
	HeartbeatAt time.Time
	ExpiresAt   time.Time
	EndedAt     *time.Time
}

type Claim struct {
	WorkflowID     string
	ActivityID     string
	ActivityName   string
	StepKey        string
	Position       int
	ActivityInput  json.RawMessage
	AttemptID      string
	AttemptNumber  int
	LeaseID        string
	LeaseToken     string
	WorkerID       string
	LeaseExpiresAt time.Time
}

type TimelineEvent struct {
	Sequence   int64
	WorkflowID string
	ActivityID *string
	AttemptID  *string
	Type       string
	Details    json.RawMessage
	RecordedAt time.Time
}
