package durable

import (
	"encoding/json"
	"time"
)

type WorkflowState string

const (
	WorkflowRunning         WorkflowState = "running"
	WorkflowCompleted       WorkflowState = "completed"
	WorkflowFailed          WorkflowState = "failed"
	WorkflowTimedOut        WorkflowState = "timed_out"
	WorkflowCancelRequested WorkflowState = "cancel_requested"
	WorkflowCancelled       WorkflowState = "cancelled"
)

type ActivityState string

const (
	ActivityBlocked   ActivityState = "blocked"
	ActivityReady     ActivityState = "ready"
	ActivityRunning   ActivityState = "running"
	ActivityWaiting   ActivityState = "retry_wait"
	ActivityCompleted ActivityState = "completed"
	ActivityFailed    ActivityState = "failed"
	ActivityTimedOut  ActivityState = "timed_out"
	ActivityCancelled ActivityState = "cancelled"
)

type AttemptState string

const (
	AttemptRunning   AttemptState = "running"
	AttemptCompleted AttemptState = "completed"
	AttemptFailed    AttemptState = "failed"
	AttemptAbandoned AttemptState = "abandoned"
	AttemptTimedOut  AttemptState = "timed_out"
	AttemptCancelled AttemptState = "cancelled"
)

type LeaseState string

const (
	LeaseActive   LeaseState = "active"
	LeaseReleased LeaseState = "released"
	LeaseExpired  LeaseState = "expired"
)

type WorkflowExecution struct {
	ID                  string
	DefinitionName      string
	StartKey            *string
	Input               json.RawMessage
	Output              json.RawMessage
	State               WorkflowState
	CreatedAt           time.Time
	UpdatedAt           time.Time
	CompletedAt         *time.Time
	TerminalReason      string
	LastError           string
	DeadlineAt          time.Time
	Timeout             time.Duration
	PreviousExecutionID *string
	CancelActor         string
	CancelReason        string
	CancelRequestedAt   *time.Time
	CancelSettledAt     *time.Time
}

type Activity struct {
	ID             string
	WorkflowID     string
	Position       int
	StepKey        string
	Name           string
	Input          json.RawMessage
	Output         json.RawMessage
	State          ActivityState
	AttemptCount   int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
	RetryPolicy    RetryPolicy
	AvailableAt    time.Time
	AttemptTimeout time.Duration
	LastError      string
}

type Attempt struct {
	ID            string
	ActivityID    string
	Number        int
	WorkerID      string
	State         AttemptState
	Error         string
	StartedAt     time.Time
	EndedAt       *time.Time
	RetryDecision string
	RetryDueAt    *time.Time
	DeadlineAt    time.Time
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
	WorkflowID      string
	ActivityID      string
	ActivityName    string
	StepKey         string
	Position        int
	ActivityInput   json.RawMessage
	AttemptID       string
	AttemptNumber   int
	LeaseID         string
	LeaseToken      string
	WorkerID        string
	LeaseExpiresAt  time.Time
	ClaimedAt       time.Time
	AttemptDeadline time.Time
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
