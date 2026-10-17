package durable

import (
	"encoding/json"
	"time"
)

// WorkflowState describes forward execution, cancellation, cleanup, or a terminal outcome.
type WorkflowState string

const (
	WorkflowRunning            WorkflowState = "running"
	WorkflowCompleted          WorkflowState = "completed"
	WorkflowFailed             WorkflowState = "failed"
	WorkflowTimedOut           WorkflowState = "timed_out"
	WorkflowCancelRequested    WorkflowState = "cancel_requested"
	WorkflowCancelled          WorkflowState = "cancelled"
	WorkflowCompensating       WorkflowState = "compensating"
	WorkflowCompensationFailed WorkflowState = "compensation_failed"
)

// ActivityState describes one persisted step's scheduling and execution state.
type ActivityState string

// ActivityKind distinguishes a forward step from its explicitly declared cleanup.
type ActivityKind string

const (
	ActivityForward      ActivityKind = "forward"
	ActivityCompensation ActivityKind = "compensation"
)

// CompensationState is independent of the original workflow failure or cancellation.
type CompensationState string

const (
	CompensationNone      CompensationState = "none"
	CompensationRunning   CompensationState = "running"
	CompensationCompleted CompensationState = "completed"
	CompensationFailed    CompensationState = "failed"
)

// CompensationPlan is the immutable cleanup declaration stored on a forward
// activity. Cleanup handlers can also read committed forward step results.
type CompensationPlan struct {
	Name           string          `json:"name"`
	Version        string          `json:"version"`
	InputSchema    string          `json:"input_schema"`
	OutputSchema   string          `json:"output_schema"`
	Contract       string          `json:"contract"`
	Input          json.RawMessage `json:"input"`
	RetryPolicy    RetryPolicy     `json:"retry_policy"`
	AttemptTimeout time.Duration   `json:"attempt_timeout_ns"`
}

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

// AttemptState describes one delivery; every claim consumes the activity's budget.
type AttemptState string

const (
	AttemptRunning   AttemptState = "running"
	AttemptCompleted AttemptState = "completed"
	AttemptFailed    AttemptState = "failed"
	AttemptAbandoned AttemptState = "abandoned"
	AttemptTimedOut  AttemptState = "timed_out"
	AttemptCancelled AttemptState = "cancelled"
)

// LeaseState records ownership release or expiry. Only active, unexpired leases acknowledge work.
type LeaseState string

const (
	LeaseActive   LeaseState = "active"
	LeaseReleased LeaseState = "released"
	LeaseExpired  LeaseState = "expired"
)

// WorkflowExecution is a persisted execution snapshot, including application data.
// Use ListWorkflows for redacted operator summaries.
type WorkflowExecution struct {
	DefinitionVersion      string
	InputSchema            string
	ID                     string
	DefinitionName         string
	StartKey               *string
	Input                  json.RawMessage
	Output                 json.RawMessage
	State                  WorkflowState
	CreatedAt              time.Time
	UpdatedAt              time.Time
	CompletedAt            *time.Time
	TerminalReason         string
	LastError              string
	DeadlineAt             time.Time
	Timeout                time.Duration
	PreviousExecutionID    *string
	CancelActor            string
	CancelReason           string
	CancelRequestedAt      *time.Time
	CancelSettledAt        *time.Time
	OriginalState          WorkflowState
	CompensationState      CompensationState
	CompensationError      string
	CompensationTimeout    time.Duration
	CompensationDeadlineAt *time.Time
}

// Activity is a persisted forward or cleanup step. ListActivities omits payloads.
type Activity struct {
	Contract              string
	Version               string
	InputSchema           string
	OutputSchema          string
	Kind                  ActivityKind
	CompensatesActivityID *string
	Compensation          *CompensationPlan
	ID                    string
	WorkflowID            string
	Position              int
	StepKey               string
	Name                  string
	Input                 json.RawMessage
	Output                json.RawMessage
	State                 ActivityState
	AttemptCount          int
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CompletedAt           *time.Time
	RetryPolicy           RetryPolicy
	AvailableAt           time.Time
	AttemptTimeout        time.Duration
	LastError             string
}

// Attempt records a delivery's outcome, deadline, and retry decision.
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

// Lease records ownership timing. ListLeases never returns its Token.
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

// Claim carries low-level delivery ownership. Keep LeaseToken private; it is a
// fencing capability, not a stable identity for deduplicating external effects.
type Claim struct {
	ActivityVersion       string
	InputSchema           string
	OutputSchema          string
	Kind                  ActivityKind
	CompensatesActivityID *string
	WorkflowID            string
	ActivityID            string
	ActivityName          string
	StepKey               string
	Position              int
	ActivityInput         json.RawMessage
	AttemptID             string
	AttemptNumber         int
	LeaseID               string
	LeaseToken            string
	WorkerID              string
	LeaseExpiresAt        time.Time
	ClaimedAt             time.Time
	AttemptDeadline       time.Time
}

// TimelineEvent records a committed transition in increasing sequence order.
type TimelineEvent struct {
	Sequence   int64
	WorkflowID string
	ActivityID *string
	AttemptID  *string
	Type       string
	Details    json.RawMessage
	RecordedAt time.Time
}
