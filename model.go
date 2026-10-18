package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
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

var (
	ErrNoActivity          = errors.New("no activity is ready")
	ErrLeaseLost           = errors.New("activity lease is no longer valid")
	ErrUnknownWorkflow     = errors.New("workflow definition is not registered")
	ErrNotFound            = errors.New("execution or step not found")
	ErrInvalidInput        = errors.New("invalid input")
	ErrPayloadTooLarge     = errors.New("JSON payload exceeds limit")
	ErrResultUnavailable   = errors.New("step result is not available")
	ErrTypeMismatch        = errors.New("activity type does not match persisted contract")
	ErrStartConflict       = errors.New("start key already used with different input")
	ErrWorkerBusy          = errors.New("worker instance is already running")
	ErrHandlerUnresponsive = errors.New("activity handler did not stop within shutdown grace period")
	ErrExecutionDeadline   = errors.New("execution deadline exceeded")
	ErrIncompatibleSchema  = errors.New("database schema is incompatible with this binary")
	ErrExecutionExpired    = errors.New("execution was removed by retention")
)

// ExecutionExpiredError identifies a keyed execution whose details were purged.
// The retained start key still prevents another execution from using that key.
type ExecutionExpiredError struct{ ID string }

func (e *ExecutionExpiredError) Error() string {
	return fmt.Sprintf("%s: %s", ErrExecutionExpired, e.ID)
}
func (e *ExecutionExpiredError) Unwrap() error { return ErrExecutionExpired }

// HandlerPanicError records an abnormal handler exit as a task failure. Panics
// in goroutines created by application handlers remain the application's concern.
type HandlerPanicError struct{ Value any }

func (e *HandlerPanicError) Error() string {
	return fmt.Sprintf("activity handler panicked: %v", e.Value)
}

func (e *HandlerPanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// TaskError means a handler failure was successfully recorded. RunOnce exposes
// it to callers; Run keeps processing other executions. Unwrap preserves causes.
type TaskError struct {
	WorkflowID string
	ActivityID string
	AttemptID  string
	Cause      error
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("activity %s failed: %v", e.ActivityID, e.Cause)
}
func (e *TaskError) Unwrap() error { return e.Cause }

const defaultVersion = "1"
const legacySchema = "legacy"

func validIdentity(value string, limit int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func activityKey(name, version string) string { return name + "\x00" + version }

type capability struct {
	name, version, contract, inputSchema, outputSchema string
}

func (c capability) signature() string {
	return c.contract + "\x00" + c.inputSchema + "\x00" + c.outputSchema
}

func (c capability) validate() error {
	if !validIdentity(c.name, 200) || !validIdentity(c.version, 100) || !validIdentity(c.inputSchema, 200) || !validIdentity(c.outputSchema, 200) {
		return fmt.Errorf("%w: activity name, version, and payload schema identities are required", ErrInvalidInput)
	}
	return nil
}
