package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

// ActivitySpec is a serialized step declaration persisted as part of a plan.
type ActivitySpec struct {
	Name           string
	Version        string
	InputSchema    string
	OutputSchema   string
	Key            string
	Input          json.RawMessage
	contract       string
	policy         RetryPolicy
	attemptTimeout time.Duration
	compensation   *CompensationPlan
}

// NewActivity constructs an untyped-output step. Prefer DefineActivity for typed
// handlers and results. Omitted keys receive stable positional keys at start.
func NewActivity[Input any](name string, input Input, options ...StepOption) (ActivitySpec, error) {
	if strings.TrimSpace(name) == "" {
		return ActivitySpec{}, fmt.Errorf("%w: activity name is required", ErrInvalidInput)
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return ActivitySpec{}, fmt.Errorf("%w: encode activity %q input: %w", ErrInvalidInput, name, err)
	}
	if err := validatePayload(encoded); err != nil {
		return ActivitySpec{}, err
	}
	spec := ActivitySpec{Name: name, Input: encoded}
	if err := normalizeActivity(&spec, options); err != nil {
		return ActivitySpec{}, err
	}
	return spec, nil
}

// Definition turns typed workflow input into a deterministic linear activity
// plan. A name and version must retain the same meaning while executions using
// them are in progress. Existing executions continue their stored plan.
type Definition[Input any] struct {
	Name            string
	Version         string
	InputSchema     string
	explicitVersion bool
	Plan            func(Input) ([]ActivitySpec, error)
}

// Define declares a workflow plan. The plan must be deterministic and free of
// side effects; duplicate keyed starts can evaluate it again before deduplication.
func Define[Input any](name string, plan func(Input) ([]ActivitySpec, error)) Definition[Input] {
	return Definition[Input]{Name: name, Version: defaultVersion, InputSchema: legacySchema, Plan: plan}
}

// WithVersion declares an immutable workflow version and input schema identity.
// Register old and new versions together while old executions drain.
func (d Definition[Input]) WithVersion(version, inputSchema string) Definition[Input] {
	d.Version, d.InputSchema, d.explicitVersion = version, inputSchema, true
	return d
}

type registeredDefinition struct {
	version     string
	inputSchema string
	inputType   reflect.Type
	encodePlan  func(any) (json.RawMessage, []ActivitySpec, error)
}

// Registry stores process-local definitions and compatible activity contracts.
type Registry struct {
	mu          sync.RWMutex
	definitions map[string]registeredDefinition
	activities  map[string]string
}

// NewRegistry creates an empty concurrency-safe registry.
func NewRegistry() *Registry {
	return &Registry{definitions: make(map[string]registeredDefinition), activities: make(map[string]string)}
}

// Register adds an immutable workflow name/version and rejects duplicate entries.
func Register[Input any](registry *Registry, definition Definition[Input]) error {
	if registry == nil {
		return fmt.Errorf("registry is required")
	}
	if !validIdentity(definition.Name, 200) {
		return fmt.Errorf("workflow name is required")
	}
	if definition.Plan == nil {
		return fmt.Errorf("workflow %q plan is required", definition.Name)
	}
	if !definition.explicitVersion {
		if definition.Version == "" {
			definition.Version = defaultVersion
		}
		if definition.InputSchema == "" {
			definition.InputSchema = legacySchema
		}
	}
	if !validIdentity(definition.Version, 100) || !validIdentity(definition.InputSchema, 200) {
		return fmt.Errorf("%w: workflow version and input schema identity are required", ErrInvalidInput)
	}

	inputType := reflect.TypeFor[Input]()
	entry := registeredDefinition{
		version: definition.Version, inputSchema: definition.InputSchema,
		inputType: inputType,
		encodePlan: func(value any) (json.RawMessage, []ActivitySpec, error) {
			typed, ok := value.(Input)
			// A nil interface has no dynamic type to assert. Its zero value is
			// still a valid input when the definition declares an interface.
			if !ok && (value != nil || inputType.Kind() != reflect.Interface) {
				return nil, nil, fmt.Errorf("%w: workflow %q expects %v, got %T", ErrInvalidInput, definition.Name, inputType, value)
			}
			encoded, err := json.Marshal(typed)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: encode workflow %q input: %w", ErrInvalidInput, definition.Name, err)
			}
			if err := validatePayload(encoded); err != nil {
				return nil, nil, err
			}
			activities, err := definition.Plan(typed)
			if err != nil {
				return nil, nil, fmt.Errorf("plan workflow %q: %w", definition.Name, err)
			}
			if len(activities) == 0 {
				return nil, nil, fmt.Errorf("%w: workflow %q must schedule at least one activity", ErrInvalidInput, definition.Name)
			}
			activities = slices.Clone(activities)
			keys := make(map[string]bool, len(activities))
			for i := range activities {
				activity := &activities[i]
				if err := normalizeActivity(activity, nil); err != nil {
					return nil, nil, err
				}
				if activity.Key == "" {
					activity.Key = fmt.Sprintf("step-%d", i)
				}
				if keys[activity.Key] || strings.TrimSpace(activity.Key) == "" || len(activity.Key) > 200 {
					return nil, nil, fmt.Errorf("%w: workflow %q has invalid or duplicate step key %q", ErrInvalidInput, definition.Name, activity.Key)
				}
				keys[activity.Key] = true
				if strings.TrimSpace(activity.Name) == "" {
					return nil, nil, fmt.Errorf("%w: workflow %q activity %d has no name", ErrInvalidInput, definition.Name, i)
				}
				if err := validatePayload(activity.Input); err != nil {
					return nil, nil, fmt.Errorf("workflow %q step %q: %w", definition.Name, activity.Key, err)
				}
			}
			return encoded, activities, nil
		},
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := activityKey(definition.Name, definition.Version)
	if _, exists := registry.definitions[key]; exists {
		return fmt.Errorf("workflow %q is already registered", definition.Name)
	}
	registry.definitions[key] = entry
	return nil
}

func (r *Registry) lookup(name string) (registeredDefinition, bool) {
	return r.lookupVersion(name, defaultVersion)
}

func (r *Registry) lookupVersion(name, version string) (registeredDefinition, bool) {
	if r == nil {
		return registeredDefinition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	definition, ok := r.definitions[activityKey(name, version)]
	return definition, ok
}

// bindActivities rejects conflicting type declarations within this process.
// Persisted contracts also let separate worker processes select compatible work.
func (r *Registry) bindActivities(contracts map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, contract := range contracts {
		if previous, ok := r.activities[name]; ok && previous != contract {
			return fmt.Errorf("activity %q has incompatible type registrations", name)
		}
	}
	for name, contract := range contracts {
		r.activities[name] = contract
	}
	return nil
}

// Start persists typed input, the full plan, and initial history atomically.
// The definition must be registered. Use WithStartKey to resolve ambiguous commits.
func Start[Input any](ctx context.Context, engine *Engine, definition Definition[Input], input Input, options ...StartOption) (WorkflowExecution, error) {
	version := definition.Version
	if version == "" && !definition.explicitVersion {
		version = defaultVersion
	}
	return engine.start(ctx, definition.Name, version, input, options...)
}

// Handler is the low-level JSON handler form. Handlers must tolerate redelivery.
type Handler func(context.Context, json.RawMessage) (json.RawMessage, error)

// HandlerRegistration binds a handler to its durable compatibility identities.
type HandlerRegistration struct {
	Name            string
	Version         string
	InputSchema     string
	OutputSchema    string
	Handler         Handler
	contract        string
	explicitVersion bool
}

// Handle adapts a Go handler without a typed declaration contract. Prefer an
// ActivityDefinition's Handle method for capability filtering and typed results.
func Handle[Input, Output any](name string, handler func(context.Context, Input) (Output, error)) HandlerRegistration {
	if handler == nil {
		return HandlerRegistration{Name: name}
	}
	return HandlerRegistration{
		Name:    name,
		Version: defaultVersion, InputSchema: legacySchema, OutputSchema: legacySchema,
		Handler: func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			// Persisted input has already passed validation. jsonb can add
			// whitespace, so its returned byte size is not the original limit.
			var input Input
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, fmt.Errorf("%w: decode activity %q input: %w", ErrInvalidInput, name, err)
			}
			output, err := handler(ctx, input)
			if err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(output)
			if err != nil {
				return nil, fmt.Errorf("%w: encode activity %q output: %w", ErrInvalidInput, name, err)
			}
			if err := validatePayload(encoded); err != nil {
				return nil, err
			}
			return encoded, nil
		},
	}
}
