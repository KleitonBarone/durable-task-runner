package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type ActivitySpec struct {
	Name     string
	Key      string
	Input    json.RawMessage
	contract string
}

func NewActivity[Input any](name string, input Input) (ActivitySpec, error) {
	if strings.TrimSpace(name) == "" {
		return ActivitySpec{}, fmt.Errorf("activity name is required")
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return ActivitySpec{}, fmt.Errorf("encode activity %q input: %w", name, err)
	}
	return ActivitySpec{Name: name, Input: encoded}, nil
}

// Definition turns typed workflow input into a deterministic linear activity
// plan. Definition code is not persisted, so a registered name must retain the
// same meaning while executions using it are in progress.
type Definition[Input any] struct {
	Name string
	Plan func(Input) ([]ActivitySpec, error)
}

func Define[Input any](name string, plan func(Input) ([]ActivitySpec, error)) Definition[Input] {
	return Definition[Input]{Name: name, Plan: plan}
}

type registeredDefinition struct {
	inputType  reflect.Type
	encodePlan func(any) (json.RawMessage, []ActivitySpec, error)
}

type Registry struct {
	mu          sync.RWMutex
	definitions map[string]registeredDefinition
	activities  map[string]string
}

func NewRegistry() *Registry {
	return &Registry{definitions: make(map[string]registeredDefinition), activities: make(map[string]string)}
}

func Register[Input any](registry *Registry, definition Definition[Input]) error {
	if registry == nil {
		return fmt.Errorf("registry is required")
	}
	if strings.TrimSpace(definition.Name) == "" {
		return fmt.Errorf("workflow name is required")
	}
	if definition.Plan == nil {
		return fmt.Errorf("workflow %q plan is required", definition.Name)
	}

	inputType := reflect.TypeFor[Input]()
	entry := registeredDefinition{
		inputType: inputType,
		encodePlan: func(value any) (json.RawMessage, []ActivitySpec, error) {
			typed, ok := value.(Input)
			if !ok {
				return nil, nil, fmt.Errorf("workflow %q expects %v, got %T", definition.Name, inputType, value)
			}
			encoded, err := json.Marshal(typed)
			if err != nil {
				return nil, nil, fmt.Errorf("encode workflow %q input: %w", definition.Name, err)
			}
			activities, err := definition.Plan(typed)
			if err != nil {
				return nil, nil, fmt.Errorf("plan workflow %q: %w", definition.Name, err)
			}
			if len(activities) == 0 {
				return nil, nil, fmt.Errorf("workflow %q must schedule at least one activity", definition.Name)
			}
			keys := make(map[string]bool, len(activities))
			for i := range activities {
				activity := &activities[i]
				if activity.Key == "" {
					activity.Key = fmt.Sprintf("step-%d", i)
				}
				if keys[activity.Key] || strings.TrimSpace(activity.Key) == "" || len(activity.Key) > 200 {
					return nil, nil, fmt.Errorf("workflow %q has invalid or duplicate step key %q", definition.Name, activity.Key)
				}
				keys[activity.Key] = true
				if strings.TrimSpace(activity.Name) == "" {
					return nil, nil, fmt.Errorf("workflow %q activity %d has no name", definition.Name, i)
				}
				if len(activity.Input) == 0 || !json.Valid(activity.Input) {
					return nil, nil, fmt.Errorf("workflow %q activity %d has invalid JSON input", definition.Name, i)
				}
			}
			return encoded, activities, nil
		},
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.definitions[definition.Name]; exists {
		return fmt.Errorf("workflow %q is already registered", definition.Name)
	}
	registry.definitions[definition.Name] = entry
	return nil
}

func (r *Registry) lookup(name string) (registeredDefinition, bool) {
	if r == nil {
		return registeredDefinition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	definition, ok := r.definitions[name]
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

func Start[Input any](ctx context.Context, engine *Engine, definition Definition[Input], input Input) (WorkflowExecution, error) {
	return engine.start(ctx, definition.Name, input)
}

type Handler func(context.Context, json.RawMessage) (json.RawMessage, error)

type HandlerRegistration struct {
	Name     string
	Handler  Handler
	contract string
}

func Handle[Input, Output any](name string, handler func(context.Context, Input) (Output, error)) HandlerRegistration {
	if handler == nil {
		return HandlerRegistration{Name: name}
	}
	return HandlerRegistration{
		Name: name,
		Handler: func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			var input Input
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, fmt.Errorf("decode activity %q input: %w", name, err)
			}
			output, err := handler(ctx, input)
			if err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(output)
			if err != nil {
				return nil, fmt.Errorf("encode activity %q output: %w", name, err)
			}
			return encoded, nil
		},
	}
}
