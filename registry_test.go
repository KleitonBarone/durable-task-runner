package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
)

type registryInput struct {
	Value string `json:"value"`
}

func TestRegistryAcceptsNilInputs(t *testing.T) {
	t.Run("any", testRegistryNilInput[any])
	t.Run("nonempty interface", testRegistryNilInput[error])
	t.Run("pointer", testRegistryNilInput[*registryInput])
	t.Run("map", testRegistryNilInput[map[string]string])
	t.Run("slice", testRegistryNilInput[[]string])
}

func testRegistryNilInput[Input any](t *testing.T) {
	t.Helper()
	var zero Input
	registry := NewRegistry()
	definition := Define("nil", func(input Input) ([]ActivitySpec, error) {
		if !reflect.DeepEqual(input, zero) {
			t.Errorf("plan received %#v, want nil", input)
		}
		step, err := NewActivity("echo", input)
		return []ActivitySpec{step}, err
	})
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	registered, _ := registry.lookup("nil")
	encoded, plan, err := registered.encodePlan(zero)
	if err != nil || string(encoded) != "null" || len(plan) != 1 || string(plan[0].Input) != "null" {
		t.Fatalf("nil input plan: %s %#v %v", encoded, plan, err)
	}
}

func TestRegistryRejectsNilForConcreteInput(t *testing.T) {
	registry := NewRegistry()
	definition := Define("concrete", func(registryInput) ([]ActivitySpec, error) {
		t.Error("plan ran for an invalid input")
		return nil, nil
	})
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	registered, _ := registry.lookup("concrete")
	if _, _, err := registered.encodePlan(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("concrete input accepted untyped nil: %v", err)
	}
}

func TestRegistryDoesNotMutateReusablePlan(t *testing.T) {
	shared := []ActivitySpec{{Name: "echo", Input: json.RawMessage(`"input"`)}}
	registry := NewRegistry()
	definition := Define("reusable", func(string) ([]ActivitySpec, error) { return shared, nil })
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	registered, _ := registry.lookup(definition.Name)
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, plan, err := registered.encodePlan("input")
			if err != nil || len(plan) != 1 || plan[0].Key != "step-0" {
				t.Errorf("plan: %#v %v", plan, err)
			}
		}()
	}
	group.Wait()
	if shared[0].Key != "" {
		t.Fatalf("registry mutated the application's reusable plan: key = %q", shared[0].Key)
	}
}

func TestRegistryEncodesTypedInputAndPlan(t *testing.T) {
	registry := NewRegistry()
	definition := Define("typed", func(input registryInput) ([]ActivitySpec, error) {
		activity, err := NewActivity("record", input)
		return []ActivitySpec{activity}, err
	})
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	registered, ok := registry.lookup("typed")
	if !ok {
		t.Fatal("registered definition not found")
	}
	encoded, plan, err := registered.encodePlan(registryInput{Value: "kept"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"value":"kept"}`; got != want {
		t.Fatalf("encoded input = %s, want %s", got, want)
	}
	if len(plan) != 1 || plan[0].Name != "record" || !json.Valid(plan[0].Input) {
		t.Fatalf("unexpected activity plan: %#v", plan)
	}
	if _, _, err := registered.encodePlan("wrong type"); err == nil {
		t.Fatal("wrong input type was accepted")
	}
}

func TestRegistryRejectsDuplicateAndEmptyPlans(t *testing.T) {
	registry := NewRegistry()
	definition := Define("empty", func(registryInput) ([]ActivitySpec, error) { return nil, nil })
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	if err := Register(registry, definition); err == nil {
		t.Fatal("duplicate workflow registration was accepted")
	}
	registered, _ := registry.lookup("empty")
	if _, _, err := registered.encodePlan(registryInput{}); err == nil {
		t.Fatal("empty workflow plan was accepted")
	}
}

func TestHandlerKeepsTypedBoundary(t *testing.T) {
	registration := Handle("upper", func(_ context.Context, input registryInput) (registryInput, error) {
		return registryInput{Value: input.Value + "!"}, nil
	})
	output, err := registration.Handler(context.Background(), json.RawMessage(`{"value":"yes"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(output), `{"value":"yes!"}`; got != want {
		t.Fatalf("handler output = %s, want %s", got, want)
	}
	if _, err := registration.Handler(context.Background(), json.RawMessage(`{"value":`)); err == nil {
		t.Fatal("invalid handler input was accepted")
	}
}

func TestTypedActivityAndStepValidation(t *testing.T) {
	a := DefineActivity[string, int]("length")
	spec, err := a.Step("first", "hello")
	if err != nil || spec.Key != "first" || spec.contract != a.Handle(func(context.Context, string) (int, error) { return 5, nil }).contract {
		t.Fatalf("typed step = %#v, err = %v", spec, err)
	}
	if _, err := a.Step("", "hello"); err == nil {
		t.Fatal("empty key accepted")
	}
	r := NewRegistry()
	definition := Define("duplicate", func(string) ([]ActivitySpec, error) { return []ActivitySpec{spec, spec}, nil })
	if err := Register(r, definition); err != nil {
		t.Fatal(err)
	}
	registered, _ := r.lookup(definition.Name)
	if _, _, err := registered.encodePlan(""); err == nil {
		t.Fatal("duplicate keys accepted")
	}
	if err := r.bindActivities(map[string]string{spec.Name: spec.contract}); err != nil {
		t.Fatal(err)
	}
	b := DefineActivity[int, int]("length")
	if err := r.bindActivities(map[string]string{"length": b.contract()}); err == nil {
		t.Fatal("conflicting types accepted")
	}
}
