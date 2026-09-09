package durable

import (
	"context"
	"encoding/json"
	"testing"
)

type registryInput struct {
	Value string `json:"value"`
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
