package durable

import (
	"context"
	"strings"
	"testing"
)

func TestVersionIdentityValidation(t *testing.T) {
	for _, bad := range []string{"", " ", "nul\x00", "bad\xff", strings.Repeat("a", 201)} {
		t.Run(strings.ReplaceAll(bad, "\x00", "nul"), func(t *testing.T) {
			a := DefineActivity[string, string]("test").WithVersion(bad, "in", "out")
			if _, err := a.Step("key", "input"); err == nil {
				t.Fatal("invalid version accepted")
			}
			if err := Register(NewRegistry(), Define("test", func(string) ([]ActivitySpec, error) { return nil, nil }).WithVersion("1", bad)); err == nil {
				t.Fatal("invalid schema accepted")
			}
		})
	}
	a := DefineActivity[string, string]("test")
	step, err := a.Step("key", "input")
	registration := a.Handle(func(_ context.Context, in string) (string, error) { return in, nil })
	if err != nil || step.Version != "1" || step.InputSchema != "legacy" || registration.OutputSchema != "legacy" {
		t.Fatalf("legacy defaults: %#v %#v %v", step, registration, err)
	}
}
