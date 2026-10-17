package durable

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestRetryBackoffCapsAndDeterministicJitter(t *testing.T) {
	policy := RetryPolicy{InitialInterval: time.Second, MaximumInterval: 3 * time.Second}
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second} {
		if got := retryDelay(policy, "activity", i+1); got != want {
			t.Fatalf("attempt %d: %s != %s", i+1, got, want)
		}
	}
	policy.JitterFraction = .25
	first := retryDelay(policy, "activity", 1)
	if first < 750*time.Millisecond || first > 1250*time.Millisecond || first != retryDelay(policy, "activity", 1) {
		t.Fatalf("jitter: %s", first)
	}
	if got := retryDelay(policy, "activity", 1000); got > policy.MaximumInterval {
		t.Fatal(got)
	}
}

func TestRetryPolicyValidationAndPermanentCauses(t *testing.T) {
	for _, policy := range []RetryPolicy{{MaxAttempts: -1}, {MaxAttempts: 1001}, {InitialInterval: -1}, {InitialInterval: 2 * time.Second, MaximumInterval: time.Second}, {JitterFraction: math.NaN()}, {JitterFraction: 2}} {
		if _, err := NewActivity("test", "input", WithRetry(policy)); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted %#v: %v", policy, err)
		}
	}
	cause := errors.New("permanent")
	if !isPermanent(Permanent(cause)) || !errors.Is(Permanent(cause), cause) || isPermanent(cause) || Permanent(nil) != nil {
		t.Fatal("permanent error classification")
	}
	if !isPermanent(&HandlerPanicError{Value: "panic"}) {
		t.Fatal("handler panic is retryable")
	}
}

func TestCompensationDeclarationValidation(t *testing.T) {
	cleanup, err := NewActivity("undo", "input")
	if err != nil {
		t.Fatal(err)
	}
	forward, err := NewActivity("effect", "input", WithCompensation(cleanup))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewActivity("outer", "input", WithCompensation(forward)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nested cleanup: %v", err)
	}
	if _, err := NewActivity("effect", "input", WithCompensation(ActivitySpec{})); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty cleanup: %v", err)
	}
	if _, err := NewActivity("effect", "input", WithCompensation(cleanup), WithCompensation(cleanup)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate cleanup: %v", err)
	}
}
