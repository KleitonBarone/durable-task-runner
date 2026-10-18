package durable

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

type ContractPayload struct {
	Value int `json:"value"`
}

type embeddedContractInput = struct{ ContractPayload }
type nestedContractInput = struct{ ContractPayload ContractPayload }

// v1.0.0 encoded both embedding forms with this same durable contract.
const legacyStructContract = "struct{/ContractPayload:github.com/KleitonBarone/durable-task-runner/durable.ContractPayload:;} -> /int"

func TestAnonymousStructContractsDistinguishEmbedding(t *testing.T) {
	embedded := DefineActivity[embeddedContractInput, int]("effect")
	nested := DefineActivity[nestedContractInput, int]("effect")
	flat, _ := json.Marshal(embeddedContractInput{ContractPayload{42}})
	object, _ := json.Marshal(nestedContractInput{ContractPayload{42}})
	if string(flat) != `{"value":42}` || string(object) != `{"ContractPayload":{"value":42}}` {
		t.Fatalf("expected different JSON shapes: %s %s", flat, object)
	}
	if embedded.contract() == nested.contract() {
		t.Fatal("incompatible embedding forms share a contract")
	}
	for _, pair := range [][2]reflect.Type{
		{reflect.TypeFor[*embeddedContractInput](), reflect.TypeFor[*nestedContractInput]()},
		{reflect.TypeFor[[]embeddedContractInput](), reflect.TypeFor[[]nestedContractInput]()},
		{reflect.TypeFor[[2]embeddedContractInput](), reflect.TypeFor[[2]nestedContractInput]()},
		{reflect.TypeFor[map[string]embeddedContractInput](), reflect.TypeFor[map[string]nestedContractInput]()},
	} {
		if typeIdentity(pair[0]) == typeIdentity(pair[1]) {
			t.Errorf("container contracts collide: %v and %v", pair[0], pair[1])
		}
	}
}

func TestAnonymousStructResultRejectsIncompatibleEmbedding(t *testing.T) {
	embedded := DefineActivity[string, embeddedContractInput]("effect")
	nested := DefineActivity[string, nestedContractInput]("effect")
	stored := capability{"effect", "1", embedded.contract(), "legacy", "legacy"}
	if _, err := nested.decode(stored, json.RawMessage(`{"value":42}`)); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("incompatible result accepted: %v", err)
	}
	value, err := embedded.decode(stored, json.RawMessage(`{"value":42}`))
	if err != nil || value.Value != 42 {
		t.Fatalf("matching result: %#v %v", value, err)
	}
}

func TestAnonymousStructContractsRejectAmbiguousV1Identity(t *testing.T) {
	embedded := DefineActivity[embeddedContractInput, int]("effect")
	nested := DefineActivity[nestedContractInput, int]("effect")
	stored := capability{"effect", "1", legacyStructContract, "legacy", "legacy"}
	for _, contract := range []string{embedded.contract(), nested.contract()} {
		if contract == legacyStructContract {
			t.Fatal("new contract can still match ambiguous v1 data")
		}
	}
	if _, err := embedded.decode(stored, json.RawMessage(`42`)); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("ambiguous stored identity accepted: %v", err)
	}
}

func TestTypeContractsPreserveV1NamedAndScalarIdentities(t *testing.T) {
	for _, test := range []struct {
		typeOf reflect.Type
		want   string
	}{
		{reflect.TypeFor[string](), "/string"},
		{reflect.TypeFor[any](), "interface {}"},
		{reflect.TypeFor[ContractPayload](), "github.com/KleitonBarone/durable-task-runner/durable.ContractPayload"},
		{reflect.TypeFor[[]int](), "slice[0]/int"},
		{reflect.TypeFor[map[string]*int](), "map[/string]ptr[0]/int"},
	} {
		if got := typeIdentity(test.typeOf); got != test.want {
			t.Errorf("%v identity = %q, want %q", test.typeOf, got, test.want)
		}
	}
}

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
