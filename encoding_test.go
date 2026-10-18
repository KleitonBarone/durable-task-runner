package durable

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
)

func TestPayloadBoundariesAndEncodingErrors(t *testing.T) {
	if err := validatePayload([]byte(`"` + strings.Repeat("a", MaxPayloadBytes-2) + `"`)); err != nil {
		t.Fatal(err)
	}
	if err := validatePayload([]byte(`"` + strings.Repeat("a", MaxPayloadBytes-1) + `"`)); !errors.Is(err, ErrPayloadTooLarge) || !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("oversize: %v", err)
	}
	for _, raw := range []string{``, `{`, `{"value":"\u0000"}`, `1e9999`, string([]byte{'"', 255, '"'})} {
		if err := validatePayload([]byte(raw)); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted %q: %v", raw, err)
		}
	}
	_, err := NewActivity("bad", func() {})
	var unsupported *json.UnsupportedTypeError
	if !errors.Is(err, ErrInvalidInput) || !errors.As(err, &unsupported) {
		t.Fatalf("encoding error lost: %v", err)
	}
	registration := Handle("bad", func(context.Context, int) (string, error) { return "", nil })
	_, err = registration.Handler(context.Background(), []byte(`"not an integer"`))
	var mismatch *json.UnmarshalTypeError
	if !errors.Is(err, ErrInvalidInput) || !errors.As(err, &mismatch) {
		t.Fatalf("decode error lost: %v", err)
	}
}

func TestNewIDReturnsUUID(t *testing.T) {
	id, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !pattern.MatchString(id) {
		t.Fatalf("generated id %q is not a version 4 UUID", id)
	}
}
