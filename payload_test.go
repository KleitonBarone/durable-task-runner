package durable

import (
	"context"
	"encoding/json"
	"errors"
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
