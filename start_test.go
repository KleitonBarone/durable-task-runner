package durable

import (
	"errors"
	"strings"
	"testing"
)

func TestStartKeyValidation(t *testing.T) {
	if key, err := startKey(nil); err != nil || key != nil {
		t.Fatalf("unkeyed start: %v %v", key, err)
	}
	for _, key := range []string{"", "   ", "a\x00b", strings.Repeat("a", 201), string([]byte{255})} {
		if _, err := startKey([]StartOption{WithStartKey(key)}); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid key %q: %v", key, err)
		}
	}
	if _, err := startKey([]StartOption{WithStartKey("one"), WithStartKey("two")}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("multiple keys: %v", err)
	}
}
