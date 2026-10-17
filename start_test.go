package durable

import (
	"errors"
	"strings"
	"testing"
	"time"
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

func TestWorkflowTimeoutOptions(t *testing.T) {
	config, err := parseStartOptions([]StartOption{WithStartKey("key"), WithWorkflowTimeout(time.Hour)})
	if err != nil || config.key == nil || config.timeout != time.Hour {
		t.Fatalf("options: %#v %v", config, err)
	}
	for _, timeout := range []time.Duration{0, -1, time.Nanosecond, 366 * 24 * time.Hour} {
		if _, err := parseStartOptions([]StartOption{WithWorkflowTimeout(timeout)}); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted %s: %v", timeout, err)
		}
	}
}
