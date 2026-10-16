package durable

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type StartOption struct{ key string }

// WithStartKey deduplicates starts within a workflow definition name and database.
// Reusing a key with JSON-equivalent input returns the original execution,
// including terminal executions. A conflicting input returns ErrStartConflict.
func WithStartKey(key string) StartOption { return StartOption{key: key} }

func startKey(options []StartOption) (*string, error) {
	if len(options) == 0 {
		return nil, nil
	}
	if len(options) != 1 {
		return nil, fmt.Errorf("%w: supply at most one start key", ErrInvalidInput)
	}
	key := options[0].key
	if strings.TrimSpace(key) == "" || len(key) > 200 || !utf8.ValidString(key) || strings.ContainsRune(key, 0) {
		return nil, fmt.Errorf("%w: start key must contain 1-200 UTF-8 bytes without NUL", ErrInvalidInput)
	}
	return &key, nil
}
