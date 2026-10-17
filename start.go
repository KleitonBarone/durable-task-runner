package durable

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"unicode/utf8"
)

type StartOption struct {
	kind, key string
	timeout   time.Duration
	previous  string
}

// WithStartKey deduplicates starts within a workflow definition name and database.
// Reusing a key with JSON-equivalent input returns the original execution,
// including terminal executions. A conflicting input returns ErrStartConflict.
func WithStartKey(key string) StartOption { return StartOption{kind: "key", key: key} }

func WithWorkflowTimeout(timeout time.Duration) StartOption {
	return StartOption{kind: "timeout", timeout: timeout}
}

// WithPredecessor links a new reconciliation execution to a terminal execution.
// The identifier remains recorded if retention later removes the predecessor.
func WithPredecessor(id string) StartOption { return StartOption{kind: "predecessor", previous: id} }

type startConfig struct {
	key      *string
	timeout  time.Duration
	previous *string
}

func parseStartOptions(options []StartOption) (startConfig, error) {
	config := startConfig{timeout: 24 * time.Hour}
	timeoutSet := false
	for _, option := range options {
		switch {
		case option.kind == "key" && config.key == nil:
			key := option.key
			if strings.TrimSpace(key) == "" || len(key) > 200 || !utf8.ValidString(key) || strings.ContainsRune(key, 0) {
				return config, fmt.Errorf("%w: start key must contain 1-200 UTF-8 bytes without NUL", ErrInvalidInput)
			}
			config.key = &key
		case option.kind == "timeout" && !timeoutSet:
			if option.timeout < time.Microsecond || option.timeout > 365*24*time.Hour {
				return config, fmt.Errorf("%w: workflow timeout outside supported range", ErrInvalidInput)
			}
			config.timeout = option.timeout
			timeoutSet = true
		case option.kind == "predecessor" && config.previous == nil:
			var id pgtype.UUID
			if err := id.Scan(option.previous); err != nil || !id.Valid {
				return config, fmt.Errorf("%w: invalid predecessor ID", ErrInvalidInput)
			}
			previous := option.previous
			config.previous = &previous
		default:
			return config, fmt.Errorf("%w: duplicate or invalid start option", ErrInvalidInput)
		}
	}
	return config, nil
}

func startKey(options []StartOption) (*string, error) {
	config, err := parseStartOptions(options)
	return config.key, err
}
