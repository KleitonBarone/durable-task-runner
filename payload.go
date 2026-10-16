package durable

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxPayloadBytes limits each workflow input, step input, and step output before
// persistence. JSON uses encoding/json and PostgreSQL jsonb semantics.
const MaxPayloadBytes = 1 << 20

func validatePayload(raw json.RawMessage) error {
	if len(raw) > MaxPayloadBytes {
		return fmt.Errorf("%w: %w: maximum %d bytes", ErrInvalidInput, ErrPayloadTooLarge, MaxPayloadBytes)
	}
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return fmt.Errorf("%w: expected valid UTF-8 JSON", ErrInvalidInput)
	}
	// PostgreSQL jsonb cannot represent a NUL character in a JSON string.
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	if containsNUL(value) {
		return fmt.Errorf("%w: JSON strings cannot contain NUL", ErrInvalidInput)
	}
	return nil
}

func containsNUL(value any) bool {
	switch value := value.(type) {
	case string:
		return strings.ContainsRune(value, 0)
	case []any:
		for _, item := range value {
			if containsNUL(item) {
				return true
			}
		}
	case map[string]any:
		for key, item := range value {
			if strings.ContainsRune(key, 0) || containsNUL(item) {
				return true
			}
		}
	}
	return false
}
