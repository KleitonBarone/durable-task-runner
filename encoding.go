package durable

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

// MaxPayloadBytes limits each workflow input, step input, and step output before
// persistence. JSON uses encoding/json and PostgreSQL jsonb semantics.
const MaxPayloadBytes = 1 << 20

// PostgreSQL rejects some JSON accepted by encoding/json, such as unpaired
// UTF-16 surrogate escapes. Keep its cause while identifying invalid payloads.
func payloadDatabaseError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "22P02" || pgErr.Code == "22P05" || pgErr.Code == "22003") {
		return fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	return err
}

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

// Failure messages are diagnostic text, bounded separately from JSON payloads.
func failureMessage(err error) string {
	message := strings.ReplaceAll(strings.ToValidUTF8(err.Error(), "\uFFFD"), "\x00", "\\u0000")
	if len(message) > 16*1024 {
		message = message[:16*1024]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return message
}

func newID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
