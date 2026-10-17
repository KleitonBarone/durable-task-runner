package durable

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const defaultVersion = "1"
const legacySchema = "legacy"

func validIdentity(value string, limit int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func activityKey(name, version string) string { return name + "\x00" + version }

type capability struct {
	name, version, contract, inputSchema, outputSchema string
}

func (c capability) signature() string {
	return c.contract + "\x00" + c.inputSchema + "\x00" + c.outputSchema
}

func (c capability) validate() error {
	if !validIdentity(c.name, 200) || !validIdentity(c.version, 100) || !validIdentity(c.inputSchema, 200) || !validIdentity(c.outputSchema, 200) {
		return fmt.Errorf("%w: activity name, version, and payload schema identities are required", ErrInvalidInput)
	}
	return nil
}
