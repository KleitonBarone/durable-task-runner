package durable

import (
	"regexp"
	"testing"
)

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
