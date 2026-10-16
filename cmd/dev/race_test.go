package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWindowsRaceCleansItsContainersAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var removedRace, removedDatabase bool
	r := &fakeRunner{respond: func(c command) (string, error) {
		switch c.args[0] {
		case "env":
			return "/cache", nil
		case "run":
			joined := strings.Join(c.args, " ")
			if !strings.Contains(joined, "--network container:abc123def456") || !strings.Contains(joined, "dst=/workspace,readonly") {
				t.Fatalf("race container isolation = %s", joined)
			}
			cancel()
			return "", context.Canceled
		case "rm":
			removedRace = true
			return "Error: No such container", errors.New("already removed")
		case "compose":
			switch c.args[5] {
			case "port":
				return "127.0.0.1:49123", nil
			case "ps":
				return "abc123def456", nil
			case "down":
				removedDatabase = true
			}
		}
		return "", nil
	}}
	a := testApp(r)
	a.goos = "windows"
	if err := a.race(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("race error = %v", err)
	}
	if !removedRace || !removedDatabase {
		t.Fatalf("race removed = %v, database removed = %v", removedRace, removedDatabase)
	}
	if r.contexts[len(r.contexts)-1] != nil || r.contexts[len(r.contexts)-3] != nil {
		t.Fatal("cleanup inherited cancellation")
	}
}
