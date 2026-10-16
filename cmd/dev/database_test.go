package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestExplicitIntegrationRequiresDatabase(t *testing.T) {
	r := &fakeRunner{}
	a := testApp(r)
	if err := a.run(context.Background(), []string{"integration"}); err == nil {
		t.Fatal("missing database passed")
	}
	if len(r.commands) != 0 {
		t.Fatal("tests ran without selecting a database")
	}
}

func TestIsolatedDatabaseLifecycle(t *testing.T) {
	for _, failure := range []string{"", "startup", "test", "cleanup", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			var cleanupCalled, callbackCalled bool
			var project string
			r := &fakeRunner{respond: func(c command) (string, error) {
				if c.input != verificationCompose || len(c.args) < 6 {
					t.Fatalf("unexpected compose command: %#v", c)
				}
				if project == "" {
					project = c.args[2]
				}
				if c.args[2] != project || !strings.HasPrefix(project, "durable-check-") {
					t.Fatal("cleanup changed project ownership")
				}
				switch c.args[5] {
				case "up":
					if failure == "startup" {
						return "", errors.New("startup failed")
					}
				case "port":
					return "127.0.0.1:49123\n", nil
				case "down":
					cleanupCalled = true
					if failure == "cleanup" {
						return "", errors.New("cleanup failed")
					}
				}
				return "", nil
			}}
			a := testApp(r)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := a.withDatabase(ctx, func(_ context.Context, url string) error {
				callbackCalled = true
				if url != "postgres://durable:durable@localhost:49123/durable?sslmode=disable" {
					t.Fatalf("database URL = %q", url)
				}
				if failure == "test" {
					return errors.New("assertion failed")
				}
				if failure == "cancel" {
					cancel()
					return ctx.Err()
				}
				return nil
			})
			if (err != nil) != (failure != "") {
				t.Fatalf("result = %v for failure %q", err, failure)
			}
			if !cleanupCalled || callbackCalled != (failure != "startup") {
				t.Fatalf("cleanup = %v, callback = %v", cleanupCalled, callbackCalled)
			}
			if r.contexts[len(r.contexts)-1] != nil {
				t.Fatal("cleanup inherited a cancelled test context")
			}
			if strings.Contains(strings.Join(r.commands[len(r.commands)-1].args, " "), "durable-task-runner") {
				t.Fatal("verification touched development project")
			}
		})
	}
}
