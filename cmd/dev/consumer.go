package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// consumer builds outside the library module. Set DURABLE_CONSUMER_VERSION to a
// pushed revision to verify normal module resolution without a local replacement.
func (a *app) consumer(ctx context.Context, databaseURL string) error {
	dir, err := os.MkdirTemp("", "durable-consumer-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	fixture, err := os.ReadFile(filepath.Join(a.root, "testdata", "consumer", "main.go"))
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), fixture, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/consumer\n\ngo 1.24.0\n"), 0o600); err != nil {
		return err
	}
	version := a.getenv("DURABLE_CONSUMER_VERSION")
	env := []string{"DATABASE_URL=" + databaseURL, "GOWORK=off"}
	if version != "" {
		env = append(env, "GOMODCACHE="+filepath.Join(dir, "modcache"))
	}
	run := func(args ...string) error {
		_, err := a.runner.run(ctx, command{name: "go", args: args, dir: dir, env: env})
		return err
	}
	if version == "" {
		if err := run("mod", "edit", "-replace=github.com/KleitonBarone/durable-task-runner="+filepath.ToSlash(a.root)); err != nil {
			return err
		}
		version = "v0.0.0"
	}
	if err := run("get", "github.com/KleitonBarone/durable-task-runner@"+version); err != nil {
		return fmt.Errorf("install consumer dependency: %w", err)
	}
	return run("run", ".")
}
