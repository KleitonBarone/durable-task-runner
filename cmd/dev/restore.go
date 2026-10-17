package main

import (
	"context"
	"fmt"
	"strings"
)

// restoreRehearsal touches only the two isolated databases owned by this check.
// The dump stays in memory and is never printed into verification logs.
func (a *app) restoreRehearsal(ctx context.Context, source database) error {
	seed := command{name: "go", args: []string{"run", "./cmd/example", "-role", "snapshot"}, dir: a.root, env: []string{"DATABASE_URL=" + source.url}, quiet: true}
	id, err := a.runner.run(ctx, seed)
	if err != nil {
		return fmt.Errorf("seed snapshot: %w", err)
	}
	id = strings.TrimSpace(id)
	if len(id) != 36 || strings.ContainsAny(id, " \r\n") {
		return fmt.Errorf("invalid snapshot execution identity")
	}
	dumpCmd := source.docker.command(a.root, "exec", source.container, "pg_dump", "-U", "durable", "-d", "durable", "--no-owner", "--no-privileges")
	dumpCmd.quiet = true
	dump, err := a.runner.run(ctx, dumpCmd)
	if err != nil {
		return fmt.Errorf("dump isolated snapshot: %w", err)
	}
	return a.withDatabase(ctx, func(ctx context.Context, target database) error {
		restoreCmd := target.docker.command(a.root, "exec", "-i", target.container, "psql", "-U", "durable", "-d", "durable", "-v", "ON_ERROR_STOP=1")
		restoreCmd.input, restoreCmd.quiet = dump, true
		if _, err := a.runner.run(ctx, restoreCmd); err != nil {
			return fmt.Errorf("restore isolated snapshot: %w", err)
		}
		_, err := a.runner.run(ctx, command{name: "go", args: []string{"run", "./cmd/example", "-role", "restore-verify", "-id", id}, dir: a.root, env: []string{"DATABASE_URL=" + target.url}})
		return err
	})
}
