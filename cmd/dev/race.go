package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (a *app) race(ctx context.Context, args []string) error {
	return a.withDatabase(ctx, func(ctx context.Context, db database) (result error) {
		arguments := append([]string{"test", "-race", "-count=1", "-timeout=3m", "-tags=integration"}, args...)
		if len(args) == 0 {
			arguments = append(arguments, "./...")
		}
		if a.goos != "windows" {
			_, err := a.runner.run(ctx, command{name: "go", args: arguments, dir: a.root,
				env: []string{"DURABLE_TEST_DATABASE_URL=" + db.url, "CGO_ENABLED=1"}})
			return err
		}
		// Windows need not install a C toolchain. The Linux Go container shares
		// only this run's PostgreSQL network namespace and mounts source read-only.
		if _, err := a.command(ctx, "go", "mod", "download"); err != nil {
			return err
		}
		cache, err := a.command(ctx, "go", "env", "GOMODCACHE")
		if err != nil {
			return err
		}
		cache = strings.TrimSpace(cache)
		if len(db.docker.prefix) > 0 {
			c := command{name: db.docker.name, dir: a.root, quiet: true,
				args: []string{"-d", db.docker.prefix[1], "--exec", "wslpath", "-a", cache}}
			cache, err = a.runner.run(ctx, c)
			if err != nil {
				return err
			}
			cache = strings.TrimSpace(cache)
		}
		if cache == "" || strings.ContainsAny(cache, "\r\n") {
			return errors.New("Go returned an invalid module cache path")
		}
		name := db.project + "-race"
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c := db.docker.command(a.root, "rm", "--force", name)
			c.quiet = true
			output, err := a.runner.run(cleanupCtx, c)
			if err != nil && !strings.Contains(output, "No such container") {
				result = errors.Join(result, fmt.Errorf("cleanup race container %s: %w", name, err))
			}
		}()
		commandArgs := []string{"run", "--rm", "--name", name, "--network", "container:" + db.container,
			"--mount", "type=bind,src=" + db.docker.root + ",dst=/workspace,readonly", "--workdir", "/workspace",
			"--mount", "type=bind,src=" + cache + ",dst=/go/pkg/mod,readonly",
			"--env", "DURABLE_TEST_DATABASE_URL=postgres://durable:durable@localhost:5432/durable?sslmode=disable",
			"--env", "CGO_ENABLED=1", "--env", "GOPROXY=off", "golang:1.26.9-bookworm", "go"}
		_, err = a.runner.run(ctx, db.docker.command(a.root, append(commandArgs, arguments...)...))
		return err
	})
}
