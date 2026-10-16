package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Verification uses the existing bridge and an ephemeral loopback port. It needs
// neither a new Docker subnet nor the development database's named volume.
const verificationCompose = `services:
  postgres:
    image: postgres:17-alpine
    network_mode: bridge
    environment:
      POSTGRES_DB: durable
      POSTGRES_PASSWORD: durable
      POSTGRES_USER: durable
    ports:
      - "127.0.0.1::5432"
    tmpfs:
      - /var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U durable -d durable"]
      interval: 1s
      timeout: 5s
      retries: 30
`

type database struct {
	url       string
	project   string
	container string
	docker    tool
}

func (a *app) withDatabase(ctx context.Context, use func(context.Context, database) error) (result error) {
	docker, err := a.docker(ctx)
	if err != nil {
		return err
	}
	if len(docker.prefix) > 0 {
		// Keep WSL alive across native Windows Go commands, then close stdin to
		// release it. timeout also bounds the remote process if its client dies.
		release, err := a.runner.hold(ctx, command{name: docker.name, dir: a.root,
			args: []string{"-d", docker.prefix[1], "--exec", "timeout", "900", "cat"}})
		if err != nil {
			return fmt.Errorf("keep WSL available during verification: %w", err)
		}
		defer func() { result = errors.Join(result, release()) }()
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return err
	}
	project := "durable-check-" + hex.EncodeToString(suffix[:])
	compose := func(ctx context.Context, args ...string) (string, error) {
		c := docker.command(a.root, append([]string{"compose", "-p", project, "-f", "-"}, args...)...)
		c.input = verificationCompose
		c.quiet = len(args) > 0 && args[0] == "port"
		return a.runner.run(ctx, c)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if result != nil {
			_, _ = compose(cleanupCtx, "logs", "--no-color", "--tail", "100")
		}
		_, cleanupErr := compose(cleanupCtx, "down", "--volumes", "--remove-orphans", "--timeout", "5")
		if cleanupErr != nil {
			result = errors.Join(result, fmt.Errorf("cleanup project %s: %w", project, cleanupErr))
		}
	}()
	if _, err := compose(ctx, "up", "-d", "--wait", "--wait-timeout", "60", "--quiet-pull"); err != nil {
		return fmt.Errorf("start isolated PostgreSQL project %s: %w", project, err)
	}
	binding, err := compose(ctx, "port", "postgres", "5432")
	if err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(binding))
	if err != nil || host != "127.0.0.1" || port == "" {
		return fmt.Errorf("unexpected isolated PostgreSQL binding %q", strings.TrimSpace(binding))
	}
	databaseURL := "postgres://durable:durable@" + net.JoinHostPort("localhost", port) + "/durable?sslmode=disable"
	container, err := compose(ctx, "ps", "-q", "postgres")
	if err != nil {
		return err
	}
	container = strings.TrimSpace(container)
	if container == "" || strings.ContainsAny(container, "\r\n ") {
		return errors.New("isolated PostgreSQL container identity is missing")
	}
	return use(ctx, database{url: databaseURL, project: project, container: container, docker: docker})
}

func (a *app) integration(ctx context.Context, databaseURL string, args []string) error {
	if databaseURL == "" {
		return errors.New("DURABLE_TEST_DATABASE_URL is required; use an isolated check or explicitly select a disposable database")
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(pingCtx, databaseURL)
	if err != nil {
		// pgx connection errors can contain the URL or credentials.
		return errors.New("test PostgreSQL is unreachable; check DURABLE_TEST_DATABASE_URL and start the selected database")
	}
	if err := conn.Close(pingCtx); err != nil {
		return errors.New("closing PostgreSQL preflight connection failed")
	}
	arguments := append([]string{"test", "-count=1", "-timeout=2m", "-tags=integration"}, args...)
	if len(args) == 0 {
		arguments = append(arguments, "./...")
	}
	_, err = a.runner.run(ctx, command{name: "go", args: arguments, dir: a.root,
		env: []string{"DURABLE_TEST_DATABASE_URL=" + databaseURL}})
	return err
}
