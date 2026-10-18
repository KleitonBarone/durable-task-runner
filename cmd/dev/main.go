// Command dev provides the repository's local development and verification loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

type app struct {
	root     string
	runner   commandRunner
	goos     string
	lookPath func(string) (string, error)
	getenv   func(string) string
}

func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, "dev:", err)
		os.Exit(1)
	}
}

func execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	root, err := repositoryRoot()
	if err != nil {
		return err
	}
	var output io.Writer = os.Stdout
	if artifactDir := os.Getenv("DURABLE_TEST_ARTIFACT_DIR"); artifactDir != "" {
		if err := os.MkdirAll(artifactDir, 0o755); err != nil {
			return err
		}
		log, err := os.CreateTemp(artifactDir, "verification-*.log")
		if err != nil {
			return err
		}
		defer log.Close()
		output = io.MultiWriter(output, log)
	}
	a := app{root: root, runner: executor{out: output}, goos: runtime.GOOS, lookPath: exec.LookPath, getenv: os.Getenv}
	return a.run(ctx, os.Args[1:])
}

func repositoryRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("run this command inside the durable-task-runner checkout")
		}
		dir = parent
	}
}

func (a *app) run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Println("Usage: go run ./cmd/dev <doctor|up|down|test|integration|e2e|check|race|bench|restore|vuln|example|consumer|fmt|fmt-check|lint> [go test arguments]")
		return nil
	}
	switch args[0] {
	case "doctor":
		if _, err := a.command(ctx, "go", "version"); err != nil {
			return err
		}
		docker, err := a.docker(ctx)
		if err != nil {
			return err
		}
		for _, arguments := range [][]string{
			{"context", "inspect", "--format", "{{.Endpoints.docker.Host}}"},
			{"version", "--format", "{{.Server.Version}}"},
			{"compose", "version"},
			{"ps", "--filter", "publish=54329", "--format", "{{.Names}} {{.Ports}}"},
		} {
			if _, err := a.runner.run(ctx, docker.command(a.root, arguments...)); err != nil {
				return fmt.Errorf("Docker is unavailable; check the selected endpoint and daemon: %w", err)
			}
		}
		return nil
	case "test":
		arguments := append([]string{"test", "-count=1", "-timeout=2m"}, args[1:]...)
		if len(args) == 1 {
			arguments = append(arguments, "./...")
		}
		_, err := a.command(ctx, "go", arguments...)
		return err
	case "up", "down":
		docker, err := a.docker(ctx)
		if err != nil {
			return err
		}
		arguments := []string{"compose", "-p", "durable-task-runner", "-f", "compose.yaml"}
		if args[0] == "up" {
			arguments = append(arguments, "up", "-d", "--wait", "--wait-timeout", "60")
		} else {
			arguments = append(arguments, "down")
		}
		_, err = a.runner.run(ctx, docker.command(a.root, arguments...))
		return err
	case "integration":
		return a.integration(ctx, a.getenv("DURABLE_TEST_DATABASE_URL"), args[1:])
	case "bench":
		return a.withDatabase(ctx, func(ctx context.Context, db database) error {
			arguments := append([]string{"-run=^$", "-bench=Benchmark", "-benchtime=3x", "-count=3"}, args[1:]...)
			if len(args) == 1 {
				arguments = append(arguments, "./tests/benchmarks")
			}
			return a.integration(ctx, db.url, arguments)
		})
	case "e2e":
		return a.withDatabase(ctx, func(ctx context.Context, db database) error {
			return a.processTests(ctx, db.url, args[1:])
		})
	case "check":
		if len(args) != 1 {
			return errors.New("check always runs the full suite; use test, integration, or e2e for selections")
		}
		for _, check := range []string{"fmt-check", "lint", "test"} {
			if err := a.run(ctx, []string{check}); err != nil {
				return err
			}
		}
		return a.withDatabase(ctx, func(ctx context.Context, db database) error {
			if err := a.integration(ctx, db.url, nil); err != nil {
				return err
			}
			if err := a.processTests(ctx, db.url, nil); err != nil {
				return err
			}
			if err := a.example(ctx, db.url); err != nil {
				return err
			}
			if err := a.consumer(ctx, db.url); err != nil {
				return err
			}
			return a.restoreRehearsal(ctx, db)
		})
	case "restore":
		return a.withDatabase(ctx, a.restoreRehearsal)
	case "consumer":
		return a.withDatabase(ctx, func(ctx context.Context, db database) error {
			return a.consumer(ctx, db.url)
		})
	case "race":
		return a.race(ctx, args[1:])
	case "example":
		url := a.getenv("DATABASE_URL")
		if url == "" {
			url = "postgres://durable:durable@localhost:54329/durable?sslmode=disable"
		}
		return a.example(ctx, url)
	case "fmt":
		_, err := a.command(ctx, "gofmt", "-w", ".")
		return err
	case "fmt-check":
		output, err := a.command(ctx, "gofmt", "-l", ".")
		if err == nil && strings.TrimSpace(output) != "" {
			return errors.New("Go formatting differs; run go run ./cmd/dev fmt")
		}
		return err
	case "lint":
		if _, err := a.command(ctx, "go", "vet", "./..."); err != nil {
			return err
		}
		_, err := a.command(ctx, "go", "vet", "-tags=integration,e2e", "./...")
		return err
	case "vuln":
		if _, err := a.command(ctx, "go", "run", "golang.org/x/vuln/cmd/govulncheck@v1.9.0", "./..."); err != nil {
			return err
		}
		_, err := a.command(ctx, "go", "run", "golang.org/x/vuln/cmd/govulncheck@v1.9.0", "-test", "-tags=integration,e2e", "./...")
		return err
	default:
		return fmt.Errorf("unknown command %q; run go run ./cmd/dev help", args[0])
	}
}

func (a *app) command(ctx context.Context, name string, args ...string) (string, error) {
	return a.runner.run(ctx, command{name: name, args: args, dir: a.root})
}
