// Command dev provides the repository's local development and verification loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	root, err := repositoryRoot()
	if err == nil {
		a := app{root: root, runner: executor{out: os.Stdout}, goos: runtime.GOOS, lookPath: exec.LookPath, getenv: os.Getenv}
		err = a.run(ctx, os.Args[1:])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "dev:", err)
		os.Exit(1)
	}
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
		fmt.Println("Usage: go run ./cmd/dev <doctor|test|fmt|fmt-check|lint> [go test arguments]")
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
		_, err := a.command(ctx, "go", "vet", "-tags=integration", "./...")
		return err
	default:
		return fmt.Errorf("unknown command %q; run go run ./cmd/dev help", args[0])
	}
}

func (a *app) command(ctx context.Context, name string, args ...string) (string, error) {
	return a.runner.run(ctx, command{name: name, args: args, dir: a.root})
}
