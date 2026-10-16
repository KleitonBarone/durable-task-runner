package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type command struct {
	name  string
	args  []string
	dir   string
	env   []string
	input string
	quiet bool
}

type commandRunner interface {
	run(context.Context, command) (string, error)
}

type executor struct{ out io.Writer }

func (r executor) run(ctx context.Context, c command) (string, error) {
	cmd := exec.CommandContext(ctx, c.name, c.args...)
	cmd.Dir = c.dir
	cmd.Env = append(os.Environ(), c.env...)
	cmd.Stdin = strings.NewReader(c.input)
	cmd.WaitDelay = 5 * time.Second
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if !c.quiet {
		cmd.Stdout = io.MultiWriter(r.out, &output)
		cmd.Stderr = cmd.Stdout
	}
	if err := cmd.Run(); err != nil {
		return output.String(), fmt.Errorf("%s failed: %w", c.name, err)
	}
	return output.String(), nil
}

// tool invokes Docker directly or through one explicitly selected WSL distro.
type tool struct {
	name   string
	prefix []string
}

func (t tool) command(dir string, args ...string) command {
	return command{name: t.name, args: append(append([]string{}, t.prefix...), args...), dir: dir}
}

func (a *app) docker(ctx context.Context) (tool, error) {
	if path, err := a.lookPath("docker"); err == nil {
		return tool{name: path}, nil
	}
	if a.goos != "windows" {
		return tool{}, fmt.Errorf("Docker with Compose is required; docker was not found on PATH")
	}
	wsl, err := a.lookPath("wsl.exe")
	if err != nil {
		return tool{}, fmt.Errorf("Docker is missing; install Docker with Compose or make it available in WSL")
	}
	distro := a.getenv("DURABLE_WSL_DISTRO")
	if distro == "" {
		distro = "Ubuntu"
	}
	pathCommand := command{name: wsl, args: []string{"-d", distro, "--exec", "wslpath", "-a", a.root}, dir: a.root, quiet: true}
	linuxRoot, err := a.runner.run(ctx, pathCommand)
	if err != nil {
		return tool{}, fmt.Errorf("resolve repository path in WSL %q: %w", distro, err)
	}
	linuxRoot = strings.TrimSpace(linuxRoot)
	if !strings.HasPrefix(linuxRoot, "/") || strings.ContainsAny(linuxRoot, "\r\n") {
		return tool{}, fmt.Errorf("WSL returned an invalid repository path")
	}
	return tool{name: wsl, prefix: []string{"-d", distro, "--cd", linuxRoot, "--exec", "docker"}}, nil
}
