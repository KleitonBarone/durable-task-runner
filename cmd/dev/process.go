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
	hold(context.Context, command) (func() error, error)
}

type executor struct{ out io.Writer }

// hold keeps an input-driven process alive until the returned closer is called.
// WSL can stop systemd services when no foreground Linux process remains.
func (r executor) hold(ctx context.Context, c command) (func() error, error) {
	processCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(processCtx, c.name, c.args...)
	cmd.Dir = c.dir
	cmd.Env = append(os.Environ(), c.env...)
	cmd.Stdout, cmd.Stderr = r.out, r.out
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		_ = stdin.Close()
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return func() error {
		defer cancel()
		_ = stdin.Close()
		select {
		case err := <-done:
			if ctx.Err() != nil {
				return nil
			}
			return err
		case <-time.After(5 * time.Second):
			cancel()
			return fmt.Errorf("WSL keep-alive did not terminate after closing input")
		}
	}, nil
}

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
	root   string
}

func (t tool) command(dir string, args ...string) command {
	return command{name: t.name, args: append(append([]string{}, t.prefix...), args...), dir: dir}
}

func (a *app) docker(ctx context.Context) (tool, error) {
	if path, err := a.lookPath("docker"); err == nil {
		return tool{name: path, root: a.root}, nil
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
	return tool{name: wsl, root: linuxRoot, prefix: []string{"-d", distro, "--cd", linuxRoot, "--exec", "docker"}}, nil
}
