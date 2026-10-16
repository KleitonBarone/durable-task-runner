package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeRunner struct {
	commands []command
	respond  func(command) (string, error)
}

func (r *fakeRunner) run(_ context.Context, c command) (string, error) {
	r.commands = append(r.commands, c)
	if r.respond != nil {
		return r.respond(c)
	}
	return "", nil
}

func testApp(r *fakeRunner) app {
	return app{root: "/repo", runner: r, goos: "linux", lookPath: func(name string) (string, error) {
		return "/bin/" + name, nil
	}, getenv: func(string) string { return "" }}
}

func TestUnitCommandForwardsSelectionAndFailure(t *testing.T) {
	wantErr := errors.New("test assertion failed")
	r := &fakeRunner{respond: func(command) (string, error) { return "", wantErr }}
	a := testApp(r)
	err := a.run(context.Background(), []string{"test", "-run", "TestClaim", "."})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	want := []string{"test", "-count=1", "-timeout=2m", "-run", "TestClaim", "."}
	if !reflect.DeepEqual(r.commands[0].args, want) {
		t.Fatalf("arguments = %v, want %v", r.commands[0].args, want)
	}
}

func TestFormattingDifferenceFails(t *testing.T) {
	r := &fakeRunner{respond: func(command) (string, error) { return "worker.go\n", nil }}
	a := testApp(r)
	if err := a.run(context.Background(), []string{"fmt-check"}); err == nil {
		t.Fatal("unformatted files passed")
	}
}

func TestDockerSelection(t *testing.T) {
	t.Run("native", func(t *testing.T) {
		a := testApp(&fakeRunner{})
		got, err := a.docker(context.Background())
		if err != nil || got.name != "/bin/docker" || len(got.prefix) != 0 {
			t.Fatalf("Docker = %#v, err = %v", got, err)
		}
	})
	t.Run("WSL paths stay individual arguments", func(t *testing.T) {
		r := &fakeRunner{respond: func(command) (string, error) { return "/mnt/c/My Projects/runner\r\n", nil }}
		a := testApp(r)
		a.goos = "windows"
		a.root = `C:\My Projects\runner`
		a.lookPath = func(name string) (string, error) {
			if name == "docker" {
				return "", errors.New("not found")
			}
			return name, nil
		}
		got, err := a.docker(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"-d", "Ubuntu", "--cd", "/mnt/c/My Projects/runner", "--exec", "docker", "compose", "version"}
		if c := got.command(a.root, "compose", "version"); !reflect.DeepEqual(c.args, want) {
			t.Fatalf("arguments = %#v, want %#v", c.args, want)
		}
	})
	t.Run("missing prerequisite", func(t *testing.T) {
		a := testApp(&fakeRunner{})
		a.lookPath = func(string) (string, error) { return "", errors.New("not found") }
		if _, err := a.docker(context.Background()); err == nil {
			t.Fatal("missing Docker passed")
		}
	})
}

func TestExecutorHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (executor{out: io.Discard}).run(ctx, command{name: "go", args: []string{"version"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestDoctorStopsOnUnavailableDaemon(t *testing.T) {
	r := &fakeRunner{respond: func(c command) (string, error) {
		if strings.Contains(strings.Join(c.args, " "), "version --format") {
			return "", errors.New("daemon offline")
		}
		return "", nil
	}}
	a := testApp(r)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.run(ctx, []string{"doctor"}); err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("doctor error = %v", err)
	}
	if len(r.commands) != 3 {
		t.Fatalf("doctor ran %d commands after daemon failure", len(r.commands))
	}
}
