package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

type childProcess struct {
	role   string
	stdin  io.WriteCloser
	done   chan error
	waited bool
}

// verify runs unchanged when copied into an external consumer module. Child
// processes use only the public runtime API; SQL below inspects fake effects.
func (a *application) verify(ctx context.Context) (result error) {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	migrate := exec.CommandContext(ctx, executable, "-role", "migrate")
	migrate.Stdout, migrate.Stderr = os.Stdout, os.Stderr
	if err := migrate.Run(); err != nil {
		return err
	}
	var children []*childProcess
	defer func() {
		for _, child := range children {
			_ = child.stdin.Close()
		}
		for _, child := range children {
			if !child.waited {
				if err := <-child.done; err != nil {
					result = errors.Join(result, fmt.Errorf("%s shutdown: %w", child.role, err))
				}
			}
		}
	}()
	for _, role := range []string{"worker", "scheduler"} {
		cmd := exec.CommandContext(ctx, executable, "-role", role, "-stdin-shutdown")
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		cmd.WaitDelay = 5 * time.Second
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			_ = stdin.Close()
			return err
		}
		child := &childProcess{role: role, stdin: stdin, done: make(chan error, 1)}
		children = append(children, child)
		go func() { child.done <- cmd.Wait() }()
	}
	key := fmt.Sprintf("consumer-%d-%d", os.Getpid(), time.Now().UnixNano())
	start := exec.CommandContext(ctx, executable, "-role", "start", "-key", key)
	start.Stderr = os.Stderr
	outputID, err := start.Output()
	if err != nil {
		return err
	}
	execution, err := a.engine.Workflow(ctx, strings.TrimSpace(string(outputID)))
	if err != nil {
		return err
	}
	duplicate, err := a.start(ctx, key)
	if err != nil || duplicate.ID != execution.ID {
		return fmt.Errorf("duplicate start did not resolve to %s: %v", execution.ID, err)
	}
	if err := a.wait(ctx, execution.ID, children); err != nil {
		return err
	}
	output, err := sendEmail.WorkflowResult(ctx, a.engine, execution.ID)
	if err != nil || output.AccountID != "account-42" || output.Email != "customer@example.com" || output.Status != "sent" {
		return fmt.Errorf("unexpected final result %#v: %v", output, err)
	}
	activities, err := a.engine.Activities(ctx, execution.ID)
	if err != nil {
		return err
	}
	for _, activity := range activities {
		attempts, err := a.engine.Attempts(ctx, activity.ID)
		if err != nil || len(attempts) != 1 || attempts[0].State != durable.AttemptCompleted {
			return fmt.Errorf("unexpected attempts for %s: %#v %v", activity.StepKey, attempts, err)
		}
	}
	var effects int
	if err := a.pool.QueryRow(ctx, `SELECT count(*) FROM example_effects WHERE workflow_id=$1`, execution.ID).Scan(&effects); err != nil {
		return err
	}
	if effects != 2 {
		return fmt.Errorf("fake effects = %d, want 2", effects)
	}
	fmt.Println("external consumer: separate worker/scheduler, keyed start, prior result, final result and attempts passed")
	status := exec.CommandContext(ctx, executable, "-role", "status", "-id", execution.ID)
	status.Stdout, status.Stderr = os.Stdout, os.Stderr
	return status.Run()
}
