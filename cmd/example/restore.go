package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

// snapshot leaves a committed fake effect and an unacknowledged delivery. The
// owner process exits, so recovery must use the durable lease and attempt budget.
func (a *application) snapshot(ctx context.Context) error {
	if err := a.migrate(ctx); err != nil {
		return err
	}
	execution, err := a.start(ctx, fmt.Sprintf("snapshot-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err != nil {
		return err
	}
	claim, err := a.engine.ClaimActivity(ctx, "snapshot-owner", 500*time.Millisecond)
	if err != nil {
		return err
	}
	if claim.WorkflowID != execution.ID {
		return errors.New("snapshot requires a database without other ready work")
	}
	if _, err := a.pool.Exec(ctx, `INSERT INTO example_effects(effect_key,workflow_id,activity,payload) VALUES($1,$2,$3,$4)`, "durable:"+claim.ActivityID, execution.ID, claim.ActivityName, []byte(`{"account_id":"account-42"}`)); err != nil {
		return err
	}
	fmt.Println(execution.ID)
	return nil
}

func (a *application) assertRecovered(ctx context.Context, id string, children []*childProcess) error {
	if err := a.wait(ctx, id, children); err != nil {
		return err
	}
	activities, err := a.engine.Activities(ctx, id)
	if err != nil {
		return err
	}
	if len(activities) != 2 {
		return errors.New("restored plan lost activities")
	}
	attempts, err := a.engine.Attempts(ctx, activities[0].ID)
	if err != nil || len(attempts) != 2 || attempts[0].State != durable.AttemptAbandoned || attempts[1].State != durable.AttemptCompleted {
		return fmt.Errorf("abandoned owner recovery: %#v %v", attempts, err)
	}
	var effects int
	if err := a.pool.QueryRow(ctx, `SELECT count(*) FROM example_effects WHERE workflow_id=$1`, id).Scan(&effects); err != nil {
		return err
	}
	if effects != 2 {
		return fmt.Errorf("deduplicated effects = %d, want 2", effects)
	}
	result, err := sendEmail.WorkflowResult(ctx, a.engine, id)
	if err != nil || result.Status != "sent" || result.AccountID != "account-42" {
		return fmt.Errorf("recovered result: %#v %v", result, err)
	}
	fmt.Println("recovery: abandoned then completed, 2 deliveries share 1 account effect, successor completed")
	return nil
}

func (a *application) verifyRestored(ctx context.Context, id string) (result error) {
	if err := a.engine.CheckSchema(ctx); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	var children []*childProcess
	defer func() {
		for _, child := range children {
			_ = child.stdin.Close()
		}
		for _, child := range children {
			if !child.waited {
				result = errors.Join(result, <-child.done)
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
	return a.assertRecovered(ctx, id, children)
}
