package main

import (
	"context"
	"fmt"
	"os"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	echo := durable.DefineActivity[string, string]("echo")
	definition := durable.Define("external_consumer", func(input string) ([]durable.ActivitySpec, error) {
		first, err := echo.Step("first", input)
		if err != nil {
			return nil, err
		}
		second, err := echo.Step("second", input+"!")
		return []durable.ActivitySpec{first, second}, err
	})
	registry := durable.NewRegistry()
	if err := durable.Register(registry, definition); err != nil {
		return err
	}
	engine, err := durable.Open(ctx, os.Getenv("DATABASE_URL"), registry, durable.SystemClock{})
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := engine.Migrate(ctx); err != nil {
		return err
	}
	execution, err := durable.Start(ctx, engine, definition, "hello")
	if err != nil {
		return err
	}
	worker, err := durable.NewWorker(engine, durable.WorkerConfig{ID: "consumer", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond, PollInterval: time.Millisecond}, echo.Handle(func(ctx context.Context, input string) (string, error) {
		metadata, ok := durable.Metadata(ctx)
		if !ok || metadata.WorkflowID != execution.ID || metadata.StepKey == "" || metadata.AttemptNumber != 1 || metadata.EffectKey == "" {
			return "", fmt.Errorf("invalid handler metadata: %#v", metadata)
		}
		if input == "hello" {
			return input, nil
		}
		previous, err := echo.Result(ctx, "first")
		return previous + input, err
	}))
	if err != nil {
		return err
	}
	for range 2 {
		if _, err := worker.RunOnce(ctx); err != nil {
			return err
		}
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil {
		return err
	}
	if result.State != durable.WorkflowCompleted {
		return fmt.Errorf("consumer workflow state: %s", result.State)
	}
	output, err := echo.WorkflowResult(ctx, engine, execution.ID)
	if err != nil || output != "hellohello!" {
		return fmt.Errorf("consumer result %q: %v", output, err)
	}
	fmt.Println("external consumer completed")
	return nil
}
