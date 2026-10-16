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
	definition := durable.Define("external_consumer", func(input string) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("echo", input)
		return []durable.ActivitySpec{step}, err
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
	worker, err := durable.NewWorker(engine, durable.WorkerConfig{ID: "consumer", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond, PollInterval: time.Millisecond}, durable.Handle("echo", func(_ context.Context, input string) (string, error) { return input, nil }))
	if err != nil {
		return err
	}
	if _, err := worker.RunOnce(ctx); err != nil {
		return err
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil {
		return err
	}
	if result.State != durable.WorkflowCompleted {
		return fmt.Errorf("consumer workflow state: %s", result.State)
	}
	fmt.Println("external consumer completed")
	return nil
}
