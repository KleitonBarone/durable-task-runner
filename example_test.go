package durable_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func ExampleDefineActivity() {
	double := durable.DefineActivity[int, int]("double").
		WithVersion("1", "integer/1", "integer/1")
	step, err := double.Step("double-input", 21,
		durable.WithAttemptTimeout(time.Minute),
		durable.WithRetry(durable.RetryPolicy{MaxAttempts: 3, RetryErrors: true,
			InitialInterval: time.Second, MaximumInterval: time.Minute}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(step.Name, step.Key, string(step.Input))
	// Output: double double-input 21
}

// This example needs PostgreSQL. The repository's cmd/example also runs worker
// and scheduler processes, reads typed results, and demonstrates deduplication.
func ExampleStart() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	double := durable.DefineActivity[int, int]("double")
	definition := durable.Define("calculate", func(input int) ([]durable.ActivitySpec, error) {
		step, err := double.Step("double-input", input)
		return []durable.ActivitySpec{step}, err
	})
	registry := durable.NewRegistry()
	if err := durable.Register(registry, definition); err != nil {
		log.Fatal(err)
	}
	engine, err := durable.Open(ctx, os.Getenv("DATABASE_URL"), registry, durable.SystemClock{})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	execution, err := durable.Start(ctx, engine, definition, 21,
		durable.WithStartKey("calculation/42"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(execution.ID)
	// Application workers register double.Handle and run alongside one scheduler.
}
