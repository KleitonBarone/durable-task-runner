// Command example demonstrates application-owned startup, workers, and recovery.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/jackc/pgx/v5/pgxpool"
)

type application struct {
	pool      *pgxpool.Pool
	engine    *durable.Engine
	worker    *durable.Worker
	scheduler *durable.Scheduler
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	role := flag.String("role", "demo", "demo, migrate, start, status, worker, scheduler, verify, snapshot, or restore-verify")
	id := flag.String("id", "", "workflow ID for status")
	key := flag.String("key", "welcome/account-42", "caller start key")
	stdinShutdown := flag.Bool("stdin-shutdown", false, "stop a child runtime when stdin closes")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *stdinShutdown {
		go func() { _, _ = io.Copy(io.Discard, os.Stdin); stop() }()
	}
	if *role == "demo" || *role == "verify" || *role == "snapshot" || *role == "restore-verify" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("DATABASE_URL is required")
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}
	config.MaxConns, config.ConnConfig.ConnectTimeout = 4, 5*time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close() // This application owns the pool supplied to durable.New.
	registry := durable.NewRegistry()
	if err := durable.Register(registry, welcomeWorkflow); err != nil {
		return err
	}
	engine, err := durable.New(pool, registry, durable.SystemClock{})
	if err != nil {
		return err
	}
	worker, err := durable.NewWorker(engine, durable.WorkerConfig{ID: fmt.Sprintf("example-%d", os.Getpid())},
		createAccount.Handle(func(ctx context.Context, input accountInput) (accountResult, error) {
			return fakeEffect(ctx, pool, accountResult{AccountID: input.AccountID})
		}),
		deleteAccount.Handle(func(ctx context.Context, _ accountInput) (accountResult, error) {
			account, err := createAccount.Result(ctx, "account")
			if err != nil {
				return accountResult{}, err
			}
			return fakeEffect(ctx, pool, account)
		}),
		sendEmail.Handle(func(ctx context.Context, input emailInput) (welcomeResult, error) {
			account, err := createAccount.Result(ctx, "account")
			if err != nil {
				return welcomeResult{}, err
			}
			return fakeEffect(ctx, pool, welcomeResult{AccountID: account.AccountID, Email: input.Email, Status: "sent"})
		}),
	)
	if err != nil {
		return err
	}
	scheduler, err := durable.NewScheduler(engine, 0, 0)
	if err != nil {
		return err
	}
	app := &application{pool: pool, engine: engine, worker: worker, scheduler: scheduler}
	switch *role {
	case "migrate":
		return app.migrate(ctx)
	case "worker":
		return runtimeExit(worker.Run(ctx))
	case "scheduler":
		return runtimeExit(scheduler.Run(ctx))
	case "start":
		execution, err := app.start(ctx, *key)
		if err == nil {
			fmt.Println(execution.ID)
		}
		return err
	case "status":
		if *id == "" {
			return errors.New("status requires -id")
		}
		return app.printExecution(ctx, *id)
	case "demo":
		if err := app.migrate(ctx); err != nil {
			return err
		}
		return app.demo(ctx, *key)
	case "verify":
		if err := app.migrate(ctx); err != nil {
			return err
		}
		return app.verify(ctx)
	case "snapshot":
		return app.snapshot(ctx)
	case "restore-verify":
		if *id == "" {
			return errors.New("restore-verify requires -id")
		}
		return app.verifyRestored(ctx, *id)
	default:
		return fmt.Errorf("unknown role %q", *role)
	}
}

func runtimeExit(err error) error {
	if errors.Is(err, durable.ErrHandlerUnresponsive) {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (a *application) migrate(ctx context.Context) error {
	if err := a.engine.Migrate(ctx); err != nil {
		return err
	}
	// Application-owned fake effects commit independently of engine acknowledgements.
	_, err := a.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS example_effects (effect_key text PRIMARY KEY, workflow_id text NOT NULL, activity text NOT NULL, payload jsonb NOT NULL)`)
	return err
}

func (a *application) start(ctx context.Context, key string) (durable.WorkflowExecution, error) {
	return durable.Start(ctx, a.engine, welcomeWorkflow, welcomeInput{AccountID: "account-42", Email: "customer@example.com"}, durable.WithStartKey(key))
}

func (a *application) demo(ctx context.Context, key string) (result error) {
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan error, 2)
	for _, run := range []func(context.Context) error{a.worker.Run, a.scheduler.Run} {
		go func() {
			err := run(ctx)
			if runtimeExit(err) != nil {
				cancel(err)
			}
			done <- err
		}()
	}
	defer func() {
		cancel(context.Canceled)
		for range 2 {
			if err := runtimeExit(<-done); err != nil {
				result = errors.Join(result, err)
			}
		}
	}()
	execution, err := a.start(ctx, key)
	if err != nil {
		return err
	}
	if err := a.wait(ctx, execution.ID, nil); err != nil {
		return err
	}
	return a.printExecution(ctx, execution.ID)
}

func (a *application) wait(ctx context.Context, id string, children []*childProcess) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, child := range children {
			select {
			case err := <-child.done:
				child.waited = true
				return fmt.Errorf("%s exited before completion: %v", child.role, err)
			default:
			}
		}
		execution, err := a.engine.Workflow(ctx, id)
		if err != nil {
			return err
		}
		if execution.State == durable.WorkflowCompleted {
			return nil
		}
		if execution.State == durable.WorkflowFailed {
			return fmt.Errorf("workflow %s failed; inspect its timeline and attempts", id)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (a *application) printExecution(ctx context.Context, id string) error {
	execution, err := a.engine.Workflow(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("workflow %s: %s\n", id, execution.State)
	timeline, err := a.engine.Timeline(ctx, id)
	if err != nil {
		return err
	}
	for i, event := range timeline {
		fmt.Printf("%02d  %-20s %s\n", i+1, event.Type, event.Details)
	}
	if execution.State == durable.WorkflowCompleted {
		result, err := sendEmail.WorkflowResult(ctx, a.engine, id)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	return nil
}
