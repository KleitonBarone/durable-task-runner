package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	durable "github.com/kleiton/durable-task-runner"
)

type welcomeInput struct {
	AccountID string `json:"account_id"`
	Email     string `json:"email"`
}

type accountActivity struct {
	AccountID string `json:"account_id"`
}

type emailActivity struct {
	Email   string `json:"email"`
	Subject string `json:"subject"`
}

var welcomeWorkflow = durable.Define("welcome_customer", func(input welcomeInput) ([]durable.ActivitySpec, error) {
	createAccount, err := durable.NewActivity("create_account", accountActivity{AccountID: input.AccountID})
	if err != nil {
		return nil, err
	}
	sendEmail, err := durable.NewActivity("send_welcome_email", emailActivity{
		Email: input.Email, Subject: "Welcome",
	})
	if err != nil {
		return nil, err
	}
	return []durable.ActivitySpec{createAccount, sendEmail}, nil
})

func main() {
	ctx := context.Background()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	registry := durable.NewRegistry()
	if err := durable.Register(registry, welcomeWorkflow); err != nil {
		log.Fatal(err)
	}
	engine, err := durable.Open(ctx, databaseURL, registry, durable.SystemClock{})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	execution, err := durable.Start(ctx, engine, welcomeWorkflow, welcomeInput{
		AccountID: "account-42",
		Email:     "customer@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}

	worker, err := durable.NewWorker(engine, durable.WorkerConfig{
		ID:                "example-worker",
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		PollInterval:      100 * time.Millisecond,
	},
		durable.Handle("create_account", func(_ context.Context, input accountActivity) (map[string]string, error) {
			return map[string]string{"account_id": input.AccountID, "status": "created"}, nil
		}),
		durable.Handle("send_welcome_email", func(_ context.Context, input emailActivity) (map[string]string, error) {
			return map[string]string{"email": input.Email, "status": "sent"}, nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	for {
		worked, err := worker.RunOnce(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if !worked {
			break
		}
	}

	timeline, err := engine.Timeline(ctx, execution.ID)
	if err != nil {
		log.Fatal(err)
	}
	for i, event := range timeline {
		fmt.Printf("%02d  %-20s %s\n", i+1, event.Type, event.Details)
	}
}
