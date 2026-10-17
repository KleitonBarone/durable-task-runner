package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/jackc/pgx/v5/pgxpool"
)

type welcomeInput struct {
	AccountID string `json:"account_id"`
	Email     string `json:"email"`
}
type accountInput struct {
	AccountID string `json:"account_id"`
}
type accountResult struct {
	AccountID string `json:"account_id"`
}
type emailInput struct {
	Email string `json:"email"`
}
type welcomeResult struct {
	AccountID string `json:"account_id"`
	Email     string `json:"email"`
	Status    string `json:"status"`
}

var createAccount = durable.DefineActivity[accountInput, accountResult]("welcome.create_account")
var sendEmail = durable.DefineActivity[emailInput, welcomeResult]("welcome.send_email")

var welcomeWorkflow = durable.Define("welcome_customer", func(input welcomeInput) ([]durable.ActivitySpec, error) {
	if strings.TrimSpace(input.AccountID) == "" || strings.TrimSpace(input.Email) == "" {
		return nil, errors.New("account ID and email are required")
	}
	account, err := createAccount.Step("account", accountInput{AccountID: input.AccountID},
		durable.WithRetry(durable.RetryPolicy{MaxAttempts: 3, RetryErrors: true, InitialInterval: time.Second}),
		durable.WithAttemptTimeout(30*time.Second))
	if err != nil {
		return nil, err
	}
	email, err := sendEmail.Step("welcome", emailInput{Email: input.Email})
	return []durable.ActivitySpec{account, email}, err
})

// fakeEffect models a service that atomically records an effect and its key.
// It creates no real account and sends no email. Replacement attempts read the
// first committed result. A real service must implement its own equivalent.
func fakeEffect[Output any](ctx context.Context, pool *pgxpool.Pool, output Output) (Output, error) {
	var zero Output
	metadata, ok := durable.Metadata(ctx)
	if !ok {
		return zero, errors.New("handler metadata is required")
	}
	raw, err := json.Marshal(output)
	if err != nil {
		return zero, err
	}
	if _, err := pool.Exec(ctx, `INSERT INTO example_effects (effect_key, workflow_id, activity, payload) VALUES ($1, $2, $3, $4) ON CONFLICT (effect_key) DO NOTHING`, metadata.EffectKey, metadata.WorkflowID, metadata.ActivityName, raw); err != nil {
		return zero, err
	}
	if err := pool.QueryRow(ctx, `SELECT payload FROM example_effects WHERE effect_key=$1`, metadata.EffectKey).Scan(&raw); err != nil {
		return zero, err
	}
	if err := json.Unmarshal(raw, &output); err != nil {
		return zero, err
	}
	return output, nil
}
