// Command durable migrates and inspects an application-owned engine database.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Getenv("DATABASE_URL"), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "durable:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, databaseURL string, out io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		_, err := fmt.Fprintln(out, "Usage: durable <migrate|status|activities|attempts|leases|timeline|cancel> [flags]\nSet DATABASE_URL. Commands print one bounded JSON page. Use -h for flags.")
		return err
	}
	action := args[0]
	switch action {
	case "migrate", "status", "activities", "attempts", "leases", "timeline", "cancel":
	default:
		return fmt.Errorf("unknown command %q", action)
	}
	flags := flag.NewFlagSet(action, flag.ContinueOnError)
	flags.SetOutput(out)
	id := flags.String("id", "", "workflow ID, or activity ID for attempts/leases")
	state := flags.String("state", "", "workflow state filter")
	definition := flags.String("definition", "", "workflow definition name filter")
	version := flags.String("version", "", "workflow definition version filter")
	reason := flags.String("reason", "", "terminal reason filter, or cancellation reason")
	compensation := flags.String("compensation", "", "compensation state filter")
	actor := flags.String("actor", "", "actor for cancellation")
	limit := flags.Int("limit", 50, "page size, 1-500")
	cursor := flags.String("cursor", "", "next_cursor from the previous page")
	sensitive := flags.Bool("sensitive", false, "include diagnostic error/cancellation text and history details")
	timeout := flags.Duration("timeout", 10*time.Second, "maximum database operation time")
	before := flags.String("before", "", "workflow creation cutoff, RFC3339")
	if err := flags.Parse(args[1:]); errors.Is(err, flag.ErrHelp) {
		return nil
	} else if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *limit < 1 || *limit > 500 || *timeout <= 0 {
		return errors.New("limit must be 1-500 and timeout must be positive")
	}
	if action != "migrate" && action != "status" && *id == "" {
		return errors.New("this command requires -id")
	}
	if action == "cancel" && *actor == "" {
		return errors.New("cancel requires -actor")
	}
	if action == "cancel" && *cursor != "" {
		return errors.New("cancel does not accept a cursor")
	}
	var cutoff *time.Time
	if *before != "" {
		parsed, err := time.Parse(time.RFC3339, *before)
		if err != nil {
			return errors.New("before must be RFC3339")
		}
		cutoff = &parsed
	}
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return errors.New("invalid DATABASE_URL")
	}
	config.MaxConns, config.ConnConfig.ConnectTimeout = 2, 5*time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return errors.New("cannot configure PostgreSQL")
	}
	defer pool.Close()
	engine, err := durable.New(pool, durable.NewRegistry(), durable.SystemClock{})
	if err != nil {
		return err
	}
	request := durable.PageRequest{Limit: *limit, Cursor: *cursor, Sensitive: *sensitive}
	var result any
	switch action {
	case "migrate":
		err = engine.Migrate(ctx)
		result = map[string]string{"schema": "current"}
	case "status":
		result, err = engine.ListWorkflows(ctx, durable.WorkflowFilter{ID: *id, State: durable.WorkflowState(*state), DefinitionName: *definition, DefinitionVersion: *version, TerminalReason: *reason, CompensationState: durable.CompensationState(*compensation), CreatedBefore: cutoff}, request)
	case "activities":
		result, err = engine.ListActivities(ctx, *id, request)
	case "attempts":
		result, err = engine.ListAttempts(ctx, *id, request)
	case "leases":
		result, err = engine.ListLeases(ctx, *id, request)
	case "timeline":
		result, err = engine.ListTimeline(ctx, *id, request)
	case "cancel":
		_, err = engine.Cancel(ctx, *id, *actor, *reason)
		if err == nil {
			result, err = engine.ListWorkflows(ctx, durable.WorkflowFilter{ID: *id}, request)
		}
	}
	if err != nil {
		for _, safe := range []error{durable.ErrInvalidInput, durable.ErrNotFound, durable.ErrIncompatibleSchema} {
			if errors.Is(err, safe) {
				return safe
			}
		}
		return errors.New("database request failed; check connectivity and schema")
	}
	return json.NewEncoder(out).Encode(result)
}
