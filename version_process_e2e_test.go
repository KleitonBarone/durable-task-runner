//go:build e2e

package durable_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

var processOldActivity = durable.DefineActivity[string, string]("versioned-effect").WithVersion("1", "text/1", "text/1")
var processNewActivity = durable.DefineActivity[int, int]("versioned-effect").WithVersion("2", "number/1", "number/1")

func runVersionWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, engine *durable.Engine, id, version string, barrier func() error) {
	t.Helper()
	effect := func(ctx context.Context) error {
		metadata, ok := durable.Metadata(ctx)
		if !ok {
			return errors.New("missing metadata")
		}
		if _, err := pool.Exec(ctx, `INSERT INTO fixture_effects(worker,step,effect_key) VALUES($1,$2,$3) ON CONFLICT(effect_key) DO NOTHING`, id, metadata.ActivityVersion, metadata.EffectKey); err != nil {
			return err
		}
		if version == "1-crash" {
			if err := barrier(); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	var handler durable.HandlerRegistration
	if strings.HasPrefix(version, "1") {
		handler = processOldActivity.Handle(func(ctx context.Context, in string) (string, error) { return in + "-v1", effect(ctx) })
	} else {
		handler = processNewActivity.Handle(func(ctx context.Context, in int) (int, error) { return in * 2, effect(ctx) })
	}
	worker, err := durable.NewWorker(engine, durable.WorkerConfig{ID: id, LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond, PollInterval: 20 * time.Millisecond}, handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestProcessHandlerUpgradePreservesOldExecution(t *testing.T) {
	pool := testdb.Pool(t)
	registry := durable.NewRegistry()
	old := durable.Define("versioned-process", func(in string) ([]durable.ActivitySpec, error) {
		step, err := processOldActivity.Step("effect", in)
		return []durable.ActivitySpec{step}, err
	}).WithVersion("1", "text/1")
	newer := durable.Define("versioned-process", func(in int) ([]durable.ActivitySpec, error) {
		step, err := processNewActivity.Step("effect", in)
		return []durable.ActivitySpec{step}, err
	}).WithVersion("2", "number/1")
	if err := durable.Register(registry, old); err != nil {
		t.Fatal(err)
	}
	if err := durable.Register(registry, newer); err != nil {
		t.Fatal(err)
	}
	engine, err := durable.New(pool, registry, durable.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `CREATE TABLE fixture_barriers(worker text PRIMARY KEY);CREATE TABLE fixture_effects(worker text,step text,effect_key text UNIQUE)`); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(os.Getenv("DURABLE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
	parsed.RawQuery = query.Encode()
	databaseURL := parsed.String()
	first, err := durable.Start(t.Context(), engine, old, "old", durable.WithStartKey("shared"))
	if err != nil {
		t.Fatal(err)
	}
	crashed := startFixture(t, "version-worker", "version-old-A", databaseURL, "1-crash", durable.Claim{})
	waitProcessState(t, []*fixtureProcess{crashed}, func() bool {
		var ready bool
		err := pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM fixture_barriers WHERE worker='version-old-A')`).Scan(&ready)
		return err == nil && ready
	})
	crashed.kill(t)
	second, err := durable.Start(t.Context(), engine, newer, 21, durable.WithStartKey("shared"))
	if err != nil {
		t.Fatal(err)
	}
	scheduler := startFixture(t, "scheduler", "upgrade-scheduler", databaseURL, "", durable.Claim{})
	newWorker := startFixture(t, "version-worker", "version-new", databaseURL, "2", durable.Claim{})
	waitProcessState(t, []*fixtureProcess{scheduler, newWorker}, func() bool {
		state, err := engine.Workflow(t.Context(), second.ID)
		if err != nil || state.State != durable.WorkflowCompleted {
			return false
		}
		activities, err := engine.Activities(t.Context(), first.ID)
		return err == nil && activities[0].State == durable.ActivityReady
	})
	activities, err := engine.Activities(t.Context(), first.ID)
	if err != nil || activities[0].AttemptCount != 1 {
		t.Fatalf("new handler consumed old attempt: %#v %v", activities, err)
	}
	oldWorker := startFixture(t, "version-worker", "version-old-B", databaseURL, "1", durable.Claim{})
	waitProcessState(t, []*fixtureProcess{scheduler, newWorker, oldWorker}, func() bool {
		state, err := engine.Workflow(t.Context(), first.ID)
		return err == nil && state.State == durable.WorkflowCompleted
	})
	if out, err := processOldActivity.WorkflowResult(t.Context(), engine, first.ID); err != nil || out != "old-v1" {
		t.Fatalf("old output: %q %v", out, err)
	}
	if out, err := processNewActivity.WorkflowResult(t.Context(), engine, second.ID); err != nil || out != 42 {
		t.Fatalf("new output: %d %v", out, err)
	}
	attempts, err := engine.Attempts(t.Context(), activities[0].ID)
	if err != nil || len(attempts) != 2 || attempts[0].State != durable.AttemptAbandoned {
		t.Fatalf("upgraded recovery: %#v %v", attempts, err)
	}
	var effects int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM fixture_effects`).Scan(&effects); err != nil || effects != 2 {
		t.Fatalf("version effects: %d %v", effects, err)
	}
	oldWorker.stop(t)
	newWorker.stop(t)
	scheduler.stop(t)
	t.Log("old owner crashed after effect; new handler finished only v2; returning v1 handler recovered the stored old plan with one effect")
}
