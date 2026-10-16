//go:build e2e

package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

var processDefinition = durable.Define("process-recovery", func(input string) ([]durable.ActivitySpec, error) {
	first, err := durable.NewActivity("effect", input)
	if err != nil {
		return nil, err
	}
	last, err := durable.NewActivity("finish", input)
	return []durable.ActivitySpec{first, last}, err
})

func processRegistry(t *testing.T) *durable.Registry {
	t.Helper()
	registry := durable.NewRegistry()
	if err := durable.Register(registry, processDefinition); err != nil {
		t.Fatal(err)
	}
	return registry
}

// TestProcessFixture runs only in explicitly selected child processes. Barriers
// and fake effects belong to the application fixture, not to runtime APIs.
func TestProcessFixture(t *testing.T) {
	role := os.Getenv("DURABLE_PROCESS_ROLE")
	if role == "" {
		return
	}
	if role == "invalid" {
		t.Fatal("injected child startup failure")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); cancel() }()
	pool, err := pgxpool.New(ctx, os.Getenv("DURABLE_PROCESS_DATABASE_URL"))
	if err != nil {
		t.Fatal("open fixture database")
	}
	defer pool.Close()
	engine, err := durable.New(pool, processRegistry(t), durable.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	id := os.Getenv("DURABLE_PROCESS_ID")
	barrier := func() error {
		_, err := pool.Exec(ctx, `INSERT INTO fixture_barriers (worker) VALUES ($1)`, id)
		return err
	}
	switch role {
	case "scheduler":
		scheduler, err := durable.NewScheduler(engine, 20*time.Millisecond, 100)
		if err != nil {
			t.Fatal(err)
		}
		if err := barrier(); err != nil {
			t.Fatal(err)
		}
		err = scheduler.Run(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("scheduler returned: %v", err)
		}
	case "worker":
		window := os.Getenv("DURABLE_PROCESS_WINDOW")
		handler := func(step string) durable.HandlerRegistration {
			return durable.Handle(step, func(ctx context.Context, input string) (string, error) {
				if step == "effect" && window == "before" {
					if err := barrier(); err != nil {
						return "", err
					}
					<-ctx.Done()
					return "", ctx.Err()
				}
				metadata, ok := durable.Metadata(ctx)
				if !ok {
					return "", errors.New("missing activity metadata")
				}
				if _, err := pool.Exec(ctx, `INSERT INTO fixture_deliveries (step, effect_key, attempt, attempt_id, workflow_id, step_key) VALUES ($1, $2, $3, $4, $5, $6)`, step, metadata.EffectKey, metadata.AttemptNumber, metadata.AttemptID, metadata.WorkflowID, metadata.StepKey); err != nil {
					return "", err
				}
				var effectKey *string
				if window == "dedup" || window == "dedup-recover" {
					effectKey = &metadata.EffectKey
				}
				if _, err := pool.Exec(ctx, `INSERT INTO fixture_effects (worker, step, effect_key) VALUES ($1, $2, $3) ON CONFLICT (effect_key) DO NOTHING`, id, step, effectKey); err != nil {
					return "", err
				}
				if step == "effect" && (window == "after" || window == "dedup") {
					if err := barrier(); err != nil {
						return "", err
					}
					<-ctx.Done()
					return "", ctx.Err()
				}
				return input, nil
			})
		}
		worker, err := durable.NewWorker(engine, durable.WorkerConfig{
			ID: id, LeaseDuration: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond, PollInterval: 20 * time.Millisecond,
		}, handler("effect"), handler("finish"))
		if err != nil {
			t.Fatal(err)
		}
		if err := worker.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned: %v", err)
		}
	case "stale":
		var claim durable.Claim
		if err := json.Unmarshal([]byte(os.Getenv("DURABLE_PROCESS_CLAIM")), &claim); err != nil {
			t.Fatal(err)
		}
		if err := engine.CompleteActivity(ctx, claim, json.RawMessage(`"stale"`)); !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("stale acknowledgement = %v", err)
		}
		if _, err := engine.Heartbeat(ctx, claim, time.Second); !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("stale heartbeat = %v", err)
		}
	default:
		t.Fatalf("unknown process role %q", role)
	}
}

type fixtureProcess struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  chan struct{}
	err   error
	log   *os.File
}

func startFixture(t *testing.T, role, id, databaseURL, window string, claim durable.Claim) *fixtureProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	claimJSON, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestProcessFixture$", "-test.v", "-test.timeout=25s")
	cmd.Env = append(os.Environ(), "DURABLE_PROCESS_ROLE="+role, "DURABLE_PROCESS_ID="+id,
		"DURABLE_PROCESS_DATABASE_URL="+databaseURL, "DURABLE_PROCESS_WINDOW="+window, "DURABLE_PROCESS_CLAIM="+string(claimJSON))
	log, err := os.CreateTemp(t.TempDir(), id+"-*.log")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		_ = log.Close()
		t.Fatal(err)
	}
	p := &fixtureProcess{cmd: cmd, stdin: stdin, done: make(chan struct{}), log: log}
	t.Cleanup(func() {
		cancel()
		_ = stdin.Close()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Errorf("fixture %s did not terminate", id)
		}
		_ = log.Close()
		if t.Failed() {
			output, _ := os.ReadFile(log.Name())
			t.Logf("%s process output:\n%s", id, output)
		}
	})
	if err := cmd.Start(); err != nil {
		close(p.done)
		t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	return p
}

func (p *fixtureProcess) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(10 * time.Second):
		t.Fatal("fixture process did not finish")
		return nil
	}
}

func (p *fixtureProcess) stop(t *testing.T) {
	t.Helper()
	_ = p.stdin.Close()
	if err := p.wait(t); err != nil {
		t.Fatalf("graceful fixture shutdown: %v", err)
	}
}

func (p *fixtureProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := p.wait(t); err == nil {
		t.Fatal("killed fixture exited successfully")
	}
}

func waitProcessState(t *testing.T, processes []*fixtureProcess, predicate func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !predicate() {
		for _, process := range processes {
			select {
			case <-process.done:
				t.Fatalf("fixture exited before progress: %v", process.err)
			default:
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for process state")
		case <-ticker.C:
		}
	}
}

func TestProcessCrashRecovery(t *testing.T) {
	for _, window := range []string{"before", "after", "shutdown", "dedup"} {
		t.Run(window, func(t *testing.T) {
			pool := testdb.Pool(t)
			engine, err := durable.New(pool, processRegistry(t), durable.SystemClock{})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Migrate(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(t.Context(), `CREATE TABLE fixture_barriers (worker text PRIMARY KEY);
				CREATE TABLE fixture_effects (worker text NOT NULL, step text NOT NULL, effect_key text UNIQUE);
				CREATE TABLE fixture_deliveries (step text, effect_key text, attempt int, attempt_id text, workflow_id text, step_key text)`); err != nil {
				t.Fatal(err)
			}
			execution, err := durable.Start(t.Context(), engine, processDefinition, "persisted input")
			if err != nil {
				t.Fatal(err)
			}
			parsedURL, err := url.Parse(os.Getenv("DURABLE_TEST_DATABASE_URL"))
			if err != nil {
				t.Fatal("parse test database URL")
			}
			query := parsedURL.Query()
			query.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
			parsedURL.RawQuery = query.Encode()
			databaseURL := parsedURL.String()
			scheduler := startFixture(t, "scheduler", "scheduler-A", databaseURL, "", durable.Claim{})
			barrierWindow := window
			if window == "shutdown" {
				barrierWindow = "after"
			}
			workerA := startFixture(t, "worker", "worker-A", databaseURL, barrierWindow, durable.Claim{})
			waitProcessState(t, []*fixtureProcess{scheduler, workerA}, func() bool {
				var ready bool
				err := pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM fixture_barriers WHERE worker='worker-A')`).Scan(&ready)
				return err == nil && ready
			})
			var stale durable.Claim
			if err := pool.QueryRow(t.Context(), `SELECT l.token, l.activity_id, l.attempt_id
				FROM leases l JOIN activities a ON a.id=l.activity_id
				WHERE a.workflow_id=$1 AND l.state='active'`, execution.ID).Scan(&stale.LeaseToken, &stale.ActivityID, &stale.AttemptID); err != nil {
				t.Fatal(err)
			}
			if window == "shutdown" {
				workerA.stop(t)
			} else {
				workerA.kill(t)
			}
			recoveryWindow := ""
			if window == "dedup" {
				recoveryWindow = "dedup-recover"
			}
			workerB := startFixture(t, "worker", "worker-B", databaseURL, recoveryWindow, durable.Claim{})
			waitProcessState(t, []*fixtureProcess{scheduler, workerB}, func() bool {
				state, err := engine.Workflow(t.Context(), execution.ID)
				return err == nil && state.State == durable.WorkflowCompleted
			})
			staleWorker := startFixture(t, "stale", "stale-worker", databaseURL, "", stale)
			if err := staleWorker.wait(t); err != nil {
				t.Fatalf("stale-claim check failed: %v", err)
			}
			var effects int
			if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM fixture_effects WHERE step='effect'`).Scan(&effects); err != nil {
				t.Fatal(err)
			}
			wantEffects := 1
			if window != "before" && window != "dedup" {
				wantEffects = 2
			}
			if effects != wantEffects {
				t.Fatalf("effects = %d, want %d", effects, wantEffects)
			}
			if window == "dedup" {
				var deliveries, keys, attempts, workflows, steps int
				err := pool.QueryRow(t.Context(), `SELECT count(*), count(DISTINCT effect_key), count(DISTINCT attempt_id), count(DISTINCT workflow_id), count(DISTINCT step_key) FROM fixture_deliveries WHERE step='effect' AND workflow_id=$1 AND step_key='step-0' AND attempt IN (1,2)`, execution.ID).Scan(&deliveries, &keys, &attempts, &workflows, &steps)
				if err != nil || deliveries != 2 || keys != 1 || attempts != 2 || workflows != 1 || steps != 1 {
					t.Fatalf("deduplicated deliveries: %d keys=%d attempts=%d workflows=%d steps=%d err=%v", deliveries, keys, attempts, workflows, steps, err)
				}
			}
			var states []string
			rows, err := pool.Query(t.Context(), `SELECT state FROM attempts WHERE activity_id=$1 ORDER BY number`, stale.ActivityID)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var state string
				if err := rows.Scan(&state); err != nil {
					t.Fatal(err)
				}
				states = append(states, state)
			}
			rows.Close()
			if rows.Err() != nil || !reflect.DeepEqual(states, []string{"abandoned", "completed"}) {
				t.Fatalf("attempt states = %v, err = %v", states, rows.Err())
			}
			workerB.stop(t)
			scheduler.kill(t)
			restarted := startFixture(t, "scheduler", "scheduler-B", databaseURL, "", durable.Claim{})
			waitProcessState(t, []*fixtureProcess{restarted}, func() bool {
				var ready bool
				err := pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM fixture_barriers WHERE worker='scheduler-B')`).Scan(&ready)
				return err == nil && ready
			})
			restarted.stop(t)
			reconnected, err := durable.Open(t.Context(), databaseURL, processRegistry(t), durable.SystemClock{})
			if err != nil {
				t.Fatal(err)
			}
			defer reconnected.Close()
			state, err := reconnected.Workflow(t.Context(), execution.ID)
			if err != nil || state.State != durable.WorkflowCompleted {
				t.Fatalf("reconnected workflow = %#v, err = %v", state, err)
			}
			activities, err := reconnected.Activities(t.Context(), execution.ID)
			if err != nil || len(activities) != 2 || activities[0].State != durable.ActivityCompleted ||
				activities[1].State != durable.ActivityCompleted || activities[0].AttemptCount != 2 || activities[1].AttemptCount != 1 {
				t.Fatalf("reconnected activities = %#v, err = %v", activities, err)
			}
			var activeLeases int
			if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM leases WHERE state='active'`).Scan(&activeLeases); err != nil || activeLeases != 0 {
				t.Fatalf("active leases after completion = %d, err = %v", activeLeases, err)
			}
			timeline, err := reconnected.Timeline(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			types := make([]string, len(timeline))
			for i, event := range timeline {
				types[i] = event.Type
			}
			want := []string{"workflow.started", "activity.scheduled", "activity.scheduled", "activity.claimed",
				"attempt.abandoned", "activity.recovered", "activity.claimed", "activity.completed", "activity.ready",
				"activity.claimed", "activity.completed", "workflow.completed"}
			if !reflect.DeepEqual(types, want) {
				t.Fatalf("timeline = %v", types)
			}
			t.Logf("crash %s effect: attempts=%v effects=%d stale token fenced, scheduler restarted, timeline=%v", window, states, effects, types)
		})
	}
}

func TestProcessHarnessReportsChildFailure(t *testing.T) {
	process := startFixture(t, "invalid", "injected-failure", "", "", durable.Claim{})
	if err := process.wait(t); err == nil {
		t.Fatal("failed child was reported as success")
	}
	fmt.Fprintln(os.Stdout, "injected child failure correctly returned nonzero")
}
