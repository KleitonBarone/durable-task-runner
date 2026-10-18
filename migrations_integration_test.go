//go:build integration

package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func oldSchema(t *testing.T, count int) *pgxpool.Pool {
	t.Helper()
	pool := testdb.Pool(t)
	if _, err := pool.Exec(t.Context(), `CREATE TABLE schema_migrations(name text PRIMARY KEY,applied_at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	known, err := migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range known[:count] {
		if _, err := pool.Exec(t.Context(), m.sql); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(t.Context(), `INSERT INTO schema_migrations VALUES($1,clock_timestamp())`, m.name); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func TestMigrateEveryPriorSchemaPreservesActiveOwnership(t *testing.T) {
	known, _ := migrations()
	for count := 1; count < len(known); count++ {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			pool := oldSchema(t, count)
			now := time.Unix(1000, 0).UTC()
			wid, _ := newID()
			aid, _ := newID()
			atid, _ := newID()
			lid, _ := newID()
			token, _ := newID()
			statements := []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO workflow_executions(id,definition_name,input,state,created_at,updated_at) VALUES($1,'legacy','{}','running',$2,$2)`, []any{wid, now}},
				{`INSERT INTO activities(id,workflow_id,position,name,input,state,attempt_count,created_at,updated_at) VALUES($1,$2,0,'legacy-step','{}','running',1,$3,$3)`, []any{aid, wid, now}},
				{`INSERT INTO attempts(id,activity_id,number,worker_id,state,started_at) VALUES($1,$2,1,'old-worker','running',$3)`, []any{atid, aid, now}},
				{`INSERT INTO leases(id,activity_id,attempt_id,token,worker_id,state,acquired_at,heartbeat_at,expires_at) VALUES($1,$2,$3,$4,'old-worker','active',$5,$5,$6)`, []any{lid, aid, atid, token, now, now.Add(time.Minute)}},
				{`INSERT INTO execution_history(workflow_id,activity_id,attempt_id,event_type,details,recorded_at) VALUES($1,$2,$3,'activity.claimed','{"attempt":1}',$4)`, []any{wid, aid, atid, now}},
			}
			// Columns added after the base schema have no legacy insert defaults.
			if count >= 5 {
				statements[1].sql = `INSERT INTO activities(id,workflow_id,position,name,input,state,attempt_count,created_at,updated_at,available_at) VALUES($1,$2,0,'legacy-step','{}','running',1,$3,$3,$3)`
			}
			if count >= 6 {
				statements[0].sql = `INSERT INTO workflow_executions(id,definition_name,input,state,created_at,updated_at,deadline_at) VALUES($1,'legacy','{}','running',$2,$2,$2::timestamptz+interval '24 hours')`
				statements[2].sql = `INSERT INTO attempts(id,activity_id,number,worker_id,state,started_at,deadline_at) VALUES($1,$2,1,'old-worker','running',$3,$3::timestamptz+interval '5 minutes')`
			}
			if count >= 2 {
				statements[1].sql = strings.Replace(statements[1].sql, "updated_at)", "updated_at,step_key)", 1)
				statements[1].sql = strings.Replace(statements[1].sql, "available_at)", "available_at,step_key)", 1)
				statements[1].sql = strings.TrimSuffix(statements[1].sql, ")") + ",'legacy-key')"
			}
			for _, s := range statements {
				if _, err := pool.Exec(t.Context(), s.sql, s.args...); err != nil {
					t.Fatal(err)
				}
			}
			clock := NewManualClock(now)
			engine, _ := New(pool, NewRegistry(), clock)
			if _, err := engine.ClaimActivity(t.Context(), "before-upgrade", time.Second); !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatalf("old schema accepted: %v", err)
			}
			if err := engine.Migrate(t.Context()); err != nil {
				t.Fatal(err)
			}
			claim := Claim{WorkflowID: wid, ActivityID: aid, AttemptID: atid, LeaseID: lid, LeaseToken: token, WorkerID: "old-worker", AttemptNumber: 1}
			if _, err := engine.Heartbeat(t.Context(), claim, time.Minute); err != nil {
				t.Fatalf("preserved ownership: %v", err)
			}
			activities, err := engine.Activities(t.Context(), wid)
			if err != nil || activities[0].Version != "1" || activities[0].InputSchema != "legacy" {
				t.Fatalf("backfill: %#v %v", activities, err)
			}
			clock.Advance(time.Minute)
			if n, err := engine.RecoverAbandoned(t.Context(), 10); err != nil || n != 1 {
				t.Fatalf("recovery: %d %v", n, err)
			}
			replacement, err := engine.ClaimActivity(t.Context(), "new-worker", time.Minute)
			if err != nil || replacement.AttemptNumber != 2 {
				t.Fatalf("replacement: %#v %v", replacement, err)
			}
			if err := engine.CompleteActivity(t.Context(), claim, []byte(`null`)); !errors.Is(err, ErrLeaseLost) {
				t.Fatal(err)
			}
			if err := engine.CompleteActivity(t.Context(), replacement, []byte(`"done"`)); err != nil {
				t.Fatal(err)
			}
			history, err := engine.Timeline(t.Context(), wid)
			if err != nil || len(history) < 2 || history[0].Type != "activity.claimed" {
				t.Fatalf("legacy history: %#v %v", history, err)
			}
		})
	}
}

func TestMigrationCatalogGuards(t *testing.T) {
	for _, mode := range []string{"unknown", "checksum", "gap"} {
		t.Run(mode, func(t *testing.T) {
			engine, definition := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 1)
			execution, err := Start(t.Context(), engine, definition, integrationInput{})
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unknown":
				_, err = engine.pool.Exec(t.Context(), `INSERT INTO schema_migrations(name,applied_at,checksum) VALUES('999_future.sql',clock_timestamp(),'future')`)
			case "checksum":
				_, err = engine.pool.Exec(t.Context(), `UPDATE schema_migrations SET checksum='changed' WHERE name='001_initial.sql'`)
			case "gap":
				_, err = engine.pool.Exec(t.Context(), `DELETE FROM schema_migrations WHERE name='002_step_identity.sql'`)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Migrate(t.Context()); !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatal(err)
			}
			if _, err := engine.ClaimActivity(t.Context(), "unsafe", time.Minute); !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatal(err)
			}
			if _, err := engine.Workflow(t.Context(), execution.ID); !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatal(err)
			}
			var attempts int
			if err := engine.pool.QueryRow(t.Context(), `SELECT count(*) FROM attempts`).Scan(&attempts); err != nil || attempts != 0 {
				t.Fatalf("unsafe claim mutated state: %d %v", attempts, err)
			}
		})
	}
}

func TestConcurrentAndInterruptedMigrations(t *testing.T) {
	pool := oldSchema(t, 9)
	engine, _ := New(pool, NewRegistry(), NewManualClock(time.Unix(1, 0)))
	blocker, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(t.Context(), `LOCK TABLE activities IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- engine.Migrate(ctx) }()
	deadline := time.After(5 * time.Second)
	for {
		var waiting bool
		if err := pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%ALTER TABLE activities%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("migration never reached locked DDL")
		case <-time.After(time.Millisecond * 5):
		}
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("interrupted migration succeeded")
	}
	if err := blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	var applied bool
	if err := pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name='010_versions.sql')`).Scan(&applied); err != nil || applied {
		t.Fatalf("partial migration committed: %v %v", applied, err)
	}
	var column bool
	if err := pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='workflow_executions' AND column_name='definition_version')`).Scan(&column); err != nil || column {
		t.Fatalf("partial DDL committed: %v %v", column, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 6 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- engine.Migrate(t.Context()) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.CheckSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionUpgradeBackfillsKeyedExecution(t *testing.T) {
	pool := oldSchema(t, 11)
	now := time.Unix(1000, 0).UTC()
	clock := NewManualClock(now)
	wid, _ := newID()
	aid, _ := newID()
	if _, err := pool.Exec(t.Context(), `INSERT INTO workflow_executions(id,definition_name,input,state,created_at,updated_at,deadline_at,start_key) VALUES($1,'integration_workflow','{"value":"kept"}','running',$2,$2,$2::timestamptz+interval '24 hours','legacy-key')`, wid, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO activities(id,workflow_id,position,name,input,state,created_at,updated_at,available_at,step_key) VALUES($1,$2,0,'step_1','{"value":"kept"}','ready',$3,$3,$3,'step-0')`, aid, wid, now); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	definition := integrationDefinition(1)
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	engine, _ := New(pool, registry, clock)
	if err := engine.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	duplicate, err := Start(t.Context(), engine, definition, integrationInput{Value: "kept"}, WithStartKey("legacy-key"))
	if err != nil || duplicate.ID != wid {
		t.Fatalf("key upgrade: %#v %v", duplicate, err)
	}
	completeForwardSteps(t, engine, 1)
	clock.Advance(time.Second)
	if _, err := engine.PurgeTerminal(t.Context(), RetentionPolicy{CompletedBefore: clock.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(t.Context(), engine, definition, integrationInput{Value: "kept"}, WithStartKey("legacy-key")); !errors.Is(err, ErrExecutionExpired) {
		t.Fatalf("legacy key was released: %v", err)
	}
}

func TestMigrationPreservesWaitingCancellationAndCleanup(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		schema int
	}{{"retry_wait", 5}, {"cancel_requested", 8}, {"compensating", 9}} {
		t.Run(scenario.name, func(t *testing.T) {
			pool := oldSchema(t, scenario.schema)
			now := time.Unix(2000, 0).UTC()
			clock := NewManualClock(now)
			engine, _ := New(pool, NewRegistry(), clock)
			wid, _ := newID()
			aid, _ := newID()
			workflowSQL := `INSERT INTO workflow_executions(id,definition_name,input,state,created_at,updated_at) VALUES($1,'phase','{}','running',$2,$2)`
			if scenario.schema >= 6 {
				workflowSQL = `INSERT INTO workflow_executions(id,definition_name,input,state,created_at,updated_at,deadline_at) VALUES($1,'phase','{}','running',$2,$2,$2::timestamptz+interval '24 hours')`
			}
			if _, err := pool.Exec(t.Context(), workflowSQL, wid, now); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(t.Context(), `INSERT INTO activities(id,workflow_id,position,name,input,state,attempt_count,created_at,updated_at,available_at,step_key) VALUES($1,$2,0,'phase-step','{}','ready',0,$3,$3,$3,'first')`, aid, wid, now); err != nil {
				t.Fatal(err)
			}
			switch scenario.name {
			case "retry_wait":
				if _, err := pool.Exec(t.Context(), `UPDATE activities SET state='retry_wait',attempt_count=1,available_at=$2 WHERE id=$1`, aid, now.Add(time.Second*10)); err != nil {
					t.Fatal(err)
				}
				atid, _ := newID()
				if _, err := pool.Exec(t.Context(), `INSERT INTO attempts(id,activity_id,number,worker_id,state,started_at,ended_at,retry_decision,retry_due_at) VALUES($1,$2,1,'old','failed',$3,$3,'retry',$4)`, atid, aid, now, now.Add(time.Second*10)); err != nil {
					t.Fatal(err)
				}
			case "cancel_requested":
				if _, err := pool.Exec(t.Context(), `UPDATE workflow_executions SET state='cancel_requested',cancel_actor='operator',cancel_reason='saved request',cancel_requested_at=$2 WHERE id=$1`, wid, now); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(t.Context(), `UPDATE activities SET state='cancelled',completed_at=$2 WHERE id=$1`, aid, now); err != nil {
					t.Fatal(err)
				}
			case "compensating":
				if _, err := pool.Exec(t.Context(), `UPDATE activities SET state='completed',output='{}',completed_at=$2,compensation='{"name":"cleanup","contract":"","input":{},"retry_policy":{"max_attempts":3,"retry_errors":false,"maximum_interval":60000000000},"attempt_timeout":300000000000}' WHERE id=$1`, aid, now); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(t.Context(), `UPDATE workflow_executions SET state='compensating',original_state='failed',last_error='original cause',terminal_reason='handler_error',compensation_state='running',compensation_deadline_at=$2 WHERE id=$1`, wid, now.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				cid, _ := newID()
				if _, err := pool.Exec(t.Context(), `INSERT INTO activities(id,workflow_id,position,name,input,state,created_at,updated_at,available_at,step_key,kind,compensates_id) VALUES($1,$2,1,'cleanup','{}','ready',$3,$3,$3,'cleanup','compensation',$4)`, cid, wid, now, aid); err != nil {
					t.Fatal(err)
				}
			}
			if err := engine.Migrate(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
				t.Fatal(err)
			}
			switch scenario.name {
			case "retry_wait":
				activities, err := engine.Activities(t.Context(), wid)
				if err != nil || activities[0].State != ActivityWaiting || !activities[0].AvailableAt.Equal(now.Add(time.Second*10)) {
					t.Fatalf("due changed: %#v %v", activities, err)
				}
				clock.Advance(time.Second * 10)
				if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
					t.Fatal(err)
				}
			case "cancel_requested":
				execution, err := engine.Workflow(t.Context(), wid)
				if err != nil || execution.State != WorkflowCancelled || execution.CancelReason != "saved request" || execution.CancelSettledAt == nil {
					t.Fatalf("cancel lost: %#v %v", execution, err)
				}
				return
			case "compensating":
				activities, err := engine.Activities(t.Context(), wid)
				if err != nil || activities[0].Compensation.Version != "1" || activities[0].Compensation.OutputSchema != "legacy" {
					t.Fatalf("legacy cleanup backfill: %#v %v", activities, err)
				}
			}
			claim, err := engine.ClaimActivity(t.Context(), "upgraded", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.CompleteActivity(t.Context(), claim, []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
			execution, err := engine.Workflow(t.Context(), wid)
			if err != nil {
				t.Fatal(err)
			}
			if scenario.name == "retry_wait" && (claim.AttemptNumber != 2 || execution.State != WorkflowCompleted) {
				t.Fatalf("retry outcome: %#v %#v", claim, execution)
			}
			if scenario.name == "compensating" && (execution.CompensationState != CompensationCompleted || execution.LastError != "original cause" || execution.State != WorkflowFailed) {
				t.Fatalf("cleanup outcome: %#v", execution)
			}
		})
	}
}
