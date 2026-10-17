//go:build integration

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRetentionPreservesKeysAndOnlyPurgesTerminalDetails(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	definition := Define("retention-json", func(input json.RawMessage) ([]ActivitySpec, error) {
		step, err := NewActivity("step", input)
		return []ActivitySpec{step}, err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	first, err := Start(t.Context(), engine, definition, json.RawMessage(`{"a":1,"b":[2]}`), WithStartKey("stable"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`"done"`)); err != nil {
		t.Fatal(err)
	}
	active, err := Start(t.Context(), engine, definition, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	result, err := engine.PurgeTerminal(t.Context(), RetentionPolicy{CompletedBefore: clock.Now(), Limit: 1})
	if err != nil || result.Deleted != 1 || result.ExecutionIDs[0] != first.ID {
		t.Fatalf("purge: %#v %v", result, err)
	}
	if _, err := engine.Workflow(t.Context(), active.ID); err != nil {
		t.Fatal("active execution removed", err)
	}
	for _, table := range []string{"activities", "attempts", "leases", "execution_history"} {
		var count int
		query := `SELECT count(*) FROM ` + table + ` WHERE workflow_id=$1`
		if table == "attempts" || table == "leases" {
			query = `SELECT count(*) FROM ` + table + ` WHERE activity_id=$1`
		}
		id := first.ID
		if table == "attempts" || table == "leases" {
			id = claim.ActivityID
		}
		if err := engine.pool.QueryRow(t.Context(), query, id).Scan(&count); err != nil || count != 0 {
			t.Fatalf("details retained in %s: %d %v", table, count, err)
		}
	}
	_, err = Start(t.Context(), engine, definition, json.RawMessage(`{"b":[2.0],"a":1.00}`), WithStartKey("stable"))
	var expired *ExecutionExpiredError
	if !errors.Is(err, ErrExecutionExpired) || !errors.As(err, &expired) || expired.ID != first.ID {
		t.Fatalf("expired key: %v", err)
	}
	if _, err := Start(t.Context(), engine, definition, json.RawMessage(`{"a":2,"b":[2]}`), WithStartKey("stable")); !errors.Is(err, ErrStartConflict) {
		t.Fatalf("conflicting purged key: %v", err)
	}
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	result, err = engine.PurgeTerminal(t.Context(), RetentionPolicy{CompletedBefore: clock.Now(), Limit: 1})
	if err != nil || result.Deleted != 0 {
		t.Fatalf("repeated purge: %#v %v", result, err)
	}
	for _, policy := range []RetentionPolicy{{}, {CompletedBefore: clock.Now(), Limit: 501}, {CompletedBefore: clock.Now().Add(time.Second)}} {
		if _, err := engine.PurgeTerminal(t.Context(), policy); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid retention: %#v %v", policy, err)
		}
	}
}

func TestRetentionBatchProtectsPendingCancellationAndCleanup(t *testing.T) {
	engine, clock, cleanupExecution := compensationExecution(t, RetryPolicy{})
	completeForwardSteps(t, engine, 2)
	failForward(t, engine)
	definition := integrationDefinition(1)
	worker, err := NewWorker(engine, WorkerConfig{ID: "batch"}, Handle("step_1", func(_ context.Context, input integrationInput) (string, error) { return input.Value, nil }))
	if err != nil {
		t.Fatal(err)
	}
	var completed []WorkflowExecution
	for range 3 {
		execution, err := Start(t.Context(), engine, definition, integrationInput{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := worker.RunOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		completed = append(completed, execution)
	}
	active, err := Start(t.Context(), engine, definition, integrationInput{}, WithPredecessor(completed[0].ID))
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := Start(t.Context(), engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Cancel(t.Context(), cancelled.ID, "operator", ""); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	for _, want := range []int{2, 1, 0} {
		result, err := engine.PurgeTerminal(t.Context(), RetentionPolicy{CompletedBefore: clock.Now(), Limit: 2})
		if err != nil || result.Deleted != want {
			t.Fatalf("bounded deletion: %#v %v, want %d", result, err, want)
		}
	}
	for id, want := range map[string]WorkflowState{active.ID: WorkflowRunning, cancelled.ID: WorkflowCancelRequested, cleanupExecution.ID: WorkflowCompensating} {
		state, err := engine.Workflow(t.Context(), id)
		if err != nil || state.State != want {
			t.Fatalf("active phase removed: %#v %v", state, err)
		}
	}
	child, err := engine.Workflow(t.Context(), active.ID)
	if err != nil || child.PreviousExecutionID == nil || *child.PreviousExecutionID != completed[0].ID {
		t.Fatalf("predecessor link lost: %#v %v", child, err)
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

func TestKeyReservationAndPurgeAreAtomic(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	engine, definition := newIntegrationEngine(t, clock, 1)
	if _, err := engine.pool.Exec(t.Context(), `CREATE FUNCTION reject_key_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected history failure'; END $$;CREATE TRIGGER key_history BEFORE INSERT ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_key_history()`); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(t.Context(), engine, definition, integrationInput{}, WithStartKey("atomic")); err == nil {
		t.Fatal("failed start succeeded")
	}
	var count int
	if err := engine.pool.QueryRow(t.Context(), `SELECT count(*) FROM start_keys`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed start reserved key: %d %v", count, err)
	}
	if _, err := engine.pool.Exec(t.Context(), `DROP TRIGGER key_history ON execution_history`); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(t.Context(), engine, definition, integrationInput{}, WithStartKey("atomic"))
	if err != nil {
		t.Fatal(err)
	}
	completeForwardSteps(t, engine, 1)
	clock.Advance(time.Second)
	if _, err := engine.pool.Exec(t.Context(), `CREATE TRIGGER reject_purge BEFORE DELETE ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_key_history()`); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.PurgeTerminal(t.Context(), RetentionPolicy{CompletedBefore: clock.Now()}); err == nil {
		t.Fatal("failed history deletion succeeded")
	}
	duplicate, err := Start(t.Context(), engine, definition, integrationInput{}, WithStartKey("atomic"))
	if err != nil || duplicate.ID != execution.ID || duplicate.State != WorkflowCompleted {
		t.Fatalf("failed purge changed deduplication: %#v %v", duplicate, err)
	}
	var copied bool
	if err := engine.pool.QueryRow(t.Context(), `SELECT input IS NOT NULL OR purged_at IS NOT NULL FROM start_keys WHERE execution_id=$1`, execution.ID).Scan(&copied); err != nil || copied {
		t.Fatalf("partial key tombstone committed: %v %v", copied, err)
	}
	if _, err := engine.pool.Exec(t.Context(), `DROP TRIGGER reject_purge ON execution_history`); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.PurgeTerminal(t.Context(), RetentionPolicy{CompletedBefore: clock.Now()}); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionRacesKeyedStartsAndBoundedReads(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	engine, definition := newIntegrationEngine(t, clock, 1)
	execution, err := Start(t.Context(), engine, definition, integrationInput{}, WithStartKey("race"))
	if err != nil {
		t.Fatal(err)
	}
	completeForwardSteps(t, engine, 1)
	clock.Advance(time.Second)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := engine.PurgeTerminal(t.Context(), RetentionPolicy{CompletedBefore: clock.Now(), Limit: 1})
			if err != nil {
				errs <- err
			}
		}()
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			duplicate, err := Start(t.Context(), engine, definition, integrationInput{}, WithStartKey("race"))
			var expired *ExecutionExpiredError
			if err != nil {
				if !errors.As(err, &expired) || expired.ID != execution.ID {
					errs <- err
				}
			} else if duplicate.ID != execution.ID {
				errs <- errors.New("retention allowed duplicate execution")
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := engine.ListTimeline(t.Context(), execution.ID, PageRequest{Limit: 1}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var keys, workflows int
	if err := engine.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM start_keys),(SELECT count(*) FROM workflow_executions)`).Scan(&keys, &workflows); err != nil || keys != 1 || workflows != 0 {
		t.Fatalf("deduplication lifetime: keys=%d workflows=%d %v", keys, workflows, err)
	}
}
