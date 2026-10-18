//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func TestRetentionPreservesKeysAndOnlyPurgesTerminalDetails(t *testing.T) {
	clock := durable.NewManualClock(time.Unix(1000, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	definition := durable.Define("retention-json", func(input json.RawMessage) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("step", input)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	first, err := durable.Start(t.Context(), engine.Engine, definition, json.RawMessage(`{"a":1,"b":[2]}`), durable.WithStartKey("stable"))
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
	active, err := durable.Start(t.Context(), engine.Engine, definition, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	result, err := engine.PurgeTerminal(t.Context(), durable.RetentionPolicy{CompletedBefore: clock.Now(), Limit: 1})
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
	_, err = durable.Start(t.Context(), engine.Engine, definition, json.RawMessage(`{"b":[2.0],"a":1.00}`), durable.WithStartKey("stable"))
	var expired *durable.ExecutionExpiredError
	if !errors.Is(err, durable.ErrExecutionExpired) || !errors.As(err, &expired) || expired.ID != first.ID {
		t.Fatalf("expired key: %v", err)
	}
	if _, err := durable.Start(t.Context(), engine.Engine, definition, json.RawMessage(`{"a":2,"b":[2]}`), durable.WithStartKey("stable")); !errors.Is(err, durable.ErrStartConflict) {
		t.Fatalf("conflicting purged key: %v", err)
	}
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`)); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatal(err)
	}
	result, err = engine.PurgeTerminal(t.Context(), durable.RetentionPolicy{CompletedBefore: clock.Now(), Limit: 1})
	if err != nil || result.Deleted != 0 {
		t.Fatalf("repeated purge: %#v %v", result, err)
	}
	for _, policy := range []durable.RetentionPolicy{{}, {CompletedBefore: clock.Now(), Limit: 501}, {CompletedBefore: clock.Now().Add(time.Second)}} {
		if _, err := engine.PurgeTerminal(t.Context(), policy); !errors.Is(err, durable.ErrInvalidInput) {
			t.Fatalf("invalid retention: %#v %v", policy, err)
		}
	}
}

func TestRetentionBatchProtectsPendingCancellationAndCleanup(t *testing.T) {
	engine, clock, cleanupExecution := compensationExecution(t, durable.RetryPolicy{})
	completeForwardSteps(t, engine, 2)
	failForward(t, engine)
	definition := integrationDefinition(1)
	worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: "batch"}, durable.Handle("step_1", func(_ context.Context, input integrationInput) (string, error) { return input.Value, nil }))
	if err != nil {
		t.Fatal(err)
	}
	var completed []durable.WorkflowExecution
	for range 3 {
		execution, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := worker.RunOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		completed = append(completed, execution)
	}
	active, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{}, durable.WithPredecessor(completed[0].ID))
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Cancel(t.Context(), cancelled.ID, "operator", ""); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	for _, want := range []int{2, 1, 0} {
		result, err := engine.PurgeTerminal(t.Context(), durable.RetentionPolicy{CompletedBefore: clock.Now(), Limit: 2})
		if err != nil || result.Deleted != want {
			t.Fatalf("bounded deletion: %#v %v, want %d", result, err, want)
		}
	}
	for id, want := range map[string]durable.WorkflowState{active.ID: durable.WorkflowRunning, cancelled.ID: durable.WorkflowCancelRequested, cleanupExecution.ID: durable.WorkflowCompensating} {
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

func TestKeyReservationAndPurgeAreAtomic(t *testing.T) {
	clock := durable.NewManualClock(time.Unix(1000, 0))
	engine, definition := newIntegrationEngine(t, clock, 1)
	if _, err := engine.pool.Exec(t.Context(), `CREATE FUNCTION reject_key_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected history failure'; END $$;CREATE TRIGGER key_history BEFORE INSERT ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_key_history()`); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{}, durable.WithStartKey("atomic")); err == nil {
		t.Fatal("failed start succeeded")
	}
	var count int
	if err := engine.pool.QueryRow(t.Context(), `SELECT count(*) FROM start_keys`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed start reserved key: %d %v", count, err)
	}
	if _, err := engine.pool.Exec(t.Context(), `DROP TRIGGER key_history ON execution_history`); err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{}, durable.WithStartKey("atomic"))
	if err != nil {
		t.Fatal(err)
	}
	completeForwardSteps(t, engine, 1)
	clock.Advance(time.Second)
	if _, err := engine.pool.Exec(t.Context(), `CREATE TRIGGER reject_purge BEFORE DELETE ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_key_history()`); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.PurgeTerminal(t.Context(), durable.RetentionPolicy{CompletedBefore: clock.Now()}); err == nil {
		t.Fatal("failed history deletion succeeded")
	}
	duplicate, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{}, durable.WithStartKey("atomic"))
	if err != nil || duplicate.ID != execution.ID || duplicate.State != durable.WorkflowCompleted {
		t.Fatalf("failed purge changed deduplication: %#v %v", duplicate, err)
	}
	var copied bool
	if err := engine.pool.QueryRow(t.Context(), `SELECT input IS NOT NULL OR purged_at IS NOT NULL FROM start_keys WHERE execution_id=$1`, execution.ID).Scan(&copied); err != nil || copied {
		t.Fatalf("partial key tombstone committed: %v %v", copied, err)
	}
	if _, err := engine.pool.Exec(t.Context(), `DROP TRIGGER reject_purge ON execution_history`); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.PurgeTerminal(t.Context(), durable.RetentionPolicy{CompletedBefore: clock.Now()}); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionRacesKeyedStartsAndBoundedReads(t *testing.T) {
	clock := durable.NewManualClock(time.Unix(1000, 0))
	engine, definition := newIntegrationEngine(t, clock, 1)
	execution, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{}, durable.WithStartKey("race"))
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
			_, err := engine.PurgeTerminal(t.Context(), durable.RetentionPolicy{CompletedBefore: clock.Now(), Limit: 1})
			if err != nil {
				errs <- err
			}
		}()
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			duplicate, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{}, durable.WithStartKey("race"))
			var expired *durable.ExecutionExpiredError
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
			if _, err := engine.ListTimeline(t.Context(), execution.ID, durable.PageRequest{Limit: 1}); err != nil {
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
