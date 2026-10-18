//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func TestProgressionRollsBackWithHistoryFailure(t *testing.T) {
	for _, event := range []string{"activity.ready", "workflow.completed"} {
		t.Run(event, func(t *testing.T) {
			steps := 1
			if event == "activity.ready" {
				steps = 2
			}
			engine, definition := newIntegrationEngine(t, durable.NewManualClock(time.Unix(1000, 0)), steps)
			execution, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := engine.ClaimActivity(t.Context(), "atomic-progression", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// Reject a late history write after the completion's earlier row updates.
			if _, err := engine.pool.Exec(t.Context(), fmt.Sprintf(`CREATE FUNCTION reject_progression() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type=TG_ARGV[0] THEN RAISE EXCEPTION 'injected history failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_progression BEFORE INSERT ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_progression('%s')`, event)); err != nil {
				t.Fatal(err)
			}
			if err := engine.CompleteActivity(t.Context(), claim, []byte(`"result"`)); err == nil {
				t.Fatal("history failure acknowledged completion")
			}
			workflow, err := engine.Workflow(t.Context(), execution.ID)
			if err != nil || workflow.State != durable.WorkflowRunning || workflow.Output != nil {
				t.Fatalf("workflow changed: %#v %v", workflow, err)
			}
			activities, err := engine.Activities(t.Context(), execution.ID)
			if err != nil || len(activities) != steps || activities[0].State != durable.ActivityRunning || (steps == 2 && activities[1].State != durable.ActivityBlocked) {
				t.Fatalf("partial activity progression: %#v %v", activities, err)
			}
			attempts, err := engine.Attempts(t.Context(), claim.ActivityID)
			if err != nil || len(attempts) != 1 || attempts[0].State != durable.AttemptRunning {
				t.Fatalf("attempt changed: %#v %v", attempts, err)
			}
			var leases int
			var outputUnset bool
			if err := engine.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM leases WHERE state='active'),(SELECT output IS NULL FROM activities WHERE id=$1)`, claim.ActivityID).Scan(&leases, &outputUnset); err != nil || leases != 1 || !outputUnset {
				t.Fatalf("lease or output changed: leases=%d output unset=%v %v", leases, outputUnset, err)
			}
			history, err := engine.Timeline(t.Context(), execution.ID)
			if err != nil || len(history) != steps+2 {
				t.Fatalf("partial history committed: %#v %v", history, err)
			}
			if _, err := engine.pool.Exec(t.Context(), `DROP TRIGGER reject_progression ON execution_history`); err != nil {
				t.Fatal(err)
			}
			if err := engine.CompleteActivity(t.Context(), claim, []byte(`"result"`)); err != nil {
				t.Fatal(err)
			}
			if steps == 2 {
				claim, err := engine.ClaimActivity(t.Context(), "atomic-progression", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if err := engine.CompleteActivity(t.Context(), claim, []byte(`"result"`)); err != nil {
					t.Fatal(err)
				}
			}
			workflow, err = engine.Workflow(t.Context(), execution.ID)
			if err != nil || workflow.State != durable.WorkflowCompleted {
				t.Fatalf("cannot complete after rollback: %#v %v", workflow, err)
			}
		})
	}
}
