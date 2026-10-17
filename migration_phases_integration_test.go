//go:build integration

package durable

import (
	"testing"
	"time"
)

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
