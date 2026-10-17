ALTER TABLE workflow_executions ADD COLUMN timeout_ns bigint NOT NULL DEFAULT 86400000000000 CHECK (timeout_ns > 0);
ALTER TABLE workflow_executions ADD COLUMN deadline_at timestamptz;
UPDATE workflow_executions SET deadline_at=created_at+interval '24 hours';
ALTER TABLE workflow_executions ALTER COLUMN deadline_at SET NOT NULL;
CREATE INDEX workflows_deadline_idx ON workflow_executions(deadline_at) WHERE state='running';
ALTER TABLE workflow_executions DROP CONSTRAINT workflow_executions_state_check;
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_executions_state_check CHECK (state IN ('running','completed','failed','timed_out'));
ALTER TABLE activities DROP CONSTRAINT activities_state_check;
ALTER TABLE activities ADD CONSTRAINT activities_state_check CHECK (state IN ('blocked','ready','running','retry_wait','completed','failed','timed_out'));
ALTER TABLE attempts ADD COLUMN deadline_at timestamptz;
UPDATE attempts SET deadline_at=started_at+interval '5 minutes';
ALTER TABLE attempts ALTER COLUMN deadline_at SET NOT NULL;
ALTER TABLE attempts DROP CONSTRAINT attempts_state_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_state_check CHECK (state IN ('running','completed','failed','abandoned','timed_out'));
UPDATE leases l SET expires_at=LEAST(l.expires_at,at.deadline_at,w.deadline_at)
FROM attempts at,activities a,workflow_executions w
WHERE l.attempt_id=at.id AND l.activity_id=a.id AND a.workflow_id=w.id AND l.state='active';
