ALTER TABLE workflow_executions
    ADD COLUMN cancel_actor text NOT NULL DEFAULT '',
    ADD COLUMN cancel_reason text NOT NULL DEFAULT '',
    ADD COLUMN cancel_requested_at timestamptz,
    ADD COLUMN cancel_settled_at timestamptz;
ALTER TABLE workflow_executions DROP CONSTRAINT workflow_executions_state_check;
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_executions_state_check
    CHECK (state IN ('running','completed','failed','timed_out','cancel_requested','cancelled'));
ALTER TABLE activities DROP CONSTRAINT activities_state_check;
ALTER TABLE activities ADD CONSTRAINT activities_state_check
    CHECK (state IN ('blocked','ready','running','retry_wait','completed','failed','timed_out','cancelled'));
ALTER TABLE attempts DROP CONSTRAINT attempts_state_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_state_check
    CHECK (state IN ('running','completed','failed','abandoned','timed_out','cancelled'));
