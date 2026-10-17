ALTER TABLE workflow_executions
    ADD COLUMN original_state text NOT NULL DEFAULT '' CHECK (original_state IN ('','failed','timed_out','cancelled')),
    ADD COLUMN compensation_state text NOT NULL DEFAULT 'none' CHECK (compensation_state IN ('none','running','completed','failed')),
    ADD COLUMN compensation_error text NOT NULL DEFAULT '',
    ADD COLUMN compensation_timeout_ns bigint NOT NULL DEFAULT 86400000000000 CHECK (compensation_timeout_ns > 0),
    ADD COLUMN compensation_deadline_at timestamptz;
ALTER TABLE workflow_executions DROP CONSTRAINT workflow_executions_state_check;
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_executions_state_check
    CHECK (state IN ('running','completed','failed','timed_out','cancel_requested','cancelled','compensating','compensation_failed'));
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_compensation_running_check
    CHECK ((state='compensating') = (compensation_state='running'));
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_compensation_deadline_check
    CHECK (compensation_state<>'running' OR compensation_deadline_at IS NOT NULL);
ALTER TABLE activities
    ADD COLUMN kind text NOT NULL DEFAULT 'forward' CHECK (kind IN ('forward','compensation')),
    ADD COLUMN compensates_id uuid UNIQUE REFERENCES activities(id) ON DELETE CASCADE,
    ADD COLUMN compensation jsonb;
ALTER TABLE activities ADD CONSTRAINT activity_compensation_parent_check
    CHECK ((kind='compensation') = (compensates_id IS NOT NULL));
