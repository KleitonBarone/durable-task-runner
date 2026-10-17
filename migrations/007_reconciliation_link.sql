-- Keep this identifier even when the predecessor is retained away.
ALTER TABLE workflow_executions ADD COLUMN previous_execution_id uuid;
