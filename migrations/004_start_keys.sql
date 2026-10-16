ALTER TABLE workflow_executions ADD COLUMN start_key text;
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_start_key_check CHECK (start_key IS NULL OR length(start_key) BETWEEN 1 AND 200);
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_start_key_unique UNIQUE (definition_name, start_key);
