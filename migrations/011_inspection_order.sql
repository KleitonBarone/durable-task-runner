ALTER TABLE workflow_executions ADD COLUMN ordinal bigserial UNIQUE;
ALTER TABLE leases ADD COLUMN ordinal bigserial UNIQUE;
CREATE INDEX workflows_inspect_state_idx ON workflow_executions(state, ordinal);
CREATE INDEX workflows_inspect_definition_idx ON workflow_executions(definition_name, definition_version, ordinal);
