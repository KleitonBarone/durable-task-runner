CREATE TABLE start_keys (
    definition_name text NOT NULL,
    definition_version text NOT NULL,
    start_key text NOT NULL,
    execution_id uuid NOT NULL UNIQUE,
    input_schema text NOT NULL,
    input jsonb,
    created_at timestamptz NOT NULL,
    purged_at timestamptz,
    PRIMARY KEY (definition_name, definition_version, start_key),
    CHECK ((purged_at IS NULL) = (input IS NULL))
);
INSERT INTO start_keys(definition_name,definition_version,start_key,execution_id,input_schema,created_at)
    SELECT definition_name,definition_version,start_key,id,input_schema,created_at
    FROM workflow_executions WHERE start_key IS NOT NULL;
CREATE INDEX workflows_retention_idx ON workflow_executions(completed_at,id)
    WHERE state IN ('completed','failed','timed_out','cancelled','compensation_failed');
