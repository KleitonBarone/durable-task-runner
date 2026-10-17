ALTER TABLE workflow_executions
    ADD COLUMN definition_version text NOT NULL DEFAULT '1',
    ADD COLUMN input_schema text NOT NULL DEFAULT 'legacy';
ALTER TABLE workflow_executions DROP CONSTRAINT workflow_start_key_unique;
ALTER TABLE workflow_executions ADD CONSTRAINT workflow_start_key_unique
    UNIQUE (definition_name, definition_version, start_key);
ALTER TABLE activities
    ADD COLUMN version text NOT NULL DEFAULT '1',
    ADD COLUMN input_schema text NOT NULL DEFAULT 'legacy',
    ADD COLUMN output_schema text NOT NULL DEFAULT 'legacy';
UPDATE activities SET compensation = compensation || '{"version":"1","input_schema":"legacy","output_schema":"legacy"}'::jsonb
    WHERE compensation IS NOT NULL;
