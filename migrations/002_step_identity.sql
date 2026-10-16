ALTER TABLE activities ADD COLUMN step_key text;
UPDATE activities SET step_key = 'step-' || position::text;
ALTER TABLE activities ALTER COLUMN step_key SET NOT NULL;
ALTER TABLE activities ADD CONSTRAINT activities_step_key_check CHECK (length(step_key) BETWEEN 1 AND 200);
ALTER TABLE activities ADD CONSTRAINT activities_step_key_unique UNIQUE (workflow_id, step_key);
ALTER TABLE activities ADD COLUMN contract text NOT NULL DEFAULT '';
