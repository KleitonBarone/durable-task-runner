CREATE TABLE workflow_executions (
    id uuid PRIMARY KEY,
    definition_name text NOT NULL,
    input jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('running', 'completed', 'failed')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    completed_at timestamptz
);

CREATE TABLE activities (
    id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    position integer NOT NULL CHECK (position >= 0),
    name text NOT NULL,
    input jsonb NOT NULL,
    output jsonb,
    state text NOT NULL CHECK (state IN ('blocked', 'ready', 'running', 'completed', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    completed_at timestamptz,
    UNIQUE (workflow_id, position)
);

CREATE INDEX activities_claimable_idx ON activities (position, created_at) WHERE state = 'ready';

CREATE TABLE attempts (
    id uuid PRIMARY KEY,
    activity_id uuid NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    number integer NOT NULL CHECK (number > 0),
    worker_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('running', 'completed', 'failed', 'abandoned')),
    error_text text,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    UNIQUE (activity_id, number)
);

CREATE TABLE leases (
    id uuid PRIMARY KEY,
    activity_id uuid NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    attempt_id uuid NOT NULL UNIQUE REFERENCES attempts(id) ON DELETE CASCADE,
    token uuid NOT NULL UNIQUE,
    worker_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('active', 'released', 'expired')),
    acquired_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    ended_at timestamptz
);

CREATE UNIQUE INDEX leases_one_active_per_activity_idx ON leases (activity_id) WHERE state = 'active';
CREATE INDEX leases_expired_idx ON leases (expires_at) WHERE state = 'active';

CREATE TABLE execution_history (
    sequence bigserial PRIMARY KEY,
    workflow_id uuid NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    activity_id uuid REFERENCES activities(id) ON DELETE CASCADE,
    attempt_id uuid REFERENCES attempts(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    recorded_at timestamptz NOT NULL
);

CREATE INDEX execution_history_workflow_idx ON execution_history (workflow_id, sequence);
