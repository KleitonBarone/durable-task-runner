ALTER TABLE workflow_executions ADD COLUMN output jsonb;
UPDATE workflow_executions w SET output = (
    SELECT a.output FROM activities a WHERE a.workflow_id = w.id
    ORDER BY a.position DESC LIMIT 1
) WHERE w.state = 'completed';
