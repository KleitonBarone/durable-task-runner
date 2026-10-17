package durable

import (
	"context"
	"fmt"
)

func capabilityArrays(capabilities map[string]capability) ([]string, []string, []string, []string, []string) {
	names, contracts := make([]string, 0, len(capabilities)), make([]string, 0, len(capabilities))
	versions, inputSchemas, outputSchemas := make([]string, 0, len(capabilities)), make([]string, 0, len(capabilities)), make([]string, 0, len(capabilities))
	for _, c := range capabilities {
		names = append(names, c.name)
		contracts = append(contracts, c.contract)
		versions = append(versions, c.version)
		inputSchemas = append(inputSchemas, c.inputSchema)
		outputSchemas = append(outputSchemas, c.outputSchema)
	}
	return names, versions, contracts, inputSchemas, outputSchemas
}

type UnsupportedActivity struct {
	Name         string
	Version      string
	InputSchema  string
	OutputSchema string
	Contract     string
	ReadyCount   int64
}

// UnsupportedActivities reports up to 100 ready activity types this worker
// cannot claim. It does not classify blocked steps or claim global worker health.
func (w *Worker) UnsupportedActivities(ctx context.Context) ([]UnsupportedActivity, error) {
	if err := w.engine.CheckSchema(ctx); err != nil {
		return nil, err
	}
	names, versions, contracts, inputSchemas, outputSchemas := capabilityArrays(w.capabilities)
	rows, err := w.engine.pool.Query(ctx, `SELECT a.name,a.version,a.input_schema,a.output_schema, a.contract, count(*) FROM activities a JOIN workflow_executions w ON w.id = a.workflow_id WHERE a.state = 'ready' AND ((w.state='running' AND a.kind='forward') OR (w.state='compensating' AND a.kind='compensation')) AND (a.name,a.version, a.contract,a.input_schema,a.output_schema) NOT IN (SELECT * FROM unnest($1::text[], $2::text[],$3::text[],$4::text[],$5::text[])) GROUP BY a.name,a.version,a.input_schema,a.output_schema, a.contract ORDER BY a.name,a.version, a.contract LIMIT 100`, names, versions, contracts, inputSchemas, outputSchemas)
	if err != nil {
		return nil, fmt.Errorf("read unsupported activities: %w", err)
	}
	defer rows.Close()
	var unsupported []UnsupportedActivity
	for rows.Next() {
		var activity UnsupportedActivity
		if err := rows.Scan(&activity.Name, &activity.Version, &activity.InputSchema, &activity.OutputSchema, &activity.Contract, &activity.ReadyCount); err != nil {
			return nil, fmt.Errorf("scan unsupported activity: %w", err)
		}
		unsupported = append(unsupported, activity)
	}
	return unsupported, rows.Err()
}
