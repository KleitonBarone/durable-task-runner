package durable

import (
	"context"
	"fmt"
)

func capabilityArrays(capabilities map[string]string) ([]string, []string) {
	names, contracts := make([]string, 0, len(capabilities)), make([]string, 0, len(capabilities))
	for name, contract := range capabilities {
		names = append(names, name)
		contracts = append(contracts, contract)
	}
	return names, contracts
}

type UnsupportedActivity struct {
	Name       string
	Contract   string
	ReadyCount int64
}

// UnsupportedActivities reports up to 100 ready activity types this worker
// cannot claim. It does not classify blocked steps or claim global worker health.
func (w *Worker) UnsupportedActivities(ctx context.Context) ([]UnsupportedActivity, error) {
	names, contracts := capabilityArrays(w.capabilities)
	rows, err := w.engine.pool.Query(ctx, `SELECT a.name, a.contract, count(*) FROM activities a JOIN workflow_executions w ON w.id = a.workflow_id WHERE a.state = 'ready' AND ((w.state='running' AND a.kind='forward') OR (w.state='compensating' AND a.kind='compensation')) AND (a.name, a.contract) NOT IN (SELECT * FROM unnest($1::text[], $2::text[])) GROUP BY a.name, a.contract ORDER BY a.name, a.contract LIMIT 100`, names, contracts)
	if err != nil {
		return nil, fmt.Errorf("read unsupported activities: %w", err)
	}
	defer rows.Close()
	var unsupported []UnsupportedActivity
	for rows.Next() {
		var activity UnsupportedActivity
		if err := rows.Scan(&activity.Name, &activity.Contract, &activity.ReadyCount); err != nil {
			return nil, fmt.Errorf("scan unsupported activity: %w", err)
		}
		unsupported = append(unsupported, activity)
	}
	return unsupported, rows.Err()
}
