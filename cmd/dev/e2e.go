package main

import "context"

func (a *app) processTests(ctx context.Context, databaseURL string, args []string) error {
	arguments := append([]string{"test", "-count=1", "-timeout=2m", "-tags=e2e", "-v"}, args...)
	if len(args) == 0 {
		arguments = append(arguments, "-run=TestProcess", ".")
	}
	_, err := a.runner.run(ctx, command{name: "go", args: arguments, dir: a.root,
		env: []string{"DURABLE_TEST_DATABASE_URL=" + databaseURL}})
	return err
}
