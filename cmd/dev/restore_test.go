package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRestoreFailureCleansBothOwnedDatabases(t *testing.T) {
	projects := map[string]bool{}
	cleaned := map[string]bool{}
	const dump = "private snapshot contents"
	r := &fakeRunner{respond: func(c command) (string, error) {
		args := strings.Join(c.args, " ")
		if strings.Contains(args, "-role snapshot") {
			return "12345678-1234-1234-1234-123456789abc\n", nil
		}
		if strings.Contains(args, "pg_dump") {
			if !c.quiet {
				t.Fatal("snapshot printed")
			}
			return dump, nil
		}
		if strings.Contains(args, "psql") {
			if !c.quiet || c.input != dump {
				t.Fatal("snapshot lost or printed")
			}
			return "", errors.New("injected restore failure")
		}
		if len(c.args) >= 6 && c.args[0] == "compose" {
			project := c.args[2]
			switch c.args[5] {
			case "up":
				projects[project] = true
			case "port":
				return "127.0.0.1:49123\n", nil
			case "ps":
				return project + "-postgres\n", nil
			case "down":
				cleaned[project] = true
			}
		}
		return "", nil
	}}
	a := testApp(r)
	if err := a.run(context.Background(), []string{"restore"}); err == nil {
		t.Fatal("failed restore passed")
	}
	if len(projects) != 2 || len(cleaned) != 2 {
		t.Fatalf("owned projects: %#v cleaned: %#v", projects, cleaned)
	}
	for project := range projects {
		if !cleaned[project] {
			t.Fatalf("leaked %s", project)
		}
	}
}
