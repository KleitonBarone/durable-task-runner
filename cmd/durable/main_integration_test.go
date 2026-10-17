//go:build integration

package main

import (
	"bytes"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
)

func TestOperatorCommandsUsePublicInspectionAndCancellation(t *testing.T) {
	pool := testdb.Pool(t)
	parsed, err := url.Parse(os.Getenv("DURABLE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
	parsed.RawQuery = query.Encode()
	databaseURL := parsed.String()
	command := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := run(t.Context(), args, databaseURL, &out); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return out.String()
	}
	if out := command("migrate"); !strings.Contains(out, `"schema":"current"`) {
		t.Fatal(out)
	}
	registry := durable.NewRegistry()
	definition := durable.Define("operator-test", func(in string) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("operator-step", in)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	engine, err := durable.New(pool, registry, durable.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(t.Context(), engine, definition, "private-input", durable.WithStartKey("private-key"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "operator-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"status", "-id", execution.ID}, {"activities", "-id", execution.ID}, {"attempts", "-id", claim.ActivityID}, {"leases", "-id", claim.ActivityID, "-sensitive"}, {"timeline", "-id", execution.ID, "-limit", "1"}} {
		out := command(args...)
		if strings.Contains(out, "private-") || strings.Contains(out, claim.LeaseToken) {
			t.Fatalf("private data exposed: %s", out)
		}
		if !strings.Contains(out, `"items":[`) {
			t.Fatal(out)
		}
	}
	out := command("cancel", "-id", execution.ID, "-actor", "operator", "-reason", "private-cancel-reason")
	if !strings.Contains(out, "cancel_requested") || strings.Contains(out, "private-") {
		t.Fatal(out)
	}
	if out := command("status", "-id", execution.ID, "-sensitive"); !strings.Contains(out, "private-cancel-reason") {
		t.Fatal(out)
	}
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`null`)); err == nil {
		t.Fatal("cancelled ownership remained valid")
	}
}
