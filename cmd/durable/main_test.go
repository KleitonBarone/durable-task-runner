package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestOperatorArgumentFailuresNeedNoDatabase(t *testing.T) {
	for _, args := range [][]string{{"unknown"}, {"attempts"}, {"cancel", "-id", "execution"}, {"status", "-limit", "501"}, {"status", "-timeout", "0s"}, {"status", "-before", "bad"}, {"status"}} {
		var out bytes.Buffer
		if err := run(context.Background(), args, "", &out); err == nil {
			t.Fatalf("invalid invocation passed: %v", args)
		}
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"help"}, "", &out); err != nil || !strings.Contains(out.String(), "DATABASE_URL") {
		t.Fatalf("help: %s %v", out.String(), err)
	}
}

func TestOperatorConnectionFailureRedactsCredentials(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"status", "-timeout", "20ms"}, "postgres://private-user:private-password@127.0.0.1:1/private-db", &out)
	if err == nil || strings.Contains(err.Error()+out.String(), "private-") {
		t.Fatalf("credential exposure: %v %s", err, out.String())
	}
}
