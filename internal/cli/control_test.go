package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/control"
	"github.com/peetzweg/matcrawl/internal/store"
)

func TestStripTrailingJSON(t *testing.T) {
	cases := []struct {
		in   []string
		out  []string
		json bool
	}{
		{[]string{"status", "--json"}, []string{"status"}, true},
		{[]string{"metadata", "--json"}, []string{"metadata"}, true},
		{[]string{"search", "foo", "--json"}, []string{"search", "foo"}, true},
		{[]string{"search", "--json"}, []string{"search", "--json"}, false},
		{[]string{"status"}, []string{"status"}, false},
		{[]string{"--json"}, []string{"--json"}, false},
	}
	for _, c := range cases {
		got, gotJSON := stripTrailingJSON(c.in)
		if !slices.Equal(got, c.out) || gotJSON != c.json {
			t.Errorf("stripTrailingJSON(%q) = %q, %v; want %q, %v", c.in, got, gotJSON, c.out, c.json)
		}
	}
}

func TestControlStatusFreshness(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	never := controlStatus(store.Status{}, now)
	if never.SchemaVersion != control.SchemaVersion || never.AppID != "matcrawl" {
		t.Fatalf("envelope = %#v", never.Status)
	}
	if never.State != "empty" || never.Freshness.Status != "never" {
		t.Fatalf("never synced: state=%q freshness=%#v", never.State, never.Freshness)
	}

	fresh := controlStatus(store.Status{Messages: 3, Rooms: 1, LastSyncAt: now.Add(-time.Minute)}, now)
	if fresh.State != "current" || fresh.Freshness.Status != "fresh" || fresh.LastSyncAt == "" {
		t.Fatalf("fresh: state=%q freshness=%#v", fresh.State, fresh.Freshness)
	}

	stale := controlStatus(store.Status{LastSyncAt: now.Add(-2 * staleAfter), DecryptFailures: 2}, now)
	if stale.State != "stale" || stale.Freshness.Status != "stale" || len(stale.Warnings) != 1 {
		t.Fatalf("stale: state=%q freshness=%#v warnings=%q", stale.State, stale.Freshness, stale.Warnings)
	}
}

func TestStatusTrailingJSONEmitsControlEnvelope(t *testing.T) {
	db := filepath.Join(t.TempDir(), "matcrawl.db")
	var stdout, stderr bytes.Buffer
	if err := Run(context.Background(), []string{"--db", db, "status", "--json"}, &stdout, &stderr); err != nil {
		t.Fatalf("status --json: %v (stderr=%s)", err, stderr.String())
	}
	var got map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, stdout.String())
	}
	if got["schema_version"] != control.SchemaVersion || got["app_id"] != "matcrawl" {
		t.Fatalf("missing control envelope: %s", stdout.String())
	}
}
