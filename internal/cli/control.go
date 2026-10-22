package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/openclaw/crawlkit/control"
	"github.com/peetzweg/matcrawl/internal/store"
)

func controlManifest() control.Manifest {
	m := control.NewManifest("matcrawl", "Matrix Crawl", "matcrawl")
	m.Description = "Local-first Matrix archive crawler."
	m.Branding = control.Branding{
		SymbolName:       "message.fill",
		AccentColor:      "#0DBD8B",
		BundleIdentifier: "im.vector.app",
	}
	m.Paths = control.Paths{
		DefaultConfig:   filepath.Join(defaultBaseDir(), "backup.json"),
		DefaultDatabase: defaultDBPath(),
		DefaultCache:    filepath.Join(defaultBaseDir(), "cache"),
		DefaultLogs:     filepath.Join(defaultBaseDir(), "logs"),
	}
	m.Capabilities = []string{"metadata", "doctor", "status", "sync", "search", "backup", "keys"}
	m.Privacy = control.Privacy{
		ContainsPrivateMessages: true,
		ExportsSecrets:          false,
		LocalOnlyScopes:         []string{"matrix-csapi", "sqlite", "encrypted-git-backup"},
	}
	m.Commands = map[string]control.Command{
		"metadata": {Title: "Metadata", Argv: []string{"matcrawl", "metadata", "--json"}, JSON: true},
		"doctor":   {Title: "Doctor", Argv: []string{"matcrawl", "doctor", "--json"}, JSON: true},
		"status":   {Title: "Status", Argv: []string{"matcrawl", "status", "--json"}, JSON: true},
		"sync":     {Title: "Sync", Argv: []string{"matcrawl", "--json", "sync"}, JSON: true, Mutates: true},
		"search":   {Title: "Search", Argv: []string{"matcrawl", "--json", "search"}, JSON: true},
	}
	return m
}

// staleAfter is how long after the last successful /sync the archive is
// reported stale. crawlctl's default schedule for manifest-driven apps is 15m.
const staleAfter = time.Hour

// archiveControlStatus is the crawlkit.control.v1 status envelope plus the
// Matrix-specific fields that don't map onto a generic count.
type archiveControlStatus struct {
	control.Status
	OldestEvent string `json:"oldest_event,omitempty"`
	NewestEvent string `json:"newest_event,omitempty"`
	NextBatch   string `json:"next_batch,omitempty"`
}

func controlStatus(status store.Status, now time.Time) archiveControlStatus {
	counts := []control.Count{
		control.NewCount("rooms", "Rooms", int64(status.Rooms)),
		control.NewCount("encrypted_rooms", "Encrypted rooms", int64(status.EncryptedRooms)),
		control.NewCount("messages", "Messages", int64(status.Messages)),
		control.NewCount("decrypt_failures", "Undecrypted messages", int64(status.DecryptFailures)),
		control.NewCount("media_messages", "Media messages", int64(status.MediaMessages)),
	}
	out := control.NewStatus("matcrawl", fmt.Sprintf("%d messages across %d rooms", status.Messages, status.Rooms))
	out.ConfigPath = filepath.Join(defaultBaseDir(), "backup.json")
	out.DatabasePath = status.DBPath
	out.Counts = counts
	db := control.SQLiteDatabase("primary", "Matrix archive", "archive", status.DBPath, true, counts)
	out.DatabaseBytes = db.Bytes
	out.WALBytes = fileSize(status.DBPath + "-wal")
	out.Databases = []control.Database{db}

	if status.LastSyncAt.IsZero() {
		out.State = "empty"
		out.Freshness = &control.Freshness{Status: "never", StaleAfterSeconds: int64(staleAfter.Seconds())}
		out.Warnings = append(out.Warnings, "never synced; run `matcrawl sync`")
	} else {
		out.LastSyncAt = status.LastSyncAt.UTC().Format(time.RFC3339)
		age := now.Sub(status.LastSyncAt)
		out.Freshness = &control.Freshness{Status: "fresh", AgeSeconds: int64(age.Seconds()), StaleAfterSeconds: int64(staleAfter.Seconds())}
		out.State = "current"
		if age > staleAfter {
			out.Freshness.Status = "stale"
			out.State = "stale"
		}
	}
	if status.DecryptFailures > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf("%d messages could not be decrypted; run `matcrawl keys backup-pull` or `matcrawl keys import`", status.DecryptFailures))
	}

	result := archiveControlStatus{Status: out, NextBatch: status.NextBatch}
	if !status.OldestEvent.IsZero() {
		result.OldestEvent = status.OldestEvent.UTC().Format(time.RFC3339)
	}
	if !status.NewestEvent.IsZero() {
		result.NewestEvent = status.NewestEvent.UTC().Format(time.RFC3339)
	}
	return result
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
