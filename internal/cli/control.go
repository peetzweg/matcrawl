package cli

import (
	"path/filepath"

	"github.com/openclaw/crawlkit/control"
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
		"doctor": {Title: "Doctor", Argv: []string{"matcrawl", "--json", "doctor"}, JSON: true},
		"status": {Title: "Status", Argv: []string{"matcrawl", "--json", "status"}, JSON: true},
		"sync":   {Title: "Sync", Argv: []string{"matcrawl", "--json", "sync"}, JSON: true, Mutates: true},
		"search": {Title: "Search", Argv: []string{"matcrawl", "--json", "search"}, JSON: true},
	}
	return m
}
