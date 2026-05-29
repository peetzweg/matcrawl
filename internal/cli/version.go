package cli

import "runtime/debug"

// version is injected by GoReleaser at release time via -ldflags
// "-X github.com/peetzweg/matcrawl/internal/cli.version={{ .Version }}".
// Empty in `go install` builds; resolveVersion falls back to the module
// version embedded by the Go toolchain so users still see the right tag.
var version = ""

func resolveVersion() string {
	if version != "" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		if v := info.Main.Version; v != "" && v != "(devel)" {
			return v
		}
	}
	return "dev"
}
