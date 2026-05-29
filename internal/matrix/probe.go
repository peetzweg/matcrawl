package matrix

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

// Report is the output of Probe — enough material for `matcrawl doctor` to
// render a verdict in plain text or JSON.
type Report struct {
	SessionPath          string `json:"session_path"`
	SessionPresent       bool   `json:"session_present"`
	SessionUserID        string `json:"session_user_id,omitempty"`
	SessionDeviceID      string `json:"session_device_id,omitempty"`
	HomeserverURL        string `json:"homeserver_url,omitempty"`
	HomeserverReachable  bool   `json:"homeserver_reachable"`
	HomeserverVersions   int    `json:"homeserver_versions,omitempty"`
	WhoamiOK             bool   `json:"whoami_ok"`
	WhoamiUserID         string `json:"whoami_user_id,omitempty"`
	WhoamiDeviceID       string `json:"whoami_device_id,omitempty"`
	WhoamiError          string `json:"whoami_error,omitempty"`
	ElementDesktopPath   string `json:"element_desktop_path,omitempty"`
	ElementDesktopExists bool   `json:"element_desktop_exists"`
	Note                 string `json:"note,omitempty"`
}

// Probe inspects the on-disk session, talks to the homeserver if a session is
// present, and reports what an Element Desktop install looks like on this OS.
// All probes are best-effort — failure of one does not abort the others.
func Probe(ctx context.Context, sessionPath string) Report {
	r := Report{SessionPath: sessionPath}

	r.ElementDesktopPath = ElementDesktopDir()
	if r.ElementDesktopPath != "" {
		if info, err := os.Stat(r.ElementDesktopPath); err == nil && info.IsDir() {
			r.ElementDesktopExists = true
		}
	}

	session, err := LoadSession(sessionPath)
	if err != nil || session == nil {
		if errors.Is(err, os.ErrNotExist) || session == nil {
			r.Note = "no session — run `matcrawl login` (or `matcrawl import-element` once v0.2.0 ships)"
		} else {
			r.Note = "session file unreadable: " + err.Error()
		}
		return r
	}
	r.SessionPresent = true
	r.SessionUserID = session.UserID
	r.SessionDeviceID = session.DeviceID
	r.HomeserverURL = session.Homeserver

	client, err := New(*session)
	if err != nil {
		r.WhoamiError = err.Error()
		return r
	}

	versionsCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if versions, vErr := client.Versions(versionsCtx); vErr == nil {
		r.HomeserverReachable = true
		r.HomeserverVersions = len(versions.Versions)
	}

	whoamiCtx, cancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel2()
	resp, err := client.Whoami(whoamiCtx)
	if err != nil {
		r.WhoamiError = err.Error()
		return r
	}
	r.WhoamiOK = true
	r.WhoamiUserID = string(resp.UserID)
	r.WhoamiDeviceID = string(resp.DeviceID)
	return r
}

// ElementDesktopDir returns the OS-conventional Element Desktop data dir.
// The dir is not guaranteed to exist; callers should stat it. v0.2.0's
// `import-element` subcommand will use the same locations to lift session
// credentials directly.
func ElementDesktopDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "Element")
	case "windows":
		if appdata := os.Getenv("APPDATA"); appdata != "" {
			return filepath.Join(appdata, "Element")
		}
		return filepath.Join(home, "AppData", "Roaming", "Element")
	default: // linux + freebsd + others — XDG conventions
		if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
			return filepath.Join(xdg, "Element")
		}
		return filepath.Join(home, ".config", "Element")
	}
}

// Avoid an "imported and not used" compile error if mautrix types are only
// referenced through New() / Verify() in the future; keep the import live so
// callers can pass id.UserID / id.DeviceID directly when constructing tests.
var (
	_ = id.UserID("")
	_ = mautrix.Client{}
)
