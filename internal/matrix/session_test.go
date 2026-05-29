package matrix

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSessionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "session.json")

	loaded, err := LoadSession(path)
	if err != nil {
		t.Fatalf("LoadSession on missing file: %v", err)
	}
	if loaded != nil {
		t.Fatalf("expected nil session on cold install, got %+v", loaded)
	}

	original := Session{
		Homeserver:  "https://matrix.org",
		UserID:      "@alice:matrix.org",
		DeviceID:    "ABCDEFGHIJ",
		AccessToken: "syt_secret",
	}
	if err := SaveSession(path, original); err != nil {
		t.Fatalf("SaveSession: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("session file perms = %o, want 0o600", mode)
	}

	loaded, err = LoadSession(path)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if loaded == nil || *loaded != original {
		t.Errorf("round-trip mismatch: got %+v, want %+v", loaded, original)
	}

	if err := DeleteSession(path); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if err := DeleteSession(path); err != nil {
		t.Errorf("DeleteSession on missing should be nil, got %v", err)
	}
}

func TestSessionValidate(t *testing.T) {
	cases := []struct {
		name    string
		session Session
		wantErr bool
	}{
		{"empty", Session{}, true},
		{"missing token", Session{Homeserver: "h", UserID: "u", DeviceID: "d"}, true},
		{"complete", Session{Homeserver: "h", UserID: "u", DeviceID: "d", AccessToken: "t"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.session.Validate()
			if (err != nil) != c.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}

func TestSaveSessionRejectsIncomplete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.json")
	err := SaveSession(path, Session{Homeserver: "h"})
	if err == nil {
		t.Fatal("expected error saving incomplete session")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("incomplete session file leaked to disk: %v", statErr)
	}
}

func TestElementDesktopDir(t *testing.T) {
	got := ElementDesktopDir()
	if got == "" {
		t.Skip("UserHomeDir unavailable in this environment")
	}
}
