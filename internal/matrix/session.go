package matrix

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Session is the persisted credentials needed to talk to a Matrix homeserver.
// Mirrors what `matcrawl login` collects via paste-3-strings (Path A) and what
// `matcrawl import-element` will collect via Element Desktop credential-lift
// in v0.2.0 (Path C).
type Session struct {
	Homeserver  string `json:"homeserver"`
	UserID      string `json:"user_id"`
	DeviceID    string `json:"device_id"`
	AccessToken string `json:"access_token"`
}

func (s Session) Validate() error {
	if strings.TrimSpace(s.Homeserver) == "" {
		return errors.New("session missing homeserver")
	}
	if strings.TrimSpace(s.UserID) == "" {
		return errors.New("session missing user_id")
	}
	if strings.TrimSpace(s.DeviceID) == "" {
		return errors.New("session missing device_id")
	}
	if strings.TrimSpace(s.AccessToken) == "" {
		return errors.New("session missing access_token")
	}
	return nil
}

// LoadSession reads a session from disk. Returns (nil, nil) if no session
// file exists yet — caller decides whether that's an error in their context.
func LoadSession(path string) (*Session, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read session: %w", err)
	}
	var s Session
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse session: %w", err)
	}
	return &s, nil
}

// SaveSession writes a session to disk with 0o600 perms in a 0o700 dir.
func SaveSession(path string, s Session) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir session dir: %w", err)
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write session: %w", err)
	}
	return nil
}

// DeleteSession removes a session file. Missing file is not an error.
func DeleteSession(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
