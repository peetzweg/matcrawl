package cli

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/term"

	"github.com/peetzweg/matcrawl/internal/matrix"
)

type loginResult struct {
	SessionPath string `json:"session_path"`
	Homeserver  string `json:"homeserver"`
	UserID      string `json:"user_id"`
	DeviceID    string `json:"device_id"`
}

func (r *runtime) runLogin(args []string) error {
	fs := flag.NewFlagSet("matcrawl login", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	homeserverFlag := fs.String("homeserver", "", "")
	userFlag := fs.String("user", "", "")
	deviceFlag := fs.String("device", "", "")
	tokenFlag := fs.String("token", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	homeserver := strings.TrimSpace(*homeserverFlag)
	userID := strings.TrimSpace(*userFlag)
	deviceID := strings.TrimSpace(*deviceFlag)
	accessToken := strings.TrimSpace(*tokenFlag)

	reader := bufio.NewReader(os.Stdin)
	if homeserver == "" {
		v, err := promptLine(r.stderr, reader, "homeserver URL (e.g. https://matrix.org): ")
		if err != nil {
			return err
		}
		homeserver = v
	}
	if userID == "" {
		v, err := promptLine(r.stderr, reader, "user ID (e.g. @alice:matrix.org): ")
		if err != nil {
			return err
		}
		userID = v
	}
	if deviceID == "" {
		v, err := promptLine(r.stderr, reader, "device ID (Element → Settings → Help & About → Access Token): ")
		if err != nil {
			return err
		}
		deviceID = v
	}
	if accessToken == "" {
		v, err := promptSecret(r.stderr, "access token (hidden): ")
		if err != nil {
			return err
		}
		accessToken = v
	}

	session := matrix.Session{
		Homeserver:  homeserver,
		UserID:      userID,
		DeviceID:    deviceID,
		AccessToken: accessToken,
	}
	if err := session.Validate(); err != nil {
		return err
	}

	resolvedUser, resolvedDevice, err := matrix.Verify(r.ctx, session)
	if err != nil {
		return fmt.Errorf("verify session: %w", err)
	}
	if string(resolvedUser) != session.UserID {
		fmt.Fprintf(r.stderr, "warning: server reports user_id %s, overriding local %s\n", resolvedUser, session.UserID)
		session.UserID = string(resolvedUser)
	}
	if string(resolvedDevice) != session.DeviceID {
		fmt.Fprintf(r.stderr, "warning: server reports device_id %s, overriding local %s\n", resolvedDevice, session.DeviceID)
		session.DeviceID = string(resolvedDevice)
	}

	path := sessionPath(r.dbPath)
	if err := matrix.SaveSession(path, session); err != nil {
		return err
	}
	return r.print(loginResult{
		SessionPath: path,
		Homeserver:  session.Homeserver,
		UserID:      session.UserID,
		DeviceID:    session.DeviceID,
	})
}

func (r *runtime) runLogout(args []string) error {
	fs := flag.NewFlagSet("matcrawl logout", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	path := sessionPath(r.dbPath)
	if err := matrix.DeleteSession(path); err != nil {
		return err
	}
	return r.print(map[string]any{"removed": path})
}

// sessionPath puts session.json next to the configured db file, so callers
// that override --db get a consistent set of files.
func sessionPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "session.json")
}

func promptLine(out io.Writer, r *bufio.Reader, prompt string) (string, error) {
	_, _ = io.WriteString(out, prompt)
	line, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	v := strings.TrimSpace(line)
	if v == "" {
		return "", fmt.Errorf("empty input for %q", strings.TrimSpace(prompt))
	}
	return v, nil
}

func promptSecret(out io.Writer, prompt string) (string, error) {
	_, _ = io.WriteString(out, prompt)
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		// Not a TTY — fall back to a plain read so piping/tests work.
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
		return strings.TrimSpace(line), nil
	}
	data, err := term.ReadPassword(fd)
	if err != nil {
		return "", err
	}
	_, _ = io.WriteString(out, "\n")
	return strings.TrimSpace(string(data)), nil
}
