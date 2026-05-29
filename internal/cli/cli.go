package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/peetzweg/matcrawl/internal/store"
)

type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string { return e.err.Error() }
func (e *cliError) Unwrap() error { return e.err }

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return 1
	}
	var codeErr *cliError
	if errors.As(err, &codeErr) {
		return codeErr.code
	}
	return 1
}

type runtime struct {
	ctx    context.Context
	stdout io.Writer
	stderr io.Writer
	json   bool
	dbPath string
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printUsage(stdout)
		return nil
	}
	global := flag.NewFlagSet("matcrawl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	jsonOut := global.Bool("json", false, "")
	dbPath := global.String("db", defaultDBPath(), "")
	versionFlag := global.Bool("version", false, "")
	if err := global.Parse(args); err != nil {
		return usageErr(err)
	}
	if *versionFlag {
		_, _ = io.WriteString(stdout, resolveVersion()+"\n")
		return nil
	}
	rest := global.Args()
	if len(rest) == 0 || rest[0] == "help" || rest[0] == "--help" || rest[0] == "-h" {
		printUsage(stdout)
		return nil
	}
	if rest[0] == "version" {
		_, _ = io.WriteString(stdout, resolveVersion()+"\n")
		return nil
	}
	r := &runtime{
		ctx:    ctx,
		stdout: stdout,
		stderr: stderr,
		json:   *jsonOut,
		dbPath: *dbPath,
	}
	return r.dispatch(rest)
}

func (r *runtime) dispatch(args []string) error {
	switch args[0] {
	case "metadata":
		return r.print(controlManifest())
	case "status":
		return r.runStatus(args[1:])
	case "doctor":
		return r.runDoctor(args[1:])
	case "login":
		return r.runLogin(args[1:])
	case "logout":
		return r.runLogout(args[1:])
	case "sync":
		return r.runSync(args[1:])
	case "rooms":
		return r.runRooms(args[1:])
	case "members":
		return r.runMembers(args[1:])
	case "messages":
		return r.runMessages(args[1:])
	case "search":
		return r.runSearch(args[1:])
	case "keys":
		return r.runKeys(args[1:])
	case "backup":
		return notImplemented(args[0])
	default:
		return usageErr(fmt.Errorf("unknown command %q", args[0]))
	}
}

func (r *runtime) withStore(fn func(*store.Store) error) error {
	st, err := store.Open(r.ctx, r.dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	return fn(st)
}

func (r *runtime) runStatus(args []string) error {
	fs := flag.NewFlagSet("matcrawl status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	return r.withStore(func(st *store.Store) error {
		status, err := st.Status(r.ctx)
		if err != nil {
			return err
		}
		return r.print(status)
	})
}

func (r *runtime) print(v any) error {
	enc := json.NewEncoder(r.stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func usageErr(err error) error {
	return &cliError{code: 2, err: err}
}

func notImplemented(name string) error {
	return &cliError{code: 1, err: fmt.Errorf("%s: not implemented yet", name)}
}

func printUsage(w io.Writer) {
	_, _ = io.WriteString(w, `matcrawl: Matrix archive CLI

usage:
  matcrawl [--json] doctor
  matcrawl [--json] login
  matcrawl [--json] logout
  matcrawl [--json] keys backup-pull
  matcrawl [--json] keys import <path-to-keys.txt>
  matcrawl [--json] keys retry
  matcrawl [--json] sync [--backfill]
  matcrawl [--json] status
  matcrawl [--json] rooms [--limit N] [--encrypted-only]
  matcrawl [--json] members --room <room_id>
  matcrawl [--json] messages [--room ID] [--sender ID] [--after DATE] [--before DATE] [--limit N] [--media]
  matcrawl [--json] search "query" [--room ID] [--limit N]
  matcrawl [--json] backup init|push|pull|status
  matcrawl [--json] metadata
  matcrawl version

setup:
  Authenticate against your Matrix homeserver:
    matcrawl login

notes:
  matcrawl speaks the Matrix Client-Server API directly, decrypts E2EE rooms
  using your Megolm keys, and stores history in ~/.matcrawl/matcrawl.db with
  FTS5. Backup writes age-encrypted shards to a git repo.
`)
}

func defaultDBPath() string {
	return filepath.Join(defaultBaseDir(), "matcrawl.db")
}

func defaultBaseDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".matcrawl"
	}
	return filepath.Join(home, ".matcrawl")
}
