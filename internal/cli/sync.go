package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/peetzweg/matcrawl/internal/matrix"
	"github.com/peetzweg/matcrawl/internal/store"
)

type syncResult struct {
	Sync     store.SyncStats         `json:"sync"`
	Backfill []matrix.BackfillResult `json:"backfill,omitempty"`
}

func (r *runtime) runSync(args []string) error {
	fs := flag.NewFlagSet("matcrawl sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	backfill := fs.Bool("backfill", false, "")
	quiet := fs.Bool("quiet", false, "")
	stopOnError := fs.Bool("stop-on-error", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	session, err := matrix.LoadSession(sessionPath(r.dbPath))
	if err != nil {
		return err
	}
	if session == nil {
		return errors.New("no session — run `matcrawl login` first")
	}

	client, err := matrix.New(*session)
	if err != nil {
		return err
	}

	// Progress goes to stderr so stdout stays clean for --json consumers.
	// `--quiet` suppresses entirely.
	var progress io.Writer
	if !*quiet {
		progress = r.stderr
	}

	return r.withStore(func(st *store.Store) error {
		stats, err := matrix.SyncOnce(r.ctx, client, st, progress)
		if err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		out := syncResult{Sync: stats}
		if *backfill {
			results, err := matrix.BackfillAll(r.ctx, client, st, progress, *stopOnError)
			out.Backfill = results
			if err != nil {
				_ = r.print(out)
				return fmt.Errorf("backfill: %w", err)
			}
		}
		return r.print(out)
	})
}
