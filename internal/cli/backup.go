package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/peetzweg/matcrawl/internal/backup"
	"github.com/peetzweg/matcrawl/internal/store"
)

func (r *runtime) runBackup(args []string) error {
	if len(args) == 0 {
		return usageErr(errors.New("backup needs subcommand: init, push, pull, status"))
	}
	switch args[0] {
	case "init":
		return r.backupInit(args[1:])
	case "push":
		return r.backupPush(args[1:])
	case "pull":
		return r.backupPull(args[1:])
	case "status":
		return r.backupStatus(args[1:])
	default:
		return usageErr(fmt.Errorf("unknown backup command %q", args[0]))
	}
}

func backupFlags(name string) (*flag.FlagSet, *backup.Options, *bool) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := &backup.Options{}
	fs.StringVar(&opts.ConfigPath, "config", backup.DefaultConfigPath(), "")
	fs.StringVar(&opts.Repo, "repo", "", "")
	fs.StringVar(&opts.Remote, "remote", "", "")
	fs.StringVar(&opts.Identity, "identity", "", "")
	fs.Func("recipient", "", func(value string) error {
		opts.Recipients = append(opts.Recipients, value)
		return nil
	})
	noPush := fs.Bool("no-push", false, "")
	return fs, opts, noPush
}

func (r *runtime) backupInit(args []string) error {
	fs, opts, noPush := backupFlags("matcrawl backup init")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	opts.Push = !*noPush
	cfg, recipient, err := backup.Init(r.ctx, *opts)
	if err != nil {
		return err
	}
	return r.print(map[string]any{"repo": cfg.Repo, "remote": cfg.Remote, "identity": cfg.Identity, "recipient": recipient})
}

func (r *runtime) backupPush(args []string) error {
	fs, opts, noPush := backupFlags("matcrawl backup push")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	opts.Push = !*noPush
	return r.withStore(func(st *store.Store) error {
		result, err := backup.Push(r.ctx, st, *opts)
		if err != nil {
			return err
		}
		return r.print(result)
	})
}

func (r *runtime) backupPull(args []string) error {
	fs, opts, _ := backupFlags("matcrawl backup pull")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	return r.withStore(func(st *store.Store) error {
		result, err := backup.Pull(r.ctx, st, *opts)
		if err != nil {
			return err
		}
		return r.print(result)
	})
}

func (r *runtime) backupStatus(args []string) error {
	fs, opts, _ := backupFlags("matcrawl backup status")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	manifest, repo, err := backup.Status(r.ctx, *opts)
	if err != nil {
		return err
	}
	return r.print(map[string]any{"repo": repo, "manifest": manifest})
}
